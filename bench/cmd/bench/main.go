package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"syscall"

	"github.com/Demonisreal/dmn-ledger/bench/load"
	"github.com/Demonisreal/dmn-ledger/bench/raw"
	"github.com/Demonisreal/dmn-ledger/bench/report"
)

const usage = `usage:
  bench selftest
  bench plan -dir DIR -out DIR [-repo DIR] [-smoke] [-only REGEXP]
  bench report DIR
  bench check DIR
selftest drives the null target at 200 000 transfers a second and a target that stalls for one
second, and fails when the generator itself adds more than 1 ms at p99.

plan runs the measurement plan: it builds ledgerd from the repository, checks the machine, probes
the disk, starts the container dmn-ledger-bench-pg when Postgres is part of the run, and writes one
raw file per run to OUT/raw together with plan.json, plan.txt, machine.txt and postgres.txt. DIR
holds the data of the nodes and of Postgres; it must be on ext4, and DIR/run and DIR/pg must be
empty. Runs whose raw file is there already are skipped, so a plan that stopped goes on where it
stopped. The plan refuses to start without a memory limit on its own cgroup: start it through
scripts/bench.sh. -smoke runs the same plan with one pass and judges nothing: the stages of m1
with 2 s to warm up, a window of 6 s and 3 s to drain, up to the first saturated stage, the other
runs with windows of 3 s; it takes about 20 minutes. -only keeps the runs whose id matches, together
with their repeats and later passes.

report writes RESULTS.md and the curves from DIR/raw. check compares them with the raw files and
judges every measurement; it exits with 1 when a verdict fails.`

func main() {
	os.Exit(dispatch(os.Args[1:], os.Stdout, os.Stderr))
}

func dispatch(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, usage)
		return 2
	}
	ctx, stop := stopping()
	defer stop()
	context.AfterFunc(ctx, stop)
	switch args[0] {
	case "selftest":
		return selftest(ctx, stdout, stderr)
	case "plan":
		return plan(ctx, args[1:], stdout, stderr)
	case "report", "check":
		if len(args) != 2 {
			fmt.Fprintln(stderr, usage)
			return 2
		}
		if args[0] == "report" {
			return writeReport(args[1], stdout, stderr)
		}
		return check(args[1], stdout, stderr)
	case "-h", "-help", "help":
		fmt.Fprintln(stdout, usage)
		return 0
	}
	fmt.Fprintln(stderr, usage)
	return 2
}

func stopping() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)
}

func selftest(ctx context.Context, stdout, stderr io.Writer) int {
	st, err := load.RunSelftest(ctx)
	fmt.Fprintf(stdout, "null target at %d/s: p50 %v, p99 %v, dispatcher late at p99 %v\n", load.FloorRate, st.FloorP50, st.FloorP99, st.FloorLate)
	fmt.Fprintf(stdout, "one second stall at %d/s: %d due in it, %d open at its end, p50 %v, p99 %v, max %v, p99 the second before %v\n",
		load.StallRate, st.StallDue, st.StallOpen, st.StallP50, st.StallP99, st.StallMax, st.CalmP99)
	if err != nil {
		fmt.Fprintln(stderr, "bench:", err)
		return 1
	}
	return 0
}

func plan(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("bench plan", flag.ContinueOnError)
	fs.SetOutput(stderr)
	dir := fs.String("dir", "", "bench directory for the data of the nodes, of Postgres and the binaries")
	out := fs.String("out", "", "folder for the raw files and the texts of the plan")
	repo := fs.String("repo", "..", "root of the dmn-ledger repository")
	smoke := fs.Bool("smoke", false, "one pass, short windows, every stage of m1 up to the first saturated one, no verdict")
	only := fs.String("only", "", "run only the items with a run id that matches this regular expression")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if *dir == "" || *out == "" || fs.NArg() > 0 {
		fmt.Fprintln(stderr, usage)
		return 2
	}
	b := &bench{shape: shape{smoke: *smoke}, log: stdout, root: os.DirFS("/"), files: make(map[string]*raw.File), earlier: make(map[string]bool)}
	var err error
	for _, p := range []struct {
		to   *string
		from string
	}{{&b.dir, *dir}, {&b.out, *out}, {&b.repo, *repo}} {
		if *p.to, err = filepath.Abs(p.from); err != nil {
			fmt.Fprintln(stderr, "bench:", err)
			return 2
		}
	}
	if *only != "" {
		if b.only, err = regexp.Compile(*only); err != nil {
			fmt.Fprintln(stderr, "bench: -only:", err)
			return 2
		}
	}
	b.items = b.schedule()
	err = b.prepare(ctx)
	if err == nil {
		err = b.fly(ctx)
	}
	cleanup := b.finish(ctx, err)
	if err != nil {
		fmt.Fprintln(stderr, "bench:", err)
	}
	if cleanup != nil {
		fmt.Fprintln(stderr, "bench: cleaning up:", cleanup)
	}
	if err != nil || cleanup != nil {
		return 1
	}
	fmt.Fprintf(stdout, "plan done, raw files in %s\n", filepath.Join(b.out, "raw"))
	return 0
}

func writeReport(dir string, stdout, stderr io.Writer) int {
	if err := report.Report(dir); err != nil {
		fmt.Fprintln(stderr, "bench:", err)
		return 1
	}
	fmt.Fprintf(stdout, "wrote %s\n", filepath.Join(dir, report.Results))
	return 0
}

func check(dir string, stdout, stderr io.Writer) int {
	verdicts, err := report.Check(dir)
	for _, v := range verdicts {
		mark := "ok     "
		if !v.Passed {
			mark = "refused"
		}
		fmt.Fprintf(stdout, "%s %s", mark, v.Subject)
		for _, why := range v.Why {
			fmt.Fprintf(stdout, "; %s", why)
		}
		fmt.Fprintln(stdout)
	}
	if err != nil {
		fmt.Fprintln(stderr, "bench:", err)
		return 1
	}
	return 0
}
