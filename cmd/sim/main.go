package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/Demonisreal/dmn-ledger/internal/check"
	"github.com/Demonisreal/dmn-ledger/internal/sim"
)

type options struct {
	seed, seeds, from, nodes uint64
	profile, bug             string
	trace                    bool
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	var o options
	fs := flag.NewFlagSet("sim", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Uint64Var(&o.seed, "seed", 0, "run this one seed")
	fs.Uint64Var(&o.seeds, "seeds", 0, "run this many seeds on all cores, starting at -from")
	fs.Uint64Var(&o.from, "from", 0, "first seed for -seeds")
	fs.StringVar(&o.profile, "profile", string(sim.ProfileAll), "fault profile: calm, net, crash, clock, all or evict")
	fs.Uint64Var(&o.nodes, "nodes", 0, "nodes per cluster: 1, 3 or 5, 0 takes 5 for every fifth seed and 3 otherwise")
	fs.BoolVar(&o.trace, "trace", false, "print the trace of -seed")
	fs.StringVar(&o.bug, "bug", "", "planted bug B1 to B12, needs a build with -tags simbug")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if err := o.validate(fs); err != nil {
		fmt.Fprintln(stderr, "sim:", err)
		return 2
	}
	if err := setBug(o.bug); err != nil {
		fmt.Fprintln(stderr, "sim:", err)
		return 2
	}
	if o.seeds > 0 {
		return o.sweep(stdout)
	}
	return o.single(stdout)
}

func (o *options) validate(fs *flag.FlagSet) error {
	seen := make(map[string]bool)
	fs.Visit(func(f *flag.Flag) { seen[f.Name] = true })
	switch sim.Profile(o.profile) {
	case sim.ProfileCalm, sim.ProfileNet, sim.ProfileCrash, sim.ProfileClock, sim.ProfileAll, check.ProfileEvict:
	default:
		return fmt.Errorf("unknown profile %q", o.profile)
	}
	switch {
	case fs.NArg() > 0:
		return fmt.Errorf("unexpected arguments %v", fs.Args())
	case o.nodes != 0 && o.nodes != 1 && o.nodes != 3 && o.nodes != 5:
		return fmt.Errorf("-nodes %d, want 1, 3 or 5", o.nodes)
	case seen["seeds"] && o.seeds == 0:
		return errors.New("-seeds must be at least 1")
	case o.seeds > 0 && (seen["seed"] || o.trace):
		return errors.New("-seed and -trace run one seed, -seeds runs many")
	case seen["from"] && o.seeds == 0:
		return errors.New("-from needs -seeds")
	}
	return nil
}

func (o *options) config(seed uint64) check.Config {
	return check.Config{Seed: seed, Profile: sim.Profile(o.profile), Nodes: o.nodes}
}

func (o *options) single(stdout io.Writer) int {
	cfg := o.config(o.seed)
	if o.trace {
		cfg.Trace = stdout
	}
	rep, err := check.Run(cfg)
	if err != nil {
		o.report(stdout, o.seed, err)
		return 1
	}
	fmt.Fprintf(stdout, "seed %d profile %s: ok, %d calls in %d windows, widest %d, trace %x\n",
		o.seed, o.profile, rep.Calls, rep.Windows, rep.Widest, rep.Trace)
	return 0
}

func (o *options) sweep(stdout io.Writer) int {
	var mu sync.Mutex
	widest := 0
	start := time.Now()
	seed, err := check.Sweep(o.from, o.seeds, runtime.GOMAXPROCS(0), func(seed uint64) error {
		rep, err := check.Run(o.config(seed))
		mu.Lock()
		widest = max(widest, rep.Widest)
		mu.Unlock()
		return err
	})
	if err != nil {
		o.report(stdout, seed, err)
		return 1
	}
	fmt.Fprintf(stdout, "seeds %d to %d profile %s: ok, widest window %d, %s on %d cores\n",
		o.from, o.from+o.seeds-1, o.profile, widest, time.Since(start).Round(time.Millisecond), runtime.GOMAXPROCS(0))
	return 0
}

func (o *options) report(stdout io.Writer, seed uint64, err error) {
	fmt.Fprintf(stdout, "seed %d profile %s failed: %v\n", seed, o.profile, err)
	cmd := []string{"go run"}
	if o.bug != "" {
		cmd = append(cmd, "-tags simbug ./cmd/sim -bug "+o.bug)
	} else {
		cmd = append(cmd, "./cmd/sim")
	}
	cmd = append(cmd, fmt.Sprintf("-seed %d -profile %s", seed, o.profile))
	if o.nodes != 0 {
		cmd = append(cmd, fmt.Sprintf("-nodes %d", o.nodes))
	}
	fmt.Fprintf(stdout, "reproduce: %s -trace\n", strings.Join(cmd, " "))
}
