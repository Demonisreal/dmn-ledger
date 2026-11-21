package main

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/Demonisreal/dmn-ledger/bench/control"
	"github.com/Demonisreal/dmn-ledger/bench/ledgerrun"
	"github.com/Demonisreal/dmn-ledger/bench/load"
	"github.com/Demonisreal/dmn-ledger/bench/pg"
	"github.com/Demonisreal/dmn-ledger/bench/raw"
)

const stamp = "20060102T150405Z"

func (b *bench) prepare(ctx context.Context) error {
	cg, err := ownCgroup(b.root)
	if err != nil {
		return err
	}
	limit, _, err := cgroupMemory(b.root, cg)
	if err != nil {
		return err
	}
	if limit == 0 {
		return errNoLimit
	}
	b.cgroup, b.limit = cg, limit
	if err := os.MkdirAll(filepath.Join(b.out, "raw"), 0o755); err != nil { //nolint:gosec // G301: the folder of a plan is for reading
		return err
	}
	if err := leftovers(b.dir); err != nil {
		return err
	}
	postgres := slices.ContainsFunc(b.items, func(it item) bool { return b.selected(it) && it.runs[0].target.kind == "postgres" })
	if err := refuseRunning(ctx, postgres); err != nil {
		return err
	}
	if b.machine, err = control.ReadMachine(b.dir); err != nil {
		return err
	}
	if b.machine.Filesystem != "ext4" {
		return fmt.Errorf("%s is on %q, want ext4", b.dir, b.machine.Filesystem)
	}
	port := 0
	if postgres {
		port = pg.Port
	}
	if err := control.CheckStart(b.root, b.machine, port); err != nil {
		return err
	}
	b.disk, b.th = b.machine.Disk, control.Defaults()
	if b.build, err = described(ctx, b.repo); err != nil {
		return err
	}
	if err := b.readFiles(); err != nil {
		return err
	}
	if err := b.planFile(); err != nil {
		return err
	}
	if err := b.buildAll(ctx); err != nil {
		return err
	}
	at := time.Now().UTC().Format(stamp)
	if err := writeJSON(filepath.Join(b.out, "heat-"+at+".json"), control.ReadHeat(b.root)); err != nil {
		return err
	}
	fmt.Fprintf(b.log, "disk probe, %v for each kind of write\n", b.shape.probe())
	if b.probe, err = control.RunProbe(ctx, b.dir, b.disk, b.shape.probe(), b.th); err != nil {
		return err
	}
	if err := writeJSON(filepath.Join(b.out, "probe-"+at+".json"), b.probe); err != nil {
		return err
	}
	fmt.Fprintln(b.log, "selftest of the load generator")
	st, err := load.RunSelftest(ctx)
	if werr := writeJSON(filepath.Join(b.out, "selftest-"+at+".json"), st); werr != nil {
		return werr
	}
	if err != nil {
		return err
	}
	if err := b.writeMachine(); err != nil {
		return err
	}
	if postgres {
		if err := b.startPostgres(ctx, at); err != nil {
			return err
		}
	}
	return b.writePlanText()
}

func refuseRunning(ctx context.Context, postgres bool) error {
	if _, err := exec.LookPath("docker"); err != nil && !postgres {
		return nil
	}
	running, err := containerExists(ctx)
	if err == nil && running {
		err = errRunning
	}
	return err
}

func (b *bench) startPostgres(ctx context.Context, at string) error {
	if err := refuseRunning(ctx, true); err != nil {
		return err
	}
	fmt.Fprintf(b.log, "start the container %s\n", pg.Name)
	var err error
	if b.srv, err = pg.Start(ctx, filepath.Join(b.dir, "pg")); err != nil {
		return err
	}
	if err := sameDevice(b.dir, filepath.Join(b.srv.Dir(), "data")); err != nil {
		return err
	}
	fmt.Fprintln(b.log, "pg_test_fsync in the container")
	fsync, err := b.srv.TestFsync(ctx)
	if err != nil {
		return err
	}
	if err := writeFile(filepath.Join(b.out, "pg-test-fsync-"+at+".txt"), []byte(fsync)); err != nil {
		return err
	}
	if err := b.writeMachine(); err != nil {
		return err
	}
	return b.discard(ctx)
}

func (b *bench) discard(ctx context.Context) (err error) {
	r := run{m: "m0", pass: 1, nr: 1, target: v3, lanes: r4Lanes, workload: "uniform-10k", rate: r4}
	window := 15 * time.Second
	if b.smoke {
		window = 3 * time.Second
	}
	if err := b.capTo(ctx, b.postgresLimit()); err != nil {
		return err
	}
	if err := awaitMemory(ctx, b.root, []string{b.cgroup, b.srv.Cgroup()}, b.log); err != nil {
		return err
	}
	fmt.Fprintf(b.log, "%s: a run to throw away, %d/s for %v\n", r.id(), r.rate, window)
	c, err := pg.Dial(ctx, b.srv, pg.Config{Variant: r.target.variant, Lanes: r.lanes})
	if err != nil {
		return fmt.Errorf("%s: %w", r.id(), err)
	}
	defer func() {
		if cerr := c.Close(); cerr != nil {
			err = errors.Join(err, fmt.Errorf("%s: close: %w", r.id(), cerr))
		}
	}()
	w := mustNamed(r.workload)
	if err := c.Setup(ctx, w); err != nil {
		return fmt.Errorf("%s: setup: %w", r.id(), err)
	}
	if _, err := load.Drive(ctx, c, load.NewStream(w, r.seed()), load.Plan{Rate: r.rate, Lanes: r.lanes, Window: window}); err != nil {
		return fmt.Errorf("%s: %w", r.id(), err)
	}
	b.note(r, "discarded: the first run after the start of the container")
	return nil
}

func (b *bench) writeMachine() error {
	text, err := b.machineText()
	if err != nil {
		return err
	}
	return writeFile(filepath.Join(b.out, "machine.txt"), text)
}

func described(ctx context.Context, repo string) (raw.Build, error) {
	git := func(args ...string) (string, error) {
		out, err := exec.CommandContext(ctx, "git", append([]string{"-C", repo}, args...)...).Output() //nolint:gosec // G204: git in the repository of the plan
		return strings.TrimSpace(string(out)), err
	}
	commit, err := git("rev-parse", "HEAD")
	if err != nil {
		return raw.Build{}, fmt.Errorf("commit of %s: %w", repo, err)
	}
	changes, err := git("status", "--porcelain", "--untracked-files=all", "--", ".", ":(exclude)docs/bench")
	if err != nil {
		return raw.Build{}, fmt.Errorf("status of %s: %w", repo, err)
	}
	version, err := exec.CommandContext(ctx, "go", "env", "GOVERSION").Output()
	if err != nil {
		return raw.Build{}, fmt.Errorf("go version: %w", err)
	}
	return raw.Build{Commit: commit, Dirty: changes != "", Go: strings.TrimSpace(string(version))}, nil
}

func (b *bench) readFiles() error {
	names, err := filepath.Glob(filepath.Join(b.out, "raw", "*.json"))
	if err != nil {
		return err
	}
	for _, name := range names {
		content, err := os.ReadFile(name) //nolint:gosec // G304: a raw file of this plan
		if err != nil {
			return err
		}
		f, err := raw.Read(bytes.NewReader(content))
		if err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
		if f.ID+".json" != filepath.Base(name) {
			return fmt.Errorf("%s holds the run %s", name, f.ID)
		}
		b.files[f.ID] = f
		b.earlier[f.ID] = true
	}
	return nil
}

func (b *bench) buildAll(ctx context.Context) error {
	b.bins, b.ctls = make(map[string]string), make(map[string]string)
	for _, it := range b.items {
		kind := it.runs[0].build
		if _, ok := b.bins[kind]; ok || !b.selected(it) || it.runs[0].target.kind != "ledger" {
			continue
		}
		folder := cmp.Or(kind, "normal")
		fmt.Fprintf(b.log, "build ledgerd, %s\n", folder)
		out := filepath.Join(b.dir, "bin", folder)
		if err := os.MkdirAll(out, 0o750); err != nil {
			return err
		}
		bin, ctl, err := ledgerrun.Build(ctx, b.repo, out, it.runs[0].tags()...)
		if err != nil {
			return err
		}
		b.bins[kind], b.ctls[kind] = bin, ctl
	}
	return nil
}

func (b *bench) finish(ctx context.Context, cause error) error {
	var errs []error
	if b.srv != nil {
		fmt.Fprintf(b.log, "stop the container %s\n", pg.Name)
		if err := b.srv.Stop(); err != nil {
			errs = append(errs, err)
		}
		switch running, err := containerExists(context.Background()); {
		case err != nil:
			errs = append(errs, err)
		case running:
			errs = append(errs, fmt.Errorf("%s still there after stopping it", pg.Name))
		}
	}
	for _, sub := range []string{"run", "prof"} {
		if err := os.RemoveAll(filepath.Join(b.dir, sub)); err != nil {
			errs = append(errs, err)
		}
	}
	if left := ownLedgerd(b.root); len(left) > 0 {
		errs = append(errs, fmt.Errorf("ledgerd still runs as %v", left))
	}
	if cause == nil && ctx.Err() == nil {
		fmt.Fprintln(b.log, "disk probe at the end")
		at := time.Now().UTC().Format(stamp)
		probe, err := control.RunProbe(ctx, b.dir, b.disk, b.shape.probe(), b.th)
		if err == nil {
			err = writeJSON(filepath.Join(b.out, "probe-"+at+".json"), probe)
		}
		if err == nil {
			err = writeJSON(filepath.Join(b.out, "heat-"+at+".json"), control.ReadHeat(b.root))
		}
		if err == nil {
			err = b.writeMachine()
		}
		errs = append(errs, err)
	}
	switch {
	case ctx.Err() != nil:
		b.stopped = "The plan was stopped by a signal. Start it again to go on; runs with a raw file are skipped."
	case cause != nil:
		b.stopped = "The plan stopped: " + cause.Error()
	default:
		b.stopped = "The plan finished."
	}
	if cause != nil || ctx.Err() != nil {
		b.unreached()
	}
	cleanup := errors.Join(errs...)
	if cleanup != nil {
		b.stopped += "\nCleaning up failed: " + cleanup.Error()
	}
	if _, err := os.Stat(filepath.Join(b.out, "raw")); err == nil {
		cleanup = errors.Join(cleanup, b.writePlanText())
	}
	return cleanup
}

func (b *bench) unreached() {
	noted := make(map[string]bool)
	for _, l := range b.lines {
		noted[l.id] = true
	}
	for _, it := range b.items {
		if b.selected(it) && !slices.ContainsFunc(it.runs, func(r run) bool { return noted[r.id()] }) {
			b.note(it.runs[0], "no raw file, the plan stopped before or during this run")
		}
	}
}
