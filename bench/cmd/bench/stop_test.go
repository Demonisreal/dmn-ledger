package main

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"testing/fstest"
	"time"

	"github.com/Demonisreal/dmn-ledger/bench/control"
	"github.com/Demonisreal/dmn-ledger/bench/ledgerrun"
	"github.com/Demonisreal/dmn-ledger/bench/load"
	"github.com/Demonisreal/dmn-ledger/bench/raw"
)

type sham struct {
	load.Null
	verify error
}

func (s sham) Verify(_ context.Context, want load.Books) (load.Tally, error) {
	return load.Tally{Found: len(want.Sample)}, s.verify
}

func (sham) Describe() raw.Target {
	return raw.Target{Kind: "null", Lanes: 1}
}

type elections struct {
	sham
	term *uint64
}

func (e elections) Sample(context.Context) (load.Inside, error) {
	*e.term++
	return load.Inside{{Name: "n1", Role: "leader", Term: *e.term}}, nil
}

type segments struct {
	sham
	created *uint64
}

func (s segments) Sample(context.Context) (load.Inside, error) {
	*s.created++
	return load.Inside{{Name: "postgres", Counters: map[string]uint64{"wal_init_syncs": *s.created, "checkpoints": 2, "deadlocks": 0}}}, nil
}

func (segments) Describe() raw.Target {
	return raw.Target{Kind: "postgres", Variant: "V1", Lanes: 1}
}

func TestPostgresRunWithNewSegmentsIsRefused(t *testing.T) {
	out := t.TempDir()
	if err := os.Mkdir(filepath.Join(out, "raw"), 0o750); err != nil {
		t.Fatal(err)
	}
	b := &bench{root: fstest.MapFS{}, th: control.Defaults(), log: io.Discard, out: out, files: map[string]*raw.File{}}
	r := run{m: "m2", pass: 1, nr: 1, target: v1, lanes: 1, workload: "uniform-10k", rate: 100}
	f, err := b.drive(t.Context(), r, segments{created: new(uint64)}, func() control.Watch { return control.Watch{} },
		load.NewStream(mustNamed(r.workload), 1), load.Plan{Rate: 100, Lanes: 1, Window: 2 * time.Second}, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(f.Verdict.Reasons, whySegment) || slices.Contains(f.Verdict.Reasons, whyCheckpoint) {
		t.Fatalf("reasons %q, want a new WAL segment and no checkpoint", f.Verdict.Reasons)
	}
	text, err := os.ReadFile(filepath.Join(out, "plan.txt"))
	if err != nil || !strings.Contains(string(text), "deadlocks in the window 0, transfers given up in the window 0") {
		t.Fatalf("plan.txt without deadlocks and given up transfers: %v\n%s", err, text)
	}
}

func TestTermChangesOnlyCountInM4AndM7(t *testing.T) {
	out := t.TempDir()
	if err := os.Mkdir(filepath.Join(out, "raw"), 0o750); err != nil {
		t.Fatal(err)
	}
	b := &bench{root: fstest.MapFS{}, th: control.Defaults(), log: io.Discard, out: out, files: map[string]*raw.File{}}
	watch := func() control.Watch { return control.Watch{} }
	p := load.Plan{Rate: 100, Lanes: 1, Window: 2 * time.Second}
	for _, m := range []string{"m2", "m4", "m7"} {
		r := run{m: m, pass: 1, nr: 1, target: l3, lanes: 1, workload: "uniform-10k", rate: 100}
		f, err := b.drive(t.Context(), r, elections{term: new(uint64)}, watch, load.NewStream(mustNamed(r.workload), 1), p, 1, nil)
		if err != nil {
			t.Fatal(err)
		}
		if refused := slices.Contains(f.Verdict.Reasons, raw.WhyTerm); refused == r.excused() || f.AllowStalls != r.excused() {
			t.Errorf("%s: a new term every second refused %v, stalls allowed %v", m, refused, f.AllowStalls)
		}
	}
}

func TestFailedVerifyStopsThePlan(t *testing.T) {
	out := t.TempDir()
	if err := os.Mkdir(filepath.Join(out, "raw"), 0o750); err != nil {
		t.Fatal(err)
	}
	b := &bench{root: fstest.MapFS{}, th: control.Defaults(), log: io.Discard, out: out, files: map[string]*raw.File{}, build: raw.Build{Commit: "abc", Dirty: true}}
	r := run{m: "m2", pass: 1, nr: 1, target: l1, lanes: 1, workload: "uniform-10k", rate: 100}
	watch := func() control.Watch { return control.Watch{} }
	p := load.Plan{Rate: 100, Lanes: 1, Window: time.Second}
	w := mustNamed(r.workload)
	f, halt := b.drive(t.Context(), r, sham{}, watch, load.NewStream(w, 1), p, 1, nil)
	if halt != nil || f == nil || !f.Books.Verified {
		t.Fatalf("books that add up: %v, %+v", halt, f)
	}
	if !f.Build.Dirty || f.Build.Commit != "abc" || f.ID != r.id() || f.Load.Need != 1 || f.Load.Seed != r.seed() || f.AllowStalls {
		t.Fatalf("head %+v", f.Head)
	}
	if _, err := os.Stat(filepath.Join(out, "raw", r.id()+".json")); err != nil || b.files[r.id()] != f {
		t.Fatalf("raw file of the run: %v", err)
	}
	text, err := os.ReadFile(filepath.Join(out, "plan.txt"))
	if err != nil || !strings.Contains(string(text), r.id()) {
		t.Fatalf("plan.txt without the run: %v\n%s", err, text)
	}
	if _, halt = b.drive(t.Context(), r, sham{}, watch, load.NewStream(w, 1), p, 1, nil); !errors.Is(halt, fs.ErrExist) {
		t.Fatalf("the same run again: %v, want its raw file kept", halt)
	}
	for _, c := range []struct {
		name   string
		verify error
		settle error
		halts  bool
		why    string
	}{
		{"books that do not add up", errors.New("transfer 7 missing"), nil, true, raw.WhyVerify},
		{"nodes that disagree at the same index", nil, ledgerrun.ErrDiverged, true, whyNodes},
		{"a node that fell behind", nil, ledgerrun.ErrBehind, false, whyNodes},
	} {
		r.nr++
		f, halt = b.drive(t.Context(), r, sham{verify: c.verify}, watch, load.NewStream(w, 1), p, 1, func(context.Context) error { return c.settle })
		if errors.Is(halt, errVerify) != c.halts || f == nil || b.files[r.id()] == nil {
			t.Fatalf("%s: halt %v; want halt %v and a raw file", c.name, halt, c.halts)
		}
		if f.Verdict.Valid || !slices.ContainsFunc(f.Verdict.Reasons, func(why string) bool { return strings.HasPrefix(why, c.why) }) {
			t.Errorf("%s: valid %v with %q, want %q among the reasons", c.name, f.Verdict.Valid, f.Verdict.Reasons, c.why)
		}
	}
}

func git(t *testing.T, dir string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-C", dir, "-c", "user.name=bench", "-c", "user.email=bench@example.invalid"}, args...)...) //nolint:gosec // G204: git in the repository of the test
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
}

func TestDirtyTree(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("no git")
	}
	repo := t.TempDir()
	git(t, repo, "init", "-q")
	if err := os.WriteFile(filepath.Join(repo, "a.go"), []byte("package a\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	git(t, repo, "add", "a.go")
	git(t, repo, "commit", "-q", "-m", "a")
	build, err := described(t.Context(), repo)
	if err != nil || build.Dirty || len(build.Commit) != 40 || build.Go == "" {
		t.Fatalf("clean tree: %+v, %v", build, err)
	}
	if err := os.MkdirAll(filepath.Join(repo, "docs", "bench", "2026-11-17-abc", "raw"), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repo, "docs", "bench", "2026-11-17-abc", "raw", "m2.json"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if build, _ = described(t.Context(), repo); build.Dirty {
		t.Fatal("the raw files of the plan itself make the tree dirty")
	}
	for _, c := range []struct{ name, content string }{{"a.go", "package a\n\nvar x int\n"}, {"b.go", "package a\n"}} {
		if err := os.WriteFile(filepath.Join(repo, c.name), []byte(c.content), 0o600); err != nil {
			t.Fatal(err)
		}
		if build, _ = described(t.Context(), repo); !build.Dirty {
			t.Fatalf("%s changed and the tree is not dirty", c.name)
		}
		if err := os.WriteFile(filepath.Join(repo, "a.go"), []byte("package a\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
}

func TestHangUpStopsThePlan(t *testing.T) {
	ctx, stop := stopping()
	defer stop()
	if err := syscall.Kill(os.Getpid(), syscall.SIGHUP); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ctx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("SIGHUP did not stop the plan, so it would not clean up")
	}
}

func TestStageWithoutRoomEndsTheChain(t *testing.T) {
	bins := t.TempDir()
	bin, err := ledgerrun.Build(t.Context(), "../../..", bins)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	fsys := machine("6442450944", "0", "16000000")
	cg, _ := ownCgroup(fsys)
	b := &bench{root: fsys, cgroup: cg, dir: dir, bins: map[string]string{"": bin}, th: control.Defaults(), log: io.Discard, files: map[string]*raw.File{}}
	var runs []run
	for i, rate := range []int{1_000_000, 2_000_000} {
		runs = append(runs, run{m: "m1", pass: 1, nr: 1, target: l1, lanes: 1, workload: "uniform-10k", rate: rate, stage: i + 1, timing: shape{smoke: true}.stage()})
	}
	if err := b.onLedger(t.Context(), runs); err != nil {
		t.Fatalf("a stage without room for a window of %v: %v, want the chain to end without an error", ledgerrun.MinWindow, err)
	}
	if len(b.lines) != 1 || b.lines[0].id != runs[0].id() || !strings.Contains(b.lines[0].text, "not run") || len(b.files) != 0 {
		t.Fatalf("plan.txt lines %+v, %d raw files; want one line that the stage did not run, and no raw file", b.lines, len(b.files))
	}
	if left := ownLedgerd(os.DirFS("/")); len(left) > 0 {
		t.Fatalf("ledgerd %v still runs", left)
	}
}

func TestFailedRunLeavesNoNode(t *testing.T) {
	bins := t.TempDir()
	bin, err := ledgerrun.Build(t.Context(), "../../..", bins)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	fsys := machine("6442450944", "0", "16000000")
	cg, _ := ownCgroup(fsys)
	b := &bench{root: fsys, cgroup: cg, dir: dir, bins: map[string]string{"": bin}, th: control.Defaults(), log: io.Discard}
	r := run{m: "m7", pass: 1, nr: 1, target: l1, lanes: 1, workload: "uniform-10k", rate: 100, stop: true, timing: shape{smoke: true}.fixed()}
	err = b.onLedger(t.Context(), []run{r})
	if err == nil {
		t.Fatal("stopping node 3 of a single node went through")
	}
	if left := ownLedgerd(os.DirFS("/")); len(left) > 0 {
		t.Fatalf("after %v: ledgerd %v still runs", err, left)
	}
	if _, err := os.Stat(filepath.Join(dir, "run")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("run directory after a failed run: %v", err)
	}
}
