package main

import (
	"errors"
	"regexp"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/Demonisreal/dmn-ledger/bench/control"
	"github.com/Demonisreal/dmn-ledger/bench/load"
	"github.com/Demonisreal/dmn-ledger/bench/raw"
)

func slowSeconds(n int) []control.Second {
	secs := make([]control.Second, n)
	for i := range secs {
		secs[i] = control.Second{Span: time.Second, Flushes: 300, FlushTime: 300 * 1400 * time.Microsecond}
	}
	return secs
}

func TestEnoughLeavesOutSecondsWithoutASample(t *testing.T) {
	th := control.Defaults()
	all := []sampled{{ok: true}, {ok: true}, {ok: true}, {ok: true}, {ok: true}, {ok: true}}
	if !enough(th, slowSeconds(5), all, 1, 4) {
		t.Fatal("four valid slow seconds after the warmup are not enough for four")
	}
	for _, c := range []struct {
		name string
		seen sampled
	}{{"no sample", sampled{}}, {"stale status", sampled{ok: true, stale: true}}} {
		seen := append([]sampled(nil), all...)
		seen[3] = c.seen
		if enough(th, slowSeconds(5), seen, 1, 4) {
			t.Errorf("%s in second 2: that second and the next one still counted", c.name)
		}
		if !enough(th, slowSeconds(5), seen, 1, 2) {
			t.Errorf("%s in second 2: seconds 1 and 4 are left and two are needed", c.name)
		}
	}
	if enough(th, slowSeconds(1), all[:1], 1, 1) {
		t.Error("a window without a second is enough")
	}
}

func TestGaugeFollowsNewPids(t *testing.T) {
	g := newGauge(fstest.MapFS{}, control.Watch{Disk: "nvme0n1", Pids: []int{11, 12, 13}})
	g.tick([]int{11, 12, 13})
	g.tick([]int{11, 12})
	g.tick([]int{11, 12})
	g.tick([]int{11, 12, 14})
	g.tick([]int{11, 12, 14})
	if n := len(g.seconds()); n != 5 {
		t.Fatalf("%d seconds after five ticks", n)
	}
	if got := g.watch.Pids; len(got) != 3 || got[2] != 14 {
		t.Fatalf("meter watches %v after a restart", got)
	}
}

func file(id string, pass int, valid bool, speed control.Speed, flush time.Duration, p50 time.Duration, throughput float64, reasons ...string) *raw.File {
	f := &raw.File{Head: raw.Head{ID: id, Measurement: "m2", Pass: pass}}
	f.Verdict = raw.Verdict{Valid: valid, Speed: speed, Reasons: reasons}
	band := raw.Band{Seconds: 30, Flush: flush, P50: p50, Throughput: throughput, Syncs: 0.2}
	if speed == control.Fast {
		f.Bands.Fast = band
	} else {
		f.Bands.Slow = band
	}
	return f
}

func TestMoreRunsWhereUndecided(t *testing.T) {
	ms := time.Millisecond
	for _, c := range []struct {
		name  string
		files []*raw.File
		done  bool
	}{
		{"equal disk, equal numbers", []*raw.File{file("a-1", 1, true, control.Slow, 1500*time.Microsecond, 10*ms, 2000), file("a-2", 2, true, control.Slow, 1550*time.Microsecond, 10*ms, 2000)}, true},
		{"equal disk, p50 apart", []*raw.File{file("a-1", 1, true, control.Slow, 1500*time.Microsecond, 10*ms, 2000), file("a-2", 2, true, control.Slow, 1550*time.Microsecond, 13*ms, 2000)}, true},
		{"disks apart", []*raw.File{file("a-1", 1, true, control.Slow, 1500*time.Microsecond, 10*ms, 2000), file("a-2", 2, true, control.Slow, 1800*time.Microsecond, 12*ms, 2000)}, false},
		{"one run invalid", []*raw.File{file("a-1", 1, true, control.Slow, 1500*time.Microsecond, 10*ms, 2000), file("a-2", 2, false, control.Restless, 0, 0, 0, raw.WhyRestless)}, false},
		{"only fast", []*raw.File{file("a-1", 1, true, control.Fast, 220*time.Microsecond, 2*ms, 2000), file("a-2", 2, true, control.Fast, 230*time.Microsecond, 2*ms, 2000)}, true},
		{"four runs", []*raw.File{
			file("a-1", 1, true, control.Slow, 1500*time.Microsecond, 10*ms, 2000), file("a-2", 2, true, control.Slow, 1800*time.Microsecond, 12*ms, 2000),
			file("a-3", 3, true, control.Slow, 1200*time.Microsecond, 8*ms, 2000), file("a-4", 4, true, control.Slow, 2100*time.Microsecond, 14*ms, 2000)}, true},
	} {
		if got := settled("m2", c.files); got != c.done {
			t.Errorf("%s: settled %v, want %v", c.name, got, c.done)
		}
	}
	stage := func(id string, pass, rate int, saturated bool, limit string, throughput float64, flush time.Duration) *raw.File {
		f := file(id, pass, true, control.Slow, flush, 0, throughput)
		f.Measurement, f.Load.Rate, f.Verdict.Saturated, f.Verdict.Limit = "m1", rate, saturated, limit
		return f
	}
	held := []*raw.File{stage("s1", 1, 1000, false, "", 1000, 1500*time.Microsecond), stage("s2", 1, 2000, false, "", 2000, 1500*time.Microsecond)}
	if !settled("m1", held) {
		t.Error("a pass that held every stage: undecided")
	}
	apart := []*raw.File{stage("s1", 1, 4000, true, raw.LimitTarget, 3000, 1500*time.Microsecond), stage("s2", 2, 4000, true, raw.LimitTarget, 3100, 1900*time.Microsecond)}
	if settled("m1", apart) {
		t.Error("two passes saturated on disks 27 % apart: settled")
	}
	apart[1].Verdict.Limit = raw.LimitGenerator
	if !settled("m1", apart) {
		t.Error("the generator was the limit: undecided")
	}
}

func restlessBench() (*bench, run) {
	r := run{m: "m2", pass: 1, nr: 1, target: l1, lanes: 32, workload: "uniform-10k", rate: r32}
	b := &bench{files: map[string]*raw.File{}, items: []item{{runs: []run{r}}}}
	return b, r
}

func TestRestlessRunsAreRepeatedTwice(t *testing.T) {
	b, r := restlessBench()
	b.files[r.id()] = file(r.id(), 1, false, control.Restless, 0, 0, 0, raw.WhyRestless)
	b.remember(r)
	again := b.restless(1)
	if len(again) != 1 || again[0].id() != r.id()+"r1" {
		t.Fatalf("first round repeats %v", again)
	}
	b.files[again[0].id()] = file(again[0].id(), 1, false, control.Restless, 0, 0, 0, raw.WhyRestless)
	b.remember(again[0])
	if len(b.slots) != 1 {
		t.Fatalf("a repeat counts as a run of its own: %d slots", len(b.slots))
	}
	if again = b.restless(2); len(again) != 1 || again[0].id() != r.id()+"r2" {
		t.Fatalf("second round repeats %v", again)
	}
	if got := b.restless(1); len(got) != 0 {
		t.Fatalf("first round again after its repeat: %v", got)
	}
	b.files[r.id()] = file(r.id(), 1, false, control.Slow, 0, 0, 0, "calls failed")
	delete(b.files, r.id()+"r1")
	if got := b.restless(1); len(got) != 0 {
		t.Fatalf("an invalid run that is not restless is repeated: %v", got)
	}
	b.files[r.id()] = file(r.id(), 1, false, control.Slow, 0, 0, 0, whyCheckpoint)
	if got := b.restless(1); len(got) != 1 {
		t.Fatalf("a run with a checkpoint in its window is not repeated: %v", got)
	}
	b.files[r.id()] = file(r.id(), 1, false, control.Slow, 0, 0, 0, whySegment)
	if got := b.restless(1); len(got) != 1 {
		t.Fatalf("a run with a new WAL segment in its window is not repeated: %v", got)
	}
}

func TestTooManyBadCountsAfterRepeats(t *testing.T) {
	b := &bench{files: map[string]*raw.File{}}
	var runs []run
	for i := range 8 {
		r := run{m: "m2", pass: 1, nr: 1, target: l1, lanes: 32, workload: "uniform-10k", rate: 1000 * (i + 1)}
		b.items = append(b.items, item{runs: []run{r}})
		runs = append(runs, r)
	}
	for _, r := range runs[:3] {
		b.files[r.id()] = file(r.id(), 1, false, control.Restless, 0, 0, 0, raw.WhyRestless)
		b.remember(r)
	}
	if err := b.judgeBlocks(false); err != nil {
		t.Fatalf("three restless runs of eight before their repeats: %v", err)
	}
	if err := b.judgeBlocks(true); !errors.Is(err, errTooManyBad) {
		t.Fatalf("three of three made runs restless at the end: %v, want errTooManyBad", err)
	}
	for _, r := range runs[3:] {
		b.files[r.id()] = file(r.id(), 1, true, control.Slow, 0, 0, 0)
		b.remember(r)
	}
	if err := b.judgeBlocks(true); !errors.Is(err, errTooManyBad) {
		t.Fatalf("three of eight invalid after the repeats: %v, want errTooManyBad", err)
	}
	for _, r := range runs[:3] {
		b.files[r.id()] = file(r.id(), 1, false, control.Slow, 0, 0, 0, raw.WhyFailed)
	}
	if err := b.judgeBlocks(false); !errors.Is(err, errTooManyBad) {
		t.Fatalf("three of eight invalid for good during the plan: %v, want errTooManyBad", err)
	}
}

func TestPostgresCountersInTheWindow(t *testing.T) {
	part := func(checkpoints, segments uint64) []raw.Part {
		return []raw.Part{{Name: "postgres", Counters: map[string]uint64{"checkpoints": checkpoints, "wal_init_syncs": segments}}}
	}
	f := &raw.File{Before: part(4, 1), Seconds: []raw.Second{
		{Phase: load.Warmup.String(), Parts: part(5, 3)},
		{Phase: load.Window.String(), Parts: part(5, 3)},
		{Phase: load.Window.String(), Parts: part(5, 3)},
		{Phase: load.Drain.String(), Parts: part(6, 4)},
	}}
	if grew(f, "checkpoints") != 0 || grew(f, "wal_init_syncs") != 0 {
		t.Fatal("counters that grew in the warmup and the drain count")
	}
	f.Seconds[2].Parts = part(6, 5)
	if grew(f, "checkpoints") != 1 || grew(f, "wal_init_syncs") != 2 {
		t.Fatalf("checkpoints %d and new segments %d in the window, want 1 and 2", grew(f, "checkpoints"), grew(f, "wal_init_syncs"))
	}
}

func TestPlanTextNamesEveryRun(t *testing.T) {
	b, r := restlessBench()
	b.build.Commit, b.build.Dirty = "f70d5b0", true
	f := file(r.id(), 1, false, control.Restless, 0, 0, 0, raw.WhyRestless, raw.WhyStalled)
	f.Seconds = []raw.Second{
		{Phase: load.Window.String(), Why: []string{control.WhyForeign}},
		{Phase: load.Window.String(), Why: []string{control.WhyForeign, control.WhyThrottled}},
		{Phase: load.Window.String(), Valid: true, Speed: control.Fast, Parts: []raw.Part{{Name: "n1", Counters: map[string]uint64{"peak_rss": 300 << 20}}}},
	}
	f.Verdict.Excluded = 3
	b.note(r, summary(f))
	other := run{m: "m7", pass: 1, nr: 1, target: l3, lanes: 4, workload: "uniform-10k", rate: r4}
	b.note(other, "not selected")
	b.only = regexp.MustCompile("^m2")
	b.stopped = "The plan stopped: bench: not enough memory"
	text := string(b.planText())
	for _, want := range []string{
		"at commit f70d5b0, tree with changes (dirty)",
		"m2, pass 1\n  m2-l1x32-uniform-10k-2000-1  invalid: restless; target stalled; speed restless; 0 of 3 window seconds counted (left out: foreign cpu 2, other speed 1, throttled 1); peak rss of a node 300 MiB",
		"m7, pass 1\n  m7-l3x4-uniform-10k-20000-1  not selected",
		`Only runs matching "^m2"`,
		"m6 is not part of this plan",
		"The plan stopped: bench: not enough memory",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("plan.txt lacks %q:\n%s", want, text)
		}
	}
	b.build.Dirty = false
	if strings.Contains(string(b.planText()), "dirty") {
		t.Error("a clean tree called dirty")
	}
}
