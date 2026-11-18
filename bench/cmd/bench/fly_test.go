package main

import (
	"errors"
	"regexp"
	"slices"
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

func TestEnoughEndsAnUnpacedWindow(t *testing.T) {
	th := control.Defaults()
	secs := make([]control.Second, 11)
	seen := make([]sampled, 12)
	for i := range secs {
		secs[i] = control.Second{Span: time.Second, Flushes: 8, FlushTime: 16 * time.Millisecond, Target: 1.0}
		seen[i+1].ok = true
	}
	seen[0].ok = true
	if !enough(th, secs, seen, 1, 8) {
		t.Fatal("ten seconds with 8 flushes on a full core are not enough for 8")
	}
	for _, i := range []int{3, 5, 7} {
		secs[i].Target = 0.02
	}
	if enough(th, secs, seen, 1, 8) {
		t.Error("three idle seconds of ten still left 8 valid seconds")
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

func stageFile(id string, pass, rate int, valid, saturated bool, reasons ...string) *raw.File {
	speed := control.Slow
	if !valid {
		speed = control.Restless
	}
	f := file(id, pass, valid, speed, 1500*time.Microsecond, 0, float64(rate), reasons...)
	f.Measurement, f.Load.Rate, f.Verdict.Saturated = "m1", rate, saturated
	if saturated {
		f.Verdict.Limit = raw.LimitTarget
	}
	return f
}

func TestPassWithoutAValidSaturatedStageIsNotSettled(t *testing.T) {
	files := []*raw.File{
		stageFile("a-16000-1", 1, 16000, true, false),
		stageFile("a-32000-1", 1, 32000, false, true, raw.WhyRestless, raw.WhyStalled),
		stageFile("a-16000-2", 2, 16000, true, false),
		stageFile("a-32000-2", 2, 32000, false, true, raw.WhyRestless, raw.WhyStalled),
	}
	if settled("m1", files) {
		t.Error("both passes ended in a saturated stage that is not valid: the chain has no throughput under overload, yet it counts as settled")
	}
	files[3] = stageFile("a-32000-2", 2, 32000, true, true)
	if settled("m1", files) {
		t.Error("pass 1 is open and pass 2 saturated: settled")
	}
	files = append(files, stageFile("a-32000-1r1", 1, 32000, true, true))
	if !settled("m1", files) {
		t.Error("the repeat of pass 1 saturated as pass 2 did: undecided")
	}
	held := []*raw.File{stageFile("b-16000-1", 1, 16000, true, false), stageFile("b-16000-2", 2, 16000, true, true)}
	if settled("m1", held) {
		t.Error("one pass held every stage, the other saturated at the target: settled without a second saturated run")
	}
	if settled("m1", []*raw.File{stageFile("c-16000-1", 1, 16000, false, false, raw.WhyFailed)}) {
		t.Error("a chain without a valid run counts as settled")
	}
}

func TestRepeatsComeBeforeTheThirdPass(t *testing.T) {
	b := &bench{files: map[string]*raw.File{}}
	for pass := 1; pass <= 2; pass++ {
		var it item
		for i, rate := range []int{16000, 32000} {
			it.runs = append(it.runs, run{m: "m1", pass: pass, nr: pass, target: v3, lanes: r4Lanes, workload: "uniform-10k", rate: rate, stage: i + 1})
		}
		b.items = append(b.items, it)
		for _, r := range it.runs {
			b.files[r.id()] = stageFile(r.id(), pass, r.rate, true, r.rate == 32000)
			b.remember(r)
		}
	}
	open := b.items[1].runs[1]
	b.files[open.id()] = stageFile(open.id(), 2, open.rate, false, true, raw.WhyRestless)
	var ran []string
	for step := range steps {
		for _, it := range b.extra(step) {
			for _, r := range it.runs {
				if b.files[r.id()] == nil {
					ran = append(ran, r.id())
					b.files[r.id()] = stageFile(r.id(), r.pass, r.rate, true, r.rate == 32000)
					b.remember(r)
				}
			}
		}
	}
	if want := []string{open.id() + "r1"}; !slices.Equal(ran, want) {
		t.Errorf("ran %q, want only %q: the repeat of a restless saturated stage settles the chain before a third pass is decided", ran, want)
	}
}

func TestOneBadRunDoesNotStopASmallBlock(t *testing.T) {
	b := &bench{files: map[string]*raw.File{}, out: t.TempDir()}
	var runs []run
	for _, rate := range []int{2000, 2001, 120000, 50000} {
		r := run{m: "m3", pass: 1, nr: 1, target: l1, lanes: 4, workload: "uniform-10k", rate: rate, build: "pprof"}
		b.items = append(b.items, item{runs: []run{r}})
		runs = append(runs, r)
	}
	for _, r := range runs[:2] {
		b.files[r.id()] = file(r.id(), 1, true, control.Slow, 0, 0, 0)
		b.remember(r)
	}
	for _, r := range runs[2:] {
		b.files[r.id()] = file(r.id(), 1, false, control.Slow, 0, 0, 0, raw.WhyFailed)
		b.remember(r)
		if err := b.judgeBlocks(false); err != nil {
			t.Fatalf("profile runs that failed stop the whole plan after an hour: %v", err)
		}
	}
	if err := b.judgeBlocks(true); err != nil {
		t.Fatalf("the end of the plan: %v, want the verdict in plan.txt and no error", err)
	}
	if text := string(b.planText()); !strings.Contains(text, "m3: 2 of 4 runs invalid, more than a quarter") {
		t.Errorf("plan.txt without the verdict on m3:\n%s", text)
	}
}

func TestGoingOnAfterAHaltRunsSomething(t *testing.T) {
	b := &bench{files: map[string]*raw.File{}, earlier: map[string]bool{}}
	var runs []run
	for i := range 8 {
		r := run{m: "m2", pass: 1, nr: 1, target: l1, lanes: 32, workload: "uniform-10k", rate: 1000 * (i + 1)}
		b.items = append(b.items, item{runs: []run{r}})
		runs = append(runs, r)
	}
	for _, r := range runs[:3] {
		b.files[r.id()] = file(r.id(), 1, false, control.Slow, 0, 0, 0, raw.WhyFailed)
		b.earlier[r.id()] = true
		b.remember(r)
	}
	if err := b.judgeBlocks(false); err != nil {
		t.Fatalf("the plan goes on after the halt and stops again before it ran anything: %v", err)
	}
	b.files[runs[3].id()] = file(runs[3].id(), 1, true, control.Slow, 0, 0, 0)
	b.remember(runs[3])
	if err := b.judgeBlocks(false); err != nil {
		t.Fatalf("a valid run after going on: %v", err)
	}
	b.files[runs[4].id()] = file(runs[4].id(), 1, false, control.Slow, 0, 0, 0, raw.WhyFailed)
	b.remember(runs[4])
	if err := b.judgeBlocks(false); !errors.Is(err, errTooManyBad) {
		t.Fatalf("a fourth run of eight invalid for good: %v, want errTooManyBad", err)
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
	if err := b.judgeBlocks(true); err != nil || !strings.Contains(string(b.planText()), "m2: 3 of 3 runs invalid, more than a quarter") {
		t.Fatalf("three of three made runs restless at the end: %v, want the verdict in plan.txt\n%s", err, b.planText())
	}
	for _, r := range runs[3:] {
		b.files[r.id()] = file(r.id(), 1, true, control.Slow, 0, 0, 0)
		b.remember(r)
	}
	if err := b.judgeBlocks(true); err != nil || !strings.Contains(string(b.planText()), "m2: 3 of 8 runs invalid, more than a quarter") {
		t.Fatalf("three of eight invalid after the repeats: %v\n%s", err, b.planText())
	}
	for _, r := range runs[:3] {
		b.files[r.id()] = file(r.id(), 1, false, control.Slow, 0, 0, 0, raw.WhyGenerator)
	}
	if err := b.judgeBlocks(false); err != nil {
		t.Fatalf("three of eight invalid only because the generator was at its limit: %v", err)
	}
	if err := b.judgeBlocks(true); err != nil || !strings.Contains(string(b.planText()), "m2: 0 of 8 runs invalid, 3 more only because the generator was at its limit") {
		t.Fatalf("runs at the limit of the generator at the end: %v\n%s", err, b.planText())
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
		{Phase: load.Window.String(), Why: []string{control.WhyForeign, control.WhySwapped}},
		{Phase: load.Window.String(), Valid: true, Speed: control.Fast, Parts: []raw.Part{{Name: "n1", Counters: map[string]uint64{"peak_rss": 300 << 20}}}},
	}
	f.Verdict.Excluded, f.Verdict.Throttled = 3, 2
	b.note(r, summary(f))
	quiet := file("m1-v3x4-uniform-10k-64000-1", 1, true, control.Unpaced, 0, 0, 18000)
	quiet.Bands.Unpaced.Seconds, quiet.Verdict.Saturated, quiet.Verdict.Limit, quiet.Target.Kind = 12, true, raw.LimitTarget, "postgres"
	quiet.Seconds = []raw.Second{{Phase: load.Window.String(), Shed: 40}, {Phase: load.Window.String(), Shed: 2}, {Phase: load.Drain.String(), Shed: 7}}
	b.note(r, summary(quiet))
	other := run{m: "m7", pass: 1, nr: 1, target: l3, lanes: 4, workload: "uniform-10k", rate: r4}
	b.note(other, "not selected")
	b.only = regexp.MustCompile("^m2")
	b.stopped = "The plan stopped: bench: not enough memory"
	text := string(b.planText())
	for _, want := range []string{
		"at commit f70d5b0, tree with changes (dirty)",
		"m2, pass 1\n  m2-l1x32-uniform-10k-2000-1  invalid: restless; target stalled; speed restless; 0 of 3 window seconds counted, thermal limit in 2 (left out: foreign cpu 2, other speed 1, swapped out 1); peak rss of a node 300 MiB",
		"valid; speed unpaced; saturated, limit target; 12 of 12 window seconds counted; deadlocks in the window 0, transfers given up in the window 42",
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
