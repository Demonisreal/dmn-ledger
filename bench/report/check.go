package report

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/Demonisreal/dmn-ledger/bench/control"
	"github.com/Demonisreal/dmn-ledger/bench/load"
	"github.com/Demonisreal/dmn-ledger/bench/raw"
)

var ErrRefused = errors.New("report: check refused")

const (
	WhyDirty   = "built from a tree with changes"
	WhySimbug  = "built with simbug outside m5"
	WhyHead    = "head incomplete"
	WhySeconds = "seconds disagree with their own counts"
	WhyOneRun  = "only one valid run"
	WhyStale   = "does not match the raw files"
	WhyMissing = "missing"

	WhyUnverified    = "valid but not verified"
	WhyTermChanged   = "valid with a new term in the window"
	WhyStallsAllowed = "stalls allowed outside m4 and m7"
	WhyBand          = "bands do not follow from their seconds"
)

type Verdict struct {
	Subject string
	Passed  bool
	Why     []string
}

func Check(dir string) ([]Verdict, error) {
	runs, err := read(os.DirFS(filepath.Join(dir, "raw")))
	if err != nil {
		return nil, err
	}
	verdicts := check(runs, os.DirFS(dir))
	failed := 0
	for _, v := range verdicts {
		if !v.Passed {
			failed++
		}
	}
	if failed > 0 {
		return verdicts, fmt.Errorf("%w: %d of %d verdicts failed", ErrRefused, failed, len(verdicts))
	}
	return verdicts, nil
}

func check(in []*run, written fs.FS) []Verdict {
	runs := slices.Clone(in)
	sortRuns(runs)
	var verdicts []Verdict
	for _, r := range runs {
		if why := refusals(r.f); len(why) > 0 {
			verdicts = append(verdicts, Verdict{Subject: "raw/" + r.name, Why: why})
		}
	}
	gs := groups(runs)
	for _, c := range chains(gs) {
		text, ok := c.overload().judge()
		v := Verdict{Subject: c.subject() + ", throughput under overload", Passed: ok, Why: []string{text}}
		verdicts = append(verdicts, v)
	}
	for _, g := range gs {
		text, ok := g.judge()
		v := Verdict{Subject: g.subject(), Passed: true, Why: []string{text}}
		if g.required() {
			v.Passed = ok
			if len(g.counted) == 1 {
				v.Why = append(v.Why, WhyOneRun)
			}
		}
		verdicts = append(verdicts, v)
	}
	verdicts = append(verdicts, complete(gs, written)...)
	out := render(runs)
	for _, name := range slices.Sorted(maps.Keys(out)) {
		v := Verdict{Subject: name, Passed: true}
		on, err := fs.ReadFile(written, name)
		switch {
		case err != nil:
			v.Passed, v.Why = false, []string{WhyMissing}
		case !bytes.Equal(on, out[name]):
			v.Passed, v.Why = false, []string{WhyStale + ": " + firstDifference(on, out[name])}
		}
		verdicts = append(verdicts, v)
	}
	return verdicts
}

func firstDifference(on, want []byte) string {
	a, b := strings.Split(string(on), "\n"), strings.Split(string(want), "\n")
	for i := range min(len(a), len(b)) {
		if a[i] != b[i] {
			return fmt.Sprintf("line %d reads %q, the raw files give %q", i+1, a[i], b[i])
		}
	}
	return fmt.Sprintf("%d lines, the raw files give %d", len(a), len(b))
}

func refusals(f *raw.File) []string {
	var why []string
	if f.Build.Dirty {
		why = append(why, WhyDirty)
	}
	if slices.Contains(f.Build.Tags, "simbug") && f.Measurement != "m5" {
		why = append(why, WhySimbug)
	}
	var missing []string
	for _, field := range []struct {
		name  string
		empty bool
	}{
		{"id", f.ID == ""},
		{"measurement", f.Measurement == ""},
		{"pass", f.Pass < 1},
		{"started", f.Started.IsZero()},
		{"build.commit", f.Build.Commit == ""},
		{"build.go", f.Build.Go == ""},
		{"machine.cpu", f.Machine.CPU == ""},
		{"machine.kernel", f.Machine.Kernel == ""},
		{"machine.filesystem", f.Machine.Filesystem == ""},
		{"machine.disk", f.Machine.Disk == ""},
		{"target.kind", f.Target.Kind == ""},
		{"target.lanes", f.Target.Lanes < 1},
		{"load.workload", f.Load.Workload == ""},
		{"load.rate", f.Load.Rate < 1},
		{"load.window_ns", f.Load.Window <= 0},
		{"load.need", f.Load.Need < 1},
		{"thresholds", f.Thresholds.Fast <= 0 || f.Thresholds.Slow < f.Thresholds.Fast},
		{"probe", len(f.Probe.Means) == 0},
	} {
		if field.empty {
			missing = append(missing, field.name)
		}
	}
	if len(missing) > 0 {
		why = append(why, WhyHead+": "+strings.Join(missing, ", "))
	}
	if wrong := recount(f); len(wrong) > 0 {
		why = append(why, WhySeconds+": "+strings.Join(wrong, "; "))
	}
	excused := f.Measurement == "m4" || f.Measurement == "m7"
	if f.Verdict.Valid && !f.Books.Verified {
		why = append(why, WhyUnverified)
	}
	if f.Verdict.Valid && f.Target.Kind == "ledger" && !excused && f.TermChanged() {
		why = append(why, WhyTermChanged)
	}
	if f.AllowStalls && !excused {
		why = append(why, WhyStallsAllowed)
	}
	if f.Books.Existed > 0 {
		why = append(why, fmt.Sprintf("%d transfers existed already", f.Books.Existed))
	}
	if wrong := rebuild(f); len(wrong) > 0 {
		why = append(why, WhyBand+": "+strings.Join(wrong, "; "))
	}
	return why
}

func recount(f *raw.File) []string {
	ctl := make([]control.Second, len(f.Seconds))
	for i, s := range f.Seconds {
		ctl[i] = control.Second{Flushes: s.Flushes, FlushTime: s.FlushTime, Target: s.CPU.Target, Foreign: s.CPU.Foreign, Throttled: s.Throttled, SwappedOut: s.SwappedOut}
	}
	var wrong []string
	var window []control.Mark
	inBand := map[control.Speed]int{}
	thermal := 0
	marks := f.Thresholds.Judge(ctl)
	f.Thresholds.Unpace(marks, ctl, func(i int) bool { return f.Seconds[i].Phase == load.Window.String() })
	for i, m := range marks {
		s := f.Seconds[i]
		unseen := slices.Contains(s.Why, raw.WhyNoSample) || slices.Contains(s.Why, raw.WhyStale)
		valid := m.Valid && !unseen && !slices.Contains(s.Why, control.WhyUnread)
		if s.Speed != m.Speed || s.Valid != valid {
			wrong = append(wrong, fmt.Sprintf("second %d is %v and valid %v, its flushes say %v and %v", i, s.Speed, s.Valid, m.Speed, valid))
		}
		stood := unseen || m.Speed == control.None && ctl[i].Target < control.IdleCPU || slices.Contains(m.Why, control.WhyIdle)
		if s.Phase == load.Window.String() && f.Verdict.Valid && !f.AllowStalls && stood {
			wrong = append(wrong, fmt.Sprintf("second %d stalled and the run is valid", i))
		}
		if s.Phase == load.Window.String() {
			window = append(window, control.Mark{Speed: m.Speed, Valid: valid})
			if valid {
				inBand[m.Speed]++
			}
			if s.Throttled > 0 {
				thermal++
			}
		}
	}
	if thermal != f.Verdict.Throttled {
		wrong = append(wrong, fmt.Sprintf("%d window seconds marked thermal, their counters say %d", f.Verdict.Throttled, thermal))
	}
	if f.Load.Need > 0 {
		if speed := control.Settle(window, f.Load.Need); speed != f.Verdict.Speed {
			wrong = append(wrong, fmt.Sprintf("the run is %v, its seconds say %v", f.Verdict.Speed, speed))
		}
	}
	if inBand[control.Slow] != f.Bands.Slow.Seconds || inBand[control.Fast] != f.Bands.Fast.Seconds || inBand[control.Unpaced] != f.Bands.Unpaced.Seconds {
		wrong = append(wrong, fmt.Sprintf("bands have %d slow, %d fast and %d unpaced seconds, the window has %d, %d and %d",
			f.Bands.Slow.Seconds, f.Bands.Fast.Seconds, f.Bands.Unpaced.Seconds, inBand[control.Slow], inBand[control.Fast], inBand[control.Unpaced]))
	}
	if f.Verdict.Valid != (len(f.Verdict.Reasons) == 0) {
		wrong = append(wrong, fmt.Sprintf("valid %v with reasons %q", f.Verdict.Valid, f.Verdict.Reasons))
	}
	return wrong
}

func rebuild(f *raw.File) []string {
	var wrong []string
	for _, c := range []struct {
		name  string
		speed control.Speed
		band  raw.Band
	}{{"slow", control.Slow, f.Bands.Slow}, {"fast", control.Fast, f.Bands.Fast}, {"unpaced", control.Unpaced, f.Bands.Unpaced}} {
		var n int
		var booked, latencies, syncs uint64
		prev := f.Before
		for _, s := range f.Seconds {
			moved := syncsSince(s.Parts, prev)
			prev = s.Parts
			if s.Phase != load.Window.String() || !s.Valid || s.Speed != c.speed {
				continue
			}
			n++
			booked += s.Booked
			latencies += s.Count
			syncs += moved
		}
		b := c.band
		held := b.Histogram.Never
		for _, bucket := range b.Histogram.Buckets {
			held += uint64(bucket[1]) //nolint:gosec // G115: a count of arrivals
		}
		if held != latencies {
			wrong = append(wrong, fmt.Sprintf("%s histogram holds %d latencies, its seconds %d", c.name, held, latencies))
		}
		for _, q := range []struct {
			name string
			at   float64
			got  time.Duration
		}{{"p50", 0.5, b.P50}, {"p99", 0.99, b.P99}} {
			if want := quantile(b.Histogram, q.at); want != q.got {
				wrong = append(wrong, fmt.Sprintf("%s %s %v, its histogram gives %v", c.name, q.name, q.got, want))
			}
		}
		var throughput, perTransfer float64
		if n > 0 {
			throughput = float64(booked) / float64(n)
			if booked > 0 {
				perTransfer = float64(syncs) / float64(booked)
			}
		}
		if !near(throughput, b.Throughput) {
			wrong = append(wrong, fmt.Sprintf("%s throughput %.1f, its seconds give %.1f", c.name, b.Throughput, throughput))
		}
		if !near(perTransfer, b.Syncs) {
			wrong = append(wrong, fmt.Sprintf("%s fsync per transfer %.4f, its seconds give %.4f", c.name, b.Syncs, perTransfer))
		}
	}
	return wrong
}

func syncsSince(now, prev []raw.Part) uint64 {
	var n uint64
	for _, p := range now {
		if i := slices.IndexFunc(prev, func(q raw.Part) bool { return q.Name == p.Name }); i >= 0 {
			n += p.Syncs - prev[i].Syncs
		}
	}
	return n
}

func quantile(h raw.Histogram, q float64) time.Duration {
	var done uint64
	for _, bucket := range h.Buckets {
		done += uint64(bucket[1]) //nolint:gosec // G115: a count of arrivals
	}
	total := done + h.Never
	if total == 0 {
		return 0
	}
	rank := min(max(uint64(math.Ceil(q*float64(total))), 1), total)
	var cum uint64
	for _, bucket := range h.Buckets {
		if cum += uint64(bucket[1]); cum >= rank { //nolint:gosec // G115: a count of arrivals
			return time.Duration(bucket[0])
		}
	}
	return load.Never
}

func near(a, b float64) bool {
	return math.Abs(a-b) <= 1e-9*max(math.Abs(a), math.Abs(b))
}

func (g *group) required() bool {
	return g.m == "m2" && g.workload == "uniform-10k"
}

func (g *group) subject() string {
	s := fmt.Sprintf("%s %s, %d lanes, %s at %s/s", g.m, name(g.target), g.target.Lanes, g.workload, grouped(float64(g.rate)))
	if e := distinct(g.target); e != "" {
		s += ", " + e
	}
	return s
}

func figures(runs []*run) []control.Figures {
	var figs []control.Figures
	for _, r := range runs {
		v := r.f.Verdict
		b := bandOf(r.f, v.Speed)
		figs = append(figs, control.Figures{Valid: v.Valid, Speed: v.Speed, Flush: b.Flush, Throughput: b.Throughput, P50: b.P50, Syncs: b.Syncs})
	}
	return figs
}

func (g *group) judge() (string, bool) {
	if len(g.counted) == 0 {
		return "no valid run", false
	}
	if !slices.ContainsFunc(g.counted, func(r *run) bool { return !r.f.Verdict.Saturated }) {
		return judgeThroughput(g.runs)
	}
	ag := control.Agree(figures(g.runs))
	return accordText(ag), ag.Accord == control.Agreed || ag.Accord == control.AgreedInFlushes
}

func accordText(ag control.Agreement) string {
	if ag.Why == "" {
		return ag.Accord.String()
	}
	return ag.Accord.String() + ": " + ag.Why
}

func judgeThroughput(runs []*run) (string, bool) {
	if slices.ContainsFunc(runs, func(r *run) bool { return r.f.Verdict.Valid && r.f.Verdict.Limit == raw.LimitGenerator }) {
		return "at least: the generator was the limit", true
	}
	figs := figures(runs)
	for i := range figs {
		figs[i].P50, figs[i].Syncs = 0, 0
	}
	ag := control.Agree(figs)
	if ag.Accord != control.AgreedInFlushes {
		return accordText(ag), ag.Accord == control.Agreed
	}
	a, b := figs[ag.Pair[0]].Throughput, figs[ag.Pair[1]].Throughput
	if lo, hi := min(a, b), max(a, b); hi-lo > control.Tolerance*lo {
		return fmt.Sprintf("missed: throughput %.0f against %.0f, runs with the nearest flush times", a, b), false
	}
	return "agreed in throughput, runs with the nearest flush times", true
}
