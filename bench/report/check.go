package report

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"

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
	return why
}

func recount(f *raw.File) []string {
	ctl := make([]control.Second, len(f.Seconds))
	for i, s := range f.Seconds {
		ctl[i] = control.Second{Flushes: s.Flushes, FlushTime: s.FlushTime, Foreign: s.CPU.Foreign, Throttled: s.Throttled, SwappedOut: s.SwappedOut}
	}
	var wrong []string
	var window []control.Mark
	inBand := map[control.Speed]int{}
	for i, m := range f.Thresholds.Judge(ctl) {
		s := f.Seconds[i]
		valid := m.Valid && !slices.Contains(s.Why, raw.WhyNoSample) && !slices.Contains(s.Why, control.WhyUnread)
		if s.Speed != m.Speed || s.Valid != valid {
			wrong = append(wrong, fmt.Sprintf("second %d is %v and valid %v, its flushes say %v and %v", i, s.Speed, s.Valid, m.Speed, valid))
		}
		if s.Phase == load.Window.String() {
			window = append(window, control.Mark{Speed: m.Speed, Valid: valid})
			if valid {
				inBand[m.Speed]++
			}
		}
	}
	if f.Load.Need > 0 {
		if speed := control.Settle(window, f.Load.Need); speed != f.Verdict.Speed {
			wrong = append(wrong, fmt.Sprintf("the run is %v, its seconds say %v", f.Verdict.Speed, speed))
		}
	}
	if inBand[control.Slow] != f.Bands.Slow.Seconds || inBand[control.Fast] != f.Bands.Fast.Seconds {
		wrong = append(wrong, fmt.Sprintf("bands have %d slow and %d fast seconds, the window has %d and %d",
			f.Bands.Slow.Seconds, f.Bands.Fast.Seconds, inBand[control.Slow], inBand[control.Fast]))
	}
	if f.Verdict.Valid != (len(f.Verdict.Reasons) == 0) {
		wrong = append(wrong, fmt.Sprintf("valid %v with reasons %q", f.Verdict.Valid, f.Verdict.Reasons))
	}
	return wrong
}

func (g *group) required() bool {
	return g.m == "m2" && g.workload == "uniform-10k"
}

func (g *group) subject() string {
	s := fmt.Sprintf("%s %s, %d lanes, %s at %s/s", g.m, name(g.target), g.target.Lanes, g.workload, grouped(float64(g.rate)))
	if e := env(g.target); e != "" {
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
