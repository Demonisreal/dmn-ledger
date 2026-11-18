package main

import (
	"bytes"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Demonisreal/dmn-ledger/bench/control"
	"github.com/Demonisreal/dmn-ledger/bench/raw"
	"github.com/Demonisreal/dmn-ledger/bench/report"
)

func runsOf(items []item) []run {
	var runs []run
	for _, it := range items {
		runs = append(runs, it.runs...)
	}
	return runs
}

func TestScheduleRunsOnce(t *testing.T) {
	for _, c := range []struct {
		smoke  bool
		items  map[string]int
		stages int
	}{
		{false, map[string]int{"m1": 16, "m2": 48, "m3": 4, "m4": 10, "m5": 8, "m7": 4}, 10},
		{true, map[string]int{"m1": 8, "m2": 24, "m3": 4, "m4": 5, "m5": 8, "m7": 2}, 10},
	} {
		items := shape{smoke: c.smoke}.schedule()
		got := make(map[string]int)
		for _, it := range items {
			got[it.runs[0].m]++
			if it.chain() != (it.runs[0].m == "m1") {
				t.Errorf("smoke %v: %s is a chain of %d", c.smoke, it.runs[0].id(), len(it.runs))
			}
			if it.chain() && len(it.runs) != c.stages {
				t.Errorf("smoke %v: chain %s has %d stages, want %d", c.smoke, it.runs[0].id(), len(it.runs), c.stages)
			}
		}
		for m, n := range c.items {
			if got[m] != n {
				t.Errorf("smoke %v: %d items of %s, want %d", c.smoke, got[m], m, n)
			}
		}
		if got["m6"] != 0 || len(got) != len(c.items) {
			t.Errorf("smoke %v: measurements %v", c.smoke, got)
		}
		seen := make(map[string]bool)
		for _, r := range runsOf(items) {
			if seen[r.id()] {
				t.Errorf("smoke %v: %s twice", c.smoke, r.id())
			}
			seen[r.id()] = true
			if r.excused() != (r.m == "m4" || r.m == "m7") || r.stop != (r.m == "m7") || r.bounce != (r.id() == "m4e-l3x32-uniform-1m-2000-"+string(rune('0'+r.pass))) {
				t.Errorf("smoke %v: %s stalls %v, stop %v, bounce %v", c.smoke, r.id(), r.excused(), r.stop, r.bounce)
			}
		}
	}
}

func TestStagesDouble(t *testing.T) {
	if got := (shape{}).stages(); got[0] != 1_000 || got[len(got)-1] != topStage || len(got) != 10 {
		t.Errorf("stages %v", got)
	}
	if got := (shape{smoke: true}).stages(); !slices.Equal(got, (shape{}).stages()) {
		t.Errorf("smoke stages %v, want the whole raster up to the first saturated stage", got)
	}
	want := timing{warm: 2 * time.Second, window: 6 * time.Second, longest: 6 * time.Second, drain: 3 * time.Second, need: 3}
	if got := (shape{smoke: true}).stage(); got != want {
		t.Errorf("smoke stage %+v, want %+v", got, want)
	}
}

func TestMachineTextHoldsTheHeat(t *testing.T) {
	out := t.TempDir()
	b := &bench{out: out}
	for at, h := range map[string]control.Heat{
		"20261118T190000Z": {Preference: "performance", Profile: "performance", MaxPerf: "100", PackageTemp: 52, Packages: []control.PackageHeat{{ID: "0", Throttles: 62114, ThrottledMs: 273576}}},
		"20261118T233000Z": {Preference: "performance", Profile: "performance", MaxPerf: "100", PackageTemp: 71, Packages: []control.PackageHeat{{ID: "0", Throttles: 99000, ThrottledMs: 390000}}},
	} {
		if err := writeJSON(filepath.Join(out, "heat-"+at+".json"), h); err != nil {
			t.Fatal(err)
		}
	}
	text, err := b.machineText()
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"heat heat-20261118T190000Z.json: energy_performance_preference performance, platform_profile performance, intel_pstate max_perf_pct 100, x86_pkg_temp 52.0 °C; package 0: package_throttle_count 62114, package_throttle_total_time_ms 273576",
		"heat heat-20261118T233000Z.json: energy_performance_preference performance, platform_profile performance, intel_pstate max_perf_pct 100, x86_pkg_temp 71.0 °C; package 0: package_throttle_count 99000, package_throttle_total_time_ms 390000",
	} {
		if !strings.Contains(string(text), want) {
			t.Errorf("machine.txt lacks %q:\n%s", want, text)
		}
	}
}

func TestSecondPassSwapsTheTargets(t *testing.T) {
	order := func(items []item, m string, pass int) map[string][]string {
		byCell := make(map[string][]string)
		var cells []string
		for _, it := range items {
			r := it.runs[0]
			if r.m != m || r.pass != pass {
				continue
			}
			cell := r.workload + " " + time.Duration(r.rate).String()
			if m == "m1" {
				cell = string(rune('0' + len(cells)/2))
			}
			cells = append(cells, cell)
			byCell[cell] = append(byCell[cell], r.target.label()+strings.Repeat("+", r.lanes/r4Lanes))
		}
		return byCell
	}
	items := shape{}.schedule()
	for _, m := range []string{"m1", "m2"} {
		first, second := order(items, m, 1), order(items, m, 2)
		if len(first) == 0 || len(first) != len(second) {
			t.Fatalf("%s: %d cells in pass 1, %d in pass 2", m, len(first), len(second))
		}
		for cell, targets := range first {
			back := slices.Clone(second[cell])
			slices.Reverse(back)
			if !slices.Equal(targets, back) {
				t.Errorf("%s %s: pass 1 %v, pass 2 %v, want the reverse", m, cell, targets, second[cell])
			}
		}
	}
	var at []string
	for _, it := range items {
		at = append(at, it.runs[0].m+" "+string(rune('0'+it.runs[0].pass)))
	}
	want := []string{"m1 1", "m2 1", "m5 1", "m3 1", "m4 1", "m7 1", "m1 2", "m2 2", "m4 2", "m7 2"}
	if got := slices.Compact(at); !slices.Equal(got, want) {
		t.Errorf("blocks in the order %v, want %v", got, want)
	}
}

func describe(r run) raw.Target {
	if r.target.kind == "postgres" {
		return raw.Target{Kind: "postgres", Variant: r.target.variant.String(), Lanes: r.lanes, Cores: "0-5"}
	}
	env := map[string]string{"LEDGER_CLUSTER": "1", "LEDGER_SNAPSHOT_EVERY": "4294967296", "LEDGER_SWEEP": "on", "LEDGER_MAX_CLIENTS": "128", "GOMEMLIMIT": "1500MiB"}
	for _, kv := range r.env() {
		name, given, _ := strings.Cut(kv, "=")
		env[name] = given
	}
	return raw.Target{Kind: "ledger", Nodes: r.target.nodes, Lanes: r.lanes, Env: env}
}

func TestPlanFileFitsTheReport(t *testing.T) {
	for _, smoke := range []bool{false, true} {
		dir := t.TempDir()
		if err := os.Mkdir(filepath.Join(dir, "raw"), 0o750); err != nil {
			t.Fatal(err)
		}
		b := &bench{shape: shape{smoke: smoke}, out: dir}
		b.items = b.schedule()
		if err := b.planFile(); err != nil {
			t.Fatal(err)
		}
		for _, r := range runsOf(b.items) {
			f := &raw.File{Head: raw.Head{Schema: raw.Schema, ID: r.id(), Measurement: r.m, Pass: r.pass, Build: raw.Build{Tags: r.tags()},
				Target: describe(r), Load: raw.Load{Workload: r.workload, Rate: r.rate, Need: 1}, Thresholds: control.Defaults()}}
			var buf bytes.Buffer
			if err := raw.Write(&buf, f); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, "raw", r.id()+".json"), buf.Bytes(), 0o600); err != nil {
				t.Fatal(err)
			}
		}
		verdicts, _ := report.Check(dir)
		found := false
		for _, v := range verdicts {
			if strings.Contains(strings.Join(v.Why, " "), report.WhyUnplanned) || strings.Contains(strings.Join(v.Why, " "), "no run in pass") {
				t.Errorf("smoke %v: %s: %v", smoke, v.Subject, v.Why)
			}
			if v.Subject == report.PlanFile {
				found = v.Passed
			}
		}
		if !found {
			t.Errorf("smoke %v: plan.json has no passing verdict", smoke)
		}
		p := expected(b.items)
		if n := len(p.Expect); n != 45 {
			t.Errorf("smoke %v: %d groups in plan.json, want 45", smoke, n)
		}
	}
}
