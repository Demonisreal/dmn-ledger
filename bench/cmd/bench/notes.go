package main

import (
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/Demonisreal/dmn-ledger/bench/control"
	"github.com/Demonisreal/dmn-ledger/bench/load"
	"github.com/Demonisreal/dmn-ledger/bench/raw"
)

func summary(f *raw.File) string {
	v := f.Verdict
	state := "valid"
	if !v.Valid {
		state = "invalid: " + strings.Join(v.Reasons, "; ")
	}
	parts := []string{state, "speed " + v.Speed.String()}
	if v.Saturated {
		parts = append(parts, "saturated, limit "+v.Limit)
	}
	in := 0
	if v.Speed.Rank() > 0 {
		in = bandOf(f).Seconds
	}
	counted := fmt.Sprintf("%d of %d window seconds counted", in, in+v.Excluded)
	if v.Throttled > 0 {
		counted += fmt.Sprintf(", thermal limit in %d", v.Throttled)
	}
	if left := f.LeftOut(); left != "" {
		counted += " (left out: " + left + ")"
	}
	parts = append(parts, counted)
	if rss := highest(f, "peak_rss"); rss > 0 {
		parts = append(parts, fmt.Sprintf("peak rss of a node %d MiB", rss>>20))
	}
	if f.Target.Kind == "postgres" {
		var shed uint64
		for _, s := range f.Seconds {
			if s.Phase == load.Window.String() {
				shed += s.Shed
			}
		}
		parts = append(parts, fmt.Sprintf("deadlocks in the window %d, transfers given up in the window %d", grew(f, "deadlocks"), shed))
	}
	if f.Measurement == "m4" {
		changed := "no"
		if f.TermChanged() {
			changed = "yes"
		}
		parts = append(parts, "new term around the window: "+changed)
		if size := highest(f, "snapshot_bytes"); size > 0 {
			parts = append(parts, fmt.Sprintf("largest snapshot file %d MiB", size>>20))
		}
		if b := f.Bounce; b != nil {
			back := "caught up from the log"
			if b.Snapshot > 0 {
				back = fmt.Sprintf("got a snapshot of %d MiB", b.Snapshot>>20)
			}
			parts = append(parts, fmt.Sprintf("stopped %s, a %s; after its return it %s, longest round %d ms", b.Node, b.Role, back, b.Slowest.Milliseconds()))
		}
	}
	if len(v.Problems) > 0 {
		parts = append(parts, "problems: "+strings.Join(v.Problems, "; "))
	}
	return strings.Join(parts, "; ")
}

func highest(f *raw.File, counter string) uint64 {
	var most uint64
	for _, s := range f.Seconds {
		for _, p := range s.Parts {
			most = max(most, p.Counters[counter])
		}
	}
	return most
}

func (b *bench) planText() []byte {
	var s strings.Builder
	tree := "clean tree"
	if b.build.Dirty {
		tree = "tree with changes (dirty)"
	}
	kind := "full plan"
	if b.smoke {
		kind = "smoke run, short windows, no verdict"
	}
	fmt.Fprintf(&s, "Runs at commit %s, %s, %s.\n", b.build.Commit, tree, kind)
	if b.only != nil {
		fmt.Fprintf(&s, "Only runs matching %q were selected.\n", b.only.String())
	}
	s.WriteString("Every run is listed in the order it ran, with the reasons when it is not valid. Invalid runs stay in raw/.\n" +
		"m6 is not part of this plan: there is no load for proofs, and the benchmarks of the chain do not reach RESULTS.md.\n")
	block := ""
	for _, l := range b.lines {
		if l.block != block {
			block = l.block
			fmt.Fprintf(&s, "\n%s\n", block)
		}
		fmt.Fprintf(&s, "  %s  %s\n", l.id, l.text)
	}
	if len(b.blocks) > 0 {
		s.WriteString("\nInvalid runs per measurement at the end, the last attempt of each run; bench check judges the plan:\n")
		for _, l := range b.blocks {
			fmt.Fprintf(&s, "  %s\n", l)
		}
	}
	if b.stopped != "" {
		fmt.Fprintf(&s, "\n%s\n", b.stopped)
	}
	return []byte(s.String())
}

func (b *bench) writePlanText() error {
	return writeFile(filepath.Join(b.out, "plan.txt"), b.planText())
}

func (b *bench) machineText() ([]byte, error) {
	m := b.machine
	var s strings.Builder
	dirty := "no"
	if b.build.Dirty {
		dirty = "yes"
	}
	fmt.Fprintf(&s, "commit %s, tree with changes: %s, %s\n", b.build.Commit, dirty, b.build.Go)
	fmt.Fprintf(&s, "cpu: %s, %d threads; the plan ran on cpus %s with GOMAXPROCS %d, the targets on cpus 0 to 5\n", m.CPU, m.Threads, m.Allowed, m.GoMaxProcs)
	fmt.Fprintf(&s, "ram: %d MiB\n", m.RAM>>20)
	fmt.Fprintf(&s, "kernel: %s\n", m.Kernel)
	fmt.Fprintf(&s, "governor: %s, turbo: %s, power: %s\n", m.Governor, m.Turbo, m.Power)
	fmt.Fprintf(&s, "file system: %s (%s) on %s (%s), %.0f%% used\n", m.Filesystem, m.Mount, m.Disk, m.Model, 100*m.Used)
	fmt.Fprintf(&s, "disk speed: fast below %v, slow above %v, no verdict under %d flushes a second\n", b.th.Fast, b.th.Slow, b.th.MinFlushes)
	probes, err := filepath.Glob(filepath.Join(b.out, "probe-*.json"))
	if err != nil {
		return nil, err
	}
	for _, name := range probes {
		var p control.Probe
		if err := readJSON(name, &p); err != nil {
			return nil, err
		}
		fmt.Fprintf(&s, "\ndisk probe %s, 137 bytes written and synced in a loop, means per disk speed:\n", filepath.Base(name))
		for _, mean := range p.Means {
			if mean.GrowingSeconds == 0 && mean.PreallocSeconds == 0 {
				fmt.Fprintf(&s, "  %s: no second at this speed\n", mean.Speed)
				continue
			}
			fmt.Fprintf(&s, "  %s: fsync on a growing file %v over %d s, fdatasync on a preallocated file %v over %d s, ratio %.2f\n",
				mean.Speed, mean.Growing.Round(time.Microsecond), mean.GrowingSeconds, mean.Prealloc.Round(time.Microsecond), mean.PreallocSeconds, mean.Ratio)
		}
	}
	heats, err := filepath.Glob(filepath.Join(b.out, "heat-*.json"))
	if err != nil {
		return nil, err
	}
	if len(heats) > 0 {
		s.WriteString("\nheat at the start and the end of each session of the plan:\n")
	}
	for _, name := range heats {
		var h control.Heat
		if err := readJSON(name, &h); err != nil {
			return nil, err
		}
		fmt.Fprintf(&s, "  heat %s: energy_performance_preference %s, platform_profile %s, intel_pstate max_perf_pct %s, x86_pkg_temp %.1f °C",
			filepath.Base(name), h.Preference, h.Profile, h.MaxPerf, h.PackageTemp)
		for _, p := range h.Packages {
			fmt.Fprintf(&s, "; package %s: package_throttle_count %d, package_throttle_total_time_ms %d", p.ID, p.Throttles, p.ThrottledMs)
		}
		s.WriteString("\n")
	}
	tests, err := filepath.Glob(filepath.Join(b.out, "selftest-*.json"))
	if err != nil {
		return nil, err
	}
	fsyncs, err := filepath.Glob(filepath.Join(b.out, "pg-test-fsync-*.txt"))
	if err != nil {
		return nil, err
	}
	for _, name := range fsyncs {
		text, err := os.ReadFile(name) //nolint:gosec // G304: a file the plan wrote into its own folder
		if err != nil {
			return nil, err
		}
		fmt.Fprintf(&s, "\npg_test_fsync -s 1 on the data directory of the container, %s, after the disk probe above:\n", filepath.Base(name))
		for line := range strings.Lines(string(text)) {
			fmt.Fprintf(&s, "  %s", line)
		}
	}
	for _, name := range tests {
		var st load.Selftest
		if err := readJSON(name, &st); err != nil {
			return nil, err
		}
		fmt.Fprintf(&s, "\nselftest %s: null target at %d/s p50 %v, p99 %v, dispatcher late p99 %v; a one second stall at %d/s: %d due, %d open, p50 %v, p99 %v\n",
			filepath.Base(name), load.FloorRate, st.FloorP50, st.FloorP99, st.FloorLate, load.StallRate, st.StallDue, st.StallOpen, st.StallP50, st.StallP99)
	}
	return []byte(s.String()), nil
}

func readJSON(name string, v any) error {
	b, err := os.ReadFile(name) //nolint:gosec // G304: a file the plan wrote into its own folder
	if err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}

func postgresText(t raw.Target) []byte {
	var s strings.Builder
	fmt.Fprintf(&s, "%s\n\nSettings that differ from the defaults (pg_settings where source is not default):\n", t.Version)
	for _, name := range slices.Sorted(maps.Keys(t.Settings)) {
		fmt.Fprintf(&s, "  %s = %s\n", name, t.Settings[name])
	}
	return []byte(s.String())
}
