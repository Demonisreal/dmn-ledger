package main

import (
	"fmt"
	"hash/fnv"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/Demonisreal/dmn-ledger/bench/pg"
	"github.com/Demonisreal/dmn-ledger/bench/report"
)

const (
	r32, r32Lanes = 2_000, 32
	r4, r4Lanes   = 20_000, 4
	topStage      = 512_000
	snapshotOne   = "LEDGER_SNAPSHOT_EVERY=20000"
	snapshotThree = "LEDGER_SNAPSHOT_EVERY=7000"
	bug           = "LEDGER_BUG=B13"
	stoppedNode   = 3
)

type target struct {
	kind    string
	nodes   int
	variant pg.Variant
}

var (
	l1 = target{kind: "ledger", nodes: 1}
	l3 = target{kind: "ledger", nodes: 3}
	v1 = target{kind: "postgres", variant: pg.V1}
	v2 = target{kind: "postgres", variant: pg.V2}
	v3 = target{kind: "postgres", variant: pg.V3}
	v4 = target{kind: "postgres", variant: pg.V4}
)

func (t target) label() string {
	if t.kind == "ledger" {
		return fmt.Sprintf("l%d", t.nodes)
	}
	return strings.ToLower(t.variant.String())
}

type timing struct {
	warm, window, longest, drain time.Duration
	need                         int
	stopAt, restartAt            time.Duration
}

type run struct {
	m        string
	letter   string
	pass     int
	nr       int
	repeat   int
	target   target
	lanes    int
	workload string
	rate     int
	stage    int
	stages   []int
	build    string
	bug      bool
	timing   timing
	fill     uint64
	stop     bool
	bounce   bool
}

func (r run) id() string {
	who := fmt.Sprintf("%sx%d", r.target.label(), r.lanes)
	if r.bug {
		who += "-b13"
	}
	id := fmt.Sprintf("%s%s-%s-%s-%d-%d", r.m, r.letter, who, r.workload, r.rate, r.nr)
	if r.repeat > 0 {
		id += fmt.Sprintf("r%d", r.repeat)
	}
	return id
}

func (r run) env() []string {
	var env []string
	if r.m == "m4" {
		every := snapshotOne
		if r.target.nodes == 3 {
			every = snapshotThree
		}
		env = append(env, every)
	}
	if r.bug {
		env = append(env, bug)
	}
	return env
}

func (r run) tags() []string {
	if r.build == "" {
		return nil
	}
	return []string{r.build}
}

func (r run) excused() bool {
	return r.m == "m4" || r.m == "m7"
}

func (r run) seed() uint64 {
	h := fnv.New64a()
	fmt.Fprintf(h, "%s %s %d", r.m, r.workload, r.chainRate())
	return h.Sum64()
}

func (r run) chainRate() int {
	if r.m == "m1" {
		return 0
	}
	return r.rate
}

type item struct {
	runs []run
}

func (it item) chain() bool {
	return len(it.runs) > 1
}

type shape struct {
	smoke bool
}

func (s shape) passes() int {
	if s.smoke {
		return 1
	}
	return 2
}

func (s shape) pause() time.Duration {
	if s.smoke {
		return time.Second
	}
	return 10 * time.Second
}

func (s shape) probe() time.Duration {
	if s.smoke {
		return 2 * time.Second
	}
	return 10 * time.Second
}

func (s shape) fixed() timing {
	if s.smoke {
		return s.short()
	}
	return timing{warm: 10 * time.Second, window: 30 * time.Second, longest: 60 * time.Second, drain: 5 * time.Second, need: 30}
}

func (s shape) stage() timing {
	if s.smoke {
		return timing{warm: 2 * time.Second, window: 6 * time.Second, longest: 6 * time.Second, drain: 3 * time.Second, need: 3}
	}
	return timing{warm: 5 * time.Second, window: 15 * time.Second, longest: 30 * time.Second, drain: 5 * time.Second, need: 8}
}

func (s shape) snapshot(bounce bool) timing {
	switch {
	case s.smoke && bounce:
		t := s.short()
		t.restartAt = t.warm + 2*time.Second
		t.stopAt = t.warm
		return t
	case s.smoke:
		return s.short()
	case bounce:
		return timing{warm: 5 * time.Second, window: 60 * time.Second, longest: 60 * time.Second, drain: 5 * time.Second, need: 30,
			stopAt: 8 * time.Second, restartAt: 50 * time.Second}
	}
	return timing{warm: 5 * time.Second, window: 35 * time.Second, longest: 35 * time.Second, drain: 5 * time.Second, need: 17}
}

func (s shape) short() timing {
	return timing{warm: time.Second, window: 3 * time.Second, longest: 3 * time.Second, drain: 2 * time.Second, need: 2}
}

func (s shape) stages() []int {
	var rates []int
	for rate := 1_000; rate <= topStage; rate *= 2 {
		rates = append(rates, rate)
	}
	return rates
}

func (s shape) fill(n uint64) uint64 {
	if s.smoke {
		return n / 10
	}
	return n
}

func (s shape) schedule() []item {
	var out []item
	for pass := 1; pass <= s.passes(); pass++ {
		out = append(out, s.stagings(pass)...)
		out = append(out, s.fixedLoads(pass)...)
		if pass == 1 {
			out = append(out, s.seals()...)
			out = append(out, s.profiles()...)
			out = append(out, s.snapshots(1)...)
			out = append(out, s.missing(1)...)
		}
	}
	if s.passes() > 1 {
		out = append(out, s.snapshots(2)...)
		out = append(out, s.missing(2)...)
	}
	return out
}

func swapped[T any](pass int, group []T) []T {
	if pass%2 == 0 {
		group = slices.Clone(group)
		slices.Reverse(group)
	}
	return group
}

func (s shape) stagings(pass int) []item {
	type chain struct {
		t     target
		lanes int
	}
	pairs := [][]chain{
		{{l1, r32Lanes}, {v1, r32Lanes}},
		{{l1, r4Lanes}, {v3, r4Lanes}},
		{{l3, r32Lanes}, {v2, r32Lanes}},
		{{l3, r4Lanes}, {v4, r4Lanes}},
	}
	rates := s.stages()
	var out []item
	for _, p := range pairs {
		for _, c := range swapped(pass, p) {
			var it item
			for i, rate := range rates {
				it.runs = append(it.runs, run{m: "m1", pass: pass, nr: pass, target: c.t, lanes: c.lanes, workload: "uniform-10k",
					rate: rate, stage: i + 1, stages: rates, timing: s.stage()})
			}
			out = append(out, it)
		}
	}
	return out
}

func (s shape) fixedLoads(pass int) []item {
	type cell struct {
		workload string
		rate     int
		targets  []target
	}
	cells := []cell{
		{"uniform-10k", r32, []target{l1, v1, l3, v2}},
		{"uniform-10k", r4, []target{l1, v3, l3, v4}},
		{"uniform-1m", r32, []target{l3, v1, v2}},
		{"uniform-1m", r4, []target{l3, v3, v4}},
		{"hot", r32, []target{l3, v1, v2}},
		{"hot", r4, []target{l3, v3, v4}},
		{"twophase", r32, []target{l3, v1}},
		{"twophase", r4, []target{l3, v3}},
	}
	var out []item
	for _, c := range cells {
		lanes := r32Lanes
		if c.rate == r4 {
			lanes = r4Lanes
		}
		for _, t := range swapped(pass, c.targets) {
			out = append(out, item{runs: []run{{m: "m2", pass: pass, nr: pass, target: t, lanes: lanes, workload: c.workload, rate: c.rate, timing: s.fixed()}}})
		}
	}
	return out
}

func (s shape) seals() []item {
	var out []item
	for _, t := range []target{l1, l3} {
		for i, b13 := range []bool{false, true, false, true} {
			out = append(out, item{runs: []run{{m: "m5", pass: 1, nr: 1 + i/2, target: t, lanes: r32Lanes, workload: "uniform-10k", rate: r32,
				build: "simbug", bug: b13, timing: s.fixed()}}})
		}
	}
	return out
}

func (s shape) profiles() []item {
	var out []item
	for _, r := range []run{
		{target: l1, lanes: r32Lanes, rate: r32},
		{target: l3, lanes: r32Lanes, rate: r32},
		{target: l1, lanes: r4Lanes, rate: 120_000},
		{target: l3, lanes: r4Lanes, rate: 50_000},
	} {
		r.m, r.pass, r.nr, r.workload, r.build, r.timing = "m3", 1, 1, "uniform-10k", "pprof", s.fixed()
		out = append(out, item{runs: []run{r}})
	}
	return out
}

func (s shape) snapshots(pass int) []item {
	var out []item
	for _, r := range []run{
		{letter: "a", target: l1, workload: "uniform-10k", fill: s.fill(190_000)},
		{letter: "b", target: l1, workload: "uniform-1m", fill: s.fill(1_000_000)},
		{letter: "c", target: l1, workload: "uniform-1m", fill: s.fill(4_000_000)},
		{letter: "d", target: l3, workload: "uniform-1m", fill: s.fill(1_000_000)},
		{letter: "e", target: l3, workload: "uniform-1m", fill: s.fill(1_000_000), bounce: true},
	} {
		r.m, r.pass, r.nr, r.lanes, r.rate, r.timing = "m4", pass, pass, r32Lanes, r32, s.snapshot(r.bounce)
		out = append(out, item{runs: []run{r}})
	}
	return out
}

func (s shape) missing(pass int) []item {
	var out []item
	for _, rl := range [][2]int{{r32, r32Lanes}, {r4, r4Lanes}} {
		out = append(out, item{runs: []run{{m: "m7", pass: pass, nr: pass, target: l3, lanes: rl[1], workload: "uniform-10k", rate: rl[0],
			stop: true, timing: s.fixed()}}})
	}
	return out
}

func expected(items []item) report.Planned {
	byKey := make(map[string]*report.Expect)
	var keys []string
	for _, it := range items {
		r := it.runs[0]
		e := report.Expect{Measurement: r.m, Kind: r.target.kind, Nodes: r.target.nodes, Lanes: r.lanes, Workload: r.workload,
			Rate: r.chainRate(), Passes: r.pass, Tags: r.tags()}
		if r.target.kind == "postgres" {
			e.Variant = r.target.variant.String()
		}
		for _, kv := range r.env() {
			name, given, _ := strings.Cut(kv, "=")
			if e.Env == nil {
				e.Env = map[string]string{}
			}
			e.Env[name] = given
		}
		k := fmt.Sprintf("%s|%s|%d|%s|%d|%s|%d|%v|%v", e.Measurement, e.Kind, e.Nodes, e.Variant, e.Lanes, e.Workload, e.Rate, e.Tags,
			slices.Sorted(maps.Keys(e.Env)))
		if have := byKey[k]; have != nil {
			have.Passes = max(have.Passes, e.Passes)
			continue
		}
		byKey[k] = &e
		keys = append(keys, k)
	}
	var p report.Planned
	for _, k := range keys {
		p.Expect = append(p.Expect, *byKey[k])
	}
	return p
}
