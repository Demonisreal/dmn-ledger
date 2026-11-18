package report

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/Demonisreal/dmn-ledger/bench/control"
	"github.com/Demonisreal/dmn-ledger/bench/load"
	"github.com/Demonisreal/dmn-ledger/bench/raw"
)

var stamp = time.Date(2000, 1, 1, 20, 0, 0, 0, time.UTC)

const (
	slow = 1400 * time.Microsecond
	fast = 220 * time.Microsecond
)

type plan struct {
	m, kind, variant, workload string
	nr, pass, nodes, lanes     int
	rate                       int
	env                        map[string]string
	tags                       []string
	flush                      []time.Duration
	p50                        time.Duration
	booked, syncs, retries     uint64
	slowest                    uint64
	saturated, late, reserve   bool
	throttled, foreign         []int
	few, shed                  uint64
	cpu                        float64
	lulls                      []int
	snapshot                   uint64
}

func (p plan) target() string {
	if p.kind == "ledger" {
		return fmt.Sprintf("l%d", p.nodes)
	}
	return p.variant
}

func (p plan) id() string {
	return fmt.Sprintf("%s-%s-%s-%d-%d", p.m, p.target(), p.workload, p.rate, p.nr)
}

func window(speeds ...time.Duration) []time.Duration {
	return append([]time.Duration{slow}, speeds...)
}

func (p plan) parts() []string {
	switch {
	case p.kind == "postgres":
		return []string{"postgres"}
	case p.nodes == 3:
		return []string{"n1", "n2", "n3"}
	}
	return []string{"n1"}
}

func (p plan) file(t testing.TB) *raw.File {
	run := &load.Run{Started: stamp.Add(time.Duration(p.nr+10*p.pass) * time.Minute), Saturated: p.saturated, Horizon: 6 * time.Second}
	cum := make(map[string]*load.Part)
	snapshot := func() load.Inside {
		var in load.Inside
		for _, n := range p.parts() {
			c := *cum[n]
			if c.Counters != nil {
				c.Counters = make(map[string]uint64)
				for k, v := range cum[n].Counters {
					c.Counters[k] = v
				}
			}
			in = append(in, load.Part(c))
		}
		return in
	}
	for i, n := range p.parts() {
		role := "leader"
		if i > 0 {
			role = "follower"
		}
		part := &load.Part{Name: n, Role: role, Term: 2}
		if p.kind == "ledger" {
			part.Counters = map[string]uint64{}
			for _, k := range []string{"up", "rounds", "wait", "process", "slowest", "logsync", "logsync_ns", "metasync", "metasync_ns", "othersync", "othersync_ns", "written"} {
				part.Counters[k] = 0
			}
		}
		if p.kind == "postgres" {
			part.Role = ""
		}
		cum[n] = part
	}
	run.Before = snapshot()
	var ctl []control.Second
	for i, mean := range p.flush {
		phase := load.Window
		if i == 0 {
			phase = load.Warmup
		}
		sec := load.Second{Phase: phase, Due: uint64(p.rate), Booked: p.booked, Retries: p.retries, Shed: p.shed, Latency: new(load.Histogram), Late: new(load.Histogram), Sampled: true}
		if p.reserve {
			sec.Reserve = new(load.Histogram)
		}
		for range p.booked / 10 {
			for _, f := range []float64{0.8, 0.8, 0.9, 1, 1, 1, 1.1, 1.3, 1.6, 3} {
				sec.Latency.Add(time.Duration(f * float64(p.p50)))
				if sec.Reserve != nil {
					sec.Reserve.Add(time.Duration(f * float64(p.p50) / 2))
				}
			}
		}
		late := 20 * time.Microsecond
		if p.late {
			late = 3 * time.Millisecond
		}
		for range 100 {
			sec.Late.Add(late)
		}
		for j, n := range p.parts() {
			c := cum[n]
			syncs := p.syncs
			if j > 0 {
				syncs = p.syncs * 3 / 2
			}
			c.Syncs += syncs
			c.SyncTime += time.Duration(syncs) * mean * 2
			c.Requests += p.booked / 4
			if c.Counters != nil {
				c.Counters["up"] += uint64(time.Second)
				c.Counters["rounds"] += syncs / 2
				c.Counters["wait"] += uint64(200 * time.Millisecond)
				c.Counters["process"] += uint64(300*time.Millisecond) + uint64(syncs)*uint64(2*mean)
				c.Counters["slowest"] = uint64(4 * time.Millisecond)
				if i == 2 && p.slowest > 0 {
					c.Counters["slowest"] = p.slowest
				}
				if i >= 2 && p.snapshot > 0 {
					c.Counters["snapshot_bytes"] = p.snapshot
				}
				c.Counters["logsync"] += syncs / 2
				c.Counters["logsync_ns"] += syncs / 2 * uint64(3*mean)
				c.Counters["metasync"] += syncs / 2
				c.Counters["metasync_ns"] += syncs / 2 * uint64(mean)
				c.Counters["written"] += p.booked * 130
			}
		}
		sec.Inside = snapshot()
		run.Seconds = append(run.Seconds, sec)
		flushes, target := uint64(300), 0.2
		if p.few > 0 {
			flushes, target = p.few, p.cpu
		}
		if slices.Contains(p.lulls, i) {
			flushes, target = 5, p.cpu
		}
		c := control.Second{Span: time.Second, Flushes: flushes, FlushTime: time.Duration(flushes) * mean, Writes: 900, WriteTime: 900 * 50 * time.Microsecond,
			Generator: 0.5, Target: target, Foreign: 0.2, MHz: []int{2500, 2500}, Available: 7 << 30}
		if slices.Contains(p.throttled, i) {
			c.Throttled = 600
		}
		if slices.Contains(p.foreign, i) {
			c.Foreign = 1.5
		}
		ctl = append(ctl, c)
	}
	tags := p.tags
	if tags == nil {
		tags = []string{}
	}
	accounts := 10_000
	if p.workload == "uniform-1m" {
		accounts = 1_000_000
	}
	h := raw.Head{
		ID: p.id(), Measurement: p.m, Pass: p.pass,
		Build: raw.Build{Commit: "testdata", Tags: tags, Go: "go1.27.4"},
		Machine: control.Machine{CPU: "test CPU", Threads: 16, Allowed: "8-15", GoMaxProcs: 8, RAM: 16 << 30, Kernel: "test kernel",
			Governor: "powersave", Turbo: "on", Power: "mains", Filesystem: "ext4", Mount: "rw,relatime", Disk: "nvme0n1", Model: "test disk", Used: 0.5},
		Target: raw.Target{Kind: p.kind, Nodes: p.nodes, Variant: p.variant, Lanes: p.lanes, Cores: "0,1", Env: p.env},
		Load: raw.Load{Workload: p.workload, Accounts: accounts, Rate: p.rate, Warmup: time.Second, Window: 3 * time.Second,
			MaxWindow: 6 * time.Second, Drain: time.Second, Need: 2, Seed: 7},
		Probe: control.Probe{Means: []control.ProbeMean{{Speed: control.Slow, GrowingSeconds: 5, Growing: 2 * slow, PreallocSeconds: 5, Prealloc: slow, Ratio: 2},
			{Speed: control.Fast}}},
		Thresholds: control.Defaults(),
	}
	f, err := raw.Assemble(h, run, ctl)
	if err != nil {
		t.Fatal(err)
	}
	f.Verified(load.Tally{Found: 10}, nil)
	return f
}

func ledger(m string, nodes, lanes int, workload string, rate int) plan {
	return plan{m: m, kind: "ledger", nodes: nodes, lanes: lanes, workload: workload, rate: rate, nr: 1, pass: 1,
		flush: window(slow, slow, slow), p50: 3 * time.Millisecond, booked: uint64(rate), syncs: 100}
}

func postgres(m, variant string, lanes int, rate int) plan {
	return plan{m: m, kind: "postgres", variant: variant, lanes: lanes, workload: "uniform-10k", rate: rate, nr: 1, pass: 1,
		flush: window(slow, slow, slow), p50: 1200 * time.Microsecond, booked: uint64(rate), syncs: 200}
}

func (p plan) with(edit func(*plan)) plan {
	edit(&p)
	return p
}

func testPlan() []plan {
	var plans []plan
	for _, pass := range []int{1, 2} {
		for i, rate := range []int{1000, 2000, 4000, 8000} {
			p := ledger("m1", 1, 32, "uniform-10k", rate).with(func(p *plan) {
				p.pass, p.nr = pass, pass
				p.p50 = time.Duration(30+i+pass) * 100 * time.Microsecond
			})
			if rate == 8000 {
				p.saturated, p.booked, p.p50 = true, uint64(5000+200*(pass-1)), 400*time.Millisecond
			}
			if pass == 2 && rate == 4000 {
				plans = append(plans, p.with(func(p *plan) { p.flush = window(fast, slow, fast) }))
				p.nr = 3
			}
			plans = append(plans, p)
		}
		for _, rate := range []int{1000, 2000} {
			p := ledger("m1", 3, 4, "uniform-10k", rate).with(func(p *plan) {
				p.pass, p.nr, p.p50 = pass, pass, 6*time.Millisecond
			})
			if rate == 2000 {
				p.saturated, p.late, p.booked = true, true, 1500
			}
			plans = append(plans, p)
		}
	}
	plans = append(plans,
		postgres("m1", "V1", 32, 1000).with(func(p *plan) { p.flush, p.p50 = window(fast, fast, fast), 400*time.Microsecond }),
		postgres("m1", "V1", 32, 2000),
		postgres("m1", "V1", 32, 4000).with(func(p *plan) { p.saturated, p.booked, p.p50 = true, 3100, 900*time.Millisecond }),
		postgres("m1", "V3", 4, 1000),
		postgres("m1", "V3", 4, 2000).with(func(p *plan) { p.saturated, p.booked, p.p50, p.few, p.cpu = true, 1800, 300*time.Millisecond, 9, 1.0 }),
		postgres("m1", "V3", 4, 1000).with(func(p *plan) { p.pass, p.nr = 2, 2 }),
		postgres("m1", "V3", 4, 2000).with(func(p *plan) {
			p.pass, p.nr, p.saturated, p.booked, p.p50, p.few, p.cpu, p.flush = 2, 2, true, 1900, 320*time.Millisecond, 11, 1.0, window(20*time.Millisecond, 2*time.Millisecond, fast)
		}),

		ledger("m2", 1, 32, "uniform-10k", 2000),
		ledger("m2", 1, 32, "uniform-10k", 2000).with(func(p *plan) { p.pass, p.nr, p.p50 = 2, 2, 3100*time.Microsecond }),
		ledger("m2", 1, 32, "uniform-10k", 2000).with(func(p *plan) { p.pass, p.nr, p.p50, p.flush = 2, 3, 900*time.Microsecond, window(fast, fast, fast) }),
		ledger("m2", 1, 32, "uniform-10k", 2000).with(func(p *plan) { p.pass, p.nr, p.flush = 2, 4, window(fast, slow, fast) }),
		ledger("m2", 3, 32, "uniform-10k", 2000).with(func(p *plan) { p.p50, p.foreign, p.throttled, p.retries = 6*time.Millisecond, []int{2}, []int{1}, 3 }),
		ledger("m2", 3, 32, "uniform-10k", 2000).with(func(p *plan) { p.pass, p.nr, p.p50 = 2, 2, 8500*time.Microsecond }),
		ledger("m2", 3, 32, "twophase", 2000).with(func(p *plan) { p.p50, p.reserve = 9*time.Millisecond, true }),
		ledger("m2", 3, 32, "twophase", 2000).with(func(p *plan) { p.pass, p.nr, p.p50, p.reserve = 2, 2, 9400*time.Microsecond, true }),
		postgres("m2", "V1", 32, 2000).with(func(p *plan) { p.retries = 2 }),
		postgres("m2", "V1", 32, 2000).with(func(p *plan) { p.pass, p.nr, p.retries = 2, 2, 2 }),
		postgres("m2", "V3", 4, 20000).with(func(p *plan) { p.saturated, p.booked, p.p50 = true, 14000, 700*time.Millisecond }),
		postgres("m2", "V3", 4, 20000).with(func(p *plan) { p.pass, p.nr, p.saturated, p.booked, p.p50 = 2, 2, true, 14600, 720*time.Millisecond }),

		ledger("m3", 1, 32, "uniform-10k", 2000).with(func(p *plan) { p.tags = []string{"pprof"} }),
		ledger("m4", 1, 32, "uniform-10k", 2000).with(func(p *plan) {
			p.env, p.slowest, p.snapshot = map[string]string{"LEDGER_SNAPSHOT_EVERY": "20000"}, uint64(420*time.Millisecond), 600_000_000
		}),
		ledger("m7", 3, 32, "uniform-10k", 2000).with(func(p *plan) { p.p50 = 7 * time.Millisecond }),
	)
	for nr := 1; nr <= 4; nr++ {
		p := ledger("m5", 1, 32, "uniform-10k", 2000).with(func(p *plan) { p.nr, p.tags = nr, []string{"simbug"} })
		if nr%2 == 0 {
			p.env, p.p50, p.syncs = map[string]string{"LEDGER_BUG": "B13"}, 1800*time.Microsecond, 50
		}
		plans = append(plans, p)
	}
	return plans
}
