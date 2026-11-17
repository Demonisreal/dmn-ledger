package report

import (
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"

	"github.com/Demonisreal/dmn-ledger/bench/control"
	"github.com/Demonisreal/dmn-ledger/bench/raw"
)

type chain struct {
	stages []*group
}

func chains(gs []*group) []*chain {
	var out []*chain
	byKey := make(map[string]*chain)
	for _, g := range gs {
		if g.m != "m1" {
			continue
		}
		k := strings.Join([]string{g.target.Kind, strconv.Itoa(g.target.Nodes), g.target.Variant, strconv.Itoa(g.target.Lanes),
			env(g.target), strings.Join(g.tags, ","), g.workload}, "|")
		c := byKey[k]
		if c == nil {
			c = &chain{}
			byKey[k] = c
			out = append(out, c)
		}
		c.stages = append(c.stages, g)
	}
	for _, c := range out {
		slices.SortStableFunc(c.stages, func(a, b *group) int { return a.rate - b.rate })
	}
	return out
}

func (c *chain) first() *group {
	return c.stages[0]
}

func (c *chain) subject() string {
	g := c.first()
	return fmt.Sprintf("m1 %s, %d lanes, %s", name(g.target), g.target.Lanes, g.workload)
}

type overload struct {
	passes  int
	held    spread
	never   bool
	runs    []*run
	speed   control.Speed
	counted []*run
}

func (c *chain) overload() overload {
	byPass := make(map[int][]*run)
	for _, g := range c.stages {
		for _, r := range g.runs {
			if r.f.Verdict.Valid {
				byPass[r.f.Pass] = append(byPass[r.f.Pass], r)
			}
		}
	}
	var o overload
	var held []float64
	for _, p := range slices.Sorted(maps.Keys(byPass)) {
		runs := byPass[p]
		var first *run
		for _, r := range runs {
			if r.f.Verdict.Saturated && (first == nil || r.f.Load.Rate < first.f.Load.Rate) {
				first = r
			}
		}
		last := 0
		for _, r := range runs {
			if !r.f.Verdict.Saturated && (first == nil || r.f.Load.Rate < first.f.Load.Rate) {
				last = max(last, r.f.Load.Rate)
			}
		}
		held = append(held, float64(last))
		if first == nil {
			o.never = true
		} else {
			o.runs = append(o.runs, first)
		}
	}
	o.passes = len(byPass)
	o.held = median(held)
	o.speed = speedOf(o.runs)
	o.counted = countedAt(o.runs, o.speed)
	return o
}

func (o overload) atLeast() bool {
	return o.never || slices.ContainsFunc(o.runs, func(r *run) bool { return r.f.Verdict.Limit == raw.LimitGenerator })
}

func (o overload) throughput() spread {
	var xs []float64
	for _, r := range o.counted {
		xs = append(xs, bandOf(r.f, o.speed).Throughput)
	}
	return median(xs)
}

func (o overload) judge() (string, bool) {
	switch {
	case o.passes == 0:
		return "no valid run", false
	case o.never:
		return "at least: a pass held every stage", true
	}
	return judgeThroughput(o.runs)
}

func (o overload) limit() string {
	var limits []string
	for _, r := range o.runs {
		if l := r.f.Verdict.Limit; !slices.Contains(limits, l) {
			limits = append(limits, l)
		}
	}
	slices.Sort(limits)
	if o.never {
		limits = append(limits, "none reached")
	}
	return strings.Join(limits, ", ")
}

func short(t raw.Target) string {
	if t.Kind == "postgres" {
		switch strings.ToUpper(t.Variant) {
		case "V1":
			return "Postgres single, RC"
		case "V2":
			return "Postgres single, SER"
		case "V3":
			return "Postgres batched, RC"
		case "V4":
			return "Postgres batched, SER"
		}
	}
	return name(t)
}

func roleSyncs(f *raw.File, speed control.Speed) (leader, follower float64, ok bool) {
	b := bandOf(f, speed)
	last := -1
	for i, s := range f.Seconds {
		if counts(f, i, speed) && len(s.Parts) > 0 {
			last = i
		}
	}
	if last < 0 || len(b.PartSyncs) < 2 {
		return 0, 0, false
	}
	var followers int
	for _, p := range f.Seconds[last].Parts {
		switch p.Role {
		case "leader":
			leader += b.PartSyncs[p.Name]
		case "follower":
			follower += b.PartSyncs[p.Name]
			followers++
		}
	}
	if followers > 0 {
		follower /= float64(followers)
	}
	return leader, follower, true
}

func writeStages(b *strings.Builder, gs []*group, out map[string][]byte) {
	cs := chains(gs)
	b.WriteString("## Stages (m1)\n\n")
	b.WriteString("How many transfers per second does one node hold, how many a cluster of three nodes, and how does the wait grow " +
		"with the load? Each stage offers a fixed rate for a fixed window. A stage is held when at the end of its window fewer " +
		"arrivals are open than fall due in one second; otherwise it is saturated. The last held stage comes from the raster of " +
		"stages, the throughput under overload is measured in the first saturated stage of each pass. Where the load generator " +
		"and not the target was the limit, the table says \"at least\".\n\n")
	sum := table{head: []string{"target", "lanes", "load", "passes", "last held stage", "throughput under overload", "runs", "limit", "agreement"}}
	var all []*run
	var stageGroups []*group
	for _, c := range cs {
		o := c.overload()
		g := c.first()
		held := o.held.show(func(x float64) string { return grouped(x) })
		tp := o.throughput().show(grouped)
		switch {
		case o.never && len(o.counted) == 0:
			tp = "not reached"
			held = "at least " + held
		case o.atLeast():
			held, tp = "at least "+held, "at least "+tp
		}
		text, _ := o.judge()
		sum.add(name(g.target), strconv.Itoa(g.target.Lanes), g.workload, strconv.Itoa(o.passes), held, tp,
			fmt.Sprintf("%d of %d", len(o.counted), len(o.runs)), o.limit(), text)
		for _, s := range c.stages {
			all = append(all, s.runs...)
		}
		stageGroups = append(stageGroups, c.stages...)
	}
	sum.write(b)
	writeContext(b, all, stageGroups)

	drawn := false
	for _, lanes := range []int{32, 4} {
		var lines []line
		for _, c := range cs {
			if c.first().target.Lanes != lanes {
				continue
			}
			l := line{label: short(c.first().target)}
			for _, s := range c.stages {
				if len(s.counted) > 0 {
					l.points = append(l.points, point{x: float64(s.rate), y: s.spread(func(x fig) float64 { return x.p50 }).med / 1e6, fast: s.speed == control.Fast})
				}
			}
			lines = append(lines, l)
		}
		file := fmt.Sprintf("m1-p50-%d-lanes.svg", lanes)
		title := fmt.Sprintf("p50 latency over the offered rate, %d lanes", lanes)
		if svg := plot(title, "offered rate, transfers per second (log)", "p50 latency, ms (log)", true, lines); svg != nil {
			out[file], drawn = svg, true
			fmt.Fprintf(b, "![%s](%s)\n\n", title, file)
		}
	}
	var lines []line
	for _, c := range cs {
		t := c.first().target
		single := t.Kind == "postgres" && (strings.EqualFold(t.Variant, "V1") || strings.EqualFold(t.Variant, "V2"))
		if t.Lanes != 32 || t.Kind != "ledger" && !single {
			continue
		}
		l := line{label: short(t)}
		for _, s := range c.stages {
			if len(s.counted) > 0 {
				l.points = append(l.points, point{x: float64(s.rate), y: s.spread(func(x fig) float64 { return x.syncs }).med, fast: s.speed == control.Fast})
			}
		}
		lines = append(lines, l)
	}
	const syncsFile = "m1-fsync-32-lanes.svg"
	const syncsTitle = "fsync per transfer over the offered rate, 32 lanes, all nodes added up"
	if svg := plot(syncsTitle, "offered rate, transfers per second (log)", "fsync per transfer", false, lines); svg != nil {
		out[syncsFile], drawn = svg, true
		fmt.Fprintf(b, "![%s](%s)\n\n", syncsTitle, syncsFile)
	}
	if drawn {
		b.WriteString("The curves show the medians of the tables below; p99 is only in the tables.\n\n")
	}

	for _, c := range cs {
		g := c.first()
		fmt.Fprintf(b, "### %s, %d lanes, %s\n\n", name(g.target), g.target.Lanes, g.workload)
		t := table{head: []string{"rate", "held", "speed", "flush ms", "runs", "p50 ms", "p50 flushes", "p99 ms", "p99.9 ms",
			"throughput/s", "fsync per transfer", "transfers per request", "retries per transfer", "agreement"}}
		perNode := table{head: []string{"rate", "speed", "runs", "fsync per transfer, leader", "fsync per transfer, one follower"}}
		var runs []*run
		for _, s := range c.stages {
			row := fixedRow(s)
			t.add(append([]string{grouped(float64(s.rate))}, row[4:]...)...)
			runs = append(runs, s.runs...)
			var leaders, followers []float64
			for _, r := range s.counted {
				if l, f, ok := roleSyncs(r.f, s.speed); ok {
					leaders, followers = append(leaders, l), append(followers, f)
				}
			}
			if len(leaders) > 0 {
				perNode.add(grouped(float64(s.rate)), row[5], strconv.Itoa(len(leaders)), median(leaders).show(fixed(3)), median(followers).show(fixed(3)))
			}
		}
		t.write(b)
		if len(perNode.rows) > 0 {
			b.WriteString("Each node on its own:\n\n")
			perNode.write(b)
		}
		writeContext(b, runs, c.stages)
	}
}
