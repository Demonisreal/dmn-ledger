package report

import (
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

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
	passes    int
	valid     bool
	held      spread
	heldSpeed control.Speed
	never     []string
	open      []int
	runs      []*run
	speed     control.Speed
	counted   []*run
}

func (c *chain) overload() overload {
	byPass := make(map[int][]*run)
	for _, g := range c.stages {
		for _, r := range g.runs {
			byPass[r.f.Pass] = append(byPass[r.f.Pass], r)
		}
	}
	var o overload
	var lasts []map[control.Speed]int
	for _, p := range slices.Sorted(maps.Keys(byPass)) {
		runs := byPass[p]
		sortRuns(runs)
		lowest := 0
		for _, r := range runs {
			if r.f.Verdict.Saturated && (lowest == 0 || r.f.Load.Rate < lowest) {
				lowest = r.f.Load.Rate
			}
		}
		var first *run
		last, at := 0, make(map[control.Speed]int)
		for _, r := range runs {
			switch v := r.f.Verdict; {
			case !v.Valid:
			case lowest > 0 && r.f.Load.Rate == lowest:
				first = r
			case !v.Saturated && (lowest == 0 || r.f.Load.Rate < lowest):
				last = max(last, r.f.Load.Rate)
				at[v.Speed] = max(at[v.Speed], r.f.Load.Rate)
			}
			o.valid = o.valid || r.f.Verdict.Valid
		}
		switch {
		case lowest == 0:
			o.never = append(o.never, beyond(runs, last))
		case first == nil || !first.f.Verdict.Saturated:
			o.open = append(o.open, p)
			if first != nil {
				at[first.f.Verdict.Speed] = lowest
			}
		default:
			o.runs = append(o.runs, first)
		}
		lasts = append(lasts, at)
	}
	o.passes = len(byPass)
	o.speed = speedOf(o.runs)
	o.counted = countedAt(o.runs, o.speed)
	o.heldSpeed = o.speed
	if !slices.ContainsFunc(lasts, func(at map[control.Speed]int) bool { return at[o.speed] > 0 }) {
		o.heldSpeed = control.None
		for _, at := range lasts {
			for speed := range at {
				if speed.Rank() > o.heldSpeed.Rank() {
					o.heldSpeed = speed
				}
			}
		}
	}
	var held []float64
	for _, at := range lasts {
		if at[o.heldSpeed] > 0 {
			held = append(held, float64(at[o.heldSpeed]))
		}
	}
	o.held = median(held)
	return o
}

func (o overload) heldElsewhere() bool {
	return o.held.n > 0 && o.heldSpeed != o.speed && (o.speed != control.None || o.heldSpeed != control.Slow)
}

func beyond(runs []*run, last int) string {
	if top := slices.MaxFunc(runs, func(a, b *run) int { return a.f.Load.Rate - b.f.Load.Rate }); top.f.Load.BudgetEnds {
		return "budget"
	}
	next := 0
	for _, r := range runs {
		if r.f.Load.Rate > last && (next == 0 || r.f.Load.Rate < next) {
			next = r.f.Load.Rate
		}
	}
	if slices.ContainsFunc(runs, func(r *run) bool {
		return r.f.Load.Rate == next && slices.Contains(r.f.Verdict.Reasons, raw.WhyGenerator)
	}) {
		return raw.LimitGenerator
	}
	return "none reached"
}

func (o overload) atLeast() bool {
	return len(o.never) > 0 || slices.ContainsFunc(o.runs, func(r *run) bool { return r.f.Verdict.Limit == raw.LimitGenerator })
}

func (o overload) seconds() string {
	var h time.Duration
	for _, r := range o.counted {
		if v := r.f.Verdict.Horizon; v > 0 && (h == 0 || v < h) {
			h = v
		}
	}
	return secondsOf(o.counted, o.speed, h)
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
	case !o.valid:
		return "no valid run", false
	case len(o.open) > 0:
		return fmt.Sprintf("undecided: the saturated stage of pass %d has no valid run", o.open[0]), false
	case len(o.runs) == 0:
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
	for _, l := range o.never {
		if !slices.Contains(limits, l) {
			limits = append(limits, l)
		}
	}
	if len(o.open) > 0 {
		limits = append(limits, "saturated, not valid")
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

func (g *group) roleSyncs() (int, spread, spread) {
	var leaders, followers []float64
	for _, r := range g.counted {
		if l, f, ok := roleSyncs(r.f, g.speed); ok {
			leaders, followers = append(leaders, l), append(followers, f)
		}
	}
	return len(leaders), median(leaders), median(followers)
}

func writeStages(b *strings.Builder, gs []*group, out map[string][]byte) {
	cs := chains(gs)
	b.WriteString("## Stages (m1)\n\n")
	b.WriteString("How many transfers per second does one node hold, how many a cluster of three nodes, and how does the wait grow " +
		"with the load? Each stage offers a fixed rate for a fixed window. A stage is held when at the end of its window fewer " +
		"arrivals are open than fall due in one second; otherwise it is saturated. The last held stage comes from the raster of " +
		"stages, the throughput under overload is measured in the first saturated stage of each pass. The last held stage counts " +
		"only stages that ran at the disk speed of the throughput under overload; where none held at that speed, the speed of " +
		"the stage shown stands next to it. A row without a throughput under overload counts the stages at the slow speed, else " +
		"at the fast one, and names the speed when it is not slow. Where the load generator and not the target was the limit, " +
		"the table says \"at least\"; \"budget\" in the column \"limit\" marks a chain that ended because its next stage " +
		"would not fit the transfers a run of the cluster may write.\n\n")
	sum := table{head: []string{"target", "lanes", "load", "passes", "last held stage", "throughput under overload", "runs", "seconds", "limit", "agreement"}}
	var all []*run
	var stageGroups []*group
	for _, c := range cs {
		o := c.overload()
		g := c.first()
		held := o.held.show(grouped)
		if o.atLeast() && o.held.n > 0 {
			held = "at least " + held
		}
		if o.heldElsewhere() {
			held += ", " + o.heldSpeed.String()
		}
		tp := o.throughput().show(grouped)
		switch {
		case len(o.runs) == 0 && len(o.open) == 0:
			tp = "not reached"
		case o.atLeast() && len(o.counted) > 0:
			tp = "at least " + tp
		}
		text, _ := o.judge()
		sum.add(name(g.target), strconv.Itoa(g.target.Lanes), g.workload, strconv.Itoa(o.passes), held, tp,
			fmt.Sprintf("%d of %d", len(o.counted), len(o.runs)), o.seconds(), o.limit(), text)
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
		t := table{head: []string{"rate", "held", "speed", "flush ms", "runs", "seconds", "p50 ms", "p50 flushes", "p99 ms", "p99.9 ms",
			"throughput/s", "target cpu", "fsync per transfer, all nodes", "transfers per request", "retries per transfer", "given up", "agreement"}}
		perNode := table{head: []string{"rate", "speed", "runs", "fsync per transfer, leader", "fsync per transfer, one follower"}}
		var runs []*run
		for _, s := range c.stages {
			row := fixedRow(s)
			t.add(append([]string{grouped(float64(s.rate))}, row[4:]...)...)
			runs = append(runs, s.runs...)
			if n, leader, follower := s.roleSyncs(); n > 0 {
				perNode.add(grouped(float64(s.rate)), row[5], strconv.Itoa(n), leader.show(fixed(3)), follower.show(fixed(3)))
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
