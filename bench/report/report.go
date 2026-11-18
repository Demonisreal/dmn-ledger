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
	"strconv"
	"strings"
	"time"

	"github.com/Demonisreal/dmn-ledger/bench/control"
	"github.com/Demonisreal/dmn-ledger/bench/load"
	"github.com/Demonisreal/dmn-ledger/bench/raw"
)

const Results = "RESULTS.md"

var ErrNoRuns = errors.New("report: no raw files")

type run struct {
	name   string
	f      *raw.File
	traces []traces
}

type traces struct {
	node string
	text []byte
}

func Report(dir string) error {
	runs, err := read(os.DirFS(filepath.Join(dir, "raw")))
	if err != nil {
		return err
	}
	out := render(runs)
	for _, name := range slices.Sorted(maps.Keys(out)) {
		if err := os.WriteFile(filepath.Join(dir, name), out[name], 0o644); err != nil { //nolint:gosec // G306: the report is for reading
			return err
		}
	}
	return nil
}

func read(fsys fs.FS) ([]*run, error) {
	entries, err := fs.ReadDir(fsys, ".")
	if err != nil {
		return nil, fmt.Errorf("report: %w", err)
	}
	var runs []*run
	for _, e := range entries {
		base, ok := strings.CutSuffix(e.Name(), ".json")
		if !ok || e.IsDir() {
			continue
		}
		b, err := fs.ReadFile(fsys, e.Name())
		if err != nil {
			return nil, fmt.Errorf("report: %w", err)
		}
		f, err := raw.Read(bytes.NewReader(b))
		if err != nil {
			return nil, fmt.Errorf("report: %s: %w", e.Name(), err)
		}
		r := &run{name: e.Name(), f: f}
		for _, t := range entries {
			node, ok := strings.CutPrefix(t.Name(), base+".")
			if !ok {
				continue
			}
			if node, ok = strings.CutSuffix(node, ".traces"); !ok || strings.Contains(node, ".") {
				continue
			}
			text, err := fs.ReadFile(fsys, t.Name())
			if err != nil {
				return nil, fmt.Errorf("report: %w", err)
			}
			r.traces = append(r.traces, traces{node: node, text: text})
		}
		runs = append(runs, r)
	}
	if len(runs) == 0 {
		return nil, ErrNoRuns
	}
	return runs, nil
}

type group struct {
	m        string
	target   raw.Target
	tags     []string
	workload string
	rate     int
	runs     []*run
	speed    control.Speed
	counted  []*run
}

func env(t raw.Target) string {
	var pairs []string
	for _, k := range slices.Sorted(maps.Keys(t.Env)) {
		pairs = append(pairs, k+"="+t.Env[k])
	}
	return strings.Join(pairs, " ")
}

var nodeDefaults = map[string]string{"GOMEMLIMIT": "", "LEDGER_CLUSTER": "", "LEDGER_MAX_CLIENTS": "128", "LEDGER_SNAPSHOT_EVERY": "4294967296", "LEDGER_SWEEP": "on"}

func distinct(t raw.Target) string {
	var pairs []string
	for _, k := range slices.Sorted(maps.Keys(t.Env)) {
		if usual, ok := nodeDefaults[k]; ok && (usual == "" || usual == t.Env[k]) {
			continue
		}
		pairs = append(pairs, k+"="+t.Env[k])
	}
	return strings.Join(pairs, " ")
}

func keyOf(f *raw.File) string {
	t := f.Target
	return strings.Join([]string{f.Measurement, t.Kind, strconv.Itoa(t.Nodes), t.Variant, strconv.Itoa(t.Lanes), env(t),
		strings.Join(f.Build.Tags, ","), f.Load.Workload, strconv.Itoa(f.Load.Rate)}, "|")
}

func targetRank(t raw.Target) int {
	switch t.Kind {
	case "ledger":
		return t.Nodes
	case "postgres":
		if n, err := strconv.Atoi(strings.TrimPrefix(strings.ToUpper(t.Variant), "V")); err == nil {
			return 10 + n
		}
		return 20
	case "null":
		return 30
	}
	return 40
}

func workloadRank(name string) int {
	if i := slices.IndexFunc(load.Workloads(), func(w load.Workload) bool { return w.Name == name }); i >= 0 {
		return i
	}
	return len(load.Workloads())
}

func compareGroups(a, b *group) int {
	if c := strings.Compare(a.m, b.m); c != 0 {
		return c
	}
	for _, c := range []int{
		targetRank(a.target) - targetRank(b.target),
		strings.Compare(name(a.target), name(b.target)),
		b.target.Lanes - a.target.Lanes,
		workloadRank(a.workload) - workloadRank(b.workload),
		strings.Compare(a.workload, b.workload),
		a.rate - b.rate,
		strings.Compare(env(a.target), env(b.target)),
		slices.Compare(a.tags, b.tags),
	} {
		if c != 0 {
			return c
		}
	}
	return 0
}

func groups(runs []*run) []*group {
	byKey := make(map[string]*group)
	for _, r := range runs {
		k := keyOf(r.f)
		g := byKey[k]
		if g == nil {
			g = &group{m: r.f.Measurement, target: r.f.Target, tags: r.f.Build.Tags, workload: r.f.Load.Workload, rate: r.f.Load.Rate}
			byKey[k] = g
		}
		g.runs = append(g.runs, r)
	}
	all := slices.Collect(maps.Values(byKey))
	slices.SortFunc(all, compareGroups)
	for _, g := range all {
		g.speed = speedOf(g.runs)
		g.counted = countedAt(g.runs, g.speed)
	}
	return all
}

func sortRuns(runs []*run) {
	slices.SortFunc(runs, func(a, b *run) int {
		if c := strings.Compare(a.f.ID, b.f.ID); c != 0 {
			return c
		}
		return strings.Compare(a.name, b.name)
	})
}

func speedOf(runs []*run) control.Speed {
	speed := control.None
	for _, r := range runs {
		if v := r.f.Verdict; v.Valid && v.Speed.Rank() > speed.Rank() {
			speed = v.Speed
		}
	}
	return speed
}

func countedAt(runs []*run, speed control.Speed) []*run {
	var counted []*run
	for _, r := range runs {
		if r.f.Verdict.Valid && r.f.Verdict.Speed == speed {
			counted = append(counted, r)
		}
	}
	return counted
}

func bandOf(f *raw.File, speed control.Speed) raw.Band {
	switch speed {
	case control.Fast:
		return f.Bands.Fast
	case control.Unpaced:
		return f.Bands.Unpaced
	}
	return f.Bands.Slow
}

type fig struct {
	p50, p99, p999, flushes, flush     float64
	throughput, syncs, perRequest      float64
	targetShare                        float64
	retries, shed, reserveP50          float64
	reserveP99                         float64
	saturated, generator, reserveKnown bool
}

func nanos(d time.Duration) float64 {
	if d == load.Never {
		return math.Inf(1)
	}
	return float64(d)
}

func counts(f *raw.File, i int, speed control.Speed) bool {
	s := f.Seconds[i]
	return s.Phase == load.Window.String() && s.Valid && s.Speed == speed
}

func figOf(f *raw.File, speed control.Speed) fig {
	b := bandOf(f, speed)
	x := fig{
		p50: nanos(b.P50), p99: nanos(b.P99), p999: nanos(b.P999), flushes: b.P50Flushes, flush: float64(b.Flush),
		throughput: b.Throughput, syncs: b.Syncs, perRequest: b.PerRequest, targetShare: f.Verdict.TargetShare,
		reserveP50: nanos(b.ReserveP50), reserveP99: nanos(b.ReserveP99), reserveKnown: b.Reserve != nil,
		saturated: f.Verdict.Saturated, generator: f.Verdict.Limit == raw.LimitGenerator,
	}
	if math.IsInf(x.p50, 1) {
		x.flushes = math.Inf(1)
	}
	var retries, booked, shed, due uint64
	for i, s := range f.Seconds {
		if counts(f, i, speed) {
			retries += s.Retries
			booked += s.Booked
			shed += s.Shed
			due += s.Due
		}
	}
	if booked > 0 {
		x.retries = float64(retries) / float64(booked)
	}
	if due > 0 {
		x.shed = float64(shed) / float64(due)
	}
	return x
}

type spread struct {
	med, lo, hi float64
	n           int
}

func median(xs []float64) spread {
	if len(xs) == 0 {
		return spread{}
	}
	s := slices.Sorted(slices.Values(xs))
	n := len(s)
	med := s[n/2]
	if n%2 == 0 {
		med = (s[n/2-1] + s[n/2]) / 2
	}
	return spread{med: med, lo: s[0], hi: s[n-1], n: n}
}

func (g *group) spread(pick func(fig) float64) spread {
	var xs []float64
	for _, r := range g.counted {
		xs = append(xs, pick(figOf(r.f, g.speed)))
	}
	return median(xs)
}

func (g *group) horizon() time.Duration {
	var h time.Duration
	for _, r := range g.runs {
		if v := r.f.Verdict.Horizon; v > 0 && (h == 0 || v < h) {
			h = v
		}
	}
	return h
}

func name(t raw.Target) string {
	switch t.Kind {
	case "ledger":
		switch t.Nodes {
		case 1:
			return "one node"
		case 3:
			return "three nodes"
		}
		return fmt.Sprintf("%d nodes", t.Nodes)
	case "postgres":
		switch strings.ToUpper(t.Variant) {
		case "V1":
			return "Postgres, one transaction per transfer, read committed"
		case "V2":
			return "Postgres, one transaction per transfer, serializable"
		case "V3":
			return "Postgres, batched, read committed"
		case "V4":
			return "Postgres, batched, serializable"
		}
		return "Postgres " + t.Variant
	case "null":
		return "null target"
	}
	return strings.TrimSpace(t.Kind + " " + t.Variant)
}
