package report

import (
	"bytes"
	"errors"
	"flag"
	"maps"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/Demonisreal/dmn-ledger/bench/control"
	"github.com/Demonisreal/dmn-ledger/bench/raw"
)

var update = flag.Bool("update", false, "rewrite testdata/plan")

const planDir = "testdata/plan"

const profiled = "m3-l1-uniform-10k-2000-1"

func encode(t *testing.T, f *raw.File) []byte {
	var buf bytes.Buffer
	if err := raw.Write(&buf, f); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func writePlan(t *testing.T, dir string) {
	if err := os.MkdirAll(filepath.Join(dir, "raw"), 0o750); err != nil {
		t.Fatal(err)
	}
	for _, p := range testPlan() {
		if err := os.WriteFile(filepath.Join(dir, "raw", p.id()+".json"), encode(t, p.file(t)), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	text, err := os.ReadFile("testdata/ledgerd.traces")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "raw", profiled+".n1.traces"), text, 0o644); err != nil { //nolint:gosec // G703: a folder of the test
		t.Fatal(err)
	}
}

func planRuns(t *testing.T) []*run {
	dir := t.TempDir()
	writePlan(t, dir)
	runs, err := read(os.DirFS(filepath.Join(dir, "raw")))
	if err != nil {
		t.Fatal(err)
	}
	return runs
}

func TestReportIsDeterministic(t *testing.T) {
	dir := t.TempDir()
	writePlan(t, dir)
	if err := Report(dir); err != nil {
		t.Fatal(err)
	}
	if *update {
		if err := os.RemoveAll(planDir); err != nil {
			t.Fatal(err)
		}
		if err := os.CopyFS(planDir, os.DirFS(dir)); err != nil {
			t.Fatal(err)
		}
		if err := os.RemoveAll(filepath.Join(planDir, "raw")); err != nil {
			t.Fatal(err)
		}
	}
	runs, err := read(os.DirFS(filepath.Join(dir, "raw")))
	if err != nil {
		t.Fatal(err)
	}
	if i := slices.IndexFunc(runs, func(r *run) bool { return r.f.ID == profiled }); len(runs) != len(testPlan()) || i < 0 || len(runs[i].traces) != 1 {
		t.Fatalf("%d runs read, the profile not found", len(runs))
	}
	for _, r := range runs {
		if why := refusals(r.f); len(why) > 0 {
			t.Errorf("%s refused: %q", r.name, why)
		}
	}
	out := render(runs)
	entries, err := os.ReadDir(planDir)
	if err != nil {
		t.Fatal(err)
	}
	var pinned []string
	for _, e := range entries {
		pinned = append(pinned, e.Name())
	}
	if names := slices.Sorted(maps.Keys(out)); !slices.Equal(names, pinned) {
		t.Fatalf("report writes %q, pinned are %q", names, pinned)
	}
	for _, n := range pinned {
		want, err := os.ReadFile(filepath.Join(planDir, n))
		if err != nil {
			t.Fatal(err)
		}
		written, err := os.ReadFile(filepath.Join(dir, n))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(written, want) || !bytes.Equal(out[n], want) {
			t.Fatalf("%s: %s", n, firstDifference(want, written))
		}
	}

	shuffled := slices.Clone(runs)
	slices.Reverse(shuffled)
	shuffled = append(shuffled[7:], shuffled[:7]...)
	for n, b := range render(shuffled) {
		if !bytes.Equal(b, out[n]) {
			t.Fatalf("%s depends on the order of the files: %s", n, firstDifference(out[n], b))
		}
	}
}

func TestMedianAndRange(t *testing.T) {
	for _, c := range []struct {
		xs   []float64
		want spread
		show string
	}{
		{[]float64{30, 10, 12}, spread{12, 10, 30, 3}, "12.0 (10.0–30.0)"},
		{[]float64{14, 30, 10, 12}, spread{13, 10, 30, 4}, "13.0 (10.0–30.0)"},
		{[]float64{7}, spread{7, 7, 7, 1}, "7.0"},
		{nil, spread{}, "n/a"},
	} {
		if s := median(c.xs); s != c.want || s.show(fixed(1)) != c.show {
			t.Errorf("%v: %+v shown %q, want %+v %q", c.xs, s, s.show(fixed(1)), c.want, c.show)
		}
	}
	if s := median([]float64{2e6, math.Inf(1), math.Inf(1)}); s.show(millis(5*time.Second)) != "over 5,000 (2.00–over 5,000)" {
		t.Errorf("unfinished median shown as %q", s.show(millis(5*time.Second)))
	}

	base := ledger("m2", 1, 32, "uniform-10k", 2000)
	var runs []*run
	for i, p50 := range []time.Duration{3 * time.Millisecond, 9 * time.Millisecond, 3100 * time.Microsecond, time.Millisecond} {
		p := base.with(func(p *plan) { p.nr, p.p50 = i+1, p50 })
		if i == 3 {
			p.flush = window(fast, slow, fast)
		}
		runs = append(runs, &run{name: p.id() + ".json", f: p.file(t)})
	}
	gs := groups(runs)
	if len(gs) != 1 || len(gs[0].counted) != 3 {
		t.Fatalf("%d groups; the restless run must not count", len(gs))
	}
	p50 := func(i int) float64 { return float64(runs[i].f.Bands.Slow.P50) }
	s := gs[0].spread(func(x fig) float64 { return x.p50 })
	if s.med != p50(2) || s.lo != p50(0) || s.hi != p50(1) || s.n != 3 {
		t.Fatalf("p50 %+v, want the middle run %v between %v and %v", s, p50(2), p50(0), p50(1))
	}
	row := fixedRow(gs[0])
	want := plainMillis(p50(2)) + " (" + plainMillis(p50(0)) + "–" + plainMillis(p50(1)) + ")"
	if row[7] != "3 of 4" || row[9] != want {
		t.Errorf("row shows %q runs and p50 %q, want 3 of 4 and %q", row[7], row[9], want)
	}
}

func TestSpeedsAreNotMixed(t *testing.T) {
	base := ledger("m2", 1, 32, "uniform-10k", 2000)
	mixed := []plan{
		base,
		base.with(func(p *plan) { p.nr, p.flush = 2, window(slow, slow, slow, fast, fast) }),
		base.with(func(p *plan) { p.nr, p.flush, p.p50 = 3, window(fast, fast, fast), 600*time.Microsecond }),
	}
	var runs []*run
	for _, p := range mixed {
		runs = append(runs, &run{name: p.id() + ".json", f: p.file(t)})
	}
	if runs[1].f.Bands.Fast.Seconds != 1 || runs[2].f.Verdict.Speed != control.Fast {
		t.Fatalf("test runs: %d fast seconds, speed %v", runs[1].f.Bands.Fast.Seconds, runs[2].f.Verdict.Speed)
	}
	g := groups(runs)[0]
	if g.speed != control.Slow || len(g.counted) != 2 || slices.Contains(g.counted, runs[2]) {
		t.Fatalf("speed %v with %d counted runs; the fast run must stay out", g.speed, len(g.counted))
	}
	if s := g.spread(func(x fig) float64 { return x.flush }); s.hi != float64(slow) || s.lo != float64(slow) {
		t.Fatalf("mean flush %+v, a fast second came in", s)
	}
	fastOnly := groups(runs[2:])[0]
	if fastOnly.speed != control.Fast || fixedRow(fastOnly)[5] != "fast (no slow run)" {
		t.Fatalf("fast group shown as %q", fixedRow(fastOnly)[5])
	}
	if p50 := fastOnly.spread(func(x fig) float64 { return x.p50 }).med; p50 != float64(runs[2].f.Bands.Fast.P50) || p50 == 0 {
		t.Errorf("fast group p50 %v, want %v from its fast band", p50, runs[2].f.Bands.Fast.P50)
	}
}

func TestInvalidSecondsStayOut(t *testing.T) {
	p := ledger("m2", 1, 32, "uniform-10k", 2000).with(func(p *plan) { p.flush, p.foreign, p.retries = window(slow, slow, slow, slow), []int{2}, 4 })
	f := p.file(t)
	if f.Seconds[2].Valid || f.Bands.Slow.Seconds != 3 {
		t.Fatalf("second 2 valid %v, %d counted seconds", f.Seconds[2].Valid, f.Bands.Slow.Seconds)
	}
	clean := loopsOf(f, func(i int) bool { return counts(f, i, control.Slow) })["leader"].shares()
	f.Seconds[2].Retries = 1_000_000
	for _, s := range f.Seconds[2:] {
		s.Parts[0].Counters["wait"] += uint64(400 * time.Millisecond)
		s.Parts[0].Counters["logsync_ns"] += uint64(300 * time.Millisecond)
	}
	if x := figOf(f, control.Slow); x.retries != 4.0/2000 {
		t.Errorf("retries per transfer %v, want 0.002 from the valid seconds", x.retries)
	}
	got := loopsOf(f, func(i int) bool { return counts(f, i, control.Slow) })["leader"].shares()
	for _, post := range loopPosts {
		if got[post] != clean[post] {
			t.Errorf("%s %.4f, without the invalid second %.4f", post, got[post], clean[post])
		}
	}
}

func TestAtLeast(t *testing.T) {
	out := string(render(planRuns(t))[Results])
	for _, want := range []string{
		"| three nodes | 4 | uniform-10k | 2 | at least 1,000 (1,000–1,000) | at least 1,500 (1,500–1,500) | 2 of 2 | 6 of 6 | generator |",
		"| one node | 32 | uniform-10k | 2 | 4,000 (4,000–4,000) | 5,100 (5,000–5,200) | 2 of 2 | 6 of 6 | target | agreed |",
		"| Postgres, batched, read committed | 4 | uniform-10k | 2 | 1,000 (1,000–1,000) | 1,850 (1,800–1,900) | 2 of 2 | 6 of 6 | target | agreed |",
		"| Postgres, batched, read committed | 4 | uniform-10k | 20,000 | saturated at 20,000 |",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("RESULTS.md lacks %q", want)
		}
	}
	held := ledger("m1", 1, 32, "uniform-10k", 1000)
	cs := chains(groups([]*run{{name: "a.json", f: held.file(t)}}))
	if o := cs[0].overload(); len(o.never) == 0 || !o.atLeast() {
		t.Fatalf("a chain that held every stage: %+v", o)
	}
	limited := postgres("m2", "V3", 4, 20000).with(func(p *plan) { p.saturated, p.late, p.booked = true, true, 9000 })
	g := groups([]*run{{name: "b.json", f: limited.file(t)}})[0]
	if row := fixedRow(g); !strings.HasPrefix(row[13], "at least ") || row[4] != "saturated at 20,000" {
		t.Errorf("generator at its limit: held %q, throughput %q", row[4], row[13])
	}
	var none []*run
	for pass := 1; pass <= 2; pass++ {
		p := ledger("m1", 1, 4, "uniform-10k", 1000).with(func(p *plan) { p.pass, p.nr, p.saturated, p.booked = pass, pass, true, 700 })
		none = append(none, &run{name: p.id() + ".json", f: p.file(t)})
	}
	var late []*run
	for pass := 1; pass <= 2; pass++ {
		for _, p := range []plan{
			ledger("m1", 3, 32, "uniform-10k", 1000).with(func(p *plan) { p.pass, p.nr = pass, pass }),
			ledger("m1", 3, 32, "uniform-10k", 2000).with(func(p *plan) { p.pass, p.nr, p.late = pass, pass, true }),
		} {
			late = append(late, &run{name: p.id() + ".json", f: p.file(t)})
		}
	}
	var lateOut strings.Builder
	writeStages(&lateOut, groups(late), map[string][]byte{})
	if !strings.Contains(lateOut.String(), "| three nodes | 32 | uniform-10k | 2 | at least 1,000 (1,000–1,000) | not reached | 0 of 0 | n/a | generator | at least: a pass held every stage |") {
		t.Errorf("the stage above the last held one is out because of the generator, yet the limit is not the generator:\n%s", lateOut.String())
	}
	var b strings.Builder
	writeStages(&b, groups(none), map[string][]byte{})
	if out := b.String(); strings.Contains(out, "at least 0") || !strings.Contains(out, "| one node | 4 | uniform-10k | 2 | n/a | 700 (700–700) |") {
		t.Errorf("a chain saturated in its first stage has no last held stage:\n%s", out)
	}
}

func TestGivenUpTransfersSaturate(t *testing.T) {
	var runs []*run
	for pass := 1; pass <= 2; pass++ {
		p := postgres("m2", "V4", 4, 20000).with(func(p *plan) { p.pass, p.nr, p.saturated, p.booked, p.shed = pass, pass, true, 16000, 400 })
		runs = append(runs, &run{name: p.id() + ".json", f: p.file(t)})
	}
	g := groups(runs)[0]
	row := fixedRow(g)
	if !runs[0].f.Verdict.Valid || row[4] != "saturated at 20,000" || row[18] != "2.0% (2.0%–2.0%)" {
		t.Errorf("Postgres gave up 2 %% of the transfers due: valid %v, held %q, given up %q", runs[0].f.Verdict.Valid, row[4], row[18])
	}
	if !strings.Contains(string(render(runs)[Results]), "| retries per transfer | given up | agreement |") {
		t.Error("RESULTS.md has no column for transfers given up")
	}
}

func TestTargetLeavesOutTheDefaultsOfTheNode(t *testing.T) {
	node := map[string]string{"GOMEMLIMIT": "4500MiB", "LEDGER_CLUSTER": "1", "LEDGER_MAX_CLIENTS": "128", "LEDGER_SNAPSHOT_EVERY": "4294967296", "LEDGER_SWEEP": "on"}
	with := func(k, v string) map[string]string {
		env := maps.Clone(node)
		if k != "" {
			env[k] = v
		}
		return env
	}
	for _, c := range []struct {
		env   map[string]string
		shown string
	}{
		{with("", ""), ""},
		{with("LEDGER_BUG", "B13"), ", LEDGER_BUG=B13"},
		{with("LEDGER_SNAPSHOT_EVERY", "20000"), ", LEDGER_SNAPSHOT_EVERY=20000"},
	} {
		p := ledger("m2", 1, 32, "uniform-10k", 2000).with(func(p *plan) { p.env = c.env })
		g := groups([]*run{{name: "a.json", f: p.file(t)}})[0]
		var b strings.Builder
		writeFixed(&b, []*group{g})
		if !strings.Contains(b.String(), "| one node"+c.shown+" | 32 | uniform-10k |") || g.subject() != "m2 one node, 32 lanes, uniform-10k at 2,000/s"+c.shown {
			t.Errorf("env %v: subject %q, table\n%s", c.env, g.subject(), b.String())
		}
	}
}

func TestResultsShowWhatIsLeftOut(t *testing.T) {
	out := string(render(planRuns(t))[Results])
	m2 := out[strings.Index(out, "## Fixed load (m2)"):]
	m2 = m2[:strings.Index(m2[3:], "\n## ")+3]
	for _, want := range []string{
		"| runs | seconds | p50 ms |",
		"| throughput/s | target cpu | fsync per transfer, all nodes |",
		"| three nodes | 32 | uniform-10k | 2,000 | held | slow | 1.40 (1.40–1.40) | 2 of 2 | 5 of 6, worst excluded p99 ",
		" ms, thermal limit in 1 |",
		"| 10.0% (10.0%–10.0%) |",
		"Each node on its own:",
		"| three nodes | 32 | uniform-10k | 2,000 | slow | 2 |",
	} {
		if !strings.Contains(m2, want) {
			t.Errorf("fixed load lacks %q", want)
		}
	}
	if strings.Contains(m2, "A second counts as fast") {
		t.Error("the note on fast seconds stands under a table without a fast row")
	}
	stages := out[strings.Index(out, "## Stages (m1)"):strings.Index(out, "## Fixed load (m2)")]
	for _, want := range []string{"A second counts as fast", "| runs | seconds | p50 ms |", "| target cpu | fsync per transfer, all nodes |"} {
		if !strings.Contains(stages, want) {
			t.Errorf("stages lack %q", want)
		}
	}
	if runs := out[strings.Index(out, "## Runs"):]; !strings.Contains(runs, "| seconds |") || !strings.Contains(runs, "| 2 of 3, worst excluded p99 ") {
		t.Error("the table of runs does not say how many window seconds each run counted")
	}
}

func TestOverloadIsTheFirstSaturatedStage(t *testing.T) {
	var runs []*run
	for _, p := range []plan{
		ledger("m1", 1, 32, "uniform-10k", 1000),
		ledger("m1", 1, 32, "uniform-10k", 2000).with(func(p *plan) { p.saturated, p.booked = true, 1800 }),
		ledger("m1", 1, 32, "uniform-10k", 4000).with(func(p *plan) { p.saturated, p.booked = true, 2500 }),
		ledger("m1", 1, 32, "uniform-10k", 8000).with(func(p *plan) { p.flush = window(fast, slow, fast) }),
	} {
		runs = append(runs, &run{name: p.id() + ".json", f: p.file(t)})
	}
	o := chains(groups(runs))[0].overload()
	if o.held.med != 1000 || len(o.runs) != 1 || o.runs[0].f.Load.Rate != 2000 || o.throughput().med != 1800 || o.atLeast() {
		t.Fatalf("last held %v, overload from %d runs at %d with %v/s", o.held.med, len(o.runs), o.runs[0].f.Load.Rate, o.throughput().med)
	}
}

func checked(t *testing.T, plans []plan, edit func([]*run), tamper func(fstest.MapFS)) []Verdict {
	var runs []*run
	for _, p := range plans {
		runs = append(runs, &run{name: p.id() + ".json", f: p.file(t)})
	}
	if edit != nil {
		edit(runs)
	}
	written := fstest.MapFS{PlanFile: {Data: planOf(t, plans)}}
	for n, b := range render(runs) {
		written[n] = &fstest.MapFile{Data: b}
	}
	if tamper != nil {
		tamper(written)
	}
	return check(runs, written)
}

func planOf(t *testing.T, plans []plan) []byte {
	var p Planned
	for _, pl := range plans {
		e := Expect{Measurement: pl.m, Kind: pl.kind, Nodes: pl.nodes, Variant: pl.variant, Lanes: pl.lanes, Workload: pl.workload,
			Rate: pl.rate, Passes: pl.pass, Env: pl.env, Tags: pl.tags}
		if pl.m == "m1" {
			e.Rate = 0
		}
		i := slices.IndexFunc(p.Expect, func(x Expect) bool {
			x.Passes = e.Passes
			return reflect.DeepEqual(x, e)
		})
		if i < 0 {
			p.Expect = append(p.Expect, e)
			continue
		}
		p.Expect[i].Passes = max(p.Expect[i].Passes, pl.pass)
	}
	var buf bytes.Buffer
	if err := WritePlan(&buf, p); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func failed(vs []Verdict) []string {
	var out []string
	for _, v := range vs {
		if !v.Passed {
			out = append(out, v.Subject+": "+strings.Join(v.Why, "; "))
		}
	}
	return out
}

func TestCheckRefuses(t *testing.T) {
	good := []plan{
		ledger("m2", 1, 32, "uniform-10k", 2000),
		ledger("m2", 1, 32, "uniform-10k", 2000).with(func(p *plan) { p.pass, p.nr, p.p50 = 2, 2, 3100*time.Microsecond }),
	}
	if f := failed(checked(t, good, nil, nil)); len(f) > 0 {
		t.Fatalf("two agreeing runs refused: %q", f)
	}
	quiet := []plan{
		postgres("m2", "V3", 4, 20000).with(func(p *plan) { p.few, p.cpu, p.throttled = 8, 1.0, []int{1, 2} }),
		postgres("m2", "V3", 4, 20000).with(func(p *plan) {
			p.pass, p.nr, p.few, p.cpu, p.flush = 2, 2, 12, 1.0, window(20*time.Millisecond, slow, fast)
		}),
	}
	if f := failed(checked(t, quiet, nil, nil)); len(f) > 0 {
		t.Fatalf("two agreeing unpaced runs refused: %q", f)
	}
	for _, c := range []struct {
		name   string
		plans  []plan
		edit   func([]*run)
		tamper func(fstest.MapFS)
		want   string
	}{
		{"dirty", good, func(rs []*run) { rs[0].f.Build.Dirty = true }, nil, WhyDirty},
		{"simbug outside m5", good, func(rs []*run) { rs[1].f.Build.Tags = []string{"simbug"} }, nil, WhySimbug},
		{"head", good, func(rs []*run) { rs[0].f.Machine.CPU, rs[0].f.Build.Commit = "", "" }, nil, WhyHead + ": build.commit, machine.cpu"},
		{"one run", good[:1], nil, nil, WhyOneRun},
		{"criterion", []plan{good[0], good[1].with(func(p *plan) { p.p50 = 4 * time.Millisecond })}, nil, nil, "missed: p50"},
		{"seconds", good, func(rs []*run) { rs[1].f.Seconds[2].Valid, rs[1].f.Seconds[2].Throttled = true, 1 }, nil, WhySeconds},
		{"band", good, func(rs []*run) { rs[0].f.Bands.Slow.Seconds++ }, nil, WhySeconds},
		{"changed number", good, nil, func(w fstest.MapFS) {
			text := strings.Replace(string(w[Results].Data), "| 2,000 (2,000–2,000) |", "| 2,100 (2,000–2,000) |", 1)
			w[Results] = &fstest.MapFile{Data: []byte(text)}
		}, WhyStale + ": line"},
		{"missing", good, nil, func(w fstest.MapFS) { delete(w, Results) }, WhyMissing},
		{"not verified", good, func(rs []*run) { rs[0].f.Books.Verified = false }, nil, "valid but not verified"},
		{"term", good, func(rs []*run) { rs[1].f.Seconds[2].Parts[0].Term = 3 }, nil, "valid with a new term in the window"},
		{"term in the first second of the drain", good, func(rs []*run) {
			f := rs[1].f
			f.Seconds = append(f.Seconds, f.Seconds[len(f.Seconds)-1])
			f.Seconds[len(f.Seconds)-1].Phase = "drain"
			f.Seconds[len(f.Seconds)-1].Parts = []raw.Part{{Name: "n1", Term: 3}}
		}, nil, "valid with a new term in the window"},
		{"existed", good, func(rs []*run) { rs[1].f.Books.Existed = 4 }, nil, "4 transfers existed already"},
		{"stalls allowed", good, func(rs []*run) { rs[0].f.AllowStalls = true }, nil, "stalls allowed outside m4 and m7"},
		{"stalled second in a valid run", good, func(rs []*run) { rs[0].f.Seconds[2].Valid, rs[0].f.Seconds[2].Why = false, []string{raw.WhyStale} }, nil, "second 2 stalled and the run is valid"},
		{"no plan", good, nil, func(w fstest.MapFS) { delete(w, PlanFile) }, PlanFile + ": " + WhyMissing},
		{"pass missing", good, func(rs []*run) { rs[1].f.Pass = 3 }, nil, "m2 one node, 32 lanes, uniform-10k at 2,000/s: missing: no run in pass 2"},
		{"measurement missing", good, nil, func(w fstest.MapFS) {
			w[PlanFile] = &fstest.MapFile{Data: planOf(t, append(slices.Clone(good), ledger("m2", 3, 32, "uniform-10k", 2000)))}
		}, "m2 three nodes, 32 lanes, uniform-10k at 2,000/s: missing: no run in pass 1"},
		{"not planned", append(slices.Clone(good), ledger("m2", 3, 32, "uniform-10k", 2000)), nil, func(w fstest.MapFS) {
			w[PlanFile] = &fstest.MapFile{Data: planOf(t, good)}
		}, "m2 three nodes, 32 lanes, uniform-10k at 2,000/s: not in plan.json"},
		{"band p50", good, func(rs []*run) { rs[0].f.Bands.Slow.P50 -= time.Millisecond }, nil, WhyBand + ": slow p50"},
		{"band p99", good, func(rs []*run) { rs[0].f.Bands.Slow.P99 /= 2 }, nil, WhyBand + ": slow p99"},
		{"band throughput", good, func(rs []*run) { rs[1].f.Bands.Slow.Throughput *= 1.05 }, nil, WhyBand + ": slow throughput"},
		{"band fsync", good, func(rs []*run) { rs[1].f.Bands.Slow.Syncs *= 0.9 }, nil, WhyBand + ": slow fsync per transfer"},
		{"band histogram", good, func(rs []*run) { rs[0].f.Bands.Slow.Histogram.Buckets = rs[0].f.Bands.Slow.Histogram.Buckets[1:] }, nil, WhyBand + ": slow histogram"},
		{"thermal seconds", good, func(rs []*run) { rs[0].f.Verdict.Throttled = 2 }, nil, WhySeconds + ": 2 window seconds marked thermal, their counters say 0"},
		{"unpaced with an idle second", quiet, func(rs []*run) { rs[0].f.Seconds[2].CPU.Target = 0.01 }, nil, "second 2 stalled and the run is valid"},
		{"unpaced band", quiet, func(rs []*run) { rs[1].f.Bands.Unpaced.Throughput *= 1.05 }, nil, WhyBand + ": unpaced throughput"},
		{"unpaced, but the seconds say otherwise", quiet, func(rs []*run) {
			for i := range rs[0].f.Seconds {
				rs[0].f.Seconds[i].Flushes = 300
			}
		}, nil, WhySeconds + ": second 0 is unpaced and valid true, its flushes say"},
	} {
		f := failed(checked(t, c.plans, c.edit, c.tamper))
		if !slices.ContainsFunc(f, func(s string) bool { return strings.Contains(s, c.want) }) {
			t.Errorf("%s: failed %q, want %q", c.name, f, c.want)
		}
	}

	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "raw"), 0o750); err != nil {
		t.Fatal(err)
	}
	for _, p := range good {
		if err := os.WriteFile(filepath.Join(dir, "raw", p.id()+".json"), encode(t, p.file(t)), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, PlanFile), planOf(t, good), 0o644); err != nil { //nolint:gosec // G703: a folder of the test
		t.Fatal(err)
	}
	if _, err := Check(dir); err == nil {
		t.Fatal("no RESULTS.md, and Check passed")
	}
	if err := Report(dir); err != nil {
		t.Fatal(err)
	}
	if vs, err := Check(dir); err != nil {
		t.Fatalf("%v: %q", err, failed(vs))
	}
	text, err := os.ReadFile(filepath.Join(dir, Results))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, Results), bytes.Replace(text, []byte("2 of 2"), []byte("3 of 3"), 1), 0o644); err != nil { //nolint:gosec // G703: a folder of the test
		t.Fatal(err)
	}
	if _, err := Check(dir); !errors.Is(err, ErrRefused) {
		t.Fatalf("edited RESULTS.md: %v", err)
	}
	if _, err := Check(t.TempDir()); err == nil {
		t.Error("an empty folder passed")
	}
}

func TestPlainCurves(t *testing.T) {
	if plot("t", "x", "y", true, []line{{label: "none"}}) != nil {
		t.Fatal("a curve without points was drawn")
	}
	svg := string(plot("p50", "rate", "ms", true, []line{
		{label: "one node", points: []point{{x: 1000, y: 3}, {x: 2000, y: 3.2}, {x: 4000, y: math.Inf(1)}}},
		{label: "Postgres <single>", points: []point{{x: 1000, y: 0.4, fast: true}, {x: 2000, y: 400}}},
	}))
	for _, want := range []string{`fill="#ffffff"`, `stroke-dasharray="7 4"`, "Postgres &lt;single&gt;", ">0.1<", ">1000<", ">2k<", "open circles"} {
		if !strings.Contains(svg, want) {
			t.Errorf("curve lacks %q", want)
		}
	}
	for _, banned := range []string{"Gradient", "filter", "opacity", "<image"} {
		if strings.Contains(svg, banned) {
			t.Errorf("curve has %q", banned)
		}
	}
	if n := strings.Count(svg, "<polyline"); n != 2 || strings.Count(svg, "<circle") != 4 {
		t.Errorf("%d lines and %d points, want 2 and 4 (the unfinished one left out)", n, strings.Count(svg, "<circle"))
	}
}
