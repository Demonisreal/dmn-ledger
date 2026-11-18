package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime/debug"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/Demonisreal/dmn-ledger/bench/control"
	"github.com/Demonisreal/dmn-ledger/bench/ledgerrun"
	"github.com/Demonisreal/dmn-ledger/bench/load"
	"github.com/Demonisreal/dmn-ledger/bench/pg"
	"github.com/Demonisreal/dmn-ledger/bench/raw"
	"github.com/Demonisreal/dmn-ledger/bench/report"
)

var (
	errVerify     = errors.New("the books do not add up, the plan stops")
	errTooManyBad = errors.New("more than a quarter of the runs of a measurement are invalid")
)

const (
	whyNodes      = "nodes disagree after the run"
	whyCheckpoint = "checkpoint in the window"
	whySegment    = "new WAL segment in the window"
	minStages     = 5
)

var targetCPUs = []int{0, 1, 2, 3, 4, 5}

type stand interface {
	load.Target
	Describe() raw.Target
}

type bench struct {
	shape
	dir, repo, out string
	only           *regexp.Regexp
	log            io.Writer
	root           fs.FS
	items          []item
	bins           map[string]string
	build          raw.Build
	machine        control.Machine
	probe          control.Probe
	th             control.Thresholds
	disk           string
	cgroup         string
	limit          uint64
	srv            *pg.Server
	described      bool
	files          map[string]*raw.File
	earlier        map[string]bool
	slots          []run
	lines          []line
	blocks         []string
	stopped        string
	ran            bool
}

type line struct {
	block, id, text string
}

func (b *bench) selected(it item) bool {
	return b.only == nil || slices.ContainsFunc(it.runs, func(r run) bool { return b.only.MatchString(r.id()) })
}

func (b *bench) note(r run, text string) {
	b.lines = append(b.lines, line{block: fmt.Sprintf("%s, pass %d", r.m, r.pass), id: r.id(), text: text})
}

func (b *bench) fly(ctx context.Context) error {
	for _, it := range b.items {
		if !b.selected(it) {
			b.note(it.runs[0], "not selected")
			continue
		}
		if err := b.item(ctx, it); err != nil {
			return err
		}
	}
	if b.smoke {
		return nil
	}
	if b.only == nil {
		for step := range steps {
			for _, it := range b.extra(step) {
				if err := b.item(ctx, it); err != nil {
					return err
				}
			}
		}
	}
	return b.judgeBlocks(true)
}

var steps = []struct{ round, pass int }{{round: 1}, {round: 2}, {pass: 3}, {pass: 4}, {round: 1}, {round: 2}}

func (b *bench) extra(step int) []item {
	if s := steps[step]; s.pass > 0 {
		return b.undecided(s.pass)
	}
	var out []item
	for _, r := range b.restless(steps[step].round) {
		out = append(out, item{runs: []run{r}})
	}
	return out
}

func (b *bench) item(ctx context.Context, it item) error {
	var todo []run
	for _, r := range it.runs {
		f := b.files[r.id()]
		if f == nil {
			todo = append(todo, r)
			continue
		}
		b.remember(r)
		b.note(r, "skipped, its raw file is there: "+summary(f))
		if f.Verdict.Saturated {
			break
		}
	}
	if len(todo) == 0 {
		return nil
	}
	return b.fresh(ctx, todo)
}

func (b *bench) fresh(ctx context.Context, runs []run) error {
	if b.ran && !pause(ctx, b.pause()) {
		return ctx.Err()
	}
	b.ran = true
	if runs[0].target.kind == "postgres" {
		return b.onPostgres(ctx, runs)
	}
	return b.onLedger(ctx, runs)
}

func (r run) plan() load.Plan {
	return load.Plan{Rate: r.rate, Lanes: r.lanes, Warmup: r.timing.warm, Window: r.timing.window, MaxWindow: r.timing.longest, Drain: r.timing.drain}
}

func (b *bench) onLedger(ctx context.Context, runs []run) (err error) {
	first := runs[0]
	w, err := load.Named(first.workload)
	if err != nil {
		return err
	}
	if err := b.capTo(ctx, b.limit); err != nil {
		return err
	}

	if err := awaitMemory(ctx, b.root, []string{b.cgroup}, b.log); err != nil {
		return err
	}
	cfg := ledgerrun.Config{Bin: b.bins[first.build], Dir: b.dir, Nodes: first.target.nodes, Lanes: first.lanes, Pin: true, Env: first.env()}
	if first.build == "pprof" {
		cfg.Profiles = filepath.Join(b.dir, "prof", first.id())
		if err := os.MkdirAll(cfg.Profiles, 0o750); err != nil {
			return err
		}
	}
	fmt.Fprintf(b.log, "%s: start %d ledgerd\n", first.id(), first.target.nodes)
	c, err := ledgerrun.Start(ctx, cfg)
	if err != nil {
		return fmt.Errorf("%s: %w", first.id(), err)
	}
	defer func() {
		if cerr := c.Close(); cerr != nil {
			err = errors.Join(err, fmt.Errorf("%s: a node did not end cleanly, the plan stops: %w", first.id(), cerr))
		}
		if cfg.Profiles != "" && err == nil {
			b.traces(ctx, first, cfg)
		}
	}()
	if err := c.Setup(ctx, w); err != nil {
		return fmt.Errorf("%s: setup: %w", first.id(), err)
	}
	if first.fill > 0 {
		if err := c.Fill(ctx, first.fill); err != nil {
			return fmt.Errorf("%s: %w", first.id(), err)
		}
	}
	if first.stop {
		if err := c.Stop(stoppedNode); err != nil {
			return fmt.Errorf("%s: stop n%d: %w", first.id(), stoppedNode, err)
		}
		if err := oneLeader(ctx, c); err != nil {
			return fmt.Errorf("%s: %w", first.id(), err)
		}
	}
	watch := func() control.Watch { return control.Watch{Disk: b.disk, Pids: c.Pids(), CPUs: targetCPUs} }
	stream := load.NewStream(w, first.seed())
	for i, r := range runs {
		if i > 0 {
			if err := awaitMemory(ctx, b.root, []string{b.cgroup}, b.log); err != nil {
				return err
			}
		}
		p, need := r.plan(), r.timing.need
		if r.m != "m4" {
			fitted, err := c.Fit(ctx, p)
			if errors.Is(err, ledgerrun.ErrBudget) {
				b.note(r, fmt.Sprintf("not run, the chain ends before this stage: cut to the budget it would measure less than %v (%v)", ledgerrun.MinWindow, err))
				fmt.Fprintf(b.log, "%s: not run, no room for a window of %v\n", r.id(), ledgerrun.MinWindow)
				break
			}
			if err != nil {
				return fmt.Errorf("%s: %w", r.id(), err)
			}
			if fitted.Window != p.Window {
				need = max(1, int(fitted.Window/time.Second)/2)
			}
			p = fitted
		}
		var bounced sync.WaitGroup
		var bounceErr error
		if r.bounce {
			bounced.Go(func() { bounceErr = bounce(ctx, c, r.timing) })
		}
		settle := func(ctx context.Context) error {
			bounced.Wait()
			if bounceErr != nil {
				return bounceErr
			}
			if r.target.nodes > 1 {
				return c.Converge(ctx)
			}
			return nil
		}
		f, err := b.drive(ctx, r, c, watch, stream, p, need, settle)
		if err != nil {
			return err
		}
		if f.Verdict.Saturated {
			break
		}
	}
	return nil
}

func oneLeader(ctx context.Context, c *ledgerrun.Cluster) error {
	deadline := time.Now().Add(10 * time.Second)
	for {
		inside, err := c.Sample(ctx)
		leaders := 0
		for _, p := range inside {
			if p.Role == "leader" {
				leaders++
			}
		}
		if err == nil && leaders == 1 {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("no single leader 10s after stopping n%d: %d leaders, %v", stoppedNode, leaders, err)
		}
		if !pause(ctx, 100*time.Millisecond) {
			return ctx.Err()
		}
	}
}

func bounce(ctx context.Context, c *ledgerrun.Cluster, t timing) error {
	if !pause(ctx, t.stopAt) {
		return ctx.Err()
	}
	if err := c.Stop(stoppedNode); err != nil {
		return fmt.Errorf("stop n%d: %w", stoppedNode, err)
	}
	if !pause(ctx, t.restartAt-t.stopAt) {
		return ctx.Err()
	}
	if err := c.Restart(ctx, stoppedNode); err != nil {
		return fmt.Errorf("restart n%d: %w", stoppedNode, err)
	}
	return nil
}

func (b *bench) onPostgres(ctx context.Context, runs []run) error {
	w, err := load.Named(runs[0].workload)
	if err != nil {
		return err
	}
	stream := load.NewStream(w, runs[0].seed())
	watch := func() control.Watch {
		return control.Watch{Disk: b.disk, Cgroups: []string{b.srv.Cgroup()}, CPUs: targetCPUs}
	}
	if err := b.capTo(ctx, b.postgresLimit()); err != nil {
		return err
	}
	for _, r := range runs {
		if err := awaitMemory(ctx, b.root, []string{b.cgroup, b.srv.Cgroup()}, b.log); err != nil {
			return err
		}
		f, err := b.onPostgresStage(ctx, r, w, watch, stream)
		if err != nil || f.Verdict.Saturated {
			return err
		}
	}
	return nil
}

func (b *bench) onPostgresStage(ctx context.Context, r run, w load.Workload, watch func() control.Watch, stream *load.Stream) (f *raw.File, err error) {
	fmt.Fprintf(b.log, "%s: dial postgres\n", r.id())
	c, err := pg.Dial(ctx, b.srv, pg.Config{Variant: r.target.variant, Lanes: r.lanes})
	if err != nil {
		return nil, fmt.Errorf("%s: %w", r.id(), err)
	}
	defer func() {
		if cerr := c.Close(); cerr != nil {
			err = errors.Join(err, fmt.Errorf("%s: close: %w", r.id(), cerr))
		}
	}()
	if err := c.Setup(ctx, w); err != nil {
		return nil, fmt.Errorf("%s: setup: %w", r.id(), err)
	}
	if !b.described {
		if err := writeFile(filepath.Join(b.out, "postgres.txt"), postgresText(c.Describe())); err != nil {
			return nil, err
		}
		b.described = true
	}
	return b.drive(ctx, r, c, watch, stream, r.plan(), r.timing.need, nil)
}

func grew(f *raw.File, counter string) uint64 {
	var n uint64
	prev := f.Before
	for _, s := range f.Seconds {
		if s.Phase == load.Window.String() {
			for _, p := range s.Parts {
				for _, q := range prev {
					if q.Name == p.Name && p.Counters[counter] > q.Counters[counter] {
						n += p.Counters[counter] - q.Counters[counter]
					}
				}
			}
		}
		prev = s.Parts
	}
	return n
}

type sampled struct {
	ok, stale bool
}

type watched struct {
	load.Target
	seen []sampled
}

func (w *watched) Sample(ctx context.Context) (load.Inside, error) {
	in, err := w.Target.Sample(ctx)
	w.seen = append(w.seen, sampled{ok: err == nil, stale: slices.ContainsFunc(in, func(p load.Part) bool { return p.Stale })})
	return in, err
}

func enough(th control.Thresholds, secs []control.Second, seen []sampled, warm, need int) bool {
	unseen := func(sec int) bool {
		i := sec + 1
		return sec >= 0 && i < len(seen) && (!seen[i].ok || seen[i].stale)
	}
	marks := th.Judge(secs)
	th.Unpace(marks, secs, func(i int) bool { return i >= warm })
	for i := range marks {
		if unseen(i) || unseen(i-1) {
			marks[i].Valid = false
		}
	}
	return len(marks) > warm && control.Settle(marks[warm:], need) != control.Restless
}

type gauge struct {
	root  fs.FS
	watch control.Watch
	m     *control.Meter
	done  []control.Second
}

func newGauge(root fs.FS, w control.Watch) *gauge {
	return &gauge{root: root, watch: w, m: control.NewMeter(root, w)}
}

func (g *gauge) tick(pids []int) {
	g.m.Tick()
	if !slices.Equal(pids, g.watch.Pids) {
		g.done = append(g.done, g.m.Seconds()...)
		g.watch.Pids = pids
		g.m = control.NewMeter(g.root, g.watch)
	}
}

func (g *gauge) seconds() []control.Second {
	return slices.Concat(g.done, g.m.Seconds())
}

func (b *bench) drive(ctx context.Context, r run, t stand, watch func() control.Watch, s *load.Stream, p load.Plan, need int, settle func(context.Context) error) (*raw.File, error) {
	fmt.Fprintf(b.log, "%s: %d/s on %d lanes, window %v to %v\n", r.id(), p.Rate, p.Lanes, p.Window, p.MaxWindow)
	g := newGauge(b.root, watch())
	seen := &watched{Target: t}
	warm := int(p.Warmup / time.Second)
	p.Tick = func(int) bool {
		g.tick(watch().Pids)
		return enough(b.th, g.seconds(), seen.seen, warm, need)
	}
	driven, err := load.Drive(ctx, seen, s, p)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", r.id(), err)
	}
	var settled error
	if settle != nil {
		settled = settle(ctx)
	}
	tally, verr := t.Verify(ctx, driven.Books)
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	f, err := raw.Assemble(b.head(r, t.Describe(), p, need), driven, g.seconds())
	if err != nil {
		return nil, fmt.Errorf("%s: %w", r.id(), err)
	}
	f.Verified(tally, verr)
	var halt error
	if verr != nil {
		halt = fmt.Errorf("%w: %s: %w", errVerify, r.id(), verr)
	}
	if settled != nil {
		f.Refuse(whyNodes + ": " + settled.Error())
		if errors.Is(settled, ledgerrun.ErrDiverged) {
			halt = errors.Join(halt, fmt.Errorf("%w: %s: %w", errVerify, r.id(), settled))
		}
	}
	if !r.excused() && f.TermChanged() {
		f.Refuse(raw.WhyTerm)
	}
	if grew(f, "checkpoints") > 0 {
		f.Refuse(whyCheckpoint)
	}
	if grew(f, "wal_init_syncs") > 0 {
		f.Refuse(whySegment)
	}
	err = errors.Join(halt, b.keep(r, f))
	debug.FreeOSMemory()
	return f, err
}

func (b *bench) head(r run, t raw.Target, p load.Plan, need int) raw.Head {
	build := b.build
	build.Tags = r.tags()
	return raw.Head{
		ID:          r.id(),
		Measurement: r.m,
		Pass:        r.pass,
		Build:       build,
		Machine:     b.machine,
		Target:      t,
		Load: raw.Load{Workload: r.workload, Accounts: mustNamed(r.workload).Accounts, Rate: r.rate, Stage: r.stage, Stages: r.stages,
			Warmup: p.Warmup, Window: p.Window, MaxWindow: p.MaxWindow, Drain: p.Drain, Need: need, Seed: r.seed()},
		Probe:       b.probe,
		Thresholds:  b.th,
		AllowStalls: r.excused(),
	}
}

func mustNamed(name string) load.Workload {
	w, _ := load.Named(name)
	return w
}

func (b *bench) keep(r run, f *raw.File) error {
	name := filepath.Join(b.out, "raw", r.id()+".json")
	file, err := os.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644) //nolint:gosec // G302: raw files are for reading
	if err != nil {
		return err
	}
	if err := errors.Join(raw.Write(file, f), file.Close()); err != nil {
		return err
	}
	b.files[r.id()] = f
	b.remember(r)
	b.note(r, summary(f))
	fmt.Fprintf(b.log, "%s: %s\n", r.id(), summary(f))
	if err := b.writePlanText(); err != nil {
		return err
	}
	if b.smoke {
		return nil
	}
	return b.judgeBlocks(false)
}

func (b *bench) remember(r run) {
	slot := r
	slot.repeat = 0
	if !slices.ContainsFunc(b.slots, func(s run) bool { return s.id() == slot.id() }) {
		b.slots = append(b.slots, slot)
	}
}

func (b *bench) last(slot run) (run, *raw.File) {
	latest, f := slot, b.files[slot.id()]
	for k := 1; ; k++ {
		r := slot
		r.repeat = k
		g := b.files[r.id()]
		if g == nil {
			return latest, f
		}
		latest, f = r, g
	}
}

func repeatable(f *raw.File) bool {
	return slices.ContainsFunc(f.Verdict.Reasons, func(why string) bool {
		return why == raw.WhyRestless || why == whyCheckpoint || why == whySegment
	})
}

func (b *bench) restless(round int) []run {
	var out []run
	for _, slot := range b.slots {
		r, f := b.last(slot)
		if f != nil && !f.Verdict.Valid && repeatable(f) && r.repeat == round-1 {
			r.repeat = round
			out = append(out, r)
		}
	}
	return out
}

func (b *bench) judgeBlocks(final bool) error {
	planned := make(map[string]int)
	for _, it := range b.items {
		if b.selected(it) {
			n := len(it.runs)
			if it.chain() {
				n = min(n, minStages)
			}
			planned[it.runs[0].m] += n
		}
	}
	bad, made, limited, fresh := make(map[string]int), make(map[string]int), make(map[string]int), make(map[string]int)
	for _, slot := range b.slots {
		r, f := b.last(slot)
		made[slot.m]++
		switch {
		case f == nil || f.Verdict.Valid:
		case slices.Equal(f.Verdict.Reasons, []string{raw.WhyGenerator}):
			limited[slot.m]++
		case final || !repeatable(f) || r.repeat >= 2:
			bad[slot.m]++
			if !b.earlier[r.id()] {
				fresh[slot.m]++
			}
		}
	}
	if !final {
		for _, m := range []string{"m1", "m2"} {
			runs := max(planned[m], made[m])
			if runs > 0 && control.TooManyBad(bad[m], runs) && fresh[m] > 0 {
				return fmt.Errorf("%w: %s has %d invalid of %d", errTooManyBad, m, bad[m], runs)
			}
		}
		return nil
	}
	b.blocks = nil
	for _, m := range []string{"m1", "m2", "m3", "m4", "m5", "m7"} {
		if made[m] == 0 {
			continue
		}
		verdict := fmt.Sprintf("%s: %d of %d runs invalid", m, bad[m], made[m])
		if limited[m] > 0 {
			verdict += fmt.Sprintf(", %d more only because the generator was at its limit", limited[m])
		}
		if control.TooManyBad(bad[m], made[m]) {
			verdict += ", more than a quarter"
		}
		b.blocks = append(b.blocks, verdict)
	}
	return nil
}

func (b *bench) group(slot run) []*raw.File {
	var files []*raw.File
	for _, s := range b.slots {
		key := s
		key.pass, key.nr = slot.pass, slot.nr
		if s.m != slot.m || key.id() != slot.id() {
			continue
		}
		for k := 0; ; k++ {
			r := s
			r.repeat = k
			f := b.files[r.id()]
			if f == nil {
				break
			}
			files = append(files, f)
		}
	}
	slices.SortFunc(files, func(x, y *raw.File) int { return strings.Compare(x.ID, y.ID) })
	return files
}

func (b *bench) undecided(pass int) []item {
	var out []item
	for _, it := range b.items {
		r := it.runs[0]
		if r.pass != 1 || !b.selected(it) || !r.judged() {
			continue
		}
		var runs []run
		for _, s := range it.runs {
			s.pass, s.nr = pass, pass
			runs = append(runs, s)
		}
		if b.files[runs[0].id()] != nil {
			out = append(out, item{runs: runs})
			continue
		}
		var files []*raw.File
		for _, s := range it.runs {
			files = append(files, b.group(s)...)
		}
		if passes(files) == pass-1 && !settled(r.m, files) {
			out = append(out, item{runs: runs})
		}
	}
	return out
}

func (r run) judged() bool {
	return r.m == "m1" || r.m == "m2" && r.workload == "uniform-10k"
}

func passes(files []*raw.File) int {
	most := 0
	for _, f := range files {
		most = max(most, f.Pass)
	}
	return most
}

func bandOf(f *raw.File) raw.Band {
	switch f.Verdict.Speed {
	case control.Fast:
		return f.Bands.Fast
	case control.Unpaced:
		return f.Bands.Unpaced
	}
	return f.Bands.Slow
}

func figures(files []*raw.File, flat bool) []control.Figures {
	var figs []control.Figures
	for _, f := range files {
		v := f.Verdict
		band := bandOf(f)
		fig := control.Figures{Valid: v.Valid, Speed: v.Speed, Flush: band.Flush, Throughput: band.Throughput, P50: band.P50, Syncs: band.Syncs}
		if flat {
			fig.P50, fig.Syncs = 0, 0
		}
		figs = append(figs, fig)
	}
	return figs
}

func settled(m string, files []*raw.File) bool {
	if m == "m1" {
		if !slices.ContainsFunc(files, func(f *raw.File) bool { return f.Verdict.Valid }) {
			return false
		}
		byPass := make(map[int][]*raw.File)
		for _, f := range files {
			byPass[f.Pass] = append(byPass[f.Pass], f)
		}
		var firsts []*raw.File
		for _, stages := range byPass {
			slices.SortFunc(stages, func(x, y *raw.File) int { return strings.Compare(x.ID, y.ID) })
			lowest := 0
			for _, f := range stages {
				if f.Verdict.Saturated && (lowest == 0 || f.Load.Rate < lowest) {
					lowest = f.Load.Rate
				}
			}
			if lowest == 0 {
				continue
			}
			var first *raw.File
			for _, f := range stages {
				if f.Verdict.Valid && f.Load.Rate == lowest {
					first = f
				}
			}
			if first == nil || !first.Verdict.Saturated {
				return false
			}
			firsts = append(firsts, first)
		}
		return len(firsts) == 0 || saturatedSettled(firsts)
	}
	speed := control.None
	for _, f := range files {
		if v := f.Verdict; v.Valid && v.Speed.Rank() > speed.Rank() {
			speed = v.Speed
		}
	}
	held := slices.ContainsFunc(files, func(f *raw.File) bool {
		return f.Verdict.Valid && f.Verdict.Speed == speed && !f.Verdict.Saturated
	})
	if speed != control.None && !held {
		return saturatedSettled(files)
	}
	return control.Agree(figures(files, false)).Accord != control.Undecided
}

func saturatedSettled(files []*raw.File) bool {
	if slices.ContainsFunc(files, func(f *raw.File) bool { return f.Verdict.Valid && f.Verdict.Limit == raw.LimitGenerator }) {
		return true
	}
	slices.SortFunc(files, func(x, y *raw.File) int { return strings.Compare(x.ID, y.ID) })
	return control.Agree(figures(files, true)).Accord != control.Undecided
}

func (b *bench) traces(ctx context.Context, r run, cfg ledgerrun.Config) {
	for i := 1; i <= r.target.nodes; i++ {
		node := fmt.Sprintf("n%d", i)
		name := filepath.Join(b.out, "raw", r.id()+"."+node+".traces")
		var out, stderr bytes.Buffer
		cmd := exec.CommandContext(ctx, "go", "tool", "pprof", "-traces", cfg.Bin, filepath.Join(cfg.Profiles, node+".prof")) //nolint:gosec // G204: pprof over the profile of our own node
		cmd.Stdout, cmd.Stderr = &out, &stderr
		err := cmd.Run()
		if err == nil {
			err = writeFile(name, out.Bytes())
		}
		if err != nil {
			b.note(r, fmt.Sprintf("no traces for %s: %v %s", node, err, strings.TrimSpace(stderr.String())))
		}
	}
	if err := os.RemoveAll(cfg.Profiles); err != nil {
		b.note(r, "profiles not removed: "+err.Error())
	}
}

func writeFile(name string, content []byte) error {
	return os.WriteFile(name, content, 0o644) //nolint:gosec // G306: the files of a plan are for reading
}

func writeJSON(name string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return writeFile(name, append(b, '\n'))
}

func (b *bench) planFile() error {
	var buf bytes.Buffer
	if err := report.WritePlan(&buf, expected(b.items)); err != nil {
		return err
	}
	return writeFile(filepath.Join(b.out, report.PlanFile), buf.Bytes())
}
