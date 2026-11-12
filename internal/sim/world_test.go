package sim

import (
	"bytes"
	"cmp"
	"errors"
	"io"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/Demonisreal/dmn-ledger/internal/disk"
	"github.com/Demonisreal/dmn-ledger/internal/wal"
)

const (
	scribeFile  = "/s/f"
	scribeChunk = 4096
)

type scribe struct {
	f      disk.File
	ticks  int
	before []byte
}

func startScribe(env NodeEnv) (Process, error) {
	if err := env.FS.MkdirAll("/s"); err != nil {
		return nil, err
	}
	names, err := env.FS.List("/s")
	if err != nil {
		return nil, err
	}
	if len(names) == 0 {
		f, err := env.FS.Create(scribeFile)
		if err != nil {
			return nil, err
		}
		return &scribe{f: f}, env.FS.SyncDir("/s")
	}
	f, err := env.FS.Open(scribeFile)
	if err != nil {
		return nil, err
	}
	size, err := f.Size()
	if err != nil {
		return nil, err
	}
	before := make([]byte, size)
	if _, err := f.ReadAt(before, 0); err != nil {
		return nil, err
	}
	return &scribe{f: f, before: before}, nil
}

func (s *scribe) Tick() {
	s.ticks++
}

func (s *scribe) Receive(Msg) {}

func (s *scribe) Run() ([]Msg, error) {
	if s.ticks == 0 || s.ticks > 16 {
		return nil, nil
	}
	chunk := bytes.Repeat([]byte{byte(s.ticks)}, scribeChunk)
	if _, err := s.f.WriteAt(chunk, int64(s.ticks-1)*scribeChunk); err != nil {
		return nil, err
	}
	if s.ticks == 1 {
		return nil, s.f.Sync()
	}
	return nil, nil
}

func written(ticks int) []byte {
	var b []byte
	for i := 1; i <= ticks; i++ {
		b = append(b, bytes.Repeat([]byte{byte(i)}, scribeChunk)...)
	}
	return b
}

func runUntil(w *World, at uint64) {
	for w.err == nil && len(w.events) > 0 && w.events[0].at <= at {
		w.step()
	}
}

func world(t *testing.T, cfg Config, pl plan, seed uint64) (*World, *bytes.Buffer) {
	t.Helper()
	var buf bytes.Buffer
	cfg.Trace = &buf
	if cfg.Client == nil {
		cfg.Client = func(env ClientEnv) Process { return newEchoClient(env) }
	}
	return newWorld(cfg, rand.New(rand.NewPCG(seed, 0)), pl), &buf
}

func TestProcessRestartKeepsUnsyncedData(t *testing.T) {
	w, _ := world(t, Config{Nodes: 1, Node: startScribe}, testPlan(1, 10*second, 10*second), 5)
	runUntil(w, second)
	h := w.hosts[0]

	w.stop(h, false)
	w.start(h)
	if got := h.proc.(*scribe).before; !bytes.Equal(got, written(16)) {
		t.Fatalf("after process restart: %d bytes, want all %d written", len(got), 16*scribeChunk)
	}

	w.stop(h, true)
	w.start(h)
	got := h.proc.(*scribe).before
	if !bytes.HasPrefix(got, written(1)) {
		t.Fatal("power loss destroyed the synced first chunk")
	}
	if bytes.Equal(got, written(16)) {
		t.Fatal("power loss kept every unsynced chunk")
	}
}

type locker struct{ scribe }

func startLocker(env NodeEnv) (Process, error) {
	p, err := startScribe(env)
	if err != nil {
		return nil, err
	}
	if _, err := env.FS.Lock("/s"); err != nil {
		return nil, err
	}
	return &locker{*p.(*scribe)}, nil
}

func TestProcessRestartReleasesLock(t *testing.T) {
	w, trace := world(t, Config{Nodes: 1, Node: startLocker}, testPlan(1, 10*second, 10*second), 5)
	runUntil(w, 100*millisecond)
	h := w.hosts[0]
	w.stop(h, false)
	w.start(h)
	if h.proc == nil {
		t.Fatalf("restart after a process crash failed:\n%s", trace)
	}
}

func TestDiskTimeHoldsAndBatches(t *testing.T) {
	w, _ := world(t, Config{Nodes: 1, Clients: 4, Node: startEchoNode}, testPlan(1, second, second), 4)
	h := w.hosts[0]
	var held, batched int
	for len(w.events) > 0 && w.events[0].at <= w.plan.end {
		ev := w.events[0]
		queued := 0
		for _, in := range h.backlog {
			if !in.tick {
				queued++
			}
		}
		w.step()
		switch {
		case ev.kind == evWake && ev.id == 1 && queued > 0:
			if len(h.outbox) != queued {
				t.Fatalf("wake at %d: %d inputs waited, %d replies in one run", ev.at, queued, len(h.outbox))
			}
			batched++
		case ev.kind == evDeliver && ev.id == 1 && !slices.ContainsFunc(h.backlog, func(in input) bool { return !in.tick }):
			i := slices.IndexFunc(w.events, func(e event) bool { return e.kind == evWake && e.id == 1 })
			if !h.waiting || i < 0 || w.events[i].at < ev.at+fsyncMin || len(h.outbox) != 1 {
				t.Fatalf("request at %d did not wait for its fsync", ev.at)
			}
			held++
		}
	}
	if held == 0 || batched == 0 {
		t.Fatalf("%d replies held, %d batches; want both > 0", held, batched)
	}
}

func TestSlowWindowStretchesFsync(t *testing.T) {
	pl := testPlan(1, 3*second, 3*second)
	pl.windows = []window{{kind: slow, start: second, end: 2 * second, node: 1}}
	w, _ := world(t, Config{Nodes: 1, Clients: 3, Node: startEchoNode}, pl, 4)
	h := w.hosts[0]
	var began uint64
	var slowRuns, fastRuns int
	for len(w.events) > 0 && w.events[0].at <= w.plan.end {
		was := h.waiting
		ev := w.events[0]
		w.step()
		if !was && h.waiting {
			began = w.now
		}
		if ev.kind != evWake || ev.id != 1 {
			continue
		}
		gap := ev.at - began
		inside := began >= second && began < 2*second
		switch {
		case inside && gap < slowSyncMin:
			t.Fatalf("fsync at %d took %d ns inside the slow window", began, gap)
		case inside:
			slowRuns++
		case gap >= slowSyncMin:
			t.Fatalf("fsync at %d took %d ns outside the slow window", began, gap)
		default:
			fastRuns++
		}
	}
	if slowRuns == 0 || fastRuns == 0 {
		t.Fatalf("%d slow and %d fast runs", slowRuns, fastRuns)
	}
}

func TestEIOWindowsSetTheHighestRate(t *testing.T) {
	pl := testPlan(1, second, second)
	pl.windows = []window{{kind: eio, node: 1, rate: 0.02}, {kind: eio, node: 1, rate: 0.001}}
	w, _ := world(t, Config{Nodes: 1, Node: startScribe}, pl, 1)
	d := w.Disk(1)
	rates := func() []float64 { return []float64{d.WriteErrRate, d.SyncErrRate, d.MetaErrRate, d.ReadErrRate} }
	for _, step := range []struct {
		win  int
		heal bool
		want float64
	}{{0, false, 0.02}, {1, false, 0.02}, {0, true, 0.001}, {1, true, 0}} {
		w.fault(step.win, step.heal)
		for _, r := range rates() {
			if r != step.want {
				t.Fatalf("after %v of rate %v: rates %v, want %v", step.heal, pl.windows[step.win].rate, rates(), step.want)
			}
		}
	}
}

func TestEIOWindowStopsTheNode(t *testing.T) {
	pl := testPlan(1, 3*second, 3*second)
	pl.windows = []window{{kind: eio, start: second, end: 1500 * millisecond, node: 1, rate: 1}}
	w, trace := world(t, Config{Nodes: 1, Clients: 3, Node: startEchoNode}, pl, 2)
	if err := w.Run(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(trace.String(), " fail n1 ") || !strings.Contains(trace.String(), "input/output error") {
		t.Fatal("an EIO window with rate 1 did not stop the node")
	}
}

func midwayWorld(t *testing.T, win window, node func(NodeEnv) (Process, error), clients uint64) string {
	t.Helper()
	pl := testPlan(1, 4*second, 5*second)
	pl.windows = []window{win}
	w, trace := world(t, Config{Nodes: 1, Clients: clients, Node: node}, pl, 9)
	if err := w.Run(); err != nil {
		t.Fatal(err)
	}
	return trace.String()
}

func TestMidwayCrashesAgainAndAgain(t *testing.T) {
	trace := midwayWorld(t, window{kind: midway, start: 500 * millisecond, end: 3 * second, node: 1}, startEchoNode, 3)
	_, after, _ := strings.Cut(trace, " heal midway")
	fails := strings.Count(trace, " fail n1 ")
	if fails < 2 || strings.Contains(after, " fail n1 ") {
		t.Fatalf("n1 failed %d times in the window, after it %v\n%s", fails, strings.Contains(after, " fail n1 "), trace)
	}
	if strings.Count(trace, " arm n1 access ") < fails || !strings.Contains(trace, "input/output error") {
		t.Fatal("a failure did not come from an armed access")
	}
}

type poller struct{ f disk.File }

func startPoller(env NodeEnv) (Process, error) {
	if err := env.FS.MkdirAll("/q"); err != nil {
		return nil, err
	}
	f, err := env.FS.Create("/q/f")
	return &poller{f: f}, err
}

func (p *poller) Tick()       {}
func (p *poller) Receive(Msg) {}

func (p *poller) Run() ([]Msg, error) {
	_, err := p.f.Size()
	return nil, err
}

func TestMidwayDisarmsAtEnd(t *testing.T) {
	trace := midwayWorld(t, window{kind: midway, start: second, end: second + microsecond, node: 1}, startPoller, 0)
	if !strings.Contains(trace, " arm n1 access ") || !strings.Contains(trace, " disarm n1") || strings.Contains(trace, " fail ") {
		t.Fatalf("armed counter outlived its window\n%s", trace)
	}
}

func TestFsyncgateFailsASync(t *testing.T) {
	trace := midwayWorld(t, window{kind: fsyncgate, start: 500 * millisecond, end: 3 * second, node: 1}, startEchoNode, 3)
	_, failed, _ := strings.Cut(trace, " fail n1 ")
	failed, _, _ = strings.Cut(failed, "\n")
	if !strings.Contains(trace, " arm n1 sync ") || !strings.Contains(failed, ": sync /echo/log: input/output error") {
		t.Fatalf("fsyncgate did not fail a sync\n%s", trace)
	}
}

func TestCrashWindowRestartsAtItsEnd(t *testing.T) {
	pl := testPlan(1, 4*second, 4*second)
	pl.windows = []window{
		{kind: crash, start: second, end: 2 * second, node: 1},
		{kind: crash, start: 1500 * millisecond, end: 2500 * millisecond, node: 1},
	}
	w, trace := world(t, Config{Nodes: 1, Node: startScribe}, pl, 3)
	if err := w.Run(); err != nil {
		t.Fatal(err)
	}
	var starts []string
	for line := range strings.Lines(trace.String()) {
		if strings.HasSuffix(line, " start n1\n") {
			starts = append(starts, strings.Fields(line)[0])
		}
	}
	if !slices.Equal(starts, []string{"0", "2500000000"}) || !strings.Contains(trace.String(), "1500000000 kill n1 down") {
		t.Fatalf("starts at %v\n%s", starts, trace)
	}
}

func TestBlackoutCutsPowerEverywhere(t *testing.T) {
	pl := testPlan(3, 4*second, 4*second)
	pl.windows = []window{{kind: blackout, start: second, end: 1500 * millisecond, node: 1}}
	w, trace := world(t, Config{Nodes: 3, Node: startScribe}, pl, 7)
	runUntil(w, 1400*millisecond)
	for _, h := range w.nodes() {
		if h.proc != nil {
			t.Fatalf("%s still runs in the blackout", h.name)
		}
	}
	runUntil(w, 1500*millisecond+blackoutSpread)
	starts := make(map[string]bool)
	for _, h := range w.nodes() {
		s := h.proc.(*scribe)
		if !bytes.HasPrefix(s.before, written(1)) || bytes.Equal(s.before, written(16)) {
			t.Fatalf("%s came back with %d bytes; want the synced chunk and not every unsynced one", h.name, len(s.before))
		}
	}
	for line := range strings.Lines(trace.String()) {
		if f := strings.Fields(line); f[1] == "start" && f[0] != "0" {
			starts[f[0]] = true
		}
	}
	if len(starts) != 3 {
		t.Fatalf("restarts at %v, want three different times", starts)
	}
}

func TestKillDropsHeldOutput(t *testing.T) {
	w, trace := world(t, Config{Nodes: 1, Clients: 1, Node: startEchoNode}, testPlan(1, 5*second, 5*second), 4)
	h := w.hosts[0]
	for !h.waiting || len(h.outbox) == 0 {
		w.step()
	}
	killed := w.now
	mark := trace.Len()
	w.stop(h, false)
	w.start(h)
	runUntil(w, killed+50*millisecond)
	after := trace.String()[mark:]
	if strings.Contains(after, " send n1->") {
		t.Fatalf("output of the killed process left the node\n%s", after)
	}
	if n := strings.Count(after, " wake n1\n"); n != 1 {
		t.Fatalf("%d wake-ups after the restart, want only the new process's\n%s", n, after)
	}
}

func TestTicksFollowThePeriod(t *testing.T) {
	r := newEchoRun(t, 3, ProfileCrash, nil)
	seen := make(map[uint64]bool)
	for _, h := range r.w.hosts {
		if h.period < tick*8/10 || h.period > tick*12/10 {
			t.Fatalf("%s ticks every %d ns", h.name, h.period)
		}
		seen[h.period] = true
	}
	if len(seen) < len(r.w.hosts)-1 {
		t.Fatalf("only %d distinct tick periods for %d hosts", len(seen), len(r.w.hosts))
	}

	var buf bytes.Buffer
	r = newEchoRun(t, 3, ProfileCrash, &buf)
	for _, h := range r.w.hosts {
		ticks := strings.Count(buf.String(), " tick "+h.name+"\n")
		starts := strings.Count(buf.String(), " start "+h.name+"\n")
		if limit := int(r.w.plan.end/h.period) + starts; ticks > limit {
			t.Fatalf("%s ticked %d times, at most %d fit", h.name, ticks, limit)
		}
	}
}

func TestClockJump(t *testing.T) {
	clocks := make(map[uint64]func() uint64)
	node := func(env NodeEnv) (Process, error) {
		clocks[env.ID] = env.Clock
		return startScribe(env)
	}
	w, _ := world(t, Config{Nodes: 2, Node: node}, testPlan(2, second, second), 2)
	own, other := clocks[2], clocks[1]
	base, rest := own(), other()
	for _, step := range []struct {
		win        window
		heal       bool
		own, other uint64
	}{
		{window{kind: jump, node: 2, shift: 5 * second, back: true}, false, base - 5*second, rest},
		{window{kind: jump, node: 2, shift: 5 * second, back: true}, true, base, rest},
		{window{kind: jump, node: 2, shift: 3 * second}, false, base + 3*second, rest},
		{window{kind: jump, node: 2, shift: 3 * second, keep: true}, true, base + 3*second, rest},
	} {
		w.plan.windows = []window{step.win}
		w.fault(0, step.heal)
		if own() != step.own || other() != step.other {
			t.Fatalf("%+v heal %v: n2 reads %d want %d, n1 reads %d want %d", step.win, step.heal, own(), step.own, other(), step.other)
		}
	}
}

func TestProcessRandIsolated(t *testing.T) {
	hash := func(draws int) [32]byte {
		node := func(env NodeEnv) (Process, error) {
			for range draws {
				env.Rand.Uint64()
			}
			return startScribe(env)
		}
		w, _ := world(t, Config{Nodes: 3, Node: node}, testPlan(3, second, second), 8)
		if err := w.Run(); err != nil {
			t.Fatal(err)
		}
		return w.TraceHash()
	}
	if hash(0) != hash(1000) {
		t.Fatal("draws of a process changed the world's own randomness")
	}
}

func TestJournalRateComesFromThePlan(t *testing.T) {
	var zero, set int
	for seed := range uint64(20) {
		cfg, _ := echoConfig(seed, ProfileCrash, nil)
		w, err := New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		for _, h := range w.nodes() {
			if got := w.Disk(h.id).JournalRate; got != w.plan.journal[h.id-1] {
				t.Fatalf("seed %d: %s journal rate %v, plan says %v", seed, h.name, got, w.plan.journal[h.id-1])
			}
			if w.plan.journal[h.id-1] == 0 {
				zero++
			} else {
				set++
			}
		}
	}
	if zero == 0 || set == 0 {
		t.Fatalf("%d disks without journal, %d with; want both", zero, set)
	}
}

func TestQuietStartsEveryNode(t *testing.T) {
	w, _ := world(t, Config{Nodes: 3, Node: startScribe}, testPlan(3, second, 2*second), 6)
	runUntil(w, second-1)
	w.fail(w.hosts[1], errors.New("probe"))
	runUntil(w, second)
	for _, h := range w.hosts {
		if h.proc == nil {
			t.Fatalf("%s still down when the quiet phase began", h.name)
		}
	}
}

func TestFailStopRestarts(t *testing.T) {
	w, _ := world(t, Config{Nodes: 1, Node: startScribe}, testPlan(1, 10*second, 10*second), 6)
	runUntil(w, second)
	w.fail(w.hosts[0], errors.New("probe"))
	runUntil(w, second+restartMin+restartSpan)
	if w.hosts[0].proc == nil {
		t.Fatal("node did not restart after a fail-stop")
	}
}

type sender struct{ msgs []Msg }

func (s *sender) Tick()               {}
func (s *sender) Receive(Msg)         {}
func (s *sender) Run() ([]Msg, error) { return s.msgs, nil }

func TestBrokenMessagesStopTheRun(t *testing.T) {
	for _, m := range []Msg{{To: 0, Body: &ping{}}, {To: 4, Body: &ping{}}, {To: 2}} {
		node := func(env NodeEnv) (Process, error) { return &sender{msgs: []Msg{m}}, nil }
		cfg := Config{Seed: 42, Nodes: 3, Node: node}
		w, _ := world(t, cfg, testPlan(3, second, second), 1)
		err := w.Run()
		if err == nil || !strings.Contains(err.Error(), "seed 42") {
			t.Fatalf("message %+v: run error %v", m, err)
		}
	}
}

func TestClientErrorStopsTheRun(t *testing.T) {
	client := func(env ClientEnv) Process { return &failing{} }
	w, _ := world(t, Config{Nodes: 1, Clients: 1, Node: startScribe, Client: client}, testPlan(1, second, second), 1)
	if err := w.Run(); err == nil || !strings.Contains(err.Error(), "client c1") {
		t.Fatalf("run error %v", err)
	}
}

type failing struct{ sender }

func (f *failing) Run() ([]Msg, error) { return nil, errors.New("broken") }

func TestObserverHooks(t *testing.T) {
	var said, checks int
	cfg := Config{
		Nodes:   1,
		Clients: 2,
		Node:    startEchoNode,
		Said: func(id uint64, msgs []Msg) error {
			if id == 1 && len(msgs) > 0 {
				said += len(msgs)
			}
			return nil
		},
		Check: func() error { checks++; return nil },
	}
	w, trace := world(t, cfg, testPlan(1, second, second), 3)
	if err := w.Run(); err != nil {
		t.Fatal(err)
	}
	if sent := strings.Count(trace.String(), " send n1->") + len(w.hosts[0].outbox); said == 0 || said != sent {
		t.Fatalf("Said saw %d replies, the node sent or holds %d", said, sent)
	}
	if uint64(checks) != w.seq-uint64(len(w.events)) {
		t.Fatalf("Check ran %d times for %d events", checks, w.seq-uint64(len(w.events)))
	}

	cfg.Said = func(uint64, []Msg) error { return errors.New("two leaders") }
	w, _ = world(t, cfg, testPlan(1, second, second), 3)
	if err := w.Run(); err == nil || !strings.Contains(err.Error(), "two leaders") {
		t.Fatalf("checker error got lost: %v", err)
	}
}

type liar struct{ disk.FS }

type liarFile struct{ disk.File }

func (l liar) Create(name string) (disk.File, error) {
	f, err := l.FS.Create(name)
	return liarFile{f}, err
}

func (liarFile) Sync() error { return nil }

func TestDiskSeamSitsBeforeThePlatter(t *testing.T) {
	cfg := Config{Nodes: 1, Node: startScribe, Disk: func(id uint64, fsys disk.FS) disk.FS { return liar{fsys} }}
	w, _ := world(t, cfg, testPlan(1, 10*second, 10*second), 5)
	runUntil(w, second)
	h := w.hosts[0]
	w.stop(h, true)
	w.start(h)
	if bytes.HasPrefix(h.proc.(*scribe).before, written(1)) {
		t.Fatal("a sync that lied still made the first chunk durable")
	}
}

type walNode struct {
	log   *wal.WAL
	ticks uint64
}

func startWALNode(env NodeEnv) (Process, error) {
	log, err := wal.Open(env.FS, "/wal", wal.Options{Nonces: env.Nonces})
	return &walNode{log: log}, err
}

func (n *walNode) Tick()       { n.ticks++ }
func (n *walNode) Receive(Msg) {}

func (n *walNode) Run() ([]Msg, error) {
	if n.ticks != 1 {
		return nil, nil
	}
	if err := n.log.Append(wal.Entry{Index: 1, Term: 1, Kind: 1, Body: []byte("x")}); err != nil {
		return nil, err
	}
	return nil, n.log.Sync()
}

func TestWALNoncesAreSeeded(t *testing.T) {
	segment := func(seed uint64) []byte {
		w, err := New(Config{Seed: seed, Nodes: 1, Node: startWALNode})
		if err != nil {
			t.Fatal(err)
		}
		if err := w.Run(); err != nil {
			t.Fatal(err)
		}
		d := w.Disk(1)
		names, err := d.List("/wal")
		if err != nil || len(names) != 1 {
			t.Fatalf("segments %v, %v", names, err)
		}
		return contents(t, d, "/wal/"+names[0])
	}
	if a, b := segment(1), segment(1); !bytes.Equal(a, b) {
		t.Fatal("same seed, different segment bytes: the WAL drew a nonce from outside the seed")
	}
	if bytes.Equal(segment(1), segment(2)) {
		t.Fatal("different seeds wrote the same segment nonce")
	}
}

func contents(t *testing.T, fsys disk.FS, name string) []byte {
	t.Helper()
	f, err := fsys.Open(name)
	if err != nil {
		t.Fatal(err)
	}
	size, err := f.Size()
	if err != nil {
		t.Fatal(err)
	}
	b := make([]byte, size)
	if _, err := f.ReadAt(b, 0); err != nil && !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	return b
}

func TestProfiles(t *testing.T) {
	if _, err := New(Config{Nodes: 3, Profile: "storm"}); err == nil {
		t.Fatal("unknown profile accepted")
	}
	w, err := New(Config{Nodes: 3, Node: startScribe, Client: func(env ClientEnv) Process { return newEchoClient(env) }})
	if err != nil || len(w.plan.windows) != 0 || w.plan.rearm {
		t.Fatalf("empty profile is not calm: %v, %d windows", err, len(w.plan.windows))
	}
	if _, err := New(Config{}); err == nil {
		t.Fatal("world without nodes accepted")
	}
}

func TestPlanSplitsClients(t *testing.T) {
	var with, against int
	for seed := range uint64(200) {
		pl, err := newPlan(rand.New(rand.NewPCG(seed, 1)), ProfileNet, 3, 6)
		if err != nil {
			t.Fatal(err)
		}
		for _, win := range pl.windows {
			if win.kind != split && win.kind != oneway {
				continue
			}
			for _, a := range win.side[3:] {
				if a == win.side[win.node-1] {
					with++
				} else {
					against++
				}
			}
		}
	}
	if with == 0 || against == 0 {
		t.Fatalf("clients with the cut node %d, against it %d", with, against)
	}
}

func TestRestartArmsTheCounter(t *testing.T) {
	pl := testPlan(1, 100*second, 100*second)
	pl.rearm = true
	w, trace := world(t, Config{Nodes: 1, Clients: 2, Node: startEchoNode}, pl, 12)
	for i := range uint64(60) {
		runUntil(w, (i+1)*second)
		if h := w.hosts[0]; h.proc != nil {
			w.stop(h, false)
			w.start(h)
		}
	}
	starts := strings.Count(trace.String(), " start n1\n")
	arms := strings.Count(trace.String(), " arm n1 access ")
	if arms < starts/5 || arms > starts/2 {
		t.Fatalf("%d of %d starts armed the counter, want about a third", arms, starts)
	}
	inStart := 0
	lines := strings.Split(trace.String(), "\n")
	for i := 2; i < len(lines); i++ {
		if strings.HasSuffix(lines[i-2], " start n1") && strings.Contains(lines[i-1], " arm n1 ") && strings.Contains(lines[i], " fail n1 ") {
			inStart++
		}
	}
	if inStart == 0 {
		t.Fatal("no restart failed inside its own recovery")
	}
}

func TestRestartKeepsOneTickChain(t *testing.T) {
	w, trace := world(t, Config{Nodes: 1, Node: startScribe}, testPlan(1, 10*second, 10*second), 2)
	runUntil(w, 100*millisecond)
	h := w.hosts[0]
	w.stop(h, false)
	w.start(h)
	from, mark := w.now, trace.Len()
	runUntil(w, from+second)
	if ticks := strings.Count(trace.String()[mark:], " tick n1\n"); uint64(ticks) > second/h.period+1 {
		t.Fatalf("%d ticks in one second at a period of %d ns", ticks, h.period)
	}
}

func TestQueueOrder(t *testing.T) {
	rng := rand.New(rand.NewPCG(3, 3))
	var q queue
	var want []event
	for seq := range uint64(500) {
		ev := event{at: rng.Uint64N(20), seq: seq}
		q.push(ev)
		want = append(want, ev)
	}
	slices.SortStableFunc(want, func(a, b event) int { return cmp.Compare(a.at, b.at) })
	for i, ev := range want {
		if got := q.pop(); got.at != ev.at || got.seq != ev.seq {
			t.Fatalf("pop %d = (%d, %d), want (%d, %d)", i, got.at, got.seq, ev.at, ev.seq)
		}
	}
	if len(q) != 0 {
		t.Fatalf("%d events left", len(q))
	}
}

type counter struct {
	clock             func() uint64
	ticks, msgs, runs int
	ran               []uint64
}

func (c *counter) Tick()       { c.ticks++ }
func (c *counter) Receive(Msg) { c.msgs++ }

func (c *counter) Run() ([]Msg, error) {
	c.runs++
	c.ran = append(c.ran, c.clock())
	return nil, nil
}

func counterWorld(t *testing.T, pl plan, nodes, clients uint64, leading func(w *World, id uint64) (uint64, bool)) (*World, *bytes.Buffer) {
	t.Helper()
	var w *World
	cfg := Config{Nodes: nodes, Clients: clients, Node: func(env NodeEnv) (Process, error) {
		return &counter{clock: env.Clock}, nil
	}}
	if leading != nil {
		cfg.Leading = func(id uint64) (uint64, bool) { return leading(w, id) }
	}
	w, trace := world(t, cfg, pl, 5)
	return w, trace
}

func counted(w *World, id uint64) *counter {
	c, _ := w.hosts[id-1].proc.(*counter)
	return c
}

func TestPauseFreezesTheNode(t *testing.T) {
	pl := testPlan(1, 4*second, 4*second)
	pl.windows = []window{{kind: pause, start: second, end: 1500 * millisecond, node: 1}}
	w, trace := counterWorld(t, pl, 1, 2, nil)
	runUntil(w, second)
	c := counted(w, 1)
	before := *c
	runUntil(w, 1500*millisecond-1)
	if c.ticks != before.ticks || c.msgs != before.msgs || c.runs != before.runs {
		t.Fatalf("paused node worked: %d ticks, %d messages, %d runs, before %d, %d, %d", c.ticks, c.msgs, c.runs, before.ticks, before.msgs, before.runs)
	}
	queued := len(w.hosts[0].backlog)
	runUntil(w, 1500*millisecond)
	if c.ticks != before.ticks+1 || c.msgs != before.msgs+queued || c.runs != before.runs+1 || queued == 0 {
		t.Fatalf("after the thaw %d ticks, %d messages, %d runs; want one tick and the %d queued messages in one run", c.ticks-before.ticks, c.msgs-before.msgs, c.runs-before.runs, queued)
	}
	if !strings.Contains(trace.String(), "1000000000 freeze n1\n") || !strings.Contains(trace.String(), "1500000000 thaw n1\n") {
		t.Fatalf("no freeze or thaw in the trace\n%s", trace)
	}
}

func TestPauseCrawls(t *testing.T) {
	pl := testPlan(1, 4*second, 4*second)
	pl.windows = []window{{kind: pause, start: second, end: 2 * second, node: 1, step: 100 * millisecond}}
	w, trace := counterWorld(t, pl, 1, 0, nil)
	runUntil(w, second)
	c := counted(w, 1)
	runs, ticks := c.runs, c.ticks
	runUntil(w, 2*second-1)
	nudges := strings.Count(trace.String(), " nudge n1\n")
	if nudges != 9 || c.runs-runs != nudges || c.ticks-ticks != nudges {
		t.Fatalf("%d nudges, %d runs, %d ticks in the window; want 9 of each", nudges, c.runs-runs, c.ticks-ticks)
	}
}

func TestPauseHitsTheLeader(t *testing.T) {
	pl := testPlan(3, 4*second, 4*second)
	pl.windows = []window{{kind: pause, start: second, end: 2 * second, node: 1, lead: true}}
	w, trace := counterWorld(t, pl, 3, 0, func(_ *World, id uint64) (uint64, bool) { return 0, id == 2 })
	runUntil(w, 2*second)
	if !strings.Contains(trace.String(), " freeze n2\n") || !strings.Contains(trace.String(), " thaw n2\n") {
		t.Fatalf("the pause missed the leader\n%s", trace)
	}
}

func TestLeaderCrash(t *testing.T) {
	cases := []struct {
		name    string
		rise    bool
		leading func(w *World, id uint64) (uint64, bool)
		kill    bool
	}{
		{"at once", false, func(_ *World, id uint64) (uint64, bool) { return 0, id == 3 }, true},
		{"on commit", true, func(w *World, id uint64) (uint64, bool) {
			if w.now < second {
				return 0, id == 3
			}
			return uint64(counted(w, id).ticks), id == 3
		}, true},
		{"commit stands still", true, func(_ *World, id uint64) (uint64, bool) { return 7, id == 3 }, false},
		{"no leader", false, func(*World, uint64) (uint64, bool) { return 0, false }, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pl := testPlan(3, 4*second, 4*second)
			pl.windows = []window{{kind: leadercrash, start: second, end: 2 * second, node: 1, rise: tc.rise}}
			w, trace := counterWorld(t, pl, 3, 0, tc.leading)
			if err := w.Run(); err != nil {
				t.Fatal(err)
			}
			var kills, starts []uint64
			for line := range strings.Lines(trace.String()) {
				f := strings.Fields(line)
				at, _ := strconv.ParseUint(f[0], 10, 64)
				switch {
				case f[1] == "kill" && f[2] == "n3":
					kills = append(kills, at)
				case f[1] == "start" && f[2] == "n3" && at > 0:
					starts = append(starts, at)
				}
			}
			switch {
			case !tc.kill && len(kills) > 0:
				t.Fatalf("killed n3 at %v", kills)
			case !tc.kill:
			case len(kills) != 1 || len(starts) != 1 || starts[0] != 2*second:
				t.Fatalf("kills %v, restarts %v\n%s", kills, starts, trace)
			case tc.rise && (kills[0] == second || kills[0] > second+tick*12/10):
				t.Fatalf("killed at %d, want at the first tick after %d", kills[0], second)
			case !tc.rise && kills[0] != second:
				t.Fatalf("killed at %d, want %d", kills[0], second)
			}
		})
	}
}
