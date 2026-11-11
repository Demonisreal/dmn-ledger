package sim

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"

	"github.com/Demonisreal/dmn-ledger/internal/disk"
)

type ping struct {
	Seq  uint64
	Sent uint64
	Tag  []byte
}

func (p *ping) Clone() Body {
	c := *p
	c.Tag = slices.Clone(p.Tag)
	return &c
}

func (p *ping) String() string {
	return fmt.Sprintf("ping %d tag %x", p.Seq, p.Tag)
}

type arrival struct {
	from, to uint64
	seq      uint64
	sent, at uint64
	tag      []byte
}

func (a arrival) lag() uint64 {
	return a.at - a.sent
}

type pinger struct {
	id    uint64
	peers []uint64
	burst int
	now   func() uint64
	log   *[]arrival
	f     disk.File
	seq   uint64
	last  []*ping
}

func (p *pinger) Tick() {
	for _, b := range p.last {
		b.Tag[0] = 0xff
	}
	p.last = p.last[:0]
	for range p.burst {
		p.seq++
		for range p.peers {
			p.last = append(p.last, &ping{Seq: p.seq, Sent: p.now(), Tag: binary.BigEndian.AppendUint64(nil, p.seq)})
		}
	}
}

func (p *pinger) Receive(m Msg) {
	b := m.Body.(*ping)
	*p.log = append(*p.log, arrival{from: m.From, to: p.id, seq: b.Seq, sent: b.Sent, at: p.now(), tag: slices.Clone(b.Tag)})
	b.Tag[1] = 0xee
	for _, sent := range p.last {
		sent.Tag[0] = 0xff
	}
}

func (p *pinger) Run() ([]Msg, error) {
	var out []Msg
	if len(p.last) > 0 && p.last[0].Sent == p.now() {
		for i, b := range p.last {
			out = append(out, Msg{To: p.peers[i%len(p.peers)], Body: b})
		}
	}
	if p.f != nil && len(out) > 0 {
		if _, err := p.f.WriteAt([]byte{1}, 0); err != nil {
			return nil, err
		}
		return out, p.f.Sync()
	}
	return out, nil
}

func testPlan(nodes, quiet, end uint64) plan {
	return plan{quiet: quiet, end: end, journal: make([]float64, nodes)}
}

type pingRun struct {
	log   []arrival
	trace string
}

func pingWorld(t *testing.T, pl plan, burst int) pingRun {
	return pingWorldOn(t, pl, burst, false)
}

func pingWorldOn(t *testing.T, pl plan, burst int, syncing bool) pingRun {
	t.Helper()
	var r pingRun
	var buf bytes.Buffer
	var w *World
	node := func(env NodeEnv) (Process, error) {
		peers := slices.DeleteFunc(env.Peers, func(id uint64) bool { return id == env.ID })
		p := &pinger{id: env.ID, peers: peers, burst: burst, now: func() uint64 { return w.now }, log: &r.log}
		if syncing {
			if err := env.FS.MkdirAll("/p"); err != nil {
				return nil, err
			}
			f, err := env.FS.Create("/p/f")
			if err != nil {
				return nil, err
			}
			p.f = f
		}
		return p, nil
	}
	w = newWorld(Config{Nodes: 3, Node: node, Trace: &buf}, rand.New(rand.NewPCG(11, 0)), pl)
	if err := w.Run(); err != nil {
		t.Fatal(err)
	}
	r.trace = buf.String()
	return r
}

func isolated(node uint64, start, end uint64) window {
	side := make([]bool, 3)
	side[node-1] = true
	return window{kind: isolate, start: start, end: end, node: node, side: side}
}

func TestPartitionHoldsMessages(t *testing.T) {
	win := isolated(1, second, 2*second)
	pl := testPlan(3, 3*second, 4*second)
	pl.windows = []window{win}
	during := func(ts uint64) bool { return ts >= win.start && ts < win.end }
	var inside, after int
	for _, a := range pingWorld(t, pl, 1).log {
		cross := a.from == 1 || a.to == 1
		switch {
		case cross && during(a.sent):
			t.Fatalf("n%d->n%d ping %d sent %d arrived %d across the partition", a.from, a.to, a.seq, a.sent, a.at)
		case cross && during(a.at):
			t.Fatalf("n%d->n%d ping %d arrived %d inside the partition", a.from, a.to, a.seq, a.at)
		case !cross && during(a.at):
			inside++
		case cross && a.sent >= win.end:
			after++
		}
	}
	if inside == 0 || after == 0 {
		t.Fatalf("inside %d, across after heal %d; want both > 0", inside, after)
	}
}

func TestOneWayPartition(t *testing.T) {
	win := isolated(1, second, 2*second)
	win.kind = oneway
	pl := testPlan(3, 3*second, 4*second)
	pl.windows = []window{win}
	var in int
	for _, a := range pingWorld(t, pl, 1).log {
		during := a.sent >= win.start && a.sent < win.end
		if during && a.from == 1 {
			t.Fatalf("n1->n%d ping %d passed the one-way cut", a.to, a.seq)
		}
		if during && a.to == 1 {
			in++
		}
	}
	if in == 0 {
		t.Fatal("nothing reached n1 during the one-way cut")
	}
}

func TestOverlappingPartitionsHealLast(t *testing.T) {
	pl := testPlan(3, 4*second, 5*second)
	pl.windows = []window{isolated(1, second, 3*second), isolated(1, 2*second, 2500*millisecond)}
	for _, a := range pingWorld(t, pl, 1).log {
		if (a.from == 1 || a.to == 1) && a.sent >= 2500*millisecond && a.sent < 3*second {
			t.Fatalf("n%d->n%d ping %d sent %d crossed while the outer partition still held", a.from, a.to, a.seq, a.sent)
		}
	}
}

func TestBridgeCutsOnePair(t *testing.T) {
	win := window{kind: bridge, start: second, end: 2 * second, node: 1, peer: 3}
	pl := testPlan(3, 3*second, 4*second)
	pl.windows = []window{win}
	via := make(map[[2]uint64]int)
	for _, a := range pingWorld(t, pl, 1).log {
		if a.sent < win.start || a.sent >= win.end {
			continue
		}
		if a.from+a.to == 4 {
			t.Fatalf("n%d->n%d ping %d crossed the bridge cut", a.from, a.to, a.seq)
		}
		via[[2]uint64{a.from, a.to}]++
	}
	for _, pair := range [][2]uint64{{1, 2}, {2, 1}, {2, 3}, {3, 2}} {
		if via[pair] == 0 {
			t.Fatalf("nothing from n%d to n%d while only n1 and n3 were cut", pair[0], pair[1])
		}
	}
}

func TestPartitionHoldsSomeInFlight(t *testing.T) {
	pl := testPlan(3, 9*second, 10*second)
	for i := range uint64(40) {
		pl.windows = append(pl.windows, isolated(1+i%3, 100*millisecond+i*200*millisecond, 100*millisecond+i*200*millisecond+50*millisecond))
	}
	trace := pingWorld(t, pl, 4).trace
	if strings.Count(trace, " cut ") == 0 {
		t.Fatal("no message was dropped at a partition")
	}
	for line := range strings.Lines(trace) {
		route, until, ok := strings.Cut(strings.TrimSpace(line), " until ")
		if _, held, _ := strings.Cut(route, " hold "); ok && strings.Contains(trace, "\n"+until+" deliver "+held+"\n") {
			return
		}
	}
	t.Fatal("no held message arrived after its partition healed")
}

func TestDuplicatesAndReordering(t *testing.T) {
	pl := testPlan(3, 4*second, 4*second)
	pl.dup = 1
	type pair struct{ from, to uint64 }
	type key struct {
		pair
		seq uint64
	}
	seen := make(map[key]bool)
	top := make(map[pair]uint64)
	var dups, reordered int
	log := pingWorld(t, pl, 1).log
	for _, a := range log {
		p := pair{a.from, a.to}
		if seen[key{p, a.seq}] {
			dups++
			continue
		}
		seen[key{p, a.seq}] = true
		if a.seq < top[p] {
			reordered++
		}
		top[p] = max(top[p], a.seq)
	}
	if dups == 0 || reordered == 0 {
		t.Fatalf("%d duplicates, %d reordered of %d arrivals; want both > 0", dups, reordered, len(log))
	}
}

func TestJitterReordersABurst(t *testing.T) {
	top := make(map[[2]uint64]uint64)
	var reordered int
	for _, a := range pingWorld(t, testPlan(3, 2*second, 2*second), 4).log {
		p := [2]uint64{a.from, a.to}
		if a.seq < top[p] && a.lag() < lagMin {
			reordered++
		}
		top[p] = max(top[p], a.seq)
	}
	if reordered == 0 {
		t.Fatal("jitter never reordered messages sent in the same tick")
	}
}

func TestLatencyOutliers(t *testing.T) {
	var lagged, stalled, lateStall int
	quiet := 30 * second
	for _, a := range pingWorld(t, testPlan(3, quiet, quiet+5*second), 1).log {
		switch {
		case a.lag() >= stallMin && a.sent >= quiet:
			lateStall++
		case a.lag() >= stallMin:
			stalled++
		case a.lag() >= lagMin:
			lagged++
		}
	}
	if lagged == 0 || stalled == 0 || lateStall > 0 {
		t.Fatalf("%d lagged, %d stalled in the disturbance, %d stalled in the quiet phase", lagged, stalled, lateStall)
	}
}

func TestLossHappens(t *testing.T) {
	pl := testPlan(3, 2*second, 2*second)
	pl.loss = 1
	r := pingWorld(t, pl, 1)
	lost := strings.Count(r.trace, " lose ")
	if lost == 0 || len(r.log) == 0 {
		t.Fatalf("%d lost, %d arrived; want both > 0", lost, len(r.log))
	}
}

func TestMessagesAreCopied(t *testing.T) {
	for _, syncing := range []bool{false, true} {
		pl := testPlan(3, 2*second, 2*second)
		pl.dup = 1
		log := pingWorldOn(t, pl, 1, syncing).log
		for _, a := range log {
			if want := binary.BigEndian.AppendUint64(nil, a.seq); !bytes.Equal(a.tag, want) {
				t.Fatalf("syncing %v: n%d->n%d ping %d arrived with tag %x, want %x", syncing, a.from, a.to, a.seq, a.tag, want)
			}
		}
		if len(log) == 0 {
			t.Fatal("no arrivals")
		}
	}
}
