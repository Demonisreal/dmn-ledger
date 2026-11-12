package sim

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"io"
	"math/rand/v2"
	"path/filepath"
	"slices"
	"strings"

	"github.com/Demonisreal/dmn-ledger/internal/disk"
)

type Process interface {
	Tick()
	Receive(m Msg)
	Run() ([]Msg, error)
}

type NodeEnv struct {
	ID     uint64
	Peers  []uint64
	FS     disk.FS
	Clock  func() uint64
	Rand   *rand.Rand
	Nonces io.Reader
}

type ClientEnv struct {
	ID    uint64
	Nodes []uint64
	Now   func() uint64
	Rand  *rand.Rand
}

type Config struct {
	Seed    uint64
	Profile Profile
	Nodes   uint64
	Clients uint64
	Node    func(NodeEnv) (Process, error)
	Client  func(ClientEnv) Process
	Disk    func(id uint64, fsys disk.FS) disk.FS
	Said    func(id uint64, msgs []Msg) error
	Check   func() error
	Trace   io.Writer
}

type World struct {
	cfg    Config
	now    uint64
	seq    uint64
	calm   bool
	events queue
	plan   plan
	net    network
	hosts  []*host
	peers  []uint64
	hash   hash.Hash
	out    io.Writer
	line   []byte
	err    error
}

type host struct {
	id      uint64
	name    string
	node    bool
	proc    Process
	epoch   uint64
	period  uint64
	rng     *rand.Rand
	disk    *disk.Sim
	meter   *meter
	fsys    disk.FS
	clock   uint64
	crashes int
	midways int
	eio     []float64
	waiting bool
	outbox  []Msg
	backlog []input
}

type input struct {
	tick bool
	msg  Msg
}

func (in input) feed(p Process) {
	if in.tick {
		p.Tick()
	} else {
		p.Receive(in.msg)
	}
}

func New(cfg Config) (*World, error) {
	if cfg.Nodes == 0 {
		return nil, errors.New("sim: need at least one node")
	}
	root := pcg(cfg.Seed, 0)
	pl, err := newPlan(fork(root), cfg.Profile, cfg.Nodes, cfg.Nodes+cfg.Clients)
	if err != nil {
		return nil, err
	}
	return newWorld(cfg, root, pl), nil
}

func newWorld(cfg Config, root *rand.Rand, pl plan) *World {
	w := &World{cfg: cfg, plan: pl, hash: sha256.New()}
	w.out = w.hash
	if cfg.Trace != nil {
		w.out = io.MultiWriter(w.hash, cfg.Trace)
	}
	w.net = newNetwork(fork(root), cfg.Nodes+cfg.Clients, pl.loss, pl.dup)
	for range cfg.Nodes + cfg.Clients {
		h := &host{id: uint64(len(w.hosts)) + 1, rng: fork(root)}
		h.node = h.id <= cfg.Nodes
		h.period = tick*8/10 + h.rng.Uint64N(tick*4/10+1)
		if h.node {
			h.name = fmt.Sprintf("n%d", h.id)
			h.disk = disk.NewSim(fork(h.rng))
			h.disk.JournalRate = pl.journal[h.id-1]
			h.meter = &meter{fsys: h.disk, rng: fork(h.rng)}
			h.fsys = h.meter
			if cfg.Disk != nil {
				h.fsys = cfg.Disk(h.id, h.meter)
			}
			h.clock = clockBase + h.rng.Uint64N(clockSkew)
			w.peers = append(w.peers, h.id)
		} else {
			h.name = fmt.Sprintf("c%d", h.id-cfg.Nodes)
		}
		w.hosts = append(w.hosts, h)
	}
	for _, h := range w.nodes() {
		w.trace("plan %s journal %.2f", h.name, h.disk.JournalRate)
	}
	for i := range pl.windows {
		w.trace("plan %s", w.describe(&pl.windows[i]))
		w.push(event{at: pl.windows[i].start, kind: evFault, win: i})
		w.push(event{at: pl.windows[i].end, kind: evHeal, win: i})
	}
	w.push(event{at: pl.quiet, kind: evQuiet})
	for _, h := range w.hosts {
		w.start(h)
	}
	return w
}

func fork(r *rand.Rand) *rand.Rand {
	return pcg(r.Uint64(), r.Uint64())
}

func pcg(seed1, seed2 uint64) *rand.Rand {
	return rand.New(rand.NewPCG(seed1, seed2)) //nolint:gosec
}

func nonces(r *rand.Rand) *rand.ChaCha8 {
	var seed [32]byte
	for i := 0; i < len(seed); i += 8 {
		binary.LittleEndian.PutUint64(seed[i:], r.Uint64())
	}
	return rand.NewChaCha8(seed)
}

func (w *World) Run() error {
	for w.err == nil && len(w.events) > 0 && w.events[0].at <= w.plan.end {
		w.step()
		if w.err == nil && w.cfg.Check != nil {
			if err := w.cfg.Check(); err != nil {
				w.abort(err)
			}
		}
	}
	return w.err
}

func (w *World) TraceHash() [sha256.Size]byte {
	var sum [sha256.Size]byte
	w.hash.Sum(sum[:0])
	return sum
}

func (w *World) Now() uint64 {
	return w.now
}

func (w *World) Quiet() uint64 {
	return w.plan.quiet
}

func (w *World) End() uint64 {
	return w.plan.end
}

func (w *World) Disk(id uint64) *disk.Sim {
	return w.hosts[id-1].disk
}

func (w *World) nodes() []*host {
	return w.hosts[:w.cfg.Nodes]
}

func (w *World) abort(err error) {
	if w.err == nil {
		w.err = fmt.Errorf("sim: seed %d at %d: %w", w.cfg.Seed, w.now, err)
	}
}

func (w *World) step() {
	ev := w.events.pop()
	w.now = ev.at
	switch ev.kind {
	case evFault:
		w.fault(&w.plan.windows[ev.win], false)
		return
	case evHeal:
		w.fault(&w.plan.windows[ev.win], true)
		return
	case evQuiet:
		w.quiet()
		return
	}
	h := w.hosts[ev.id-1]
	switch ev.kind {
	case evTick:
		if ev.epoch != h.epoch {
			return
		}
		w.trace("tick %s", h.name)
		w.push(event{at: w.now + h.period, kind: evTick, id: h.id, epoch: h.epoch})
		w.input(h, input{tick: true})
	case evDeliver:
		w.deliver(h, ev)
	case evWake:
		if ev.epoch == h.epoch {
			w.trace("wake %s", h.name)
			w.wake(h)
		}
	case evRestart:
		if ev.epoch == h.epoch && h.proc == nil {
			w.start(h)
		}
	}
}

func (w *World) deliver(h *host, ev event) {
	l := w.net.at(ev.msg.From, ev.msg.To)
	switch {
	case l.cut > 0 && l.rng.IntN(2) == 0:
		ev.at = w.now + holdMin + l.rng.Uint64N(holdSpan)
		w.trace("hold %s until %d", w.route(ev.msg), ev.at)
		w.push(ev)
	case l.cut > 0:
		w.trace("cut %s", w.route(ev.msg))
	case h.proc == nil:
		w.trace("drop %s", w.route(ev.msg))
	default:
		w.trace("deliver %s", w.route(ev.msg))
		w.input(h, input{msg: ev.msg})
	}
}

func (w *World) start(h *host) {
	w.trace("start %s", h.name)
	rng := fork(h.rng)
	if !h.node {
		h.proc = w.cfg.Client(ClientEnv{ID: h.id, Nodes: slices.Clone(w.peers), Now: w.Now, Rand: rng})
		w.push(event{at: w.now + h.rng.Uint64N(h.period), kind: evTick, id: h.id, epoch: h.epoch})
		return
	}
	if !w.calm && (h.midways > 0 || w.plan.rearm && h.rng.IntN(restartArmOdds) == 0) {
		w.arm(h)
	}
	p, err := w.cfg.Node(NodeEnv{
		ID:     h.id,
		Peers:  slices.Clone(w.peers),
		FS:     h.fsys,
		Clock:  func() uint64 { return w.now + h.clock },
		Rand:   rng,
		Nonces: nonces(h.rng),
	})
	if err != nil {
		w.fail(h, err)
		return
	}
	h.proc = p
	w.push(event{at: w.now + h.rng.Uint64N(h.period), kind: evTick, id: h.id, epoch: h.epoch})
	w.settle(h, nil)
}

func (w *World) arm(h *host) {
	k := 1 + h.rng.Uint64N(armSpan)
	h.disk.FailAfter(k)
	w.trace("arm %s access %d", h.name, k)
}

func (w *World) input(h *host, in input) {
	if h.waiting {
		h.backlog = append(h.backlog, in)
		return
	}
	in.feed(h.proc)
	w.run(h)
}

func (w *World) run(h *host) {
	msgs, err := h.proc.Run()
	switch {
	case err != nil && h.node:
		w.fail(h, err)
		return
	case err != nil:
		w.abort(fmt.Errorf("client %s: %w", h.name, err))
		return
	}
	for _, m := range msgs {
		if m.To == 0 || m.To > uint64(len(w.hosts)) || m.Body == nil {
			w.abort(fmt.Errorf("%s sent a message to %d with body %v", h.name, m.To, m.Body))
			return
		}
	}
	if w.cfg.Said != nil {
		if err := w.cfg.Said(h.id, msgs); err != nil {
			w.abort(err)
			return
		}
	}
	w.settle(h, msgs)
}

func (w *World) settle(h *host, msgs []Msg) {
	if !h.node || h.meter.cost == 0 {
		w.send(h, msgs)
		return
	}
	h.waiting = true
	h.outbox = msgs
	w.push(event{at: w.now + h.meter.take(), kind: evWake, id: h.id, epoch: h.epoch})
}

func (w *World) wake(h *host) {
	h.waiting = false
	outbox := h.outbox
	h.outbox = nil
	w.send(h, outbox)
	if len(h.backlog) == 0 {
		return
	}
	for _, in := range h.backlog {
		in.feed(h.proc)
	}
	clear(h.backlog)
	h.backlog = h.backlog[:0]
	w.run(h)
}

func (w *World) send(h *host, msgs []Msg) {
	for _, m := range msgs {
		m.From = h.id
		l := w.net.at(m.From, m.To)
		route := w.route(m)
		switch {
		case l.cut > 0:
			w.trace("cut %s", route)
		case l.lost():
			w.trace("lose %s", route)
		default:
			copies := 1
			if l.doubled() {
				copies = 2
			}
			for range copies {
				at := w.now + l.delay(!w.calm)
				w.push(event{at: at, kind: evDeliver, id: m.To, msg: Msg{From: m.From, To: m.To, Body: m.Body.Clone()}})
				w.trace("send %s due %d", route, at)
			}
		}
	}
}

func (w *World) stop(h *host, power bool) {
	h.proc = nil
	h.epoch++
	h.waiting = false
	h.outbox = nil
	h.backlog = nil
	h.meter.cost = 0
	h.meter.release()
	if power {
		h.disk.Crash()
	}
}

func (w *World) fail(h *host, err error) {
	power := h.rng.IntN(2) == 0
	w.stop(h, power)
	w.trace("fail %s %s: %s", h.name, outage(power), filepath.ToSlash(err.Error()))
	w.push(event{at: w.now + restartMin + h.rng.Uint64N(restartSpan), kind: evRestart, id: h.id, epoch: h.epoch})
}

func outage(power bool) string {
	if power {
		return "power"
	}
	return "process"
}

func (w *World) fault(win *window, heal bool) {
	if heal {
		w.trace("heal %s", w.describe(win))
	} else {
		w.trace("fault %s", w.describe(win))
	}
	by := 1
	if heal {
		by = -1
	}
	h := w.hosts[win.node-1]
	switch win.kind {
	case split, isolate:
		w.net.partition(win.side, false, by)
	case oneway:
		w.net.partition(win.side, true, by)
	case bridge:
		w.net.sever(win.node, win.peer, by)
	case crash:
		h.crashes += by
		switch {
		case heal:
			if h.crashes == 0 && h.proc == nil {
				w.start(h)
			}
		case h.proc == nil:
			w.stop(h, false)
			w.trace("kill %s down", h.name)
		default:
			power := h.rng.IntN(2) == 0
			w.stop(h, power)
			w.trace("kill %s %s", h.name, outage(power))
		}
	case blackout:
		for _, n := range w.nodes() {
			n.crashes += by
			switch {
			case !heal:
				w.stop(n, true)
			case n.crashes == 0 && n.proc == nil:
				w.push(event{at: w.now + n.rng.Uint64N(blackoutSpread), kind: evRestart, id: n.id, epoch: n.epoch})
			}
		}
	case midway:
		h.midways += by
		switch {
		case !heal && h.proc != nil:
			w.arm(h)
		case heal && h.midways == 0:
			h.disk.FailAfter(0)
			w.trace("disarm %s", h.name)
		}
	case fsyncgate:
		if heal {
			h.disk.FailSyncAfter(0)
			w.trace("disarm %s", h.name)
		} else {
			k := 1 + h.rng.Uint64N(gateSpan)
			h.disk.FailSyncAfter(k)
			w.trace("arm %s sync %d", h.name, k)
		}
	case slow:
		h.meter.slow += by
	case eio:
		if heal {
			i := slices.Index(h.eio, win.rate)
			h.eio = slices.Delete(h.eio, i, i+1)
		} else {
			h.eio = append(h.eio, win.rate)
		}
		var rate float64
		if len(h.eio) > 0 {
			rate = slices.Max(h.eio)
		}
		h.disk.WriteErrRate, h.disk.SyncErrRate, h.disk.MetaErrRate, h.disk.ReadErrRate = rate, rate, rate, rate
	case jump:
		switch {
		case heal && win.keep:
		case win.back != heal:
			h.clock -= win.shift
		default:
			h.clock += win.shift
		}
	}
}

func (w *World) quiet() {
	w.calm = true
	w.trace("quiet")
	w.net.calm()
	for _, h := range w.nodes() {
		h.disk.FailAfter(0)
		h.disk.FailSyncAfter(0)
	}
	for _, h := range w.hosts {
		if h.proc == nil {
			w.start(h)
		}
	}
}

func (w *World) route(m Msg) string {
	return w.hosts[m.From-1].name + "->" + w.hosts[m.To-1].name + " " + m.Body.String()
}

func (w *World) describe(win *window) string {
	span := fmt.Sprintf("%s %d-%d", win.kind, win.start, win.end)
	name := w.hosts[win.node-1].name
	switch win.kind {
	case split, oneway, isolate:
		var in, out []string
		for i, a := range win.side {
			if a {
				in = append(in, w.hosts[i].name)
			} else {
				out = append(out, w.hosts[i].name)
			}
		}
		return span + " " + strings.Join(in, " ") + " | " + strings.Join(out, " ")
	case bridge:
		return span + " " + name + " " + w.hosts[win.peer-1].name
	case blackout:
		return span
	case eio:
		return fmt.Sprintf("%s %s %.4f", span, name, win.rate)
	case jump:
		sign := "+"
		if win.back {
			sign = "-"
		}
		if win.keep {
			return fmt.Sprintf("%s %s %s%d keep", span, name, sign, win.shift)
		}
		return fmt.Sprintf("%s %s %s%d", span, name, sign, win.shift)
	}
	return span + " " + name
}

func (w *World) trace(format string, args ...any) {
	w.line = fmt.Appendf(w.line[:0], "%d ", w.now)
	w.line = fmt.Appendf(w.line, format, args...)
	w.line = append(w.line, '\n')
	if _, err := w.out.Write(w.line); err != nil {
		w.abort(fmt.Errorf("trace: %w", err))
	}
}

func (w *World) push(ev event) {
	w.seq++
	ev.seq = w.seq
	w.events.push(ev)
}
