package wal

import (
	"bytes"
	"cmp"
	"errors"
	"fmt"
	"maps"
	"math/rand/v2"
	"path/filepath"
	"slices"
	"syscall"
	"testing"

	"github.com/Demonisreal/dmn-ledger/internal/disk"
)

type hookFS struct {
	*disk.Sim
	after func(op string)
}

type hookFile struct {
	disk.File
	after func(op string)
}

func (h hookFS) Create(name string) (disk.File, error) {
	f, err := h.Sim.Create(name)
	h.after("create")
	if err != nil {
		return nil, err
	}
	return hookFile{f, h.after}, nil
}

func (h hookFS) Open(name string) (disk.File, error) {
	f, err := h.Sim.Open(name)
	if err != nil {
		return nil, err
	}
	return hookFile{f, h.after}, nil
}

func (h hookFS) Remove(name string) error {
	err := h.Sim.Remove(name)
	h.after("remove")
	return err
}

func (h hookFS) Rename(oldname, newname string) error {
	err := h.Sim.Rename(oldname, newname)
	h.after("rename")
	return err
}

func (h hookFS) SyncDir(dir string) error {
	err := h.Sim.SyncDir(dir)
	h.after("syncdir")
	return err
}

func (f hookFile) WriteAt(p []byte, off int64) (int, error) {
	n, err := f.File.WriteAt(p, off)
	f.after("write")
	return n, err
}

func (f hookFile) Sync() error {
	err := f.File.Sync()
	f.after("sync")
	return err
}

func (f hookFile) Truncate(size int64) error {
	err := f.File.Truncate(size)
	f.after("truncate")
	return err
}

func quiet(sim *disk.Sim) *disk.Sim {
	sim.WriteErrRate, sim.SyncErrRate, sim.MetaErrRate, sim.ReadErrRate = 0, 0, 0, 0
	return sim
}

func fork(sim *disk.Sim, seed uint64) *disk.Sim {
	c := quiet(sim.Clone(rand.New(rand.NewPCG(seed, 0x5eed))))
	c.JournalRate = 0
	return c
}

type logModel struct {
	first, last uint64
	next        uint64
	term        uint64
	gen         uint64
	bodyMax     uint64
	log         map[uint64]Entry
	seen        map[uint64][]Entry
	durable     uint64
	sealed      uint64
	repair      bool
	floor       uint64
}

func newModel(bodyMax uint64) *logModel {
	return &logModel{next: 1, term: 1, bodyMax: bodyMax, log: map[uint64]Entry{}, seen: map[uint64][]Entry{}}
}

func (m *logModel) clone() *logModel {
	c := *m
	c.log = maps.Clone(m.log)
	c.seen = make(map[uint64][]Entry, len(m.seen))
	for i, ents := range m.seen {
		c.seen[i] = slices.Clone(ents)
	}
	return &c
}

func sameEntry(a, b Entry) bool {
	return a.Index == b.Index && a.Term == b.Term && a.Kind == b.Kind && bytes.Equal(a.Body, b.Body)
}

func (m *logModel) entry(i uint64) Entry {
	m.gen++
	b := make([]byte, (i*31+m.gen*7)%m.bodyMax)
	for k := range b {
		b[k] = byte(i + m.gen + uint64(k))
	}
	return Entry{Index: i, Term: m.term, Kind: Kind(m.gen % 3), Body: b}
}

func (m *logModel) verify(w *WAL, need uint64) error {
	first, err := w.FirstIndex()
	if err != nil {
		return err
	}
	last, _ := w.LastIndex()
	for i := max(m.floor, m.first, 1); i <= need; i++ {
		if i < first || i > last || last == 0 {
			return fmt.Errorf("entry %d lost, log is [%d, %d], synced up to %d, sealed up to %d", i, first, last, m.durable, m.sealed)
		}
	}
	if last == 0 {
		return nil
	}
	ents, err := w.Entries(first, last+1, noLimit)
	if err != nil {
		return err
	}
	for _, e := range ents {
		i := e.Index
		if !slices.ContainsFunc(m.seen[i], func(s Entry) bool { return sameEntry(s, e) }) {
			return fmt.Errorf("entry %d term %d was never written, was cut away or was lost in a crash before", i, e.Term)
		}
		if i >= m.floor && i <= need && !sameEntry(e, m.log[i]) {
			return fmt.Errorf("entry %d term %d differs from the synced one with term %d", i, e.Term, m.log[i].Term)
		}
	}
	return nil
}

func (m *logModel) adopt(w *WAL) error {
	first, _ := w.FirstIndex()
	last, _ := w.LastIndex()
	clear(m.log)
	if last > 0 {
		ents, err := w.Entries(first, last+1, noLimit)
		if err != nil {
			return err
		}
		for _, e := range ents {
			m.log[e.Index] = e
		}
		m.next = last + 1
	}
	maps.DeleteFunc(m.seen, func(k uint64, _ []Entry) bool { return k > last })
	m.first, m.last, m.durable, m.sealed, m.repair = first, last, last, last, false
	return nil
}

func (m *logModel) bounds(w *WAL) error {
	first, _ := w.FirstIndex()
	last, err := w.LastIndex()
	if err != nil {
		return err
	}
	if first != m.first || last != m.last {
		return fmt.Errorf("log is [%d, %d], model says [%d, %d]", first, last, m.first, m.last)
	}
	return nil
}

func (m *logModel) appendN(w *WAL, n uint64) error {
	ents := make([]Entry, n)
	for k := range ents {
		e := m.entry(m.next + uint64(k))
		m.seen[e.Index] = append(m.seen[e.Index], e)
		ents[k] = e
	}
	if err := w.Append(ents...); err != nil {
		return err
	}
	for _, e := range ents {
		m.log[e.Index] = e
	}
	if m.last == 0 {
		m.first = m.next
	}
	m.last = m.next + n - 1
	m.next = m.last + 1
	return nil
}

func (m *logModel) sync(w *WAL) error {
	if err := w.Sync(); err != nil {
		return err
	}
	m.durable, m.sealed = m.last, m.last
	return nil
}

func (m *logModel) opts(o Options) Options {
	o.Sealed = m.sealed
	return o
}

func (m *logModel) truncate(w *WAL, i uint64, rng *rand.Rand) error {
	m.durable, m.sealed = min(m.durable, i), min(m.sealed, i)
	if err := w.TruncateAfter(i); err != nil {
		return err
	}
	if i < m.last {
		maps.DeleteFunc(m.log, func(k uint64, _ Entry) bool { return k > i })
		maps.DeleteFunc(m.seen, func(k uint64, _ []Entry) bool { return k > i })
		m.durable, m.last, m.next = i, i, i+1
		switch {
		case i < m.first:
			m.first, m.last, m.durable = 0, 0, 0
			m.term = 1 + rng.Uint64N(m.term)
		case rng.IntN(2) == 0:
			m.term = m.log[i].Term
		default:
			m.term++
		}
	}
	return m.bounds(w)
}

func (m *logModel) compact(w *WAL, c uint64, rng *rand.Rand) error {
	m.floor = max(m.floor, c)
	if err := w.CompactBefore(c); err != nil {
		return err
	}
	first, err := w.FirstIndex()
	if err != nil {
		return err
	}
	if m.last < c {
		m.first, m.last, m.durable, m.next = 0, 0, 0, c
		m.term = 1 + rng.Uint64N(m.term)
	} else if first > m.first && first <= c {
		m.first = first
	}
	maps.DeleteFunc(m.log, func(k uint64, _ Entry) bool { return k < m.first })
	return m.bounds(w)
}

func (m *logModel) apply(w *WAL, rng *rand.Rand) error {
	switch r := rng.IntN(100); {
	case r < 45:
		return m.appendN(w, 1+rng.Uint64N(4))
	case r < 70:
		return m.sync(w)
	case r < 80 && m.last > 0:
		return m.truncate(w, m.first-1+rng.Uint64N(m.last-m.first+2), rng)
	case r < 87 && m.last > 0:
		return m.compact(w, m.first+rng.Uint64N(m.last-m.first+2), rng)
	default:
		m.term++
	}
	return nil
}

type crashCheck struct {
	t       *testing.T
	o       Options
	rng     *rand.Rand
	shred   bool
	forks   uint64
	where   string
	points  int
	faulty  int
	rotted  int
	damaged int
}

func (c *crashCheck) fork(sim *disk.Sim) *disk.Sim {
	c.forks++
	return fork(sim, c.forks)
}

func (c *crashCheck) fail(format string, args ...any) {
	c.t.Helper()
	c.t.Fatalf(c.where+": "+format, args...)
}

func (c *crashCheck) open(fsys disk.FS, m *logModel) (*WAL, uint64, error) {
	o := m.opts(c.o)
	w, err := Open(fsys, dir, o)
	switch {
	case !c.shred:
		return w, m.durable, err
	case errors.Is(err, ErrCorrupt):
		c.damaged++
		m.repair = true
		o.Salvage = true
		w, err = Open(fsys, dir, o)
		return w, 0, err
	case m.repair:
		return w, 0, err
	}
	return w, m.sealed, err
}

func (c *crashCheck) reopen(sim *disk.Sim, m *logModel, depth int, tag string) {
	c.t.Helper()
	c.points++
	m = m.clone()
	var fsys disk.FS = sim
	if depth > 0 {
		fsys = hookFS{Sim: sim, after: func(op string) {
			lost := c.fork(sim)
			lost.Crash()
			c.reopen(lost, m, depth-1, tag+", power loss after "+op)
		}}
	}
	w, need, err := c.open(fsys, m)
	if err != nil {
		c.fail("%s: open: %v", tag, err)
	}
	if err := m.verify(w, need); err != nil {
		c.fail("%s: %v", tag, err)
	}
	if err := m.adopt(w); err != nil {
		c.fail("%s: %v", tag, err)
	}
	for range 3 {
		if err := m.apply(w, c.rng); err != nil {
			c.fail("%s: after reopen: %v", tag, err)
		}
	}
	if err := m.sync(w); err != nil {
		c.fail("%s: sync after reopen: %v", tag, err)
	}
	_ = w.Close()
	after := c.fork(sim)
	after.Crash()
	if w, err = Open(after, dir, m.opts(c.o)); err != nil {
		c.fail("%s: open after power loss: %v", tag, err)
	}
	if err := m.verify(w, m.durable); err != nil {
		c.fail("%s: reported by Open or synced after it, then lost: %v", tag, err)
	}
	_ = w.Close()
}

func (c *crashCheck) rot(sim *disk.Sim, m *logModel) {
	c.t.Helper()
	sim = c.fork(sim)
	w, err := Open(sim, dir, m.opts(c.o))
	if err != nil {
		c.fail("open before the damage: %v", err)
	}
	lo, hi := max(w.first(), 1), min(m.sealed, w.last())
	if w.last() == 0 || lo > hi {
		_ = w.Close()
		return
	}
	c.rotted++
	i := lo + c.rng.Uint64N(hi-lo+1)
	k, _ := slices.BinarySearchFunc(w.segs, i, func(s *segment, i uint64) int { return cmp.Compare(s.last(), i) })
	seg := w.segs[k]
	at := seg.offs[i-seg.first]
	off := at + c.rng.Int64N(seg.end(i-seg.first+1)-at)
	before, err := w.Entries(w.first(), i, noLimit)
	if err != nil {
		c.fail("%v", err)
	}
	keep := i - 1
	if i == w.first() {
		keep = 0
	}
	_ = w.Close()

	harm, kind, sector := sim.Corrupt, "bit flip", false
	if c.rng.IntN(3) == 0 {
		harm, kind, sector = sim.Garble, "garbled sector", true
	}
	if err := harm(filepath.Join(dir, seg.name), off); err != nil {
		c.fail("%v", err)
	}
	where := fmt.Sprintf("%s in entry %d at %s:%d, sealed %d", kind, i, seg.name, off, m.sealed)
	o := m.opts(c.o)
	if _, err := Open(sim, dir, o); !errors.Is(err, ErrCorrupt) {
		c.fail("%s: open = %v, want ErrCorrupt", where, err)
	}
	o.Salvage = true
	if w, err = Open(sim, dir, o); err != nil {
		c.fail("%s: salvage: %v", where, err)
	}
	defer func() { _ = w.Close() }()
	if w.last() > keep || !sector && w.last() != keep {
		c.fail("%s: salvage kept [%d, %d], want up to %d", where, w.first(), w.last(), keep)
	}
	if w.last() == 0 {
		return
	}
	kept, err := w.Entries(w.first(), w.last()+1, noLimit)
	if err != nil {
		c.fail("%s: %v", where, err)
	}
	if !slices.EqualFunc(kept, before[:len(kept)], sameEntry) {
		c.fail("%s: salvage kept [%d, %d], not a start of the old log", where, w.first(), w.last())
	}
}

func (c *crashCheck) faultyReopen(sim *disk.Sim, m *logModel) {
	c.t.Helper()
	c.faulty++
	m = m.clone()
	sim.WriteErrRate, sim.SyncErrRate, sim.MetaErrRate, sim.ReadErrRate = 0.15, 0.15, 0.15, 0.05
	for try := 0; ; try++ {
		if try == 12 {
			quiet(sim)
		}
		w, need, err := c.open(sim, m)
		if errors.Is(err, syscall.EIO) {
			if c.rng.IntN(3) == 0 {
				sim.Crash()
			}
			continue
		}
		if err != nil {
			c.fail("open try %d: %v", try, err)
		}
		quiet(sim)
		if err := m.verify(w, need); err != nil {
			c.fail("after %d failed opens: %v", try, err)
		}
		if err := m.adopt(w); err != nil {
			c.fail("%v", err)
		}
		_ = w.Close()
		quiet(sim).Crash()
		if w, err = Open(sim, dir, m.opts(c.o)); err != nil {
			c.fail("open after failed opens and power loss: %v", err)
		}
		if err := m.verify(w, m.durable); err != nil {
			c.fail("reported by Open after failed opens, then lost: %v", err)
		}
		_ = w.Close()
		return
	}
}

type crashConfig struct {
	seeds    uint64
	steps    int
	segSizes []int64
	bodyMax  uint64
	journal  float64
	shred    float64
	depth    int
}

type crashStats struct {
	checks, faulty, rotted, damaged, restarts, failedOpens int
}

func crashRun(t *testing.T, cfg crashConfig, seed uint64) crashStats {
	var st crashStats
	rng := rand.New(rand.NewPCG(seed, 99))
	sim := newSim(seed)
	sim.Sector = []int64{disk.SectorSize, 4096}[seed%2]
	sim.JournalRate = cfg.journal
	sim.ShredRate = cfg.shred
	c := &crashCheck{
		t:     t,
		rng:   rand.New(rand.NewPCG(seed, 100)),
		shred: cfg.shred > 0,
		o:     Options{SegmentSize: cfg.segSizes[seed/2%uint64(len(cfg.segSizes))], Nonces: rand.NewChaCha8([32]byte{byte(seed)})},
	}
	m := newModel(cfg.bodyMax)
	step, hooks := 0, 0
	fsys := hookFS{Sim: sim, after: func(op string) {
		hooks++
		c.where = fmt.Sprintf("seed %d step %d after %s", seed, step, op)
		switch hooks % 3 {
		case 0:
			lost := c.fork(sim)
			lost.Crash()
			if !c.shred {
				c.rot(lost, m)
			}
			c.reopen(lost, m, cfg.depth, "power loss")
		case 1:
			c.reopen(c.fork(sim), m, cfg.depth, "process crash")
		case 2:
			proc := c.fork(sim)
			if c.rng.IntN(2) == 0 {
				proc.Crash()
			}
			c.faultyReopen(proc, m)
		}
	}}
	faults := seed%3 != 0
	setFaults := func() {
		if faults {
			sim.WriteErrRate, sim.SyncErrRate, sim.MetaErrRate, sim.ReadErrRate = 0.01, 0.02, 0.02, 0.01
		}
	}
	setFaults()
	w := mustOpen(t, fsys, c.o)
	restart := func(crash bool) {
		st.restarts++
		_ = w.Close()
		if crash {
			sim.Crash()
		}
		var need uint64
		for {
			var err error
			if w, need, err = c.open(fsys, m); err == nil {
				break
			}
			if !errors.Is(err, syscall.EIO) {
				t.Fatalf("seed %d step %d: open: %v", seed, step, err)
			}
			st.failedOpens++
			if rng.IntN(3) == 0 {
				sim.Crash()
			}
		}
		quiet(sim)
		if err := m.verify(w, need); err != nil {
			t.Fatalf("seed %d step %d: after restart: %v", seed, step, err)
		}
		if err := m.adopt(w); err != nil {
			t.Fatalf("seed %d step %d: %v", seed, step, err)
		}
		setFaults()
	}
	for step = range cfg.steps {
		if rng.IntN(100) < 4 {
			restart(rng.IntN(2) == 0)
			continue
		}
		if err := m.apply(w, rng); err != nil {
			if !errors.Is(err, syscall.EIO) {
				t.Fatalf("seed %d step %d: %v", seed, step, err)
			}
			restart(rng.IntN(2) == 0)
		}
	}
	_ = w.Close()
	st.checks, st.faulty, st.rotted, st.damaged = c.points, c.faulty, c.rotted, c.damaged
	return st
}

func TestCrashAtEveryWrite(t *testing.T) {
	cases := map[string]crashConfig{
		"small frames":     {seeds: 24, steps: 300, segSizes: []int64{300, 1500, 4096, 20000}, bodyMax: 97, depth: 1},
		"nested":           {seeds: 4, steps: 80, segSizes: []int64{300, 1500}, bodyMax: 97, depth: 2},
		"large frames":     {seeds: 8, steps: 120, segSizes: []int64{6000, 20000, 65536, 200000}, bodyMax: 3001, depth: 1},
		"eager namespace":  {seeds: 8, steps: 300, segSizes: []int64{300, 1500, 4096, 20000}, bodyMax: 97, journal: 0.5, depth: 1},
		"shredded sectors": {seeds: 8, steps: 300, segSizes: []int64{300, 1500, 4096, 20000}, bodyMax: 97, shred: 1, depth: 1},
		"some shredded":    {seeds: 8, steps: 300, segSizes: []int64{300, 1500, 4096, 20000}, bodyMax: 97, shred: 0.3, depth: 1},
	}
	for name, cfg := range cases {
		t.Run(name, func(t *testing.T) {
			if testing.Short() {
				cfg.seeds, cfg.steps = cfg.seeds/4, cfg.steps/2
			}
			var total crashStats
			for seed := range cfg.seeds {
				st := crashRun(t, cfg, seed)
				total.checks += st.checks
				total.faulty += st.faulty
				total.rotted += st.rotted
				total.damaged += st.damaged
				total.restarts += st.restarts
				total.failedOpens += st.failedOpens
			}
			if !testing.Short() && (total.restarts == 0 || total.failedOpens == 0 || total.faulty == 0 || (total.rotted == 0) != (cfg.shred > 0) || (total.damaged == 0) != (cfg.shred == 0)) {
				t.Errorf("coverage: %+v", total)
			}
			t.Logf("%d checked recoveries, %d with faults during Open, %d with damage up to the seal, %d salvaged after shredded sectors, %d restarts, %d failed opens in the main run",
				total.checks, total.faulty, total.rotted, total.damaged, total.restarts, total.failedOpens)
		})
	}
}

func TestCrashAtEveryCutAndCompact(t *testing.T) {
	const n = 45
	points := 0
	for _, sector := range []int64{disk.SectorSize, 4096} {
		for _, segSize := range []int64{300, 700} {
			if (segSize == 700) != (sector == 4096) || testing.Short() && sector == 4096 {
				continue
			}
			base := newSim(uint64(segSize + sector))
			base.Sector = sector
			o := Options{SegmentSize: segSize, Nonces: rand.NewChaCha8([32]byte{9})}
			start := newModel(97)
			w := mustOpen(t, base, o)
			for range n / 3 {
				if err := start.appendN(w, 3); err != nil {
					t.Fatal(err)
				}
				if err := start.sync(w); err != nil {
					t.Fatal(err)
				}
				start.term += uint64(start.next % 2)
			}
			_ = w.Close()

			for cut := uint64(0); cut <= n+1; cut++ {
				for _, compact := range []bool{false, true} {
					sim := base.Clone(rand.New(rand.NewPCG(cut, 1)))
					m := start.clone()
					c := &crashCheck{t: t, o: o, rng: rand.New(rand.NewPCG(cut, 5))}
					c.where = fmt.Sprintf("sector %d segment %d cut %d compact %v", sector, segSize, cut, compact)
					w := mustOpen(t, hookFS{Sim: sim, after: func(op string) {
						lost := c.fork(sim)
						lost.Crash()
						c.reopen(lost, m, 1, "power loss after "+op)
						c.reopen(c.fork(sim), m, 0, "process crash after "+op)
					}}, o)
					var err error
					if compact {
						err = m.compact(w, cut, c.rng)
					} else {
						err = m.truncate(w, cut, c.rng)
					}
					if err != nil {
						c.fail("%v", err)
					}
					if err := m.appendN(w, 2); err != nil {
						c.fail("append after the cut with term %d: %v", m.term, err)
					}
					if err := m.sync(w); err != nil {
						c.fail("%v", err)
					}
					_ = w.Close()
					points += c.points
				}
			}
		}
	}
	t.Logf("%d checked recoveries", points)
}
