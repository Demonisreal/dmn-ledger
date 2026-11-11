package wal

import (
	"bytes"
	"errors"
	"fmt"
	"maps"
	"math/rand/v2"
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

type logModel struct {
	first, last uint64
	next        uint64
	term        uint64
	terms       map[uint64]uint64
	seen        map[uint64][]uint64
	durable     uint64
	floor       uint64
}

func (m *logModel) clone() *logModel {
	c := *m
	c.terms = maps.Clone(m.terms)
	c.seen = make(map[uint64][]uint64, len(m.seen))
	for i, terms := range m.seen {
		c.seen[i] = slices.Clone(terms)
	}
	return &c
}

func (m *logModel) verify(w *WAL) error {
	first, err := w.FirstIndex()
	if err != nil {
		return err
	}
	last, _ := w.LastIndex()
	for i := max(m.floor, m.first, 1); i <= m.durable; i++ {
		if i < first || i > last || last == 0 {
			return fmt.Errorf("synced entry %d lost, log is [%d, %d]", i, first, last)
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
		if !slices.Contains(m.seen[i], e.Term) || e.Kind != Kind(i%3) || !bytes.Equal(e.Body, body(i, e.Term)) {
			return fmt.Errorf("entry %d term %d was never written or was truncated", i, e.Term)
		}
		if i >= m.floor && i <= m.durable && e.Term != m.terms[i] {
			return fmt.Errorf("entry %d has term %d, synced term %d", i, e.Term, m.terms[i])
		}
	}
	return nil
}

func (m *logModel) adopt(w *WAL) {
	m.first, _ = w.FirstIndex()
	m.last, _ = w.LastIndex()
	clear(m.terms)
	for i := m.first; i <= m.last && m.last > 0; i++ {
		m.terms[i], _ = w.Term(i)
	}
	if m.last > 0 {
		m.next = m.last + 1
	}
	m.durable = m.last
}

func (m *logModel) apply(w *WAL, rng *rand.Rand) error {
	switch r := rng.IntN(100); {
	case r < 45:
		n := 1 + rng.Uint64N(4)
		ents := span(m.next, m.next+n, m.term)
		for _, e := range ents {
			if !slices.Contains(m.seen[e.Index], m.term) {
				m.seen[e.Index] = append(m.seen[e.Index], m.term)
			}
		}
		if err := w.Append(ents...); err != nil {
			return err
		}
		for _, e := range ents {
			m.terms[e.Index] = m.term
		}
		if m.last == 0 {
			m.first = m.next
		}
		m.last = m.next + n - 1
		m.next = m.last + 1
	case r < 70:
		if err := w.Sync(); err != nil {
			return err
		}
		m.durable = m.last
	case r < 80 && m.last > 0:
		i := m.first - 1 + rng.Uint64N(m.last-m.first+2)
		m.durable = min(m.durable, i)
		durable, last := m.durable, m.last
		if err := w.TruncateAfter(i); err != nil {
			return err
		}
		m.adopt(w)
		if i >= last {
			m.durable = durable
		}
		maps.DeleteFunc(m.seen, func(k uint64, _ []uint64) bool { return k > i })
		m.next = i + 1
		m.term++
	case r < 87 && m.last > 0:
		c := m.first + rng.Uint64N(m.last-m.first+2)
		m.floor = max(m.floor, c)
		durable := m.durable
		if err := w.CompactBefore(c); err != nil {
			return err
		}
		m.adopt(w)
		m.durable = durable
		if m.last == 0 {
			m.next = c
		}
	default:
		m.term++
	}
	return nil
}

func TestCrashAtEveryWrite(t *testing.T) {
	seeds, steps := uint64(32), 600
	if testing.Short() {
		seeds, steps = 4, 150
	}
	var total crashStats
	for seed := range seeds {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			st := crashRun(t, seed, steps)
			total.power += st.power
			total.process += st.process
			total.recoveries += st.recoveries
			total.restarts += st.restarts
		})
	}
	if total.restarts == 0 || total.recoveries == total.restarts || total.process == 0 {
		t.Errorf("coverage: %+v", total)
	}
	t.Logf("%d power losses, %d process crashes followed by power loss, %d recoveries after EIO (%d without power loss)",
		total.power, total.process, total.recoveries, total.restarts)
}

type crashStats struct {
	power, process, recoveries, restarts int
}

func crashRun(t *testing.T, seed uint64, steps int) crashStats {
	var st crashStats
	rng := rand.New(rand.NewPCG(seed, 99))
	sim := newSim(seed)
	if seed/4%2 == 1 {
		sim.Sector = 4096
	}
	o := Options{SegmentSize: []int64{300, 4096}[seed/2%2], Nonces: rand.NewChaCha8([32]byte{byte(seed)})}
	m := &logModel{next: 1, term: 1, terms: map[uint64]uint64{}, seen: map[uint64][]uint64{}}
	step := 0
	fail := func(format string, args ...any) {
		t.Helper()
		t.Fatalf("seed %d step %d: "+format, append([]any{seed, step}, args...)...)
	}
	fsys := hookFS{Sim: sim, after: func(string) {
		st.power++
		c := quiet(sim.Clone())
		c.Crash()
		w, err := Open(c, dir, o)
		if err != nil {
			fail("open after power loss: %v", err)
		}
		if err := m.verify(w); err != nil {
			fail("after power loss: %v", err)
		}
		_ = w.Close()

		if st.power%4 != 0 {
			return
		}
		st.process++
		proc := quiet(sim.Clone())
		w, err = Open(proc, dir, o)
		if err != nil {
			fail("open after process crash: %v", err)
		}
		if err := m.verify(w); err != nil {
			fail("after process crash: %v", err)
		}
		pm := m.clone()
		pm.adopt(w)
		for range 3 {
			if err := pm.apply(w, rng); err != nil {
				fail("after process crash: %v", err)
			}
		}
		if err := w.Sync(); err != nil {
			fail("sync after process crash: %v", err)
		}
		pm.durable = pm.last
		_ = w.Close()
		proc.Crash()
		if w, err = Open(proc, dir, o); err != nil {
			fail("open after process crash and power loss: %v", err)
		}
		if err := pm.verify(w); err != nil {
			fail("after process crash and power loss: %v", err)
		}
		_ = w.Close()
	}}

	faults := [4]float64{}
	if seed%2 == 1 {
		faults = [4]float64{0.01, 0.02, 0.01, 0}
	}
	setFaults := func() {
		sim.WriteErrRate, sim.SyncErrRate, sim.MetaErrRate, sim.ReadErrRate = faults[0], faults[1], faults[2], faults[3]
	}
	w := mustOpen(t, fsys, o)
	restart := func(crash bool) {
		_ = w.Close()
		if crash {
			sim.Crash()
		}
		quiet(sim)
		w = mustOpen(t, fsys, o)
		setFaults()
		m.adopt(w)
	}
	setFaults()

	for step = range steps {
		if rng.IntN(100) < 5 {
			restart(false)
			continue
		}
		if err := m.apply(w, rng); err != nil {
			if !errors.Is(err, syscall.EIO) {
				fail("%v", err)
			}
			st.recoveries++
			crash := rng.IntN(2) == 0
			if !crash {
				st.restarts++
			}
			restart(crash)
		}
	}
	_ = w.Close()
	return st
}
