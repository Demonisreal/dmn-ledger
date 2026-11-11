package wal

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"syscall"
	"testing"

	"github.com/Demonisreal/dmn-ledger/internal/disk"
)

type hookFS struct {
	*disk.Sim
	after func()
}

type hookFile struct {
	disk.File
	after func()
}

func (h hookFS) Create(name string) (disk.File, error) {
	f, err := h.Sim.Create(name)
	h.after()
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
	h.after()
	return err
}

func (h hookFS) Rename(oldname, newname string) error {
	err := h.Sim.Rename(oldname, newname)
	h.after()
	return err
}

func (h hookFS) SyncDir(dir string) error {
	err := h.Sim.SyncDir(dir)
	h.after()
	return err
}

func (f hookFile) WriteAt(p []byte, off int64) (int, error) {
	n, err := f.File.WriteAt(p, off)
	f.after()
	return n, err
}

func (f hookFile) Sync() error {
	err := f.File.Sync()
	f.after()
	return err
}

func (f hookFile) Truncate(size int64) error {
	err := f.File.Truncate(size)
	f.after()
	return err
}

func crashedCopy(sim *disk.Sim) *disk.Sim {
	c := sim.Clone()
	c.WriteErrRate, c.SyncErrRate = 0, 0
	c.Crash()
	return c
}

type logModel struct {
	first, last uint64
	next        uint64
	terms       map[uint64]uint64
	seen        map[uint64][]uint64
	durable     uint64
	floor       uint64
}

func (m *logModel) verify(w *WAL) error {
	first, last := w.FirstIndex(), w.LastIndex()
	for i := max(m.floor, m.first, 1); i <= m.durable; i++ {
		if i < first || i > last || last == 0 {
			return fmt.Errorf("synced entry %d lost, log is [%d, %d]", i, first, last)
		}
	}
	if last == 0 {
		return nil
	}
	ents, err := w.Entries(first, last+1)
	if err != nil {
		return err
	}
	for _, e := range ents {
		i := e.Index
		if !slices.Contains(m.seen[i], e.Term) || e.Kind != uint8(i%3) || !bytes.Equal(e.Body, body(i, e.Term)) {
			return fmt.Errorf("entry %d term %d was never written", i, e.Term)
		}
		if i >= m.floor && i <= m.durable && e.Term != m.terms[i] {
			return fmt.Errorf("entry %d has term %d, synced term %d", i, e.Term, m.terms[i])
		}
	}
	return nil
}

func (m *logModel) adopt(w *WAL) {
	m.first, m.last = w.FirstIndex(), w.LastIndex()
	clear(m.terms)
	for i := m.first; i <= m.last && m.last > 0; i++ {
		m.terms[i], _ = w.Term(i)
	}
	if m.last > 0 {
		m.next = m.last + 1
	}
	m.durable = m.last
}

func TestCrashAtEveryWrite(t *testing.T) {
	seeds, steps := uint64(32), 800
	if testing.Short() {
		seeds, steps = 3, 150
	}
	var checks, recoveries int
	for seed := range seeds {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			c, r := crashRun(t, seed, steps)
			checks += c
			recoveries += r
		})
	}
	if recoveries == 0 {
		t.Error("no injected I/O error was ever hit")
	}
	t.Logf("%d crash points, %d recoveries after EIO", checks, recoveries)
}

func crashRun(t *testing.T, seed uint64, steps int) (checks, recoveries int) {
	rng := rand.New(rand.NewPCG(seed, 99))
	sim := newSim(seed)
	opts := Options{SegmentSize: []int64{300, 4096}[seed/2%2]}
	m := &logModel{next: 1, terms: map[uint64]uint64{}, seen: map[uint64][]uint64{}}
	term := uint64(1)
	step := 0
	fsys := hookFS{Sim: sim, after: func() {
		checks++
		w, err := Open(crashedCopy(sim), dir, opts)
		if err != nil {
			t.Fatalf("seed %d step %d: open after crash: %v", seed, step, err)
		}
		if err := m.verify(w); err != nil {
			t.Fatalf("seed %d step %d: %v", seed, step, err)
		}
		_ = w.Close()
	}}

	w := mustOpen(t, fsys, opts)
	restart := func(crash bool) {
		_ = w.Close()
		if crash {
			sim.Crash()
		}
		rates := [2]float64{sim.WriteErrRate, sim.SyncErrRate}
		sim.WriteErrRate, sim.SyncErrRate = 0, 0
		w = mustOpen(t, fsys, opts)
		sim.WriteErrRate, sim.SyncErrRate = rates[0], rates[1]
		m.adopt(w)
	}
	if seed%2 == 1 {
		sim.WriteErrRate, sim.SyncErrRate = 0.01, 0.02
	}

	for step = range steps {
		var err error
		switch r := rng.IntN(100); {
		case r < 45:
			n := 1 + rng.Uint64N(4)
			ents := span(m.next, m.next+n, term)
			for _, e := range ents {
				if !slices.Contains(m.seen[e.Index], term) {
					m.seen[e.Index] = append(m.seen[e.Index], term)
				}
			}
			if err = w.Append(ents...); err == nil {
				for _, e := range ents {
					m.terms[e.Index] = term
				}
				if m.last == 0 {
					m.first = m.next
				}
				m.last = m.next + n - 1
				m.next = m.last + 1
			}
		case r < 70:
			if err = w.Sync(); err == nil {
				m.durable = m.last
			}
		case r < 80 && m.last > 0:
			i := m.first - 1 + rng.Uint64N(m.last-m.first+2)
			m.durable = min(m.durable, i)
			durable, cut := m.durable, i < m.last
			if err = w.TruncateAfter(i); err == nil {
				m.adopt(w)
				if !cut {
					m.durable = durable
				}
				m.next = i + 1
				term++
			}
		case r < 87 && m.last > 0:
			c := m.first + rng.Uint64N(m.last-m.first+2)
			m.floor = max(m.floor, c)
			if err = w.CompactBefore(c); err == nil {
				durable := m.durable
				m.adopt(w)
				m.durable = durable
				if m.last == 0 {
					m.next = c
				}
			}
		case r < 92:
			restart(false)
		default:
			term++
		}
		if err != nil {
			if !errors.Is(err, syscall.EIO) {
				t.Fatalf("seed %d step %d: %v", seed, step, err)
			}
			recoveries++
			restart(true)
		}
	}
	_ = w.Close()
	return checks, recoveries
}
