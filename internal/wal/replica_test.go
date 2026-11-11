package wal

import (
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"testing"

	"github.com/Demonisreal/dmn-ledger/internal/disk"
)

const (
	nodeDir  = "/data"
	snapsDir = "/data/snap"
)

type replica struct {
	o          Options
	terms      map[uint64]uint64
	acked      uint64
	hs, voting HardState
	snap, next SnapshotMeta
	install    bool
}

type started struct {
	hs          HardState
	snap        SnapshotMeta
	first, last uint64
	emptied     bool
}

func (r *replica) start(fsys disk.FS) (started, *WAL, error) {
	var st started
	h, hs, err := OpenHardState(fsys, nodeDir)
	if err != nil {
		return st, nil, fmt.Errorf("hard state: %w", err)
	}
	_ = h.Close()
	st.hs = hs
	st.snap, err = LoadSnapshot(fsys, snapsDir, func(rd io.Reader) error {
		_, err := io.Copy(io.Discard, rd)
		return err
	})
	if err != nil && !errors.Is(err, ErrNoSnapshot) {
		return st, nil, fmt.Errorf("snapshot: %w", err)
	}
	w, err := Open(fsys, dir, r.o)
	if err != nil {
		return st, nil, err
	}
	st.first, _ = w.FirstIndex()
	st.last, _ = w.LastIndex()
	switch {
	case st.last == 0:
	case st.last < st.snap.Index:
		st.emptied = true
	case st.first > st.snap.Index+1:
		_ = w.Close()
		return st, nil, fmt.Errorf("log [%d, %d] starts after snapshot %d", st.first, st.last, st.snap.Index)
	case st.snap.Index >= st.first:
		if term, _ := w.Term(st.snap.Index); term != st.snap.Term {
			st.emptied = true
		}
	}
	return st, w, nil
}

func (r *replica) check(c *disk.Sim) error {
	st, w, err := r.start(c)
	if err != nil {
		return err
	}
	defer func() { _ = w.Close() }()
	if st.hs != r.hs && st.hs != r.voting {
		return fmt.Errorf("hard state %+v, want %+v or %+v", st.hs, r.hs, r.voting)
	}
	if st.snap != r.snap && st.snap != r.next {
		return fmt.Errorf("snapshot %+v, want %+v or %+v", st.snap, r.snap, r.next)
	}
	need := r.acked
	if r.install && st.snap == r.next {
		need = min(need, st.snap.Index)
	}
	for i := st.snap.Index + 1; i <= need; i++ {
		if st.emptied || i < st.first || i > st.last {
			return fmt.Errorf("acked entry %d lost: snapshot %d, log [%d, %d], emptied %v", i, st.snap.Index, st.first, st.last, st.emptied)
		}
		if term, _ := w.Term(i); term != r.terms[i] {
			return fmt.Errorf("acked entry %d has term %d, want %d", i, term, r.terms[i])
		}
	}
	return nil
}

func saveSnapIn(fsys disk.FS, meta SnapshotMeta) error {
	return SaveSnapshot(fsys, snapsDir, meta, func(w io.Writer) error {
		_, err := w.Write(snapBody(meta))
		return err
	})
}

func replicaRun(t *testing.T, seed uint64, steps int) int {
	rng := rand.New(rand.NewPCG(seed, 31337))
	sim := newSim(seed + 9000)
	sim.Sector = []int64{disk.SectorSize, 4096}[seed%2]
	sim.JournalRate = []float64{0, 0.5}[seed/2%2]
	r := &replica{
		o:     Options{SegmentSize: []int64{300, 1500}[seed/4%2], Nonces: rand.NewChaCha8([32]byte{byte(seed), 3})},
		terms: map[uint64]uint64{},
	}
	step, points := 0, 0
	fsys := hookFS{Sim: sim, after: func(op string) {
		points++
		lost := fork(sim, uint64(points))
		lost.Crash()
		if err := r.check(lost); err != nil {
			t.Fatalf("seed %d step %d, power loss after %s: %v", seed, step, op, err)
		}
		proc := fork(sim, uint64(points))
		if err := r.check(proc); err != nil {
			t.Fatalf("seed %d step %d, process crash after %s: %v", seed, step, op, err)
		}
		seen, w, _ := r.start(proc)
		_ = w.Close()
		proc.Crash()
		again, w, err := r.start(proc)
		if err != nil {
			t.Fatalf("seed %d step %d, process crash after %s, then power loss: %v", seed, step, op, err)
		}
		_ = w.Close()
		if again != seen {
			t.Fatalf("seed %d step %d after %s: start saw %+v, after power loss %+v", seed, step, op, seen, again)
		}
	}}

	h, _, err := OpenHardState(sim, nodeDir)
	if err != nil {
		t.Fatal(err)
	}
	_ = h.Close()
	h, _, err = OpenHardState(fsys, nodeDir)
	if err != nil {
		t.Fatal(err)
	}
	w := mustOpen(t, fsys, r.o)
	vote := func() {
		r.voting = HardState{Term: r.hs.Term + 1, Vote: rng.Uint64N(3)}
		if err := h.Save(r.voting); err != nil {
			t.Fatal(err)
		}
		r.hs = r.voting
	}
	vote()
	last := uint64(0)
	for step = range steps {
		switch k := rng.IntN(100); {
		case k < 50:
			n := 1 + rng.Uint64N(5)
			mustAppend(t, w, span(last+1, last+1+n, r.hs.Term)...)
			for i := last + 1; i <= last+n; i++ {
				r.terms[i] = r.hs.Term
			}
			last += n
			r.acked = last
		case k < 60:
			vote()
		case k < 72 && last > r.snap.Index:
			i := r.snap.Index + rng.Uint64N(last-r.snap.Index+1)
			vote()
			r.acked = min(r.acked, i)
			if err := w.TruncateAfter(i); err != nil {
				t.Fatal(err)
			}
			for k := i + 1; k <= last; k++ {
				delete(r.terms, k)
			}
			last = i
		case k < 86 && r.acked > r.snap.Index:
			c := r.snap.Index + 1 + rng.Uint64N(r.acked-r.snap.Index)
			r.next = SnapshotMeta{Index: c, Term: r.terms[c]}
			if err := saveSnapIn(fsys, r.next); err != nil {
				t.Fatal(err)
			}
			older := r.snap
			r.snap = r.next
			if err := w.CompactBefore(older.Index + 1); err != nil {
				t.Fatal(err)
			}
		case k < 94 && last > r.snap.Index:
			vote()
			s := r.snap.Index + 1 + rng.Uint64N(last-r.snap.Index)
			if rng.IntN(2) == 0 {
				s = last + 1 + rng.Uint64N(20)
			}
			r.next, r.install = SnapshotMeta{Index: s, Term: r.hs.Term}, true
			if err := saveSnapIn(fsys, r.next); err != nil {
				t.Fatal(err)
			}
			r.snap, r.acked = r.next, s
			clear(r.terms)
			if err := w.TruncateAfter(0); err != nil {
				t.Fatal(err)
			}
			r.install = false
			last = s
		}
	}
	_ = w.Close()
	_ = h.Close()
	return points
}

func TestReplicaSequence(t *testing.T) {
	seeds, steps := uint64(16), 250
	if testing.Short() {
		seeds, steps = 6, 120
	}
	points := 0
	for seed := range seeds {
		points += replicaRun(t, seed, steps)
	}
	t.Logf("%d crash points over %d seeds", points, seeds)
}
