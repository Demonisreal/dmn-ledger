package wal

import (
	"bytes"
	"errors"
	"math/rand/v2"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/Demonisreal/dmn-ledger/internal/disk"
)

const dir = "/data/wal"

func newSim(seed uint64) *disk.Sim {
	return disk.NewSim(rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15)))
}

func body(i, term uint64) []byte {
	b := make([]byte, (i*31+term*7)%97)
	for k := range b {
		b[k] = byte(i + term + uint64(k))
	}
	return b
}

func ent(i, term uint64) Entry {
	return Entry{Index: i, Term: term, Kind: uint8(i % 3), Body: body(i, term)}
}

func span(lo, hi, term uint64) []Entry {
	var ents []Entry
	for i := lo; i < hi; i++ {
		ents = append(ents, ent(i, term))
	}
	return ents
}

func mustOpen(t *testing.T, fsys disk.FS, opts Options) *WAL {
	t.Helper()
	w, err := Open(fsys, dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func mustAppend(t *testing.T, w *WAL, ents ...Entry) {
	t.Helper()
	if err := w.Append(ents...); err != nil {
		t.Fatal(err)
	}
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
}

func checkTerms(t *testing.T, w *WAL, lo uint64, terms ...uint64) {
	t.Helper()
	hi := lo + uint64(len(terms))
	if w.FirstIndex() != lo || w.LastIndex() != hi-1 {
		t.Fatalf("log is [%d, %d], want [%d, %d]", w.FirstIndex(), w.LastIndex(), lo, hi-1)
	}
	got, err := w.Entries(lo, hi)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(terms) {
		t.Fatalf("got %d entries, want %d", len(got), len(terms))
	}
	for k, term := range terms {
		e, want := got[k], ent(lo+uint64(k), term)
		if e.Index != want.Index || e.Term != want.Term || e.Kind != want.Kind || !bytes.Equal(e.Body, want.Body) {
			t.Fatalf("entry %d = %+v, want %+v", want.Index, e, want)
		}
		if term, err := w.Term(want.Index); err != nil || term != want.Term {
			t.Fatalf("Term(%d) = %d, %v", want.Index, term, err)
		}
	}
}

func repeat(term uint64, n int) []uint64 {
	terms := make([]uint64, n)
	for i := range terms {
		terms[i] = term
	}
	return terms
}

func segments(t *testing.T, fsys disk.FS) []string {
	t.Helper()
	names, err := fsys.List(dir)
	if err != nil {
		t.Fatal(err)
	}
	var segs []string
	for _, name := range names {
		if strings.HasPrefix(name, "wal-") {
			segs = append(segs, name)
		}
	}
	return segs
}

func TestAppendReopen(t *testing.T) {
	sim := newSim(1)
	opts := Options{SegmentSize: 512}
	w := mustOpen(t, sim, opts)
	for lo := uint64(1); lo <= 200; lo += 7 {
		mustAppend(t, w, span(lo, lo+7, 3)...)
	}
	checkTerms(t, w, 1, repeat(3, 203)...)
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if n := len(segments(t, sim)); n < 10 {
		t.Fatalf("only %d segments for 203 entries", n)
	}
	if got := segments(t, sim)[0]; got != "wal-00000000000000000001.log" {
		t.Fatalf("first segment %q", got)
	}

	w = mustOpen(t, sim, opts)
	checkTerms(t, w, 1, repeat(3, 203)...)
	mustAppend(t, w, ent(204, 4))
	checkTerms(t, w, 1, append(repeat(3, 203), 4)...)
}

func TestAppendRejects(t *testing.T) {
	cases := map[string][]Entry{
		"gap":       {ent(7, 1)},
		"duplicate": {ent(5, 1)},
		"not dense": {ent(6, 1), ent(8, 1)},
		"too large": {{Index: 6, Term: 1, Body: make([]byte, MaxBodySize+1)}},
	}
	for name, ents := range cases {
		t.Run(name, func(t *testing.T) {
			w := mustOpen(t, newSim(1), Options{})
			mustAppend(t, w, span(1, 6, 1)...)
			err := w.Append(ents...)
			if !errors.Is(err, ErrOutOfOrder) && !errors.Is(err, ErrTooLarge) {
				t.Fatalf("append = %v", err)
			}
			mustAppend(t, w, ent(6, 2))
			checkTerms(t, w, 1, 1, 1, 1, 1, 1, 2)
		})
	}

	w := mustOpen(t, newSim(1), Options{})
	if err := w.Append(ent(0, 1)); !errors.Is(err, ErrOutOfOrder) {
		t.Fatalf("index 0: %v", err)
	}
}

func TestEmptyLogAcceptsAnyStart(t *testing.T) {
	sim := newSim(1)
	w := mustOpen(t, sim, Options{})
	if w.FirstIndex() != 0 || w.LastIndex() != 0 {
		t.Fatalf("empty log is [%d, %d]", w.FirstIndex(), w.LastIndex())
	}
	mustAppend(t, w, span(1000, 1003, 9)...)
	_ = w.Close()
	w = mustOpen(t, sim, Options{})
	checkTerms(t, w, 1000, 9, 9, 9)
}

func TestRangeErrors(t *testing.T) {
	w := mustOpen(t, newSim(1), Options{SegmentSize: 256})
	if _, err := w.Entries(1, 2); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("empty log: %v", err)
	}
	mustAppend(t, w, span(1, 40, 1)...)
	if err := w.CompactBefore(20); err != nil {
		t.Fatal(err)
	}
	first := w.FirstIndex()
	if first <= 1 || first > 20 {
		t.Fatalf("first after compaction = %d", first)
	}

	cases := []struct {
		lo, hi uint64
		want   error
	}{
		{first - 1, first + 1, ErrCompacted},
		{first, 41, ErrUnavailable},
		{40, 41, ErrUnavailable},
	}
	for _, c := range cases {
		if _, err := w.Entries(c.lo, c.hi); !errors.Is(err, c.want) {
			t.Errorf("Entries(%d, %d) = %v, want %v", c.lo, c.hi, err, c.want)
		}
	}
	if _, err := w.Term(first - 1); !errors.Is(err, ErrCompacted) {
		t.Errorf("Term(first-1) = %v", err)
	}
	if _, err := w.Term(40); !errors.Is(err, ErrUnavailable) {
		t.Errorf("Term(40) = %v", err)
	}
	if ents, err := w.Entries(5, 5); err != nil || len(ents) != 0 {
		t.Errorf("empty range = %v, %v", ents, err)
	}
}

func TestTruncateAfter(t *testing.T) {
	cases := map[string]uint64{
		"last segment":  58,
		"older segment": 12,
		"keep one":      1,
		"everything":    0,
		"no-op":         60,
	}
	for name, i := range cases {
		t.Run(name, func(t *testing.T) {
			sim := newSim(1)
			opts := Options{SegmentSize: 400}
			w := mustOpen(t, sim, opts)
			mustAppend(t, w, span(1, 61, 1)...)
			if err := w.TruncateAfter(i); err != nil {
				t.Fatal(err)
			}
			if w.LastIndex() != i {
				t.Fatalf("last = %d, want %d", w.LastIndex(), i)
			}
			mustAppend(t, w, span(i+1, i+4, 2)...)
			_ = w.Close()

			sim.Crash()
			w = mustOpen(t, sim, opts)
			checkTerms(t, w, 1, append(repeat(1, int(i)), 2, 2, 2)...)
		})
	}
}

func TestCompactBefore(t *testing.T) {
	sim := newSim(1)
	opts := Options{SegmentSize: 400}
	w := mustOpen(t, sim, opts)
	mustAppend(t, w, span(1, 61, 1)...)
	before := len(segments(t, sim))

	if err := w.CompactBefore(30); err != nil {
		t.Fatal(err)
	}
	first := w.FirstIndex()
	if first > 30 || first == 1 {
		t.Fatalf("first = %d", first)
	}
	if after := len(segments(t, sim)); after >= before {
		t.Fatalf("segments %d -> %d", before, after)
	}
	sim.Crash()
	w = mustOpen(t, sim, opts)
	checkTerms(t, w, first, repeat(1, int(61-first))...)

	if err := w.CompactBefore(61); err != nil {
		t.Fatal(err)
	}
	if n := len(segments(t, sim)); n != 0 || w.LastIndex() != 0 {
		t.Fatalf("after full compaction: %d segments, last %d", n, w.LastIndex())
	}
	mustAppend(t, w, span(500, 502, 4)...)
	sim.Crash()
	w = mustOpen(t, sim, opts)
	checkTerms(t, w, 500, 4, 4)
}

func lastSegment(t *testing.T, sim *disk.Sim) disk.File {
	t.Helper()
	segs := segments(t, sim)
	f, err := sim.Open(filepath.Join(dir, segs[len(segs)-1]))
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func TestTornTailIsCut(t *testing.T) {
	cases := map[string]func(t *testing.T, f disk.File, size int64){
		"half frame": func(t *testing.T, f disk.File, size int64) {
			_ = f.Truncate(size - 5)
		},
		"half header": func(t *testing.T, f disk.File, size int64) {
			_ = f.Truncate(size - int64(frameSize(ent(10, 1))) + 3)
		},
		"zeros after": func(t *testing.T, f disk.File, size int64) {
			_, _ = f.WriteAt(make([]byte, 700), size)
		},
		"garbage after": func(t *testing.T, f disk.File, size int64) {
			_, _ = f.WriteAt(bytes.Repeat([]byte{0xa5}, 700), size)
		},
	}
	for name, tear := range cases {
		t.Run(name, func(t *testing.T) {
			sim := newSim(1)
			w := mustOpen(t, sim, Options{})
			mustAppend(t, w, span(1, 11, 1)...)
			_ = w.Close()

			f := lastSegment(t, sim)
			size, _ := f.Size()
			tear(t, f, size)
			_ = f.Sync()
			_ = f.Close()

			w = mustOpen(t, sim, Options{})
			last := w.LastIndex()
			if last != 9 && last != 10 {
				t.Fatalf("last = %d", last)
			}
			checkTerms(t, w, 1, repeat(1, int(last))...)
			mustAppend(t, w, ent(last+1, 2))
			_ = w.Close()

			w = mustOpen(t, sim, Options{})
			checkTerms(t, w, 1, append(repeat(1, int(last)), 2)...)
		})
	}
}

func TestTornFirstFrameEmptiesLog(t *testing.T) {
	sim := newSim(1)
	w := mustOpen(t, sim, Options{})
	mustAppend(t, w, ent(1, 1))
	_ = w.Close()
	f := lastSegment(t, sim)
	_ = f.Truncate(10)
	_ = f.Sync()

	w = mustOpen(t, sim, Options{})
	if w.LastIndex() != 0 || len(segments(t, sim)) != 0 {
		t.Fatalf("last %d, segments %v", w.LastIndex(), segments(t, sim))
	}
	mustAppend(t, w, ent(7, 2))
	checkTerms(t, w, 7, 2)
}

func frameOffset(lo, i, term uint64) int64 {
	var off int64
	for k := lo; k < i; k++ {
		off += int64(frameSize(ent(k, term)))
	}
	return off
}

func corruptAt(t *testing.T, sim *disk.Sim, seg string, off int64) {
	t.Helper()
	if err := sim.Corrupt(filepath.Join(dir, seg), off); err != nil {
		t.Fatal(err)
	}
	sim.Crash()
}

func wantCorrupt(t *testing.T, err error, seg string, off int64) {
	t.Helper()
	var ce *CorruptError
	if !errors.As(err, &ce) || !errors.Is(err, ErrCorrupt) {
		t.Fatalf("open = %v, want ErrCorrupt", err)
	}
	if ce.File != seg || ce.Offset != off {
		t.Fatalf("corrupt at %s:%d, want %s:%d", ce.File, ce.Offset, seg, off)
	}
}

func TestBitFlipBeforeLaterSyncIsCorrupt(t *testing.T) {
	sim := newSim(1)
	w := mustOpen(t, sim, Options{})
	mustAppend(t, w, span(1, 11, 1)...)
	mustAppend(t, w, span(11, 21, 1)...)
	_ = w.Close()

	seg := segments(t, sim)[0]
	off := frameOffset(1, 5, 1)
	corruptAt(t, sim, seg, off+hdrSize+2)
	_, err := Open(sim, dir, Options{})
	wantCorrupt(t, err, seg, off)
}

func TestBitFlipInOlderSegmentIsCorrupt(t *testing.T) {
	sim := newSim(1)
	opts := Options{SegmentSize: 400}
	w := mustOpen(t, sim, opts)
	mustAppend(t, w, span(1, 41, 1)...)
	_ = w.Close()

	segs := segments(t, sim)
	f, _ := sim.Open(filepath.Join(dir, segs[1]))
	size, _ := f.Size()
	first, _ := parseSegmentName(segs[1])
	second, _ := parseSegmentName(segs[2])
	off := frameOffset(first, second-1, 1)
	if off >= size {
		t.Fatalf("offset %d beyond segment size %d", off, size)
	}
	corruptAt(t, sim, segs[1], off+1)
	_, err := Open(sim, dir, opts)
	wantCorrupt(t, err, segs[1], off)
}

func TestMissingSegmentIsCorrupt(t *testing.T) {
	sim := newSim(1)
	opts := Options{SegmentSize: 400}
	w := mustOpen(t, sim, opts)
	mustAppend(t, w, span(1, 41, 1)...)
	_ = w.Close()

	segs := segments(t, sim)
	_ = sim.Remove(filepath.Join(dir, segs[2]))
	_, err := Open(sim, dir, opts)
	wantCorrupt(t, err, segs[3], 0)
}

func TestBitFlipInLastSyncedBatchLooksTorn(t *testing.T) {
	sim := newSim(1)
	w := mustOpen(t, sim, Options{})
	mustAppend(t, w, span(1, 11, 1)...)
	mustAppend(t, w, span(11, 21, 1)...)
	_ = w.Close()

	corruptAt(t, sim, segments(t, sim)[0], frameOffset(1, 15, 1)+hdrSize)
	w = mustOpen(t, sim, Options{})
	checkTerms(t, w, 1, repeat(1, 14)...)
}

func TestSyncErrorIsSticky(t *testing.T) {
	sim := newSim(1)
	w := mustOpen(t, sim, Options{})
	mustAppend(t, w, span(1, 6, 1)...)

	sim.SyncErrRate = 1
	if err := w.Append(span(6, 9, 1)...); err != nil {
		t.Fatal(err)
	}
	if err := w.Sync(); !errors.Is(err, syscall.EIO) {
		t.Fatalf("sync = %v", err)
	}
	sim.SyncErrRate = 0
	if err := w.Sync(); !errors.Is(err, syscall.EIO) {
		t.Fatalf("second sync = %v", err)
	}
	if err := w.Append(ent(9, 1)); !errors.Is(err, syscall.EIO) {
		t.Fatalf("append after failed sync = %v", err)
	}
	_ = w.Close()

	sim.Crash()
	w = mustOpen(t, sim, Options{})
	if w.LastIndex() < 5 {
		t.Fatalf("synced entries lost: last %d", w.LastIndex())
	}
	checkTerms(t, w, 1, repeat(1, int(w.LastIndex()))...)
}

func TestOpenSyncsWhatItRead(t *testing.T) {
	sim := newSim(1)
	w := mustOpen(t, sim, Options{})
	if err := w.Append(span(1, 30, 1)...); err != nil {
		t.Fatal(err)
	}
	_ = w.Close()
	w = mustOpen(t, sim, Options{})
	_ = w.Close()

	sim.Crash()
	w = mustOpen(t, sim, Options{})
	checkTerms(t, w, 1, repeat(1, 29)...)
}

func TestHoleInUnsyncedTailIsTorn(t *testing.T) {
	sim := newSim(1)
	w := mustOpen(t, sim, Options{})
	mustAppend(t, w, span(1, 11, 1)...)
	if err := w.Append(span(11, 21, 1)...); err != nil {
		t.Fatal(err)
	}
	_ = w.Close()

	f := lastSegment(t, sim)
	_, _ = f.WriteAt(make([]byte, 40), frameOffset(1, 12, 1))
	_ = f.Sync()

	w = mustOpen(t, sim, Options{})
	checkTerms(t, w, 1, repeat(1, 11)...)
}

func FuzzOpen(f *testing.F) {
	f.Add(uint8(0), uint16(40), []byte{0x01}, uint16(0))
	f.Add(uint8(1), uint16(0), []byte{0xff, 0xff, 0xff, 0x7f}, uint16(0))
	f.Add(uint8(2), uint16(100), []byte{}, uint16(17))
	f.Add(uint8(3), uint16(300), make([]byte, 64), uint16(0))
	f.Fuzz(func(t *testing.T, which uint8, off uint16, xor []byte, cut uint16) {
		sim := newSim(1)
		opts := Options{SegmentSize: 512}
		w := mustOpen(t, sim, opts)
		for lo := uint64(1); lo < 31; lo += 10 {
			mustAppend(t, w, span(lo, lo+10, 1)...)
		}
		_ = w.Close()

		segs := segments(t, sim)
		seg, err := sim.Open(filepath.Join(dir, segs[int(which)%len(segs)]))
		if err != nil {
			t.Fatal(err)
		}
		size, _ := seg.Size()
		raw := make([]byte, size)
		_, _ = seg.ReadAt(raw, 0)
		for k, x := range xor {
			raw[(int(off)+k)%len(raw)] ^= x
		}
		_, _ = seg.WriteAt(raw, 0)
		if cut > 0 {
			_ = seg.Truncate(size - int64(cut)%size)
		}

		w, err = Open(sim, dir, opts)
		if err != nil {
			if !errors.Is(err, ErrCorrupt) {
				t.Fatalf("open = %v, want ErrCorrupt", err)
			}
			return
		}
		defer func() { _ = w.Close() }()
		last := w.LastIndex()
		if last == 0 {
			return
		}
		if w.FirstIndex() != 1 {
			t.Fatalf("first = %d", w.FirstIndex())
		}
		checkTerms(t, w, 1, repeat(1, int(last))...)
	})
}
