package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"maps"
	"math"
	"math/rand/v2"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"

	"github.com/Demonisreal/dmn-ledger/internal/disk"
)

const (
	dir     = "/data/wal"
	noLimit = math.MaxInt64
)

func newSim(seed uint64) *disk.Sim {
	sim := disk.NewSim(rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15)))
	_ = sim.MkdirAll(dir)
	return sim
}

func opts(segSize int64) Options {
	return Options{SegmentSize: segSize, Nonces: rand.NewChaCha8([32]byte{7})}
}

func body(i, term uint64) []byte {
	b := make([]byte, (i*31+term*7)%97)
	for k := range b {
		b[k] = byte(i + term + uint64(k))
	}
	return b
}

func ent(i, term uint64) Entry {
	return Entry{Index: i, Term: term, Kind: Kind(i % 3), Body: body(i, term)}
}

func span(lo, hi, term uint64) []Entry {
	var ents []Entry
	for i := lo; i < hi; i++ {
		ents = append(ents, ent(i, term))
	}
	return ents
}

func mustOpen(t *testing.T, fsys disk.FS, o Options) *WAL {
	t.Helper()
	w, err := Open(fsys, dir, o)
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

func bounds(t *testing.T, w *WAL) (uint64, uint64) {
	t.Helper()
	first, err := w.FirstIndex()
	if err != nil {
		t.Fatal(err)
	}
	last, err := w.LastIndex()
	if err != nil {
		t.Fatal(err)
	}
	return first, last
}

func lastIndex(t *testing.T, w *WAL) uint64 {
	t.Helper()
	_, last := bounds(t, w)
	return last
}

func checkTerms(t *testing.T, w *WAL, lo uint64, terms ...uint64) {
	t.Helper()
	hi := lo + uint64(len(terms))
	if first, last := bounds(t, w); first != lo || last != hi-1 {
		t.Fatalf("log is [%d, %d], want [%d, %d]", first, last, lo, hi-1)
	}
	got, err := w.Entries(lo, hi, noLimit)
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
	o := opts(512)
	w := mustOpen(t, sim, o)
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

	w = mustOpen(t, sim, o)
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
		"term back": {ent(6, 2), ent(7, 1)},
	}
	for name, ents := range cases {
		t.Run(name, func(t *testing.T) {
			w := mustOpen(t, newSim(1), opts(0))
			mustAppend(t, w, span(1, 6, 1)...)
			err := w.Append(ents...)
			if !errors.Is(err, ErrOutOfOrder) && !errors.Is(err, ErrTooLarge) {
				t.Fatalf("append = %v", err)
			}
			mustAppend(t, w, ent(6, 2))
			checkTerms(t, w, 1, 1, 1, 1, 1, 1, 2)
		})
	}

	w := mustOpen(t, newSim(1), opts(0))
	if err := w.Append(ent(0, 1)); !errors.Is(err, ErrOutOfOrder) {
		t.Fatalf("index 0: %v", err)
	}
}

func TestEmptyLogAcceptsAnyStart(t *testing.T) {
	sim := newSim(1)
	w := mustOpen(t, sim, opts(0))
	if first, last := bounds(t, w); first != 0 || last != 0 {
		t.Fatalf("empty log is [%d, %d]", first, last)
	}
	mustAppend(t, w, span(1000, 1003, 9)...)
	_ = w.Close()
	w = mustOpen(t, sim, opts(0))
	checkTerms(t, w, 1000, 9, 9, 9)
}

func TestSegmentNames(t *testing.T) {
	cases := map[string]uint64{
		"wal-00000000000000000001.log":  1,
		"wal-18446744073709551615.log":  math.MaxUint64,
		"wal-00000000000000000000.log":  0,
		"wal-0000000000000000001.log":   0,
		"wal-000000000000000000001.log": 0,
		"wal-0000000000000000000x.log":  0,
		"wal-00000000000000000001.tmp":  0,
		"snap-00000000000000000001.log": 0,
	}
	for name, want := range cases {
		first, ok := parseSegmentName(name)
		if ok != (want > 0) || first != want {
			t.Errorf("%s: %d, %v", name, first, ok)
		}
	}
	if got := segmentName(42); got != "wal-00000000000000000042.log" {
		t.Errorf("segmentName(42) = %s", got)
	}
}

func TestRangeErrors(t *testing.T) {
	w := mustOpen(t, newSim(1), opts(256))
	if _, err := w.Entries(1, 2, noLimit); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("empty log: %v", err)
	}
	mustAppend(t, w, span(1, 40, 1)...)
	if err := w.CompactBefore(20); err != nil {
		t.Fatal(err)
	}
	first, _ := bounds(t, w)
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
		if _, err := w.Entries(c.lo, c.hi, noLimit); !errors.Is(err, c.want) {
			t.Errorf("Entries(%d, %d) = %v, want %v", c.lo, c.hi, err, c.want)
		}
	}
	if _, err := w.Term(first - 1); !errors.Is(err, ErrCompacted) {
		t.Errorf("Term(first-1) = %v", err)
	}
	if _, err := w.Term(40); !errors.Is(err, ErrUnavailable) {
		t.Errorf("Term(40) = %v", err)
	}
	if ents, err := w.Entries(5, 5, noLimit); err != nil || len(ents) != 0 {
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
			o := opts(400)
			w := mustOpen(t, sim, o)
			mustAppend(t, w, span(1, 61, 1)...)
			if err := w.TruncateAfter(i); err != nil {
				t.Fatal(err)
			}
			if last := lastIndex(t, w); last != i {
				t.Fatalf("last = %d, want %d", last, i)
			}
			mustAppend(t, w, span(i+1, i+4, 2)...)
			_ = w.Close()

			sim.Crash()
			w = mustOpen(t, sim, o)
			checkTerms(t, w, 1, append(repeat(1, int(i)), 2, 2, 2)...)
		})
	}
}

func TestCompactBefore(t *testing.T) {
	sim := newSim(1)
	o := opts(400)
	w := mustOpen(t, sim, o)
	mustAppend(t, w, span(1, 61, 1)...)
	before := len(segments(t, sim))

	if err := w.CompactBefore(30); err != nil {
		t.Fatal(err)
	}
	first, _ := bounds(t, w)
	if first > 30 || first == 1 {
		t.Fatalf("first = %d", first)
	}
	if after := len(segments(t, sim)); after >= before {
		t.Fatalf("segments %d -> %d", before, after)
	}
	sim.Crash()
	w = mustOpen(t, sim, o)
	checkTerms(t, w, first, repeat(1, int(61-first))...)

	if err := w.CompactBefore(61); err != nil {
		t.Fatal(err)
	}
	if n := len(segments(t, sim)); n != 0 || lastIndex(t, w) != 0 {
		t.Fatalf("after full compaction: %d segments, last %d", n, lastIndex(t, w))
	}
	mustAppend(t, w, span(500, 502, 4)...)
	sim.Crash()
	w = mustOpen(t, sim, o)
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
	cases := map[string]struct {
		tear func(f disk.File, size int64)
		last uint64
	}{
		"half frame":    {func(f disk.File, size int64) { _ = f.Truncate(size - 5) }, 9},
		"half header":   {func(f disk.File, size int64) { _ = f.Truncate(size - int64(frameSize(ent(10, 1))) + 3) }, 9},
		"zeros after":   {func(f disk.File, size int64) { _, _ = f.WriteAt(make([]byte, 700), size) }, 10},
		"garbage after": {func(f disk.File, size int64) { _, _ = f.WriteAt(bytes.Repeat([]byte{0xa5}, 700), size) }, 10},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			sim := newSim(1)
			w := mustOpen(t, sim, opts(0))
			mustAppend(t, w, span(1, 11, 1)...)
			_ = w.Close()

			f := lastSegment(t, sim)
			size, _ := f.Size()
			tc.tear(f, size)
			_ = f.Sync()
			_ = f.Close()

			w = mustOpen(t, sim, opts(0))
			checkTerms(t, w, 1, repeat(1, int(tc.last))...)
			mustAppend(t, w, ent(tc.last+1, 2))
			_ = w.Close()

			w = mustOpen(t, sim, opts(0))
			checkTerms(t, w, 1, append(repeat(1, int(tc.last)), 2)...)
		})
	}
}

func TestTornFirstFrameEmptiesLog(t *testing.T) {
	for _, cut := range []int64{segHdrLen, segHdrLen + 10} {
		sim := newSim(1)
		w := mustOpen(t, sim, opts(0))
		mustAppend(t, w, ent(1, 1))
		_ = w.Close()
		f := lastSegment(t, sim)
		_ = f.Truncate(cut)
		_ = f.Sync()

		w = mustOpen(t, sim, opts(0))
		if lastIndex(t, w) != 0 || len(segments(t, sim)) != 0 {
			t.Fatalf("cut at %d: last %d, segments %v", cut, lastIndex(t, w), segments(t, sim))
		}
		mustAppend(t, w, ent(7, 2))
		checkTerms(t, w, 7, 2)
	}
}

func frameOffset(lo, i, term uint64) int64 {
	off := int64(segHdrLen)
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
	w := mustOpen(t, sim, opts(0))
	mustAppend(t, w, span(1, 11, 1)...)
	mustAppend(t, w, span(11, 21, 1)...)
	_ = w.Close()

	seg := segments(t, sim)[0]
	off := frameOffset(1, 5, 1)
	corruptAt(t, sim, seg, off+frameHdrLen+2)
	_, err := Open(sim, dir, opts(0))
	wantCorrupt(t, err, seg, off)
}

func TestBitFlipInOlderSegmentIsCorrupt(t *testing.T) {
	sim := newSim(1)
	o := opts(400)
	w := mustOpen(t, sim, o)
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
	_, err := Open(sim, dir, o)
	wantCorrupt(t, err, segs[1], off)
}

func TestMissingSegmentIsCorrupt(t *testing.T) {
	sim := newSim(1)
	o := opts(400)
	w := mustOpen(t, sim, o)
	mustAppend(t, w, span(1, 41, 1)...)
	_ = w.Close()

	segs := segments(t, sim)
	_ = sim.Remove(filepath.Join(dir, segs[2]))
	_, err := Open(sim, dir, o)
	wantCorrupt(t, err, segs[3], 0)
}

type damage struct {
	segSize int64
	term    uint64
	sealed  uint64
	strict  bool
	torn    bool
	harm    func(t *testing.T, sim *disk.Sim, segs []string) spot
}

type spot struct {
	seg  string
	off  int64
	keep uint64
}

func firstOf(seg string) uint64 {
	first, _ := parseSegmentName(seg)
	return first
}

func frameAt(segs []string, i uint64) (string, int64) {
	k := len(segs) - 1
	for firstOf(segs[k]) > i {
		k--
	}
	return segs[k], frameOffset(firstOf(segs[k]), i, 1)
}

func flipFrame(i uint64) func(*testing.T, *disk.Sim, []string) spot {
	return func(t *testing.T, sim *disk.Sim, segs []string) spot {
		seg, off := frameAt(segs, i)
		corruptAt(t, sim, seg, off+offTerm)
		return spot{seg, off, i - 1}
	}
}

func halfLastFrame(t *testing.T, sim *disk.Sim, segs []string) spot {
	seg, off := frameAt(segs, 40)
	f, err := sim.Open(filepath.Join(dir, seg))
	if err != nil {
		t.Fatal(err)
	}
	_ = f.Truncate(off + 7)
	_ = f.Sync()
	return spot{seg, off, 39}
}

func flipHeader(k int) func(*testing.T, *disk.Sim, []string) spot {
	return func(t *testing.T, sim *disk.Sim, segs []string) spot {
		seg := segs[(k+len(segs))%len(segs)]
		corruptAt(t, sim, seg, segOffNonce)
		return spot{seg, 0, firstOf(seg) - 1}
	}
}

func damages() map[string]damage {
	return map[string]damage{
		"flip behind the seal":                  {sealed: 35, torn: true, harm: flipFrame(36)},
		"flip at the seal":                      {sealed: 36, harm: flipFrame(36)},
		"flip behind the seal, strict":          {sealed: 35, strict: true, harm: flipFrame(36)},
		"half frame behind the seal":            {sealed: 39, torn: true, harm: halfLastFrame},
		"half frame under the seal":             {sealed: 40, harm: halfLastFrame},
		"half frame, strict":                    {strict: true, harm: halfLastFrame},
		"no frame in the tail, behind the seal": {segSize: 1, sealed: 39, torn: true, harm: halfLastFrame},
		"no frame in the tail, under the seal":  {segSize: 1, sealed: 40, harm: halfLastFrame},
		"no frame in the tail, strict":          {segSize: 1, strict: true, harm: halfLastFrame},
		"flip before a later sync":              {harm: flipFrame(5)},
		"flip in an older segment":              {segSize: 400, harm: flipFrame(12)},
		"flip in the first frame":               {segSize: 400, harm: flipFrame(1)},
		"header of an older segment":            {segSize: 400, harm: flipHeader(2)},
		"header of the first segment":           {segSize: 400, harm: flipHeader(0)},
		"header of the last segment":            {segSize: 400, harm: flipHeader(-1)},
		"missing segment": {segSize: 400, harm: func(_ *testing.T, sim *disk.Sim, segs []string) spot {
			_ = sim.Remove(filepath.Join(dir, segs[2]))
			_ = sim.SyncDir(dir)
			return spot{segs[3], 0, firstOf(segs[2]) - 1}
		}},
		"term going back": {segSize: 1, term: 5, harm: func(t *testing.T, sim *disk.Sim, segs []string) spot {
			low := newSim(2)
			w := mustOpen(t, low, opts(1))
			mustAppend(t, w, span(1, 41, 1)...)
			_ = w.Close()
			f, _ := sim.Create(filepath.Join(dir, segs[7]))
			_, _ = f.WriteAt(contents(t, low, filepath.Join(dir, segs[7])), 0)
			_ = f.Sync()
			return spot{segs[7], segHdrLen, 7}
		}},
		"oversized segment": {segSize: 400, harm: func(_ *testing.T, sim *disk.Sim, segs []string) spot {
			seg := segs[len(segs)-1]
			f, _ := sim.Open(filepath.Join(dir, seg))
			_ = f.Truncate(maxSegmentFile + 1)
			_ = f.Sync()
			return spot{seg, maxSegmentFile, firstOf(seg) - 1}
		}},
	}
}

func damagedLog(t *testing.T, d damage) (*disk.Sim, Options, spot) {
	t.Helper()
	sim := newSim(1)
	o := opts(d.segSize)
	w := mustOpen(t, sim, o)
	mustAppend(t, w, span(1, 21, max(d.term, 1))...)
	mustAppend(t, w, span(21, 41, max(d.term, 1))...)
	_ = w.Close()
	at := d.harm(t, sim, segments(t, sim))
	o.Sealed, o.Strict = d.sealed, d.strict
	return sim, o, at
}

func checkKept(t *testing.T, w *WAL, d damage, keep uint64) {
	t.Helper()
	if keep > 0 {
		checkTerms(t, w, 1, repeat(max(d.term, 1), int(keep))...)
	} else if first, last := bounds(t, w); first != 0 || last != 0 {
		t.Fatalf("log is [%d, %d], want an empty log", first, last)
	}
}

func contents(t *testing.T, fsys disk.FS, name string) []byte {
	t.Helper()
	f, err := fsys.Open(name)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	size, _ := f.Size()
	b := make([]byte, size)
	if _, err := f.ReadAt(b, 0); err != nil && !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	return b
}

func files(t *testing.T, sim *disk.Sim) map[string][]byte {
	t.Helper()
	names, err := sim.List(dir)
	if err != nil {
		t.Fatal(err)
	}
	held := map[string][]byte{}
	for _, name := range names {
		held[name] = contents(t, sim, filepath.Join(dir, name))
	}
	return held
}

func TestOpenJudgesDamageByTheSeal(t *testing.T) {
	for name, d := range damages() {
		t.Run(name, func(t *testing.T) {
			sim, o, at := damagedLog(t, d)
			before := files(t, sim)
			w, err := Open(sim, dir, o)
			if !d.torn {
				wantCorrupt(t, err, at.seg, at.off)
				if !maps.EqualFunc(before, files(t, sim), bytes.Equal) {
					t.Fatal("open reported corruption and changed the files")
				}
				return
			}
			if err != nil {
				t.Fatalf("open = %v, want the torn tail cut", err)
			}
			checkKept(t, w, d, at.keep)
			mustAppend(t, w, ent(at.keep+1, 9))
			_ = w.Close()
			w = mustOpen(t, sim, o)
			if last := lastIndex(t, w); last != at.keep+1 {
				t.Fatalf("append after the cut: last %d, want %d", last, at.keep+1)
			}
		})
	}
}

func TestSalvageKeepsAValidStart(t *testing.T) {
	for name, d := range damages() {
		t.Run(name, func(t *testing.T) {
			sim, o, at := damagedLog(t, d)
			salvage := o
			salvage.Salvage = true
			w := mustOpen(t, sim, salvage)
			checkKept(t, w, d, at.keep)
			if err := w.Append(ent(at.keep+1, 0)); at.keep > 0 && !errors.Is(err, ErrOutOfOrder) {
				t.Fatalf("append with a term below the kept entries = %v", err)
			}
			_ = w.Close()
			sim.Crash()

			w = mustOpen(t, sim, o)
			checkKept(t, w, d, at.keep)
			mustAppend(t, w, ent(at.keep+1, 9))
			_ = w.Close()
			w = mustOpen(t, sim, o)
			if last := lastIndex(t, w); last != at.keep+1 {
				t.Fatalf("append after salvage: last %d, want %d", last, at.keep+1)
			}
		})
	}
}

func TestCrashDuringSalvage(t *testing.T) {
	points := 0
	for name, d := range damages() {
		sim, o, at := damagedLog(t, d)
		salvage := o
		salvage.Salvage = true
		check := func(c *disk.Sim, tag string) {
			points++
			w, err := Open(c, dir, o)
			switch {
			case errors.Is(err, ErrCorrupt):
			case err != nil:
				t.Fatalf("%s, %s: open = %v", name, tag, err)
			default:
				checkKept(t, w, d, at.keep)
				_ = w.Close()
			}
			w = mustOpen(t, c, salvage)
			checkKept(t, w, d, at.keep)
			_ = w.Close()
		}
		fsys := hookFS{Sim: sim, after: func(op string) {
			lost := fork(sim, uint64(points))
			lost.Crash()
			check(lost, "power loss after "+op)
			check(fork(sim, uint64(points)), "process crash after "+op)
		}}
		w := mustOpen(t, fsys, salvage)
		_ = w.Close()
	}
	if points < 100 {
		t.Fatalf("only %d crash points", points)
	}
}

func TestSalvageSyncsWhatItKeeps(t *testing.T) {
	sim := newSim(1)
	w := mustOpen(t, sim, opts(0))
	mustAppend(t, w, span(1, 6, 1)...)
	if err := w.Append(span(6, 9, 1)...); err != nil {
		t.Fatal(err)
	}
	sim.FailSyncAfter(1)
	if err := w.Sync(); !errors.Is(err, syscall.EIO) {
		t.Fatalf("sync = %v", err)
	}
	_ = w.Close()
	f := lastSegment(t, sim)
	size, _ := f.Size()
	_, _ = f.WriteAt(bytes.Repeat([]byte{0xa5}, 100), size)
	_ = f.Sync()

	o := opts(0)
	o.Sealed, o.Strict = 5, true
	_, err := Open(sim, dir, o)
	wantCorrupt(t, err, segments(t, sim)[0], size)
	o.Salvage = true
	w = mustOpen(t, sim, o)
	kept := lastIndex(t, w)
	_ = w.Close()
	sim.Crash()
	w = mustOpen(t, sim, opts(0))
	if got := lastIndex(t, w); kept != 8 || got != kept {
		t.Fatalf("salvage kept %d entries, after power loss %d", kept, got)
	}
}

func TestSyncErrorIsSticky(t *testing.T) {
	sim := newSim(1)
	w := mustOpen(t, sim, opts(0))
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
	w = mustOpen(t, sim, opts(0))
	last := lastIndex(t, w)
	if last < 5 {
		t.Fatalf("synced entries lost: last %d", last)
	}
	checkTerms(t, w, 1, repeat(1, int(last))...)
}

func TestOpenSyncsWhatItRead(t *testing.T) {
	sim := newSim(1)
	w := mustOpen(t, sim, opts(0))
	if err := w.Append(span(1, 30, 1)...); err != nil {
		t.Fatal(err)
	}
	_ = w.Close()
	w = mustOpen(t, sim, opts(0))
	_ = w.Close()

	sim.Crash()
	w = mustOpen(t, sim, opts(0))
	checkTerms(t, w, 1, repeat(1, 29)...)
}

func TestHoleInUnsyncedTailIsTorn(t *testing.T) {
	sim := newSim(1)
	w := mustOpen(t, sim, opts(0))
	mustAppend(t, w, span(1, 11, 1)...)
	if err := w.Append(span(11, 21, 1)...); err != nil {
		t.Fatal(err)
	}
	_ = w.Close()

	f := lastSegment(t, sim)
	_, _ = f.WriteAt(make([]byte, 40), frameOffset(1, 12, 1))
	_ = f.Sync()

	w = mustOpen(t, sim, opts(0))
	checkTerms(t, w, 1, repeat(1, 11)...)
}

func TestUnknownVersionIsReported(t *testing.T) {
	sim := newSim(1)
	w := mustOpen(t, sim, opts(0))
	mustAppend(t, w, span(1, 4, 1)...)
	_ = w.Close()

	f := lastSegment(t, sim)
	hdr := make([]byte, segHdrLen)
	_, _ = f.ReadAt(hdr, 0)
	binary.LittleEndian.PutUint16(hdr[segOffVersion:], segVersion+1)
	binary.LittleEndian.PutUint32(hdr[segOffCRC:], crc32.Checksum(hdr[:segOffCRC], castagnoli))
	_, _ = f.WriteAt(hdr, 0)

	before := files(t, sim)
	for _, salvage := range []bool{false, true} {
		o := opts(0)
		o.Sealed, o.Salvage = 3, salvage
		if _, err := Open(sim, dir, o); !errors.Is(err, ErrVersion) || errors.Is(err, ErrCorrupt) {
			t.Fatalf("open with salvage %v = %v, want ErrVersion", salvage, err)
		}
		if !maps.EqualFunc(before, files(t, sim), bytes.Equal) {
			t.Fatalf("open with salvage %v changed a segment of another version", salvage)
		}
	}
}

func TestFramesFromOtherSegmentsProveNothing(t *testing.T) {
	for _, foreign := range []bool{true, false} {
		sim := newSim(1)
		w := mustOpen(t, sim, opts(0))
		mustAppend(t, w, span(1, 11, 1)...)
		seed := w.segs[0].seed
		_ = w.Close()

		if foreign {
			other := mustOpen(t, newSim(2), Options{Nonces: rand.NewChaCha8([32]byte{8})})
			mustAppend(t, other, ent(1, 1))
			seed = other.segs[0].seed
			_ = other.Close()
		}
		off := frameOffset(1, 11, 1)
		forged := appendFrame(make([]byte, 40), Entry{Index: 12, Term: 1}, 11, seed)
		f := lastSegment(t, sim)
		_, _ = f.WriteAt(forged, off)
		_ = f.Sync()

		_, err := Open(sim, dir, opts(0))
		switch {
		case foreign && err != nil:
			t.Fatalf("frame with foreign nonce counted as proof: %v", err)
		case !foreign:
			wantCorrupt(t, err, segments(t, sim)[0], off)
		}
	}
}

func TestTermGoingBackIsCorrupt(t *testing.T) {
	high, low := newSim(1), newSim(2)
	for _, c := range []struct {
		sim  *disk.Sim
		term uint64
	}{{high, 5}, {low, 1}} {
		w := mustOpen(t, c.sim, opts(1))
		mustAppend(t, w, span(1, 4, c.term)...)
		_ = w.Close()
	}
	name := segments(t, low)[2]
	src, _ := low.Open(filepath.Join(dir, name))
	size, _ := src.Size()
	raw := make([]byte, size)
	_, _ = src.ReadAt(raw, 0)
	dst, _ := high.Create(filepath.Join(dir, name))
	_, _ = dst.WriteAt(raw, 0)

	_, err := Open(high, dir, opts(1))
	wantCorrupt(t, err, name, segHdrLen)
}

func TestOversizedSegmentIsCorrupt(t *testing.T) {
	sim := newSim(1)
	w := mustOpen(t, sim, opts(0))
	mustAppend(t, w, span(1, 4, 1)...)
	_ = w.Close()
	f := lastSegment(t, sim)
	_ = f.Truncate(maxSegmentFile + 1)

	_, err := Open(sim, dir, opts(0))
	wantCorrupt(t, err, segments(t, sim)[0], maxSegmentFile)
}

func TestFailedAppendHidesUnwrittenEntries(t *testing.T) {
	sim := newSim(1)
	w := mustOpen(t, sim, opts(0))
	mustAppend(t, w, span(1, 4, 1)...)
	sim.WriteErrRate = 1
	if err := w.Append(span(4, 8, 1)...); !errors.Is(err, syscall.EIO) {
		t.Fatalf("append = %v", err)
	}
	if _, err := w.LastIndex(); !errors.Is(err, syscall.EIO) {
		t.Fatalf("LastIndex = %v", err)
	}
	if _, err := w.Term(5); !errors.Is(err, syscall.EIO) {
		t.Fatalf("Term = %v", err)
	}
	if _, err := w.Entries(1, 4, noLimit); !errors.Is(err, syscall.EIO) {
		t.Fatalf("Entries = %v", err)
	}
	if n := len(w.segs[0].terms); n != 3 {
		t.Fatalf("index holds %d entries, 3 were written", n)
	}

	w = mustOpen(t, newSim(2), opts(0))
	_ = w.Close()
	if _, err := w.Entries(1, 2, noLimit); !errors.Is(err, ErrClosed) {
		t.Fatalf("Entries after close = %v", err)
	}
}

func TestEntriesMaxBytes(t *testing.T) {
	w := mustOpen(t, newSim(1), opts(300))
	var ents []Entry
	for i := uint64(1); i <= 20; i++ {
		ents = append(ents, Entry{Index: i, Term: 1, Body: make([]byte, 63)})
	}
	mustAppend(t, w, ents...)
	const size = frameHdrLen + 63

	cases := []struct {
		lo, hi   uint64
		maxBytes int64
		want     int
	}{
		{1, 21, noLimit, 20},
		{1, 21, 0, 1},
		{1, 21, size - 1, 1},
		{1, 21, size, 1},
		{1, 21, 3*size + 5, 3},
		{2, 21, 10 * size, 10},
		{18, 21, 10 * size, 3},
	}
	for _, c := range cases {
		got, err := w.Entries(c.lo, c.hi, c.maxBytes)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != c.want || got[0].Index != c.lo || got[len(got)-1].Index != c.lo+uint64(c.want)-1 {
			t.Errorf("Entries(%d, %d, %d) = %d entries, want %d", c.lo, c.hi, c.maxBytes, len(got), c.want)
		}
	}
}

func freezeAt(sim *disk.Sim, op string, armed *bool) (hookFS, func() *disk.Sim) {
	var frozen *disk.Sim
	fsys := hookFS{Sim: sim, after: func(got string) {
		if *armed && got == op && frozen == nil {
			frozen = fork(sim, 1)
		}
	}}
	return fsys, func() *disk.Sim { return frozen }
}

func TestProcessCrashInRollKeepsAckedEntries(t *testing.T) {
	sim := newSim(1)
	armed := false
	fsys, frozen := freezeAt(sim, "create", &armed)
	w := mustOpen(t, fsys, opts(300))
	mustAppend(t, w, span(1, 6, 1)...)
	armed = true
	for i := uint64(6); frozen() == nil; i++ {
		mustAppend(t, w, ent(i, 1))
	}

	proc := frozen()
	w = mustOpen(t, proc, opts(300))
	last := lastIndex(t, w)
	mustAppend(t, w, span(last+1, last+4, 2)...)
	_ = w.Close()

	proc.Crash()
	w = mustOpen(t, proc, opts(300))
	if got := lastIndex(t, w); got != last+3 {
		t.Fatalf("acknowledged up to %d, after power loss %d", last+3, got)
	}
}

func TestProcessCrashInTruncateDoesNotResurrect(t *testing.T) {
	sim := newSim(1)
	armed := false
	fsys, frozen := freezeAt(sim, "remove", &armed)
	w := mustOpen(t, fsys, opts(400))
	for i := uint64(1); i <= 3; i++ {
		mustAppend(t, w, Entry{Index: i, Term: 1, Body: make([]byte, 50)})
	}
	mustAppend(t, w, Entry{Index: 4, Term: 2, Body: make([]byte, 200)})
	if n := len(segments(t, sim)); n != 2 {
		t.Fatalf("%d segments, want 2", n)
	}
	armed = true
	if err := w.TruncateAfter(3); err != nil {
		t.Fatal(err)
	}

	proc := frozen()
	w = mustOpen(t, proc, opts(400))
	if err := w.TruncateAfter(2); err != nil {
		t.Fatal(err)
	}
	mustAppend(t, w, Entry{Index: 3, Term: 3})
	_ = w.Close()

	proc.Crash()
	w, err := Open(proc, dir, opts(400))
	if err != nil {
		t.Fatal(err)
	}
	if _, last := bounds(t, w); last != 3 {
		t.Fatalf("last = %d after power loss, removed segment came back", last)
	}
	if term, _ := w.Term(3); term != 3 {
		t.Fatalf("term at 3 = %d", term)
	}
}

func TestOpenRewritesPagesAfterFailedSync(t *testing.T) {
	dropped := 0
	for seed := range uint64(30) {
		sim := newSim(seed)
		w := mustOpen(t, sim, opts(0))
		mustAppend(t, w, span(1, 6, 1)...)
		if err := w.Append(span(6, 9, 1)...); err != nil {
			t.Fatal(err)
		}
		sim.SyncErrRate = 1
		if err := w.Sync(); !errors.Is(err, syscall.EIO) {
			t.Fatalf("sync = %v", err)
		}
		sim.SyncErrRate = 0
		_ = w.Close()

		c := fork(sim, seed)
		c.Crash()
		if w, err := Open(c, dir, opts(0)); err == nil && lastIndex(t, w) == 5 {
			dropped++
		}

		w = mustOpen(t, sim, opts(0))
		acked := lastIndex(t, w)
		_ = w.Close()
		sim.Crash()
		w = mustOpen(t, sim, opts(0))
		if got := lastIndex(t, w); got != acked {
			t.Fatalf("seed %d: open reported %d entries, after power loss %d", seed, acked, got)
		}
	}
	if dropped == 0 {
		t.Fatal("failed sync never dropped the dirty pages")
	}
}

func FuzzOpen(f *testing.F) {
	f.Add(uint8(0), uint16(40), []byte{0x01}, uint16(0), uint8(0), false, false)
	f.Add(uint8(1), uint16(0), []byte{0xff, 0xff, 0xff, 0x7f}, uint16(0), uint8(30), false, true)
	f.Add(uint8(2), uint16(100), []byte{}, uint16(17), uint8(29), false, false)
	f.Add(uint8(2), uint16(100), []byte{}, uint16(17), uint8(0), true, true)
	f.Add(uint8(3), uint16(300), make([]byte, 64), uint16(0), uint8(12), true, false)
	f.Fuzz(func(t *testing.T, which uint8, off uint16, xor []byte, cut uint16, sealed uint8, strict, salvage bool) {
		sim := newSim(1)
		o := opts(512)
		w := mustOpen(t, sim, o)
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

		o.Sealed, o.Strict, o.Salvage = uint64(sealed), strict, salvage
		w, err = Open(sim, dir, o)
		if err != nil {
			if salvage && errors.Is(err, ErrCorrupt) {
				t.Fatalf("open with Salvage = %v", err)
			}
			if !errors.Is(err, ErrCorrupt) && !errors.Is(err, ErrVersion) {
				t.Fatalf("open = %v, want ErrCorrupt", err)
			}
			return
		}
		first, last := bounds(t, w)
		if last > 0 {
			if first != 1 {
				t.Fatalf("first = %d", first)
			}
			checkTerms(t, w, 1, repeat(1, int(last))...)
		}
		_ = w.Close()

		o.Salvage = false
		w, err = Open(sim, dir, o)
		if err != nil {
			t.Fatalf("second open = %v, the first one left [%d, %d]", err, first, last)
		}
		defer func() { _ = w.Close() }()
		if again, end := bounds(t, w); again != first || end != last {
			t.Fatalf("second open sees [%d, %d], the first one left [%d, %d]", again, end, first, last)
		}
	})
}

func TestCompactToEmptyResetsTerm(t *testing.T) {
	sim := newSim(1)
	w := mustOpen(t, sim, opts(0))
	mustAppend(t, w, span(1, 10, 4)...)
	mustAppend(t, w, ent(10, 5))
	if err := w.CompactBefore(11); err != nil {
		t.Fatal(err)
	}
	if err := w.Append(ent(16, 4)); err != nil {
		t.Fatalf("term 4 after compacting away term 5: %v", err)
	}
	_ = w.Close()
	w = mustOpen(t, sim, opts(0))
	checkTerms(t, w, 16, 4)
}

func TestLowerTermAfterTruncate(t *testing.T) {
	w := mustOpen(t, newSim(1), opts(0))
	mustAppend(t, w, span(1, 6, 2)...)
	mustAppend(t, w, span(6, 9, 4)...)
	if err := w.TruncateAfter(5); err != nil {
		t.Fatal(err)
	}
	if err := w.Append(ent(6, 3)); err != nil {
		t.Fatalf("term 3 after cutting the entries of term 4: %v", err)
	}
	if err := w.Append(ent(7, 2)); !errors.Is(err, ErrOutOfOrder) {
		t.Fatalf("term 2 after term 3: %v", err)
	}
	if err := w.TruncateAfter(0); err != nil {
		t.Fatal(err)
	}
	if err := w.Append(ent(1, 1)); err != nil {
		t.Fatalf("term 1 after emptying the log: %v", err)
	}
}

func TestOpenCutsTornTailOnDisk(t *testing.T) {
	sim := newSim(1)
	w := mustOpen(t, sim, opts(0))
	fixed := func(i uint64, fill byte) Entry {
		return Entry{Index: i, Term: 1, Body: bytes.Repeat([]byte{fill}, 60)}
	}
	mustAppend(t, w, fixed(1, 'a'), fixed(2, 'a'))
	if err := w.Append(fixed(3, 'a'), fixed(4, 'a')); err != nil {
		t.Fatal(err)
	}
	_ = w.Close()

	f := lastSegment(t, sim)
	third := int64(segHdrLen + 2*frameSize(fixed(1, 'a')))
	_, _ = f.WriteAt(make([]byte, 20), third+frameHdrLen)
	_ = f.Sync()

	w = mustOpen(t, sim, opts(0))
	if last := lastIndex(t, w); last != 2 {
		t.Fatalf("last = %d after torn entry 3", last)
	}
	mustAppend(t, w, fixed(3, 'b'))
	_ = w.Close()

	w = mustOpen(t, sim, opts(0))
	if last := lastIndex(t, w); last != 3 {
		t.Fatalf("last = %d: entry 4 from before the crash came back behind a new entry 3", last)
	}
}

func TestOlderSegmentWithoutHeaderIsCorrupt(t *testing.T) {
	sim := newSim(1)
	o := opts(400)
	w := mustOpen(t, sim, o)
	mustAppend(t, w, span(1, 41, 1)...)
	_ = w.Close()
	segs := segments(t, sim)
	f, _ := sim.Open(filepath.Join(dir, segs[1]))
	_, _ = f.WriteAt(make([]byte, segHdrLen), 0)
	_, err := Open(sim, dir, o)
	wantCorrupt(t, err, segs[1], 0)
}

func TestTornHeaderIsCorrupt(t *testing.T) {
	for _, cut := range []int64{0, 10} {
		sim := newSim(1)
		w := mustOpen(t, sim, opts(0))
		mustAppend(t, w, ent(1, 1))
		_ = w.Close()
		_ = lastSegment(t, sim).Truncate(cut)
		_, err := Open(sim, dir, opts(0))
		wantCorrupt(t, err, segmentName(1), 0)
	}
}

func TestCrashInRollLeavesNoSegment(t *testing.T) {
	sim := newSim(1)
	o := opts(300)
	armed := false
	fsys, frozen := freezeAt(sim, "create", &armed)
	w := mustOpen(t, fsys, o)
	mustAppend(t, w, span(1, 4, 1)...)
	armed = true
	for i := uint64(4); frozen() == nil; i++ {
		mustAppend(t, w, ent(i, 1))
	}
	proc := frozen()
	names, _ := proc.List(dir)
	if !slices.ContainsFunc(names, func(name string) bool { return strings.HasSuffix(name, ".tmp") }) {
		t.Fatalf("no temporary segment while rolling: %v", names)
	}
	last := lastIndex(t, w)
	w = mustOpen(t, proc, o)
	if got := lastIndex(t, w); got != last-1 {
		t.Fatalf("last = %d, want %d", got, last-1)
	}
	if names, _ := proc.List(dir); slices.ContainsFunc(names, func(name string) bool { return strings.HasSuffix(name, ".tmp") }) {
		t.Fatalf("temporary segment left after open: %v", names)
	}
}

func TestEmptyLastSegmentIsRemoved(t *testing.T) {
	sim := newSim(1)
	o := opts(300)
	w := mustOpen(t, sim, o)
	mustAppend(t, w, span(1, 20, 1)...)
	_ = w.Close()
	segs := segments(t, sim)
	_ = lastSegment(t, sim).Truncate(segHdrLen)
	first, _ := parseSegmentName(segs[len(segs)-1])

	w = mustOpen(t, sim, o)
	if got := segments(t, sim); len(got) != len(segs)-1 {
		t.Fatalf("segments %v after open, before %v", got, segs)
	}
	if err := w.CompactBefore(first); err != nil {
		t.Fatal(err)
	}
	if lo, hi := bounds(t, w); lo > hi {
		t.Fatalf("log is [%d, %d]", lo, hi)
	}
}

func TestOpenRewritesOnlyUnprovenTail(t *testing.T) {
	sim := newSim(1)
	w := mustOpen(t, sim, opts(0))
	pad := make([]byte, 128)
	for i := uint64(1); i <= 20000; i += 1000 {
		var ents []Entry
		for k := range uint64(1000) {
			ents = append(ents, Entry{Index: i + k, Term: 1, Body: pad})
		}
		mustAppend(t, w, ents...)
	}
	_ = w.Close()

	fsys := &windowFS{Sim: sim, open: map[int64]bool{}}
	w = mustOpen(t, fsys, opts(0))
	if batch := int64(1000 * (frameHdrLen + 128)); fsys.bytes != batch {
		t.Fatalf("open rewrote %d bytes, the last unproven batch has %d", fsys.bytes, batch)
	}
	if err := w.Append(Entry{Index: 20001, Term: 1}); err != nil {
		t.Fatal(err)
	}
	_ = w.Close()
	fsys.bytes = 0
	w = mustOpen(t, fsys, opts(0))
	if fsys.bytes != frameHdrLen {
		t.Fatalf("open rewrote %d bytes after one unsynced entry", fsys.bytes)
	}
	_ = w.Close()
}

func TestResetFromTheBack(t *testing.T) {
	const snap = 10
	points := 0
	for _, sector := range []int64{disk.SectorSize, 4096} {
		sim := newSim(uint64(sector))
		sim.Sector = sector
		o := opts(400)
		check := func(op string) {}
		fsys := hookFS{Sim: sim, after: func(op string) { check(op) }}
		w := mustOpen(t, fsys, o)
		mustAppend(t, w, span(1, 61, 1)...)
		check = func(op string) {
			points++
			c := fork(sim, uint64(points))
			c.Crash()
			r := mustOpen(t, c, o)
			first, last := bounds(t, r)
			switch {
			case last == 0:
			case first == 1:
				checkTerms(t, r, 1, repeat(1, int(last))...)
			case first == snap+1:
				checkTerms(t, r, snap+1, repeat(2, int(last-snap))...)
			default:
				t.Fatalf("power loss after %s: log [%d, %d] next to a snapshot at %d", op, first, last, snap)
			}
			_ = r.Close()
		}
		if err := w.TruncateAfter(0); err != nil {
			t.Fatal(err)
		}
		mustAppend(t, w, span(snap+1, snap+30, 2)...)
		_ = w.Close()
	}
	if points < 40 {
		t.Fatalf("only %d crash points", points)
	}
}

func TestCloseAfterFailedRemove(t *testing.T) {
	sim := newSim(1)
	w := mustOpen(t, sim, opts(400))
	mustAppend(t, w, span(1, 61, 1)...)
	sim.MetaErrRate = 1
	if err := w.CompactBefore(30); !errors.Is(err, syscall.EIO) {
		t.Fatalf("compact = %v", err)
	}
	sim.MetaErrRate = 0
	if err := w.Close(); err != nil {
		t.Fatalf("close after failed compaction: %v", err)
	}
}

func TestOpenCreatesDirectories(t *testing.T) {
	sim := disk.NewSim(rand.New(rand.NewPCG(1, 1)))
	m, _, err := OpenMeta(sim, "/node/a")
	if err != nil {
		t.Fatal(err)
	}
	_ = m.Close()
	if err := SaveSnapshot(sim, "/node/a/snap", SnapshotMeta{Index: 1, Term: 1}, func(io.Writer) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if _, _, err := LoadSnapshot(sim, "/node/b/snap", func(io.Reader) error { return nil }); !errors.Is(err, ErrNoSnapshot) {
		t.Fatalf("load from a new directory: %v", err)
	}
	w, err := Open(sim, "/node/a/wal", opts(0))
	if err != nil {
		t.Fatal(err)
	}
	mustAppend(t, w, ent(1, 1))
	_ = w.Close()
	sim.Crash()
	w, err = Open(sim, "/node/a/wal", opts(0))
	if err != nil {
		t.Fatal(err)
	}
	if last := lastIndex(t, w); last != 1 {
		t.Fatalf("last = %d after power loss", last)
	}
}

func TestOpenIsLocked(t *testing.T) {
	sim := newSim(1)
	w := mustOpen(t, sim, opts(0))
	if _, err := Open(sim, dir, opts(0)); !errors.Is(err, disk.ErrLocked) {
		t.Fatalf("second open = %v", err)
	}
	_ = w.Close()
	w = mustOpen(t, sim, opts(0))
	_ = w.Close()
}

func TestSegmentSizeOutOfRange(t *testing.T) {
	for _, size := range []int64{-1, MaxSegmentSize + 1} {
		if _, err := Open(newSim(1), dir, opts(size)); err == nil {
			t.Errorf("segment size %d accepted", size)
		}
	}
}

func TestRealFilesystem(t *testing.T) {
	root := t.TempDir()
	fsys := disk.OS{}
	o := Options{SegmentSize: 400}
	walDir := filepath.Join(root, "node", "wal")
	w, err := Open(fsys, walDir, o)
	if err != nil {
		t.Fatal(err)
	}
	mustAppend(t, w, span(1, 61, 1)...)
	if err := w.TruncateAfter(40); err != nil {
		t.Fatal(err)
	}
	mustAppend(t, w, span(41, 51, 2)...)
	if err := w.CompactBefore(20); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(fsys, walDir, o); !errors.Is(err, disk.ErrLocked) {
		t.Fatalf("second open = %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	names, _ := fsys.List(walDir)
	f, err := fsys.Open(filepath.Join(walDir, names[len(names)-1]))
	if err != nil {
		t.Fatal(err)
	}
	size, _ := f.Size()
	if err := f.Truncate(size - 7); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()

	w, err = Open(fsys, walDir, o)
	if err != nil {
		t.Fatal(err)
	}
	first, last := bounds(t, w)
	if first == 0 || first > 20 || last != 49 {
		t.Fatalf("log is [%d, %d], want [<=20, 49]", first, last)
	}
	terms := repeat(1, int(41-first))
	checkTerms(t, w, first, append(terms, repeat(2, 9)...)...)
	_ = w.Close()

	nodeDir := filepath.Join(root, "node")
	m, st, err := OpenMeta(fsys, nodeDir)
	if err != nil || st != (Meta{}) || !m.Intact() {
		t.Fatalf("meta: %+v, %v", st, err)
	}
	if err := m.Settle(); err != nil {
		t.Fatal(err)
	}
	for term := uint64(1); term <= 3; term++ {
		if err := m.Save(term, term, term == 2); err != nil {
			t.Fatal(err)
		}
		if err := m.Seal(term * 10); err != nil {
			t.Fatal(err)
		}
	}
	_ = m.Close()
	if m, st, err = OpenMeta(fsys, nodeDir); err != nil || st != (Meta{Term: 3, Vote: 3, Sealed: 30}) || !m.Intact() {
		t.Fatalf("meta after reopen: %+v, %v", st, err)
	}
	_ = m.Close()

	snapDir := filepath.Join(nodeDir, "snap")
	for _, meta := range []SnapshotMeta{{10, 1}, {20, 1}, {30, 2}} {
		if err := SaveSnapshot(fsys, snapDir, meta, func(w io.Writer) error {
			_, err := w.Write(snapBody(meta))
			return err
		}); err != nil {
			t.Fatal(err)
		}
	}
	var got []byte
	meta, broken, err := LoadSnapshot(fsys, snapDir, func(r io.Reader) error {
		var err error
		got, err = io.ReadAll(r)
		return err
	})
	if err != nil || meta != (SnapshotMeta{30, 2}) || len(broken) != 0 || !bytes.Equal(got, snapBody(meta)) {
		t.Fatalf("snapshot: %+v, broken %+v, %v", meta, broken, err)
	}
	if err := RemoveSnapshot(fsys, snapDir, SnapshotMeta{20, 1}); err != nil {
		t.Fatal(err)
	}
	if names, _ := fsys.List(snapDir); !slices.Equal(names, []string{snapshotName(meta)}) {
		t.Fatalf("snapshot files: %v", names)
	}
}
