package wal

import (
	"cmp"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/Demonisreal/dmn-ledger/internal/disk"
)

const DefaultSegmentSize = 64 << 20

var (
	ErrCorrupt     = errors.New("wal: corrupt")
	ErrCompacted   = errors.New("wal: index compacted")
	ErrUnavailable = errors.New("wal: index unavailable")
	ErrOutOfOrder  = errors.New("wal: index out of order")
	ErrTooLarge    = errors.New("wal: entry too large")
	ErrClosed      = errors.New("wal: closed")
)

type CorruptError struct {
	File   string
	Offset int64
}

func (e *CorruptError) Error() string {
	return fmt.Sprintf("wal: corrupt data in %s at offset %d", e.File, e.Offset)
}

func (e *CorruptError) Is(target error) bool {
	return target == ErrCorrupt
}

type Options struct {
	SegmentSize int64
}

type WAL struct {
	fs      disk.FS
	dir     string
	segSize int64
	segs    []*segment
	synced  uint64
	enc     []byte
	err     error
}

type segment struct {
	name  string
	f     disk.File
	first uint64
	offs  []int64
	terms []uint64
	size  int64
}

func (s *segment) last() uint64 {
	return s.first + uint64(len(s.terms)) - 1
}

func (s *segment) end(k uint64) int64 {
	if k < uint64(len(s.offs)) {
		return s.offs[k]
	}
	return s.size
}

func segmentName(first uint64) string {
	return fmt.Sprintf("wal-%020d.log", first)
}

func parseSegmentName(name string) (uint64, bool) {
	digits, ok := strings.CutPrefix(name, "wal-")
	if !ok {
		return 0, false
	}
	digits, ok = strings.CutSuffix(digits, ".log")
	if !ok || len(digits) != 20 {
		return 0, false
	}
	first, err := strconv.ParseUint(digits, 10, 64)
	return first, err == nil
}

func Open(fsys disk.FS, dir string, opts Options) (_ *WAL, err error) {
	w := &WAL{fs: fsys, dir: dir, segSize: cmp.Or(opts.SegmentSize, DefaultSegmentSize)}
	defer func() {
		if err != nil {
			_ = w.Close()
		}
	}()
	if err := fsys.MkdirAll(dir); err != nil {
		return nil, err
	}
	names, err := fsys.List(dir)
	if err != nil {
		return nil, err
	}
	for _, name := range names {
		first, ok := parseSegmentName(name)
		if !ok {
			continue
		}
		f, err := fsys.Open(filepath.Join(dir, name))
		if err != nil {
			return nil, err
		}
		w.segs = append(w.segs, &segment{name: name, f: f, first: first})
	}

	var raw []byte
	for i, seg := range w.segs {
		size, err := seg.f.Size()
		if err != nil {
			return nil, err
		}
		raw = slices.Grow(raw[:0], int(size))[:size]
		if _, err := seg.f.ReadAt(raw, 0); err != nil {
			return nil, err
		}
		if i > 0 && seg.first != w.segs[i-1].last()+1 {
			return nil, &CorruptError{File: seg.name, Offset: 0}
		}
		seg.scan(raw)
		if seg.size == size {
			continue
		}
		if i < len(w.segs)-1 || provenSynced(raw[seg.size:], seg.last()+1) {
			return nil, &CorruptError{File: seg.name, Offset: seg.size}
		}
		if err := seg.f.Truncate(seg.size); err != nil {
			return nil, err
		}
	}

	if len(w.segs) == 1 && len(w.segs[0].terms) == 0 {
		if err := w.removeSegment(w.segs[0]); err != nil {
			return nil, err
		}
		w.segs = nil
	}
	if seg := w.active(); seg != nil {
		if err := seg.f.Sync(); err != nil {
			return nil, err
		}
	}
	w.synced = w.LastIndex()
	return w, nil
}

func (s *segment) scan(raw []byte) {
	off := 0
	for off < len(raw) {
		fr, ok := decodeFrame(raw[off:])
		if !ok || fr.Index != s.first+uint64(len(s.terms)) {
			break
		}
		s.offs = append(s.offs, int64(off))
		s.terms = append(s.terms, fr.Term)
		off += fr.size
	}
	s.size = int64(off)
}

func (w *WAL) active() *segment {
	if len(w.segs) == 0 {
		return nil
	}
	return w.segs[len(w.segs)-1]
}

func (w *WAL) FirstIndex() uint64 {
	if len(w.segs) == 0 {
		return 0
	}
	return w.segs[0].first
}

func (w *WAL) LastIndex() uint64 {
	if len(w.segs) == 0 {
		return 0
	}
	return w.active().last()
}

func (w *WAL) Append(ents ...Entry) error {
	if w.err != nil {
		return w.err
	}
	if len(ents) == 0 {
		return nil
	}
	next := ents[0].Index
	if len(w.segs) > 0 {
		next = w.LastIndex() + 1
	}
	if next == 0 {
		return fmt.Errorf("%w: index 0", ErrOutOfOrder)
	}
	for i, e := range ents {
		if want := next + uint64(i); e.Index != want {
			return fmt.Errorf("%w: got %d, want %d", ErrOutOfOrder, e.Index, want)
		}
		if len(e.Body) > MaxBodySize {
			return fmt.Errorf("%w: index %d has %d bytes", ErrTooLarge, e.Index, len(e.Body))
		}
	}

	for _, e := range ents {
		seg := w.active()
		if pos := w.pos(); seg == nil || (pos > 0 && pos+int64(frameSize(e)) > w.segSize) {
			if err := w.roll(e.Index); err != nil {
				return w.fail(err)
			}
			seg = w.active()
		}
		seg.offs = append(seg.offs, w.pos())
		seg.terms = append(seg.terms, e.Term)
		w.enc = appendFrame(w.enc, e, w.synced)
	}
	if err := w.flush(); err != nil {
		return w.fail(err)
	}
	return nil
}

func (w *WAL) pos() int64 {
	if seg := w.active(); seg != nil {
		return seg.size + int64(len(w.enc))
	}
	return 0
}

func (w *WAL) flush() error {
	if len(w.enc) == 0 {
		return nil
	}
	seg := w.active()
	if _, err := seg.f.WriteAt(w.enc, seg.size); err != nil {
		return err
	}
	seg.size += int64(len(w.enc))
	w.enc = w.enc[:0]
	return nil
}

func (w *WAL) roll(first uint64) error {
	if seg := w.active(); seg != nil {
		if err := w.flush(); err != nil {
			return err
		}
		if err := seg.f.Sync(); err != nil {
			return err
		}
		w.synced = seg.last()
	}
	name := segmentName(first)
	f, err := w.fs.Create(filepath.Join(w.dir, name))
	if err != nil {
		return err
	}
	w.segs = append(w.segs, &segment{name: name, f: f, first: first})
	return w.fs.SyncDir(w.dir)
}

func (w *WAL) Sync() error {
	if w.err != nil {
		return w.err
	}
	if w.synced == w.LastIndex() {
		return nil
	}
	if err := w.active().f.Sync(); err != nil {
		return w.fail(err)
	}
	w.synced = w.LastIndex()
	return nil
}

func (w *WAL) fail(err error) error {
	w.err = fmt.Errorf("wal: unusable after I/O error: %w", err)
	return w.err
}

func (w *WAL) checkRange(lo, hi uint64) error {
	switch {
	case lo > hi:
		return fmt.Errorf("wal: invalid range [%d, %d)", lo, hi)
	case lo < w.FirstIndex():
		return fmt.Errorf("%w: %d < first %d", ErrCompacted, lo, w.FirstIndex())
	case len(w.segs) == 0 || hi > w.LastIndex()+1:
		return fmt.Errorf("%w: %d > last %d", ErrUnavailable, hi-1, w.LastIndex())
	}
	return nil
}

func (w *WAL) Term(i uint64) (uint64, error) {
	if err := w.checkRange(i, i+1); err != nil {
		return 0, err
	}
	seg := w.segs[sort.Search(len(w.segs), func(k int) bool { return w.segs[k].first > i })-1]
	return seg.terms[i-seg.first], nil
}

func (w *WAL) Entries(lo, hi uint64) ([]Entry, error) {
	if lo == hi {
		return nil, nil
	}
	if err := w.checkRange(lo, hi); err != nil {
		return nil, err
	}
	ents := make([]Entry, 0, hi-lo)
	for _, seg := range w.segs {
		from, to := max(lo, seg.first), min(hi, seg.last()+1)
		if from >= to {
			continue
		}
		start, end := seg.offs[from-seg.first], seg.end(to-seg.first)
		raw := make([]byte, end-start)
		if _, err := seg.f.ReadAt(raw, start); err != nil {
			return nil, err
		}
		for off := 0; off < len(raw); {
			fr, ok := decodeFrame(raw[off:])
			if !ok || fr.Index != lo+uint64(len(ents)) {
				return nil, &CorruptError{File: seg.name, Offset: start + int64(off)}
			}
			ents = append(ents, fr.Entry)
			off += fr.size
		}
	}
	return ents, nil
}

func (w *WAL) TruncateAfter(i uint64) error {
	if w.err != nil {
		return w.err
	}
	if i >= w.LastIndex() {
		return nil
	}
	for seg := w.active(); seg != nil && seg.first > i; seg = w.active() {
		if err := w.removeSegment(seg); err != nil {
			return w.fail(err)
		}
		w.segs = w.segs[:len(w.segs)-1]
	}
	seg := w.active()
	if seg == nil {
		w.synced = 0
		return nil
	}
	if i < seg.last() {
		keep := i - seg.first + 1
		end := seg.offs[keep]
		if err := seg.f.Truncate(end); err != nil {
			return w.fail(err)
		}
		if err := seg.f.Sync(); err != nil {
			return w.fail(err)
		}
		seg.offs, seg.terms, seg.size = seg.offs[:keep], seg.terms[:keep], end
	}
	w.synced = i
	return nil
}

func (w *WAL) CompactBefore(i uint64) error {
	if w.err != nil {
		return w.err
	}
	for len(w.segs) > 0 && w.segs[0].last() < i {
		if err := w.removeSegment(w.segs[0]); err != nil {
			return w.fail(err)
		}
		w.segs = w.segs[1:]
	}
	if len(w.segs) == 0 {
		w.synced = 0
	}
	return nil
}

func (w *WAL) removeSegment(seg *segment) error {
	if err := seg.f.Close(); err != nil {
		return err
	}
	if err := w.fs.Remove(filepath.Join(w.dir, seg.name)); err != nil {
		return err
	}
	return w.fs.SyncDir(w.dir)
}

func (w *WAL) Close() error {
	var errs []error
	for _, seg := range w.segs {
		errs = append(errs, seg.f.Close())
	}
	w.segs = nil
	if w.err == nil {
		w.err = ErrClosed
	}
	return errors.Join(errs...)
}
