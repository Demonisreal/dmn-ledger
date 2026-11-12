package wal

import (
	"cmp"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/Demonisreal/dmn-ledger/internal/disk"
)

const (
	MaxSegmentSize = 64 << 20
	maxSegmentFile = MaxSegmentSize + segHdrLen + frameHdrLen + MaxBodySize
)

var (
	ErrCorrupt     = errors.New("wal: corrupt")
	ErrVersion     = errors.New("wal: unknown format version")
	ErrCompacted   = errors.New("wal: index compacted")
	ErrUnavailable = errors.New("wal: index unavailable")
	ErrOutOfOrder  = errors.New("wal: entry out of order")
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
	Nonces      io.Reader
	Sealed      uint64
	Strict      bool
	Salvage     bool
}

type WAL struct {
	fs      disk.FS
	dir     string
	lock    io.Closer
	segSize int64
	nonces  io.Reader
	segs    []*segment
	synced  uint64
	term    uint64
	err     error

	enc      []byte
	encOffs  []int64
	encTerms []uint64
}

type segment struct {
	name  string
	f     disk.File
	first uint64
	seed  uint32
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
	return first, err == nil && first > 0
}

func Open(fsys disk.FS, dir string, opts Options) (_ *WAL, err error) {
	if opts.SegmentSize < 0 || opts.SegmentSize > MaxSegmentSize {
		return nil, fmt.Errorf("wal: segment size %d not in [0, %d]", opts.SegmentSize, MaxSegmentSize)
	}
	if err := fsys.MkdirAll(dir); err != nil {
		return nil, err
	}
	lock, err := fsys.Lock(dir)
	if err != nil {
		return nil, err
	}
	w := &WAL{
		fs:      fsys,
		dir:     dir,
		lock:    lock,
		segSize: cmp.Or(opts.SegmentSize, MaxSegmentSize),
		nonces:  cmp.Or[io.Reader](opts.Nonces, rand.Reader),
	}
	defer func() {
		if err != nil {
			_ = w.Close()
		}
	}()
	if err := w.openSegments(); err != nil {
		return nil, err
	}
	var raw []byte
	for i := 0; i < len(w.segs); i++ {
		raw, err = w.readSegment(i, opts)
		if opts.Salvage && errors.Is(err, ErrCorrupt) {
			err = w.removeAfter(i)
		}
		if err != nil {
			return nil, err
		}
	}
	if len(w.segs) > 0 {
		if err := w.recoverTail(raw); err != nil {
			return nil, err
		}
	}
	if err := fsys.SyncDir(dir); err != nil {
		return nil, err
	}
	w.synced = w.last()
	return w, nil
}

func (w *WAL) openSegments() error {
	names, err := w.fs.List(w.dir)
	if err != nil {
		return err
	}
	for _, name := range names {
		if base, ok := strings.CutSuffix(name, ".tmp"); ok {
			if _, ok := parseSegmentName(base); ok {
				if err := w.fs.Remove(filepath.Join(w.dir, name)); err != nil {
					return err
				}
			}
			continue
		}
		first, ok := parseSegmentName(name)
		if !ok {
			continue
		}
		f, err := w.fs.Open(filepath.Join(w.dir, name))
		if err != nil {
			return err
		}
		w.segs = append(w.segs, &segment{name: name, f: f, first: first})
	}
	return nil
}

func (w *WAL) readSegment(i int, opts Options) ([]byte, error) {
	seg, tail := w.segs[i], i == len(w.segs)-1
	size, err := seg.f.Size()
	if err != nil {
		return nil, err
	}
	if size > maxSegmentFile {
		return nil, &CorruptError{File: seg.name, Offset: maxSegmentFile}
	}
	raw := make([]byte, size)
	if _, err := seg.f.ReadAt(raw, 0); err != nil {
		return nil, err
	}
	if err := seg.readHeader(raw); err != nil {
		return raw, err
	}
	if i > 0 && seg.first != w.segs[i-1].last()+1 {
		return raw, &CorruptError{File: seg.name, Offset: 0}
	}
	if w.term, err = seg.scan(raw, w.term); err != nil {
		return raw, err
	}
	if seg.size == size {
		return raw, nil
	}
	next := seg.last() + 1
	if !tail || opts.Strict || next <= opts.Sealed || provenSynced(raw[seg.size:], next, seg.seed) {
		return raw, &CorruptError{File: seg.name, Offset: seg.size}
	}
	return raw, nil
}

func (w *WAL) removeAfter(i int) error {
	for len(w.segs) > i+1 {
		seg := w.active()
		w.segs = w.segs[:len(w.segs)-1]
		if err := w.removeSegment(seg); err != nil {
			return err
		}
	}
	return nil
}

func (w *WAL) recoverTail(raw []byte) error {
	seg := w.active()
	if seg.size < int64(len(raw)) {
		if err := seg.f.Truncate(seg.size); err != nil {
			return err
		}
	}
	if len(seg.terms) == 0 {
		w.segs = w.segs[:len(w.segs)-1]
		return w.removeSegment(seg)
	}
	from := seg.unproven(raw)
	if _, err := seg.f.WriteAt(raw[from:seg.size], from); err != nil {
		return err
	}
	return seg.f.Sync()
}

func (s *segment) readHeader(raw []byte) error {
	if len(raw) < segHdrLen ||
		binary.LittleEndian.Uint32(raw[segOffCRC:]) != crc32.Checksum(raw[:segOffCRC], castagnoli) ||
		binary.LittleEndian.Uint32(raw) != segMagic {
		return &CorruptError{File: s.name, Offset: 0}
	}
	if v := binary.LittleEndian.Uint16(raw[segOffVersion:]); v != segVersion {
		return fmt.Errorf("%w: %s has version %d", ErrVersion, s.name, v)
	}
	s.seed = seedOf(binary.LittleEndian.Uint64(raw[segOffNonce:]))
	return nil
}

func (s *segment) scan(raw []byte, term uint64) (uint64, error) {
	var err error
	off := segHdrLen
	for off < len(raw) {
		fr, ok := decodeFrame(raw[off:], s.seed)
		if !ok || fr.Index != s.first+uint64(len(s.terms)) {
			break
		}
		if fr.Term < term {
			err = &CorruptError{File: s.name, Offset: int64(off)}
			break
		}
		term = fr.Term
		s.offs = append(s.offs, int64(off))
		s.terms = append(s.terms, fr.Term)
		off += fr.size
	}
	s.size = int64(off)
	return term, err
}

func (s *segment) unproven(raw []byte) int64 {
	fr, _ := decodeFrame(raw[s.offs[len(s.offs)-1]:], s.seed)
	if fr.synced < s.first {
		return s.offs[0]
	}
	return s.offs[fr.synced-s.first+1]
}

func (w *WAL) active() *segment {
	if len(w.segs) == 0 {
		return nil
	}
	return w.segs[len(w.segs)-1]
}

func (w *WAL) first() uint64 {
	if len(w.segs) == 0 {
		return 0
	}
	return w.segs[0].first
}

func (w *WAL) last() uint64 {
	if len(w.segs) == 0 {
		return 0
	}
	return w.active().last()
}

func (w *WAL) FirstIndex() (uint64, error) {
	return w.first(), w.err
}

func (w *WAL) LastIndex() (uint64, error) {
	return w.last(), w.err
}

func (w *WAL) Append(ents ...Entry) error {
	if w.err != nil {
		return w.err
	}
	if len(ents) == 0 {
		return nil
	}
	next, term := ents[0].Index, w.term
	if len(w.segs) > 0 {
		next = w.last() + 1
	}
	if next == 0 {
		return fmt.Errorf("%w: index 0", ErrOutOfOrder)
	}
	for i, e := range ents {
		if want := next + uint64(i); e.Index != want {
			return fmt.Errorf("%w: index %d, want %d", ErrOutOfOrder, e.Index, want)
		}
		if e.Term < term {
			return fmt.Errorf("%w: term %d at index %d after term %d", ErrOutOfOrder, e.Term, e.Index, term)
		}
		if len(e.Body) > MaxBodySize {
			return fmt.Errorf("%w: index %d has %d bytes", ErrTooLarge, e.Index, len(e.Body))
		}
		term = e.Term
	}

	for _, e := range ents {
		if w.active() == nil || (w.pos() > segHdrLen && w.pos()+int64(frameSize(e)) > w.segSize) {
			if err := w.roll(e.Index); err != nil {
				return w.fail(err)
			}
		}
		w.encOffs = append(w.encOffs, w.pos())
		w.encTerms = append(w.encTerms, e.Term)
		w.enc = appendFrame(w.enc, e, w.synced, w.active().seed)
	}
	if err := w.flush(); err != nil {
		return w.fail(err)
	}
	w.term = term
	return nil
}

func (w *WAL) pos() int64 {
	return w.active().size + int64(len(w.enc))
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
	seg.offs = append(seg.offs, w.encOffs...)
	seg.terms = append(seg.terms, w.encTerms...)
	w.enc, w.encOffs, w.encTerms = w.enc[:0], w.encOffs[:0], w.encTerms[:0]
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
	var nonce [8]byte
	if _, err := io.ReadFull(w.nonces, nonce[:]); err != nil {
		return err
	}
	name := segmentName(first)
	path := filepath.Join(w.dir, name)
	f, err := w.fs.Create(path + ".tmp")
	if err != nil {
		return err
	}
	n := binary.LittleEndian.Uint64(nonce[:])
	_, err = f.WriteAt(appendSegmentHeader(nil, n), 0)
	if err == nil {
		err = f.Sync()
	}
	if err == nil {
		err = w.fs.Rename(path+".tmp", path)
	}
	if err == nil {
		err = w.fs.SyncDir(w.dir)
	}
	if err != nil {
		_ = f.Close()
		return err
	}
	w.segs = append(w.segs, &segment{name: name, f: f, first: first, seed: seedOf(n), size: segHdrLen})
	return nil
}

func (w *WAL) Sync() error {
	if w.err != nil {
		return w.err
	}
	if w.synced == w.last() {
		return nil
	}
	if err := w.active().f.Sync(); err != nil {
		return w.fail(err)
	}
	w.synced = w.last()
	return nil
}

func (w *WAL) fail(err error) error {
	w.err = fmt.Errorf("wal: stopped after error: %w", err)
	return w.err
}

func (w *WAL) checkRange(lo, hi uint64) error {
	switch {
	case w.err != nil:
		return w.err
	case lo > hi:
		return fmt.Errorf("wal: invalid range [%d, %d)", lo, hi)
	case lo < w.first():
		return fmt.Errorf("%w: %d < first %d", ErrCompacted, lo, w.first())
	case len(w.segs) == 0 || hi > w.last()+1:
		return fmt.Errorf("%w: %d > last %d", ErrUnavailable, hi-1, w.last())
	}
	return nil
}

func (w *WAL) Term(i uint64) (uint64, error) {
	if err := w.checkRange(i, i+1); err != nil {
		return 0, err
	}
	k, found := slices.BinarySearchFunc(w.segs, i, func(s *segment, i uint64) int { return cmp.Compare(s.first, i) })
	if !found {
		k--
	}
	seg := w.segs[k]
	return seg.terms[i-seg.first], nil
}

func (w *WAL) Entries(lo, hi uint64, maxBytes int64) ([]Entry, error) {
	if w.err != nil {
		return nil, w.err
	}
	if lo == hi {
		return nil, nil
	}
	if err := w.checkRange(lo, hi); err != nil {
		return nil, err
	}
	var ents []Entry
	var size int64
	for _, seg := range w.segs {
		from, to := max(lo, seg.first), min(hi, seg.last()+1)
		if from >= to {
			continue
		}
		a, b, full := from-seg.first, from-seg.first, false
		for ; b < to-seg.first; b++ {
			n := seg.end(b+1) - seg.offs[b]
			if size+n > maxBytes && (len(ents) > 0 || b > a) {
				full = true
				break
			}
			size += n
		}
		if b > a {
			start := seg.offs[a]
			raw := make([]byte, seg.end(b)-start)
			if _, err := seg.f.ReadAt(raw, start); err != nil {
				return nil, err
			}
			for off := 0; off < len(raw); {
				fr, ok := decodeFrame(raw[off:], seg.seed)
				if !ok || fr.Index != lo+uint64(len(ents)) {
					return nil, &CorruptError{File: seg.name, Offset: start + int64(off)}
				}
				ents = append(ents, fr.Entry)
				off += fr.size
			}
		}
		if full {
			break
		}
	}
	return ents, nil
}

func (w *WAL) TruncateAfter(i uint64) error {
	if w.err != nil {
		return w.err
	}
	if i >= w.last() {
		return nil
	}
	for seg := w.active(); seg != nil && seg.first > i; seg = w.active() {
		w.segs = w.segs[:len(w.segs)-1]
		if err := w.removeSegment(seg); err != nil {
			return w.fail(err)
		}
	}
	seg := w.active()
	if seg == nil {
		w.synced, w.term = 0, 0
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
	w.synced, w.term = i, seg.terms[len(seg.terms)-1]
	return nil
}

func (w *WAL) CompactBefore(i uint64) error {
	if w.err != nil {
		return w.err
	}
	for len(w.segs) > 0 && w.segs[0].last() < i {
		seg := w.segs[0]
		w.segs = w.segs[1:]
		if err := w.removeSegment(seg); err != nil {
			return w.fail(err)
		}
	}
	if len(w.segs) == 0 {
		w.synced, w.term = 0, 0
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
	if w.lock != nil {
		errs = append(errs, w.lock.Close())
		w.lock = nil
	}
	if w.err == nil {
		w.err = ErrClosed
	}
	return errors.Join(errs...)
}
