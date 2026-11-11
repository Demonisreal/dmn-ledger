package disk

import (
	"io"
	"io/fs"
	"maps"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"syscall"
)

const SectorSize = 512

type Sim struct {
	WriteErrRate float64
	SyncErrRate  float64

	rng     *rand.Rand
	live    map[string]*simNode
	durable map[string]*simNode
}

type simNode struct {
	cur    []byte
	synced []byte
	dirty  bool
	lo     int64
}

type simFile struct {
	sim    *Sim
	node   *simNode
	name   string
	closed bool
}

func NewSim(rng *rand.Rand) *Sim {
	return &Sim{
		rng:     rng,
		live:    make(map[string]*simNode),
		durable: make(map[string]*simNode),
	}
}

func (s *Sim) Create(name string) (File, error) {
	name = filepath.Clean(name)
	n := s.live[name]
	if n == nil {
		n = &simNode{}
		s.live[name] = n
	} else {
		n.resize(0)
	}
	return &simFile{sim: s, node: n, name: name}, nil
}

func (s *Sim) Open(name string) (File, error) {
	name = filepath.Clean(name)
	n := s.live[name]
	if n == nil {
		return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrNotExist}
	}
	return &simFile{sim: s, node: n, name: name}, nil
}

func (s *Sim) Remove(name string) error {
	name = filepath.Clean(name)
	if s.live[name] == nil {
		return &fs.PathError{Op: "remove", Path: name, Err: fs.ErrNotExist}
	}
	delete(s.live, name)
	return nil
}

func (s *Sim) Rename(oldname, newname string) error {
	oldname, newname = filepath.Clean(oldname), filepath.Clean(newname)
	n := s.live[oldname]
	if n == nil {
		return &os.LinkError{Op: "rename", Old: oldname, New: newname, Err: fs.ErrNotExist}
	}
	delete(s.live, oldname)
	s.live[newname] = n
	return nil
}

func (s *Sim) List(dir string) ([]string, error) {
	dir = filepath.Clean(dir)
	var names []string
	for name := range s.live {
		if filepath.Dir(name) == dir {
			names = append(names, filepath.Base(name))
		}
	}
	slices.Sort(names)
	return names, nil
}

func (s *Sim) SyncDir(dir string) error {
	dir = filepath.Clean(dir)
	maps.DeleteFunc(s.durable, func(name string, _ *simNode) bool {
		return filepath.Dir(name) == dir
	})
	for name, n := range s.live {
		if filepath.Dir(name) == dir {
			s.durable[name] = n
		}
	}
	return nil
}

func (s *Sim) MkdirAll(string) error {
	return nil
}

func (s *Sim) Crash() {
	crashed := make(map[*simNode]*simNode)
	live := make(map[string]*simNode, len(s.durable))
	for _, name := range slices.Sorted(maps.Keys(s.durable)) {
		old := s.durable[name]
		n, ok := crashed[old]
		if !ok {
			n = old.crash(s.rng)
			crashed[old] = n
		}
		live[name] = n
	}
	s.live = live
	s.durable = maps.Clone(live)
}

func (s *Sim) Clone() *Sim {
	copies := make(map[*simNode]*simNode)
	cp := func(names map[string]*simNode) map[string]*simNode {
		out := make(map[string]*simNode, len(names))
		for name, n := range names {
			c, ok := copies[n]
			if !ok {
				c = &simNode{cur: slices.Clone(n.cur), synced: slices.Clone(n.synced), dirty: n.dirty, lo: n.lo}
				copies[n] = c
			}
			out[name] = c
		}
		return out
	}
	return &Sim{
		WriteErrRate: s.WriteErrRate,
		SyncErrRate:  s.SyncErrRate,
		rng:          s.rng,
		live:         cp(s.live),
		durable:      cp(s.durable),
	}
}

func (s *Sim) Corrupt(name string, off int64) error {
	name = filepath.Clean(name)
	n := s.durable[name]
	if n == nil {
		return &fs.PathError{Op: "corrupt", Path: name, Err: fs.ErrNotExist}
	}
	if off < 0 || off >= int64(len(n.synced)) {
		return &fs.PathError{Op: "corrupt", Path: name, Err: fs.ErrInvalid}
	}
	n.synced[off] ^= 1 << s.rng.IntN(8)
	return nil
}

func (s *Sim) fails(rate float64) bool {
	return rate > 0 && s.rng.Float64() < rate
}

func (n *simNode) touch(off int64) {
	if !n.dirty || off < n.lo {
		n.lo = off
	}
	n.dirty = true
}

func (n *simNode) resize(size int64) {
	old := int64(len(n.cur))
	if size <= old {
		n.cur = n.cur[:size]
		n.touch(size)
		return
	}
	n.cur = slices.Grow(n.cur, int(size-old))[:size]
	clear(n.cur[old:])
	n.touch(old)
}

func (n *simNode) sync() {
	if !n.dirty {
		return
	}
	lo := min(n.lo, int64(len(n.synced)))
	n.synced = append(n.synced[:lo], n.cur[lo:]...)
	n.dirty = false
}

func (n *simNode) crash(rng *rand.Rand) *simNode {
	if !n.dirty {
		return &simNode{cur: slices.Clone(n.synced), synced: slices.Clone(n.synced)}
	}
	size := int64(len(n.synced))
	if cur := int64(len(n.cur)); cur != size && rng.IntN(2) == 0 {
		size = cur
	}
	onDisk := make([]byte, size)
	copy(onDisk, n.synced)
	for sec := n.lo / SectorSize * SectorSize; sec < size; sec += SectorSize {
		if rng.IntN(2) == 0 || sec >= int64(len(n.cur)) {
			continue
		}
		end := min(sec+SectorSize, size, int64(len(n.cur)))
		copy(onDisk[sec:end], n.cur[sec:end])
	}
	return &simNode{cur: onDisk, synced: slices.Clone(onDisk)}
}

func (f *simFile) ReadAt(p []byte, off int64) (int, error) {
	if f.closed {
		return 0, f.fail("read", fs.ErrClosed)
	}
	n := copy(p, f.node.cur[min(off, int64(len(f.node.cur))):])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (f *simFile) WriteAt(p []byte, off int64) (int, error) {
	if f.closed {
		return 0, f.fail("write", fs.ErrClosed)
	}
	if f.sim.fails(f.sim.WriteErrRate) {
		return 0, f.fail("write", syscall.EIO)
	}
	if end := off + int64(len(p)); end > int64(len(f.node.cur)) {
		f.node.resize(end)
	}
	copy(f.node.cur[off:], p)
	f.node.touch(off)
	return len(p), nil
}

func (f *simFile) Sync() error {
	if f.closed {
		return f.fail("sync", fs.ErrClosed)
	}
	if f.sim.fails(f.sim.SyncErrRate) {
		return f.fail("sync", syscall.EIO)
	}
	f.node.sync()
	return nil
}

func (f *simFile) Truncate(size int64) error {
	if f.closed {
		return f.fail("truncate", fs.ErrClosed)
	}
	f.node.resize(size)
	return nil
}

func (f *simFile) Size() (int64, error) {
	if f.closed {
		return 0, f.fail("stat", fs.ErrClosed)
	}
	return int64(len(f.node.cur)), nil
}

func (f *simFile) Close() error {
	if f.closed {
		return f.fail("close", fs.ErrClosed)
	}
	f.closed = true
	return nil
}

func (f *simFile) fail(op string, err error) error {
	return &fs.PathError{Op: op, Path: f.name, Err: err}
}
