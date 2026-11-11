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
	MetaErrRate  float64
	ReadErrRate  float64
	Sector       int64

	rng     *rand.Rand
	dirs    map[string]bool
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
		Sector:  SectorSize,
		rng:     rng,
		dirs:    make(map[string]bool),
		live:    make(map[string]*simNode),
		durable: make(map[string]*simNode),
	}
}

func (s *Sim) hasDir(dir string) bool {
	return dir == filepath.Dir(dir) || s.dirs[dir]
}

func (s *Sim) eio(op, name string, rate float64) error {
	if rate > 0 && s.rng.Float64() < rate {
		return &fs.PathError{Op: op, Path: name, Err: syscall.EIO}
	}
	return nil
}

func (s *Sim) Create(name string) (File, error) {
	name = filepath.Clean(name)
	if !s.hasDir(filepath.Dir(name)) {
		return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrNotExist}
	}
	if err := s.eio("open", name, s.MetaErrRate); err != nil {
		return nil, err
	}
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
	if err := s.eio("remove", name, s.MetaErrRate); err != nil {
		return err
	}
	delete(s.live, name)
	return nil
}

func (s *Sim) Rename(oldname, newname string) error {
	oldname, newname = filepath.Clean(oldname), filepath.Clean(newname)
	n := s.live[oldname]
	if n == nil || !s.hasDir(filepath.Dir(newname)) {
		return &os.LinkError{Op: "rename", Old: oldname, New: newname, Err: fs.ErrNotExist}
	}
	if s.MetaErrRate > 0 && s.rng.Float64() < s.MetaErrRate {
		return &os.LinkError{Op: "rename", Old: oldname, New: newname, Err: syscall.EIO}
	}
	delete(s.live, oldname)
	s.live[newname] = n
	return nil
}

func (s *Sim) List(dir string) ([]string, error) {
	dir = filepath.Clean(dir)
	if !s.hasDir(dir) {
		return nil, &fs.PathError{Op: "open", Path: dir, Err: fs.ErrNotExist}
	}
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
	if !s.hasDir(dir) {
		return &fs.PathError{Op: "open", Path: dir, Err: fs.ErrNotExist}
	}
	if err := s.eio("sync", dir, s.MetaErrRate); err != nil {
		return err
	}
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

func (s *Sim) MkdirAll(dir string) error {
	for dir = filepath.Clean(dir); !s.hasDir(dir); dir = filepath.Dir(dir) {
		s.dirs[dir] = true
	}
	return nil
}

func (s *Sim) Crash() {
	crashed := make(map[*simNode]*simNode)
	live := make(map[string]*simNode, len(s.durable))
	for _, name := range slices.Sorted(maps.Keys(s.durable)) {
		old := s.durable[name]
		n, ok := crashed[old]
		if !ok {
			n = old.crash(s.rng, s.Sector)
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
	c := *s
	c.dirs = maps.Clone(s.dirs)
	c.live = cp(s.live)
	c.durable = cp(s.durable)
	return &c
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

func (n *simNode) write(p []byte, off int64) {
	if end := off + int64(len(p)); end > int64(len(n.cur)) {
		n.resize(end)
	}
	copy(n.cur[off:], p)
	n.touch(off)
}

func (n *simNode) sync() {
	if !n.dirty {
		return
	}
	lo := min(n.lo, int64(len(n.synced)))
	n.synced = append(n.synced[:lo], n.cur[lo:]...)
	n.dirty = false
}

func (n *simNode) dropDirty() {
	if grow := len(n.cur) - len(n.synced); grow > 0 {
		n.synced = append(n.synced, make([]byte, grow)...)
	}
	n.dirty = false
}

func (n *simNode) crash(rng *rand.Rand, sector int64) *simNode {
	if !n.dirty {
		return &simNode{cur: slices.Clone(n.synced), synced: slices.Clone(n.synced)}
	}
	short, long := min(len(n.synced), len(n.cur)), max(len(n.synced), len(n.cur))
	size := int64(short) + rng.Int64N(int64(long-short)+1)
	onDisk := make([]byte, size)
	copy(onDisk, n.synced)
	for sec := n.lo / sector * sector; sec < size; sec += sector {
		if rng.IntN(2) == 0 || sec >= int64(len(n.cur)) {
			continue
		}
		end := min(sec+sector, size, int64(len(n.cur)))
		copy(onDisk[sec:end], n.cur[sec:end])
	}
	return &simNode{cur: onDisk, synced: slices.Clone(onDisk)}
}

func (f *simFile) ReadAt(p []byte, off int64) (int, error) {
	if f.closed {
		return 0, f.fail("read", fs.ErrClosed)
	}
	if err := f.sim.eio("read", f.name, f.sim.ReadErrRate); err != nil {
		return 0, err
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
	if err := f.sim.eio("write", f.name, f.sim.WriteErrRate); err != nil {
		short := f.sim.rng.IntN(len(p) + 1)
		f.node.write(p[:short], off)
		return short, err
	}
	f.node.write(p, off)
	return len(p), nil
}

func (f *simFile) Sync() error {
	if f.closed {
		return f.fail("sync", fs.ErrClosed)
	}
	if err := f.sim.eio("sync", f.name, f.sim.SyncErrRate); err != nil {
		if f.sim.rng.IntN(2) == 0 {
			f.node.dropDirty()
		}
		return err
	}
	f.node.sync()
	return nil
}

func (f *simFile) Truncate(size int64) error {
	if f.closed {
		return f.fail("truncate", fs.ErrClosed)
	}
	if err := f.sim.eio("truncate", f.name, f.sim.MetaErrRate); err != nil {
		return err
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
