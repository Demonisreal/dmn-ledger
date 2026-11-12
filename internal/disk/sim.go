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
	JournalRate  float64
	ShredRate    float64
	Sector       int64

	rng      *rand.Rand
	ops      uint64
	failAt   uint64
	syncs    uint64
	failSync uint64
	dirs     map[string]bool
	live     map[string]*simNode
	durable  map[string]*simNode
	locks    map[string]*simLock
}

type simNode struct {
	cur    []byte
	synced []byte
	dirty  bool
	lo     int64
	wrote  []int64
}

type DurableFile struct {
	Name string
	Size int64
}

type simFile struct {
	sim    *Sim
	node   *simNode
	name   string
	closed bool
}

type simLock struct {
	sim *Sim
	dir string
}

func NewSim(rng *rand.Rand) *Sim {
	return &Sim{
		Sector:  SectorSize,
		rng:     rng,
		dirs:    make(map[string]bool),
		live:    make(map[string]*simNode),
		durable: make(map[string]*simNode),
		locks:   make(map[string]*simLock),
	}
}

func (s *Sim) hasDir(dir string) bool {
	_, ok := s.dirs[dir]
	return ok || dir == filepath.Dir(dir)
}

func (s *Sim) FailAfter(k uint64) {
	s.failAt = 0
	if k > 0 {
		s.failAt = s.ops + k
	}
}

func (s *Sim) FailSyncAfter(k uint64) {
	s.failSync = 0
	if k > 0 {
		s.failSync = s.syncs + k
	}
}

func (s *Sim) hit(rate float64) bool {
	s.ops++
	return s.ops == s.failAt || rate > 0 && s.rng.Float64() < rate
}

func (s *Sim) eio(op, name string, rate float64) error {
	if s.hit(rate) {
		return &fs.PathError{Op: op, Path: name, Err: syscall.EIO}
	}
	return nil
}

func (s *Sim) journal() {
	if s.JournalRate > 0 && s.rng.Float64() < s.JournalRate {
		s.durable = maps.Clone(s.live)
		for d := range s.dirs {
			s.dirs[d] = true
		}
	}
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
	s.journal()
	return &simFile{sim: s, node: n, name: name}, nil
}

func (s *Sim) Open(name string) (File, error) {
	name = filepath.Clean(name)
	n := s.live[name]
	if n == nil {
		return nil, &fs.PathError{Op: "open", Path: name, Err: fs.ErrNotExist}
	}
	if err := s.eio("open", name, s.MetaErrRate); err != nil {
		return nil, err
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
	s.journal()
	return nil
}

func (s *Sim) Rename(oldname, newname string) error {
	oldname, newname = filepath.Clean(oldname), filepath.Clean(newname)
	n := s.live[oldname]
	if n == nil || !s.hasDir(filepath.Dir(newname)) {
		return &os.LinkError{Op: "rename", Old: oldname, New: newname, Err: fs.ErrNotExist}
	}
	if s.hit(s.MetaErrRate) {
		return &os.LinkError{Op: "rename", Old: oldname, New: newname, Err: syscall.EIO}
	}
	delete(s.live, oldname)
	s.live[newname] = n
	s.journal()
	return nil
}

func (s *Sim) List(dir string) ([]string, error) {
	dir = filepath.Clean(dir)
	if !s.hasDir(dir) {
		return nil, &fs.PathError{Op: "open", Path: dir, Err: fs.ErrNotExist}
	}
	if err := s.eio("readdirent", dir, s.MetaErrRate); err != nil {
		return nil, err
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
	s.commit(dir)
	return nil
}

func (s *Sim) commit(dir string) {
	maps.DeleteFunc(s.durable, func(name string, _ *simNode) bool {
		return filepath.Dir(name) == dir
	})
	for name, n := range s.live {
		if filepath.Dir(name) == dir {
			s.durable[name] = n
		}
	}
	for d := range s.dirs {
		if filepath.Dir(d) == dir {
			s.dirs[d] = true
		}
	}
}

func (s *Sim) MkdirAll(dir string) error {
	for _, d := range chain(dir) {
		if !s.hasDir(d) {
			s.dirs[d] = false
		}
		parent := filepath.Dir(d)
		if err := s.eio("sync", parent, s.MetaErrRate); err != nil {
			return err
		}
		s.commit(parent)
	}
	return nil
}

func (s *Sim) Lock(dir string) (io.Closer, error) {
	dir = filepath.Clean(dir)
	if !s.hasDir(dir) {
		return nil, &fs.PathError{Op: "open", Path: dir, Err: fs.ErrNotExist}
	}
	if s.locks[dir] != nil {
		return nil, &fs.PathError{Op: "lock", Path: dir, Err: ErrLocked}
	}
	l := &simLock{sim: s, dir: dir}
	s.locks[dir] = l
	return l, nil
}

func (l *simLock) Close() error {
	if l.sim.locks[l.dir] == l {
		delete(l.sim.locks, l.dir)
	}
	return nil
}

func (s *Sim) survives(dir string) bool {
	for ; dir != filepath.Dir(dir); dir = filepath.Dir(dir) {
		if !s.dirs[dir] {
			return false
		}
	}
	return true
}

func (s *Sim) Crash() {
	dirs := make(map[string]bool, len(s.dirs))
	for d := range s.dirs {
		if s.survives(d) {
			dirs[d] = true
		}
	}
	crashed := make(map[*simNode]*simNode)
	live := make(map[string]*simNode, len(s.durable))
	for _, name := range slices.Sorted(maps.Keys(s.durable)) {
		if !s.survives(filepath.Dir(name)) {
			continue
		}
		old := s.durable[name]
		n, ok := crashed[old]
		if !ok {
			n = old.crash(s.rng, s.Sector, s.ShredRate)
			crashed[old] = n
		}
		live[name] = n
	}
	s.dirs = dirs
	s.live = live
	s.durable = maps.Clone(live)
	s.locks = make(map[string]*simLock)
}

func (s *Sim) Clone(rng *rand.Rand) *Sim {
	copies := make(map[*simNode]*simNode)
	cp := func(names map[string]*simNode) map[string]*simNode {
		out := make(map[string]*simNode, len(names))
		for name, n := range names {
			c, ok := copies[n]
			if !ok {
				c = &simNode{cur: slices.Clone(n.cur), synced: slices.Clone(n.synced), dirty: n.dirty, lo: n.lo, wrote: slices.Clone(n.wrote)}
				copies[n] = c
			}
			out[name] = c
		}
		return out
	}
	c := *s
	c.rng = rng
	c.failAt, c.failSync = 0, 0
	c.dirs = maps.Clone(s.dirs)
	c.live = cp(s.live)
	c.durable = cp(s.durable)
	c.locks = make(map[string]*simLock)
	return &c
}

func (s *Sim) Corrupt(name string, off int64) error {
	n, err := s.atRest("corrupt", name, off)
	if err != nil {
		return err
	}
	flip := byte(1) << s.rng.IntN(8)
	n.spoil(off, n.synced[off]^flip)
	return nil
}

func (s *Sim) Garble(name string, off int64) error {
	n, err := s.atRest("garble", name, off)
	if err != nil {
		return err
	}
	sec := off - off%s.Sector
	for k := sec; k < min(sec+s.Sector, int64(len(n.synced))); k++ {
		n.spoil(k, junk(s.rng))
	}
	return nil
}

func (s *Sim) atRest(op, name string, off int64) (*simNode, error) {
	name = filepath.Clean(name)
	n := s.durable[name]
	if n == nil {
		return nil, &fs.PathError{Op: op, Path: name, Err: fs.ErrNotExist}
	}
	if off < 0 || off >= int64(len(n.synced)) {
		return nil, &fs.PathError{Op: op, Path: name, Err: fs.ErrInvalid}
	}
	return n, nil
}

func (s *Sim) Durable() []DurableFile {
	files := make([]DurableFile, 0, len(s.durable))
	for _, name := range slices.Sorted(maps.Keys(s.durable)) {
		files = append(files, DurableFile{Name: name, Size: int64(len(s.durable[name].synced))})
	}
	return files
}

func junk(rng *rand.Rand) byte {
	return byte(rng.Uint32()) //nolint:gosec // G115: keeps the low byte on purpose, any garbage will do
}

func (n *simNode) spoil(off int64, b byte) {
	n.synced[off] = b
	if off < int64(len(n.cur)) && (!n.dirty || off < n.lo) {
		n.cur[off] = b
	}
}

func (n *simNode) mark(off, end, sector int64) {
	if off == end {
		return
	}
	for sec := off / sector; sec*sector < end; sec++ {
		if k, found := slices.BinarySearch(n.wrote, sec); !found {
			n.wrote = slices.Insert(n.wrote, k, sec)
		}
	}
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
	n.wrote = n.wrote[:0]
}

func (n *simNode) dropDirty() {
	if grow := len(n.cur) - len(n.synced); grow > 0 {
		n.synced = append(n.synced, make([]byte, grow)...)
	}
	n.dirty = false
	n.wrote = n.wrote[:0]
}

func (n *simNode) crash(rng *rand.Rand, sector int64, shred float64) *simNode {
	if !n.dirty {
		return &simNode{cur: slices.Clone(n.synced), synced: slices.Clone(n.synced)}
	}
	short, long := min(len(n.synced), len(n.cur)), max(len(n.synced), len(n.cur))
	size := int64(short) + rng.Int64N(int64(long-short)+1)
	onDisk := make([]byte, size)
	copy(onDisk, n.synced)
	for k := int64(len(n.synced)); k < size; k++ {
		onDisk[k] = junk(rng)
	}
	for sec := n.lo / sector * sector; sec < size; sec += sector {
		if rng.IntN(2) == 0 || sec >= int64(len(n.cur)) {
			continue
		}
		end := min(sec+sector, size, int64(len(n.cur)))
		copy(onDisk[sec:end], n.cur[sec:end])
	}
	if shred > 0 {
		for _, sec := range n.wrote {
			at := sec * sector
			if at >= size {
				break
			}
			if rng.Float64() < shred {
				for k := at; k < min(at+sector, size); k++ {
					onDisk[k] = junk(rng)
				}
			}
		}
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
		f.node.mark(off, off+int64(short), f.sim.Sector)
		return short, err
	}
	f.node.write(p, off)
	f.node.mark(off, off+int64(len(p)), f.sim.Sector)
	return len(p), nil
}

func (f *simFile) Sync() error {
	if f.closed {
		return f.fail("sync", fs.ErrClosed)
	}
	f.sim.syncs++
	if f.sim.syncs == f.sim.failSync {
		f.node.dropDirty()
		return f.fail("sync", syscall.EIO)
	}
	if err := f.sim.eio("sync", f.name, f.sim.SyncErrRate); err != nil {
		if f.sim.rng.IntN(2) == 0 {
			f.node.dropDirty()
		}
		return err
	}
	f.node.sync()
	f.sim.journal()
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
	if size%f.sim.Sector != 0 {
		f.node.mark(size-1, size, f.sim.Sector)
	}
	return nil
}

func (f *simFile) Size() (int64, error) {
	if f.closed {
		return 0, f.fail("stat", fs.ErrClosed)
	}
	if err := f.sim.eio("stat", f.name, f.sim.ReadErrRate); err != nil {
		return 0, err
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
