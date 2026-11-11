package sim

import (
	"io"
	"math/rand/v2"

	"github.com/Demonisreal/dmn-ledger/internal/disk"
)

type meter struct {
	fsys  disk.FS
	rng   *rand.Rand
	slow  int
	cost  uint64
	locks []io.Closer
}

type meteredFile struct {
	disk.File
	m *meter
}

func (m *meter) fsync() {
	if m.slow > 0 {
		m.cost += slowSyncMin + m.rng.Uint64N(slowSyncSpan)
	} else {
		m.cost += fsyncMin + m.rng.Uint64N(fsyncSpan)
	}
}

func (m *meter) take() uint64 {
	c := m.cost
	m.cost = 0
	return c
}

func (m *meter) release() {
	for _, l := range m.locks {
		_ = l.Close()
	}
	m.locks = nil
}

func (m *meter) Create(name string) (disk.File, error) {
	f, err := m.fsys.Create(name)
	if err != nil {
		return nil, err
	}
	return &meteredFile{File: f, m: m}, nil
}

func (m *meter) Open(name string) (disk.File, error) {
	f, err := m.fsys.Open(name)
	if err != nil {
		return nil, err
	}
	return &meteredFile{File: f, m: m}, nil
}

func (m *meter) Remove(name string) error {
	return m.fsys.Remove(name)
}

func (m *meter) Rename(oldname, newname string) error {
	return m.fsys.Rename(oldname, newname)
}

func (m *meter) List(dir string) ([]string, error) {
	return m.fsys.List(dir)
}

func (m *meter) SyncDir(dir string) error {
	m.fsync()
	return m.fsys.SyncDir(dir)
}

func (m *meter) MkdirAll(dir string) error {
	m.fsync()
	return m.fsys.MkdirAll(dir)
}

func (m *meter) Lock(dir string) (io.Closer, error) {
	l, err := m.fsys.Lock(dir)
	if err != nil {
		return nil, err
	}
	m.locks = append(m.locks, l)
	return l, nil
}

func (f *meteredFile) WriteAt(p []byte, off int64) (int, error) {
	f.m.cost += uint64(len(p)) / bytesPerNs
	return f.File.WriteAt(p, off)
}

func (f *meteredFile) Sync() error {
	f.m.fsync()
	return f.File.Sync()
}
