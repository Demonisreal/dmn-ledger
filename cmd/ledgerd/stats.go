package main

import (
	"fmt"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Demonisreal/dmn-ledger/internal/disk"
	"github.com/Demonisreal/dmn-ledger/internal/server"
	"github.com/Demonisreal/dmn-ledger/internal/wal"
)

type syncs struct {
	n  atomic.Uint64
	ns atomic.Int64
}

func (s *syncs) timed(sync func() error) error {
	began := time.Now()
	err := sync()
	s.ns.Add(int64(time.Since(began)))
	s.n.Add(1)
	return err
}

type countingFS struct {
	disk.FS
	dir              string
	log, meta, other syncs
	written          atomic.Uint64
}

type countedFile struct {
	disk.File
	syncs   *syncs
	written *atomic.Uint64
}

func (c *countingFS) Create(name string) (disk.File, error) {
	f, err := c.FS.Create(name)
	if err != nil {
		return nil, err
	}
	return &countedFile{File: f, syncs: c.kind(name), written: &c.written}, nil
}

func (c *countingFS) Open(name string) (disk.File, error) {
	f, err := c.FS.Open(name)
	if err != nil {
		return nil, err
	}
	return &countedFile{File: f, syncs: c.kind(name), written: &c.written}, nil
}

func (c *countingFS) SyncDir(dir string) error {
	return c.other.timed(func() error { return c.FS.SyncDir(dir) })
}

func (c *countingFS) kind(name string) *syncs {
	rel, _ := filepath.Rel(c.dir, name)
	switch {
	case filepath.Dir(rel) == "wal":
		return &c.log
	case strings.TrimSuffix(rel, ".tmp") == wal.MetaName:
		return &c.meta
	}
	return &c.other
}

func (f *countedFile) Sync() error {
	return f.syncs.timed(f.File.Sync)
}

func (f *countedFile) WriteAt(b []byte, off int64) (int, error) {
	n, err := f.File.WriteAt(b, off)
	f.written.Add(uint64(n)) //nolint:gosec // G115: WriteAt never reports a negative count
	return n, err
}

func statsLine(up time.Duration, st server.Stats, c *countingFS) string {
	return fmt.Sprintf("stats up=%d rounds=%d wait=%d process=%d slowest=%d logsync=%d logsync_ns=%d metasync=%d metasync_ns=%d othersync=%d othersync_ns=%d written=%d",
		up, st.Rounds, st.Wait, st.Process, st.Slowest,
		c.log.n.Load(), c.log.ns.Load(), c.meta.n.Load(), c.meta.ns.Load(), c.other.n.Load(), c.other.ns.Load(), c.written.Load())
}
