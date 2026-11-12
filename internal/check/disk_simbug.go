//go:build simbug

package check

import (
	"path/filepath"
	"strings"

	"github.com/Demonisreal/dmn-ledger/internal/disk"
	"github.com/Demonisreal/dmn-ledger/internal/simbug"
)

type lyingFS struct {
	disk.FS
	rolled bool
}

type lyingFile struct {
	disk.File
}

func (lyingFile) Sync() error {
	return nil
}

func lyingDisk() func(uint64, disk.FS) disk.FS {
	if !simbug.On("B1") && !simbug.On("B2") && !simbug.On("B3") {
		return nil
	}
	return func(_ uint64, fsys disk.FS) disk.FS {
		return &lyingFS{FS: fsys}
	}
}

func inWAL(name string) bool {
	return filepath.Base(filepath.Dir(name)) == "wal"
}

func (l *lyingFS) lie(name string, f disk.File, err error) (disk.File, error) {
	switch {
	case err != nil:
		return nil, err
	case simbug.On("B1") && strings.HasPrefix(filepath.Base(name), "hardstate"),
		simbug.On("B2") && inWAL(name):
		return lyingFile{f}, nil
	}
	return f, nil
}

func (l *lyingFS) Create(name string) (disk.File, error) {
	f, err := l.FS.Create(name)
	return l.lie(name, f, err)
}

func (l *lyingFS) Open(name string) (disk.File, error) {
	f, err := l.FS.Open(name)
	return l.lie(name, f, err)
}

func (l *lyingFS) Rename(oldname, newname string) error {
	err := l.FS.Rename(oldname, newname)
	if err == nil && inWAL(newname) {
		l.rolled = true
	}
	return err
}

func (l *lyingFS) SyncDir(dir string) error {
	if simbug.On("B3") && l.rolled && filepath.Base(dir) == "wal" {
		l.rolled = false
		return nil
	}
	return l.FS.SyncDir(dir)
}
