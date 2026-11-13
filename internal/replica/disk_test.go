package replica

import (
	"github.com/Demonisreal/dmn-ledger/internal/disk"
)

type tapFS struct {
	disk.FS
	after func(op, name string)
}

type tapFile struct {
	disk.File
	name  string
	after func(op, name string)
}

func (f tapFS) Create(name string) (disk.File, error) {
	file, err := f.FS.Create(name)
	f.after("create", name)
	if err != nil {
		return nil, err
	}
	return tapFile{file, name, f.after}, nil
}

func (f tapFS) Open(name string) (disk.File, error) {
	file, err := f.FS.Open(name)
	if err != nil {
		return nil, err
	}
	return tapFile{file, name, f.after}, nil
}

func (f tapFS) Remove(name string) error {
	err := f.FS.Remove(name)
	f.after("remove", name)
	return err
}

func (f tapFS) Rename(oldname, newname string) error {
	err := f.FS.Rename(oldname, newname)
	f.after("rename", newname)
	return err
}

func (f tapFS) SyncDir(dir string) error {
	err := f.FS.SyncDir(dir)
	f.after("syncdir", dir)
	return err
}

func (f tapFS) MkdirAll(dir string) error {
	err := f.FS.MkdirAll(dir)
	f.after("mkdir", dir)
	return err
}

func (f tapFile) WriteAt(p []byte, off int64) (int, error) {
	n, err := f.File.WriteAt(p, off)
	f.after("write", f.name)
	return n, err
}

func (f tapFile) Sync() error {
	err := f.File.Sync()
	f.after("sync", f.name)
	return err
}

func (f tapFile) Truncate(size int64) error {
	err := f.File.Truncate(size)
	f.after("truncate", f.name)
	return err
}
