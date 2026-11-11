package disk

import (
	"errors"
	"os"
	"path/filepath"
)

type File interface {
	ReadAt(p []byte, off int64) (int, error)
	WriteAt(p []byte, off int64) (int, error)
	Sync() error
	Truncate(size int64) error
	Size() (int64, error)
	Close() error
}

type FS interface {
	Create(name string) (File, error)
	Open(name string) (File, error)
	Remove(name string) error
	Rename(oldname, newname string) error
	List(dir string) ([]string, error)
	SyncDir(dir string) error
	MkdirAll(dir string) error
}

type OS struct{}

type osFile struct {
	*os.File
}

func (f osFile) Size() (int64, error) {
	st, err := f.Stat()
	if err != nil {
		return 0, err
	}
	return st.Size(), nil
}

func (OS) Create(name string) (File, error) {
	return openFile(name, os.O_RDWR|os.O_CREATE|os.O_TRUNC)
}

func (OS) Open(name string) (File, error) {
	return openFile(name, os.O_RDWR)
}

func openFile(name string, flag int) (File, error) {
	f, err := os.OpenFile(filepath.Clean(name), flag, 0o600)
	if err != nil {
		return nil, err
	}
	return osFile{f}, nil
}

func (OS) Remove(name string) error {
	return os.Remove(name)
}

func (OS) Rename(oldname, newname string) error {
	return os.Rename(oldname, newname)
}

func (OS) List(dir string) ([]string, error) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(ents))
	for _, de := range ents {
		if de.Type().IsRegular() {
			names = append(names, de.Name())
		}
	}
	return names, nil
}

func (OS) SyncDir(dir string) error {
	d, err := os.Open(filepath.Clean(dir))
	if err != nil {
		return err
	}
	return errors.Join(d.Sync(), d.Close())
}

func (OS) MkdirAll(dir string) error {
	return os.MkdirAll(dir, 0o750)
}
