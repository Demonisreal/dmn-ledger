package disk

import (
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"syscall"
)

var ErrLocked = errors.New("disk: directory locked")

const LockName = "LOCK"

type File interface {
	io.ReaderAt
	io.WriterAt
	io.Closer
	Sync() error
	Truncate(size int64) error
	Size() (int64, error)
}

type FS interface {
	Create(name string) (File, error)
	Open(name string) (File, error)
	Remove(name string) error
	Rename(oldname, newname string) error
	List(dir string) ([]string, error)
	SyncDir(dir string) error
	MkdirAll(dir string) error
	Lock(dir string) (io.Closer, error)
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

func (fsys OS) MkdirAll(dir string) error {
	for _, d := range chain(dir) {
		if err := os.Mkdir(d, 0o750); err != nil && !errors.Is(err, os.ErrExist) {
			return err
		}
		if err := fsys.SyncDir(filepath.Dir(d)); err != nil {
			return err
		}
	}
	return nil
}

func (OS) Lock(dir string) (io.Closer, error) {
	f, err := os.OpenFile(filepath.Join(filepath.Clean(dir), LockName), os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, err
	}
	err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if errors.Is(err, syscall.EWOULDBLOCK) {
		err = ErrLocked
	}
	if err != nil {
		_ = f.Close()
		return nil, &fs.PathError{Op: "lock", Path: dir, Err: err}
	}
	return f, nil
}

func chain(dir string) []string {
	var dirs []string
	for dir = filepath.Clean(dir); dir != filepath.Dir(dir); dir = filepath.Dir(dir) {
		dirs = append(dirs, dir)
	}
	slices.Reverse(dirs)
	return dirs
}
