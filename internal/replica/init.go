package replica

import (
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"

	"github.com/Demonisreal/dmn-ledger/internal/disk"
	"github.com/Demonisreal/dmn-ledger/internal/wal"
)

var (
	ErrInitialised = errors.New("replica: the directory is initialised")
	ErrNotEmpty    = errors.New("replica: the directory is not empty")
)

func Init(fsys disk.FS, dir string, id wal.Identity) error {
	if err := unused(fsys, dir); err != nil {
		return err
	}
	m, st, err := wal.CreateMeta(fsys, dir, id)
	if err != nil {
		return err
	}
	if m.Identity() != id || st != (wal.Meta{}) {
		return errors.Join(fmt.Errorf("%w: %s", ErrInitialised, dir), m.Close())
	}
	return errors.Join(m.Settle(), m.Close())
}

func unused(fsys disk.FS, dir string) error {
	names, err := fsys.List(dir)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return nil
	case err != nil:
		return err
	}
	for _, name := range names {
		switch name {
		case disk.LockName, wal.MetaName + ".tmp":
		case wal.MetaName:
			return fmt.Errorf("%w: %s", ErrInitialised, dir)
		default:
			return fmt.Errorf("%w: %s holds %s", ErrNotEmpty, dir, name)
		}
	}
	for _, sub := range []string{"wal", "snap"} {
		_, err := fsys.List(filepath.Join(dir, sub))
		switch {
		case err == nil:
			return fmt.Errorf("%w: %s holds %s", ErrNotEmpty, dir, sub)
		case !errors.Is(err, fs.ErrNotExist):
			return err
		}
	}
	return nil
}
