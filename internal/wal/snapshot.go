package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/Demonisreal/dmn-ledger/internal/disk"
)

const (
	snapFooterLen = 20
	snapsKept     = 2
)

var ErrNoSnapshot = errors.New("wal: no snapshot")

type SnapshotMeta struct {
	Index uint64
	Term  uint64
}

func snapshotName(meta SnapshotMeta) string {
	return fmt.Sprintf("snap-%020d-%020d.snap", meta.Index, meta.Term)
}

func parseSnapshotName(name string) (SnapshotMeta, bool) {
	rest, ok := strings.CutPrefix(name, "snap-")
	if !ok {
		return SnapshotMeta{}, false
	}
	rest, ok = strings.CutSuffix(rest, ".snap")
	if !ok {
		return SnapshotMeta{}, false
	}
	idx, term, ok := strings.Cut(rest, "-")
	if !ok || len(idx) != 20 || len(term) != 20 {
		return SnapshotMeta{}, false
	}
	var meta SnapshotMeta
	var err1, err2 error
	meta.Index, err1 = strconv.ParseUint(idx, 10, 64)
	meta.Term, err2 = strconv.ParseUint(term, 10, 64)
	return meta, err1 == nil && err2 == nil
}

func snapshotFooter(meta SnapshotMeta, bodyCRC uint32) [snapFooterLen]byte {
	var ftr [snapFooterLen]byte
	binary.LittleEndian.PutUint64(ftr[0:], meta.Index)
	binary.LittleEndian.PutUint64(ftr[8:], meta.Term)
	binary.LittleEndian.PutUint32(ftr[16:], crc32.Update(bodyCRC, castagnoli, ftr[:16]))
	return ftr
}

type snapWriter struct {
	f   disk.File
	off int64
	crc uint32
}

func (sw *snapWriter) Write(p []byte) (int, error) {
	n, err := sw.f.WriteAt(p, sw.off)
	sw.crc = crc32.Update(sw.crc, castagnoli, p[:n])
	sw.off += int64(n)
	return n, err
}

func SaveSnapshot(fsys disk.FS, dir string, meta SnapshotMeta, write func(io.Writer) error) error {
	name := filepath.Join(dir, snapshotName(meta))
	tmp := name + ".tmp"
	f, err := fsys.Create(tmp)
	if err != nil {
		return err
	}
	sw := &snapWriter{f: f}
	err = write(sw)
	if err == nil {
		ftr := snapshotFooter(meta, sw.crc)
		_, err = f.WriteAt(ftr[:], sw.off)
	}
	if err == nil {
		err = f.Sync()
	}
	if err = errors.Join(err, f.Close()); err != nil {
		return err
	}
	if err := fsys.Rename(tmp, name); err != nil {
		return err
	}
	if err := fsys.SyncDir(dir); err != nil {
		return err
	}
	return pruneSnapshots(fsys, dir)
}

func pruneSnapshots(fsys disk.FS, dir string) error {
	names, err := fsys.List(dir)
	if err != nil {
		return err
	}
	var snaps, stale []string
	for _, name := range names {
		if _, ok := parseSnapshotName(name); ok {
			snaps = append(snaps, name)
		} else if base, ok := strings.CutSuffix(name, ".tmp"); ok {
			if _, ok := parseSnapshotName(base); ok {
				stale = append(stale, name)
			}
		}
	}
	if len(snaps) > snapsKept {
		stale = append(stale, snaps[:len(snaps)-snapsKept]...)
	}
	if len(stale) == 0 {
		return nil
	}
	for _, name := range stale {
		if err := fsys.Remove(filepath.Join(dir, name)); err != nil {
			return err
		}
	}
	return fsys.SyncDir(dir)
}

func LoadSnapshot(fsys disk.FS, dir string, read func(io.Reader) error) (SnapshotMeta, error) {
	names, err := fsys.List(dir)
	if err != nil {
		return SnapshotMeta{}, err
	}
	if err := fsys.SyncDir(dir); err != nil {
		return SnapshotMeta{}, err
	}
	for i := len(names) - 1; i >= 0; i-- {
		meta, ok := parseSnapshotName(names[i])
		if !ok {
			continue
		}
		if err := readSnapshot(fsys, filepath.Join(dir, names[i]), meta, read); err != nil {
			return SnapshotMeta{}, err
		}
		return meta, nil
	}
	return SnapshotMeta{}, ErrNoSnapshot
}

func readSnapshot(fsys disk.FS, path string, meta SnapshotMeta, read func(io.Reader) error) error {
	f, err := fsys.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	size, err := f.Size()
	if err != nil {
		return err
	}
	if size < snapFooterLen {
		return &CorruptError{File: path, Offset: 0}
	}
	bodyLen := size - snapFooterLen
	h := crc32.New(castagnoli)
	if _, err := io.Copy(h, io.NewSectionReader(f, 0, bodyLen)); err != nil {
		return err
	}
	var ftr [snapFooterLen]byte
	if _, err := f.ReadAt(ftr[:], bodyLen); err != nil {
		return err
	}
	if ftr != snapshotFooter(meta, h.Sum32()) {
		return &CorruptError{File: path, Offset: bodyLen}
	}
	return read(io.NewSectionReader(f, 0, bodyLen))
}
