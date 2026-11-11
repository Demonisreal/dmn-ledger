package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
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

func snapshotFooter(meta SnapshotMeta, body []byte) [snapFooterLen]byte {
	var ftr [snapFooterLen]byte
	binary.LittleEndian.PutUint64(ftr[0:], meta.Index)
	binary.LittleEndian.PutUint64(ftr[8:], meta.Term)
	crc := crc32.Update(crc32.Checksum(body, castagnoli), castagnoli, ftr[:16])
	binary.LittleEndian.PutUint32(ftr[16:], crc)
	return ftr
}

func SaveSnapshot(fsys disk.FS, dir string, meta SnapshotMeta, body []byte) error {
	name := filepath.Join(dir, snapshotName(meta))
	tmp := name + ".tmp"
	f, err := fsys.Create(tmp)
	if err != nil {
		return err
	}
	ftr := snapshotFooter(meta, body)
	_, err = f.WriteAt(body, 0)
	if err == nil {
		_, err = f.WriteAt(ftr[:], int64(len(body)))
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

func LoadSnapshot(fsys disk.FS, dir string) (SnapshotMeta, []byte, error) {
	names, err := fsys.List(dir)
	if err != nil {
		return SnapshotMeta{}, nil, err
	}
	for i := len(names) - 1; i >= 0; i-- {
		meta, ok := parseSnapshotName(names[i])
		if !ok {
			continue
		}
		body, err := readSnapshot(fsys, filepath.Join(dir, names[i]), meta)
		if err != nil {
			return SnapshotMeta{}, nil, err
		}
		return meta, body, nil
	}
	return SnapshotMeta{}, nil, ErrNoSnapshot
}

func readSnapshot(fsys disk.FS, path string, meta SnapshotMeta) ([]byte, error) {
	f, err := fsys.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	size, err := f.Size()
	if err != nil {
		return nil, err
	}
	if size < snapFooterLen {
		return nil, &CorruptError{File: path, Offset: 0}
	}
	raw := make([]byte, size)
	if _, err := f.ReadAt(raw, 0); err != nil {
		return nil, err
	}
	body := raw[:size-snapFooterLen]
	if snapshotFooter(meta, body) != [snapFooterLen]byte(raw[len(body):]) {
		return nil, &CorruptError{File: path, Offset: int64(len(body))}
	}
	return body, nil
}
