package wal

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"io/fs"
	"path/filepath"

	"github.com/Demonisreal/dmn-ledger/internal/disk"
)

const (
	hardStateName = "hardstate"
	slotLen       = 28
)

type HardState struct {
	Term uint64
	Vote uint64
}

type HardStateFile struct {
	f   disk.File
	seq uint64
	err error
}

func OpenHardState(fsys disk.FS, dir string) (*HardStateFile, HardState, error) {
	path := filepath.Join(dir, hardStateName)
	f, err := fsys.Open(path)
	if errors.Is(err, fs.ErrNotExist) {
		if f, err = fsys.Create(path); err == nil {
			err = fsys.SyncDir(dir)
		}
	}
	if err != nil {
		return nil, HardState{}, err
	}

	var raw [2 * disk.SectorSize]byte
	if _, err := f.ReadAt(raw[:], 0); err != nil && !errors.Is(err, io.EOF) {
		_ = f.Close()
		return nil, HardState{}, err
	}
	h := &HardStateFile{f: f}
	var hs HardState
	found, damaged := false, false
	for off := 0; off < len(raw); off += disk.SectorSize {
		slot := raw[off : off+slotLen]
		seq, st, ok := decodeSlot(slot)
		switch {
		case ok && (!found || seq > h.seq):
			h.seq, hs, found = seq, st, true
		case !ok && [slotLen]byte(slot) != [slotLen]byte{}:
			damaged = true
		}
	}
	if !found && damaged {
		_ = f.Close()
		return nil, HardState{}, &CorruptError{File: path}
	}
	return h, hs, nil
}

func decodeSlot(slot []byte) (uint64, HardState, bool) {
	if binary.LittleEndian.Uint32(slot[24:]) != crc32.Checksum(slot[:24], castagnoli) {
		return 0, HardState{}, false
	}
	hs := HardState{Term: binary.LittleEndian.Uint64(slot[8:]), Vote: binary.LittleEndian.Uint64(slot[16:])}
	return binary.LittleEndian.Uint64(slot), hs, true
}

func (h *HardStateFile) Save(hs HardState) error {
	if h.err != nil {
		return h.err
	}
	seq := h.seq + 1
	var slot [slotLen]byte
	binary.LittleEndian.PutUint64(slot[0:], seq)
	binary.LittleEndian.PutUint64(slot[8:], hs.Term)
	binary.LittleEndian.PutUint64(slot[16:], hs.Vote)
	binary.LittleEndian.PutUint32(slot[24:], crc32.Checksum(slot[:24], castagnoli))

	var off int64
	if seq%2 == 1 {
		off = disk.SectorSize
	}
	if _, err := h.f.WriteAt(slot[:], off); err != nil {
		h.err = err
		return err
	}
	if err := h.f.Sync(); err != nil {
		h.err = err
		return err
	}
	h.seq = seq
	return nil
}

func (h *HardStateFile) Close() error {
	return h.f.Close()
}
