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
	slotStride    = 4096
	slotOffTerm   = 8
	slotOffVote   = 16
	slotOffCRC    = 24
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
		f, err = fsys.Create(path)
	}
	if err != nil {
		return nil, HardState{}, err
	}
	h, hs, err := loadHardState(f, path)
	if err == nil {
		err = fsys.SyncDir(dir)
	}
	if err != nil {
		_ = f.Close()
		return nil, HardState{}, err
	}
	return h, hs, nil
}

func loadHardState(f disk.File, path string) (*HardStateFile, HardState, error) {
	var raw [slotStride + slotLen]byte
	n, err := f.ReadAt(raw[:], 0)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, HardState{}, err
	}
	h := &HardStateFile{f: f}
	var hs HardState
	found, damaged := false, int64(-1)
	for off := int64(0); off < int64(len(raw)); off += slotStride {
		slot := raw[off : off+slotLen]
		seq, st, ok := decodeSlot(slot)
		switch {
		case ok && (!found || seq > h.seq):
			h.seq, hs, found = seq, st, true
		case !ok && [slotLen]byte(slot) != [slotLen]byte{} && damaged < 0:
			damaged = off
		}
	}
	if !found && damaged >= 0 {
		return nil, HardState{}, &CorruptError{File: path, Offset: damaged}
	}
	if _, err := f.WriteAt(raw[:n], 0); err != nil {
		return nil, HardState{}, err
	}
	if err := f.Sync(); err != nil {
		return nil, HardState{}, err
	}
	return h, hs, nil
}

func decodeSlot(slot []byte) (uint64, HardState, bool) {
	if binary.LittleEndian.Uint32(slot[slotOffCRC:]) != crc32.Checksum(slot[:slotOffCRC], castagnoli) {
		return 0, HardState{}, false
	}
	hs := HardState{
		Term: binary.LittleEndian.Uint64(slot[slotOffTerm:]),
		Vote: binary.LittleEndian.Uint64(slot[slotOffVote:]),
	}
	return binary.LittleEndian.Uint64(slot), hs, true
}

func (h *HardStateFile) Save(hs HardState) error {
	if h.err != nil {
		return h.err
	}
	seq := h.seq + 1
	var slot [slotLen]byte
	binary.LittleEndian.PutUint64(slot[:], seq)
	binary.LittleEndian.PutUint64(slot[slotOffTerm:], hs.Term)
	binary.LittleEndian.PutUint64(slot[slotOffVote:], hs.Vote)
	binary.LittleEndian.PutUint32(slot[slotOffCRC:], crc32.Checksum(slot[:slotOffCRC], castagnoli))

	var off int64
	if seq%2 == 1 {
		off = slotStride
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
