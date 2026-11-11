package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
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
	hardStateLen  = slotStride + slotLen
)

type HardState struct {
	Term uint64
	Vote uint64
}

type HardStateFile struct {
	f    disk.File
	lock io.Closer
	seq  uint64
	err  error
}

func OpenHardState(fsys disk.FS, dir string) (_ *HardStateFile, _ HardState, err error) {
	if err := fsys.MkdirAll(dir); err != nil {
		return nil, HardState{}, err
	}
	lock, err := fsys.Lock(dir)
	if err != nil {
		return nil, HardState{}, err
	}
	h := &HardStateFile{lock: lock}
	defer func() {
		if err != nil {
			_ = h.Close()
		}
	}()
	path := filepath.Join(dir, hardStateName)
	h.f, err = fsys.Open(path)
	if errors.Is(err, fs.ErrNotExist) {
		h.f, err = createHardState(fsys, path)
	}
	if err != nil {
		return nil, HardState{}, err
	}
	hs, err := h.load(path)
	if err != nil {
		return nil, HardState{}, err
	}
	if err := h.put(h.seq, hs); err != nil {
		return nil, HardState{}, err
	}
	if err := h.put(h.seq+1, hs); err != nil {
		return nil, HardState{}, err
	}
	if err := fsys.SyncDir(dir); err != nil {
		return nil, HardState{}, err
	}
	return h, hs, nil
}

func createHardState(fsys disk.FS, path string) (disk.File, error) {
	tmp := path + ".tmp"
	f, err := fsys.Create(tmp)
	if err != nil {
		return nil, err
	}
	var raw [hardStateLen]byte
	slot := encodeSlot(0, HardState{})
	copy(raw[:], slot[:])
	_, err = f.WriteAt(raw[:], 0)
	if err == nil {
		err = f.Sync()
	}
	if err == nil {
		err = fsys.Rename(tmp, path)
	}
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	return f, nil
}

func (h *HardStateFile) load(path string) (HardState, error) {
	var raw [hardStateLen]byte
	if _, err := h.f.ReadAt(raw[:], 0); err != nil && !errors.Is(err, io.EOF) {
		return HardState{}, err
	}
	var hs HardState
	found := false
	for off := 0; off < len(raw); off += slotStride {
		seq, st, ok := decodeSlot(raw[off : off+slotLen])
		if ok && (!found || seq > h.seq) {
			h.seq, hs, found = seq, st, true
		}
	}
	if !found {
		return HardState{}, &CorruptError{File: path, Offset: 0}
	}
	return hs, nil
}

func encodeSlot(seq uint64, hs HardState) [slotLen]byte {
	var slot [slotLen]byte
	binary.LittleEndian.PutUint64(slot[:], seq)
	binary.LittleEndian.PutUint64(slot[slotOffTerm:], hs.Term)
	binary.LittleEndian.PutUint64(slot[slotOffVote:], hs.Vote)
	binary.LittleEndian.PutUint32(slot[slotOffCRC:], crc32.Checksum(slot[:slotOffCRC], castagnoli))
	return slot
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
	if err := h.put(h.seq+1, hs); err != nil {
		return err
	}
	return h.put(h.seq+1, hs)
}

func (h *HardStateFile) put(seq uint64, hs HardState) error {
	slot := encodeSlot(seq, hs)
	if _, err := h.f.WriteAt(slot[:], int64(seq%2)*slotStride); err != nil {
		return h.fail(err)
	}
	if err := h.f.Sync(); err != nil {
		return h.fail(err)
	}
	h.seq = seq
	return nil
}

func (h *HardStateFile) fail(err error) error {
	h.err = fmt.Errorf("wal: hard state stopped after error: %w", err)
	return h.err
}

func (h *HardStateFile) Close() error {
	var err error
	if h.f != nil {
		err = h.f.Close()
	}
	return errors.Join(err, h.lock.Close())
}
