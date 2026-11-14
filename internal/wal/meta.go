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
	MetaName      = "meta"
	MetaStride    = 4096
	metaOffTerm   = 8
	metaOffVote   = 16
	metaOffSealed = 24
	metaOffFlags  = 32
	metaOffCRC    = 36
	MetaSlotLen   = 40
	metaLen       = MetaStride + MetaSlotLen

	metaRepair uint32 = 1
)

type Meta struct {
	Term   uint64
	Vote   uint64
	Sealed uint64
	Repair bool
}

type MetaFile struct {
	fsys   disk.FS
	dir    string
	f      disk.File
	lock   io.Closer
	seq    uint64
	cur    Meta
	intact bool
	err    error
	slot   [MetaSlotLen]byte
}

func OpenMeta(fsys disk.FS, dir string) (_ *MetaFile, _ Meta, err error) {
	if err := fsys.MkdirAll(dir); err != nil {
		return nil, Meta{}, err
	}
	lock, err := fsys.Lock(dir)
	if err != nil {
		return nil, Meta{}, err
	}
	m := &MetaFile{fsys: fsys, dir: dir, lock: lock}
	defer func() {
		if err != nil {
			_ = m.Close()
		}
	}()
	path := filepath.Join(dir, MetaName)
	m.f, err = fsys.Open(path)
	if errors.Is(err, fs.ErrNotExist) {
		m.f, err = createMeta(fsys, path)
	}
	if err != nil {
		return nil, Meta{}, err
	}
	if err := m.load(path); err != nil {
		return nil, Meta{}, err
	}
	return m, m.cur, nil
}

func createMeta(fsys disk.FS, path string) (disk.File, error) {
	tmp := path + ".tmp"
	f, err := fsys.Create(tmp)
	if err != nil {
		return nil, err
	}
	var raw [metaLen]byte
	for seq := range uint64(2) {
		encodeMeta(raw[seq*MetaStride:], seq, Meta{})
	}
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

func (m *MetaFile) load(path string) error {
	var raw [metaLen]byte
	if _, err := m.f.ReadAt(raw[:], 0); err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	valid := 0
	for off := 0; off < metaLen; off += MetaStride {
		slot := raw[off : off+MetaSlotLen]
		seq, st, ok := decodeMeta(slot)
		if !ok {
			continue
		}
		if flags := binary.LittleEndian.Uint32(slot[metaOffFlags:]); flags&^metaRepair != 0 {
			return fmt.Errorf("%w: %s has flags %#x", ErrVersion, path, flags)
		}
		if valid == 0 || seq > m.seq {
			m.seq, m.cur = seq, st
		}
		valid++
	}
	if valid == 0 {
		return &CorruptError{File: path, Offset: 0}
	}
	m.intact = valid == 2
	return nil
}

func encodeMeta(slot []byte, seq uint64, st Meta) {
	clear(slot[:MetaSlotLen])
	binary.LittleEndian.PutUint64(slot, seq)
	binary.LittleEndian.PutUint64(slot[metaOffTerm:], st.Term)
	binary.LittleEndian.PutUint64(slot[metaOffVote:], st.Vote)
	binary.LittleEndian.PutUint64(slot[metaOffSealed:], st.Sealed)
	if st.Repair {
		binary.LittleEndian.PutUint32(slot[metaOffFlags:], metaRepair)
	}
	binary.LittleEndian.PutUint32(slot[metaOffCRC:], crc32.Checksum(slot[:metaOffCRC], castagnoli))
}

func decodeMeta(slot []byte) (uint64, Meta, bool) {
	if binary.LittleEndian.Uint32(slot[metaOffCRC:]) != crc32.Checksum(slot[:metaOffCRC], castagnoli) {
		return 0, Meta{}, false
	}
	st := Meta{
		Term:   binary.LittleEndian.Uint64(slot[metaOffTerm:]),
		Vote:   binary.LittleEndian.Uint64(slot[metaOffVote:]),
		Sealed: binary.LittleEndian.Uint64(slot[metaOffSealed:]),
		Repair: binary.LittleEndian.Uint32(slot[metaOffFlags:])&metaRepair != 0,
	}
	return binary.LittleEndian.Uint64(slot), st, true
}

func (m *MetaFile) Intact() bool {
	return m.intact
}

func (m *MetaFile) Sealed() uint64 {
	return m.cur.Sealed
}

func (m *MetaFile) Settle() error {
	if m.err != nil {
		return m.err
	}
	if m.intact {
		if err := m.put(m.seq, m.cur); err != nil {
			return err
		}
	}
	if err := m.fsys.SyncDir(m.dir); err != nil {
		return m.fail(err)
	}
	return nil
}

func (m *MetaFile) Save(term, vote uint64, repair bool) error {
	if m.err != nil {
		return m.err
	}
	st := Meta{Term: term, Vote: vote, Sealed: m.cur.Sealed, Repair: repair}
	for range 2 {
		if err := m.put(m.seq+1, st); err != nil {
			return err
		}
	}
	return nil
}

func (m *MetaFile) Seal(index uint64) error {
	if m.err != nil {
		return m.err
	}
	st := m.cur
	st.Sealed = index
	return m.put(m.seq+1, st)
}

func (m *MetaFile) put(seq uint64, st Meta) error {
	encodeMeta(m.slot[:], seq, st)
	if _, err := m.f.WriteAt(m.slot[:], int64(seq%2)*MetaStride); err != nil {
		return m.fail(err)
	}
	if err := m.f.Sync(); err != nil {
		return m.fail(err)
	}
	m.seq, m.cur = seq, st
	return nil
}

func (m *MetaFile) fail(err error) error {
	m.err = fmt.Errorf("wal: meta stopped after error: %w", err)
	return m.err
}

func (m *MetaFile) Close() error {
	var err error
	if m.f != nil {
		err = m.f.Close()
	}
	return errors.Join(err, m.lock.Close())
}
