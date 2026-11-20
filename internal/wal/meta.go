package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"io/fs"
	"math"
	"path/filepath"

	"github.com/Demonisreal/dmn-ledger/internal/disk"
)

const (
	MetaName       = "meta"
	MetaStride     = 4096
	metaOffTerm    = 8
	metaOffSealed  = 16
	metaOffCluster = 24
	metaOffVote    = 32
	metaOffNode    = 33
	metaOffNodes   = 34
	metaOffFlags   = 35
	metaOffCRC     = 36
	MetaSlotLen    = 40
	metaLen        = MetaStride + MetaSlotLen

	metaRepair uint8 = 1
)

var ErrNoMeta = errors.New("wal: no meta")

type Identity struct {
	Cluster uint64
	Node    uint64
	Nodes   uint64
}

func (id Identity) String() string {
	return fmt.Sprintf("node %d of %d in cluster %d", id.Node, id.Nodes, id.Cluster)
}

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
	id     Identity
	cur    Meta
	intact bool
	err    error
	slot   [MetaSlotLen]byte
}

func OpenMeta(fsys disk.FS, dir string) (*MetaFile, Meta, error) {
	return openMeta(fsys, dir, nil)
}

func CreateMeta(fsys disk.FS, dir string, id Identity) (*MetaFile, Meta, error) {
	if id.Cluster == 0 || id.Node == 0 || id.Node > id.Nodes || id.Nodes > math.MaxUint8 {
		return nil, Meta{}, fmt.Errorf("wal: %s is not an identity", id)
	}
	if err := fsys.MkdirAll(dir); err != nil {
		return nil, Meta{}, err
	}
	return openMeta(fsys, dir, &id)
}

func openMeta(fsys disk.FS, dir string, create *Identity) (_ *MetaFile, _ Meta, err error) {
	lock, err := fsys.Lock(dir)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, Meta{}, fmt.Errorf("%w: %s does not exist", ErrNoMeta, dir)
	}
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
	switch {
	case errors.Is(err, fs.ErrNotExist) && create == nil:
		return nil, Meta{}, fmt.Errorf("%w: %s", ErrNoMeta, path)
	case errors.Is(err, fs.ErrNotExist):
		m.f, err = createMeta(fsys, path, *create)
	}
	if err != nil {
		return nil, Meta{}, err
	}
	if err := m.load(path); err != nil {
		return nil, Meta{}, err
	}
	return m, m.cur, nil
}

func createMeta(fsys disk.FS, path string, id Identity) (disk.File, error) {
	tmp := path + ".tmp"
	f, err := fsys.Create(tmp)
	if err != nil {
		return nil, err
	}
	var raw [metaLen]byte
	for seq := range uint64(2) {
		encodeMeta(raw[seq*MetaStride:], seq, id, Meta{})
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
		seq, id, st, ok := decodeMeta(slot)
		if !ok {
			continue
		}
		if flags := slot[metaOffFlags]; flags&^metaRepair != 0 {
			return fmt.Errorf("%w: %s has flags %#x", ErrVersion, path, flags)
		}
		if id.Node == 0 {
			return fmt.Errorf("%w: %s holds no identity, an earlier build wrote it", ErrVersion, path)
		}
		if valid == 1 && id != m.id {
			return &CorruptError{File: path, Offset: int64(off)}
		}
		if valid == 0 || seq > m.seq {
			m.seq, m.cur = seq, st
		}
		m.id = id
		valid++
	}
	if valid == 0 {
		return &CorruptError{File: path, Offset: 0}
	}
	m.intact = valid == 2
	return nil
}

func encodeMeta(slot []byte, seq uint64, id Identity, st Meta) {
	clear(slot[:MetaSlotLen])
	binary.LittleEndian.PutUint64(slot, seq)
	binary.LittleEndian.PutUint64(slot[metaOffTerm:], st.Term)
	binary.LittleEndian.PutUint64(slot[metaOffSealed:], st.Sealed)
	binary.LittleEndian.PutUint64(slot[metaOffCluster:], id.Cluster)
	slot[metaOffVote], slot[metaOffNode], slot[metaOffNodes] = byte(st.Vote), byte(id.Node), byte(id.Nodes) //nolint:gosec // G115: CreateMeta and Save keep all three below 256
	if st.Repair {
		slot[metaOffFlags] = metaRepair
	}
	binary.LittleEndian.PutUint32(slot[metaOffCRC:], crc32.Checksum(slot[:metaOffCRC], castagnoli))
}

func decodeMeta(slot []byte) (uint64, Identity, Meta, bool) {
	if binary.LittleEndian.Uint32(slot[metaOffCRC:]) != crc32.Checksum(slot[:metaOffCRC], castagnoli) {
		return 0, Identity{}, Meta{}, false
	}
	id := Identity{
		Cluster: binary.LittleEndian.Uint64(slot[metaOffCluster:]),
		Node:    uint64(slot[metaOffNode]),
		Nodes:   uint64(slot[metaOffNodes]),
	}
	st := Meta{
		Term:   binary.LittleEndian.Uint64(slot[metaOffTerm:]),
		Vote:   uint64(slot[metaOffVote]),
		Sealed: binary.LittleEndian.Uint64(slot[metaOffSealed:]),
		Repair: slot[metaOffFlags]&metaRepair != 0,
	}
	return binary.LittleEndian.Uint64(slot), id, st, true
}

func (m *MetaFile) Identity() Identity {
	return m.id
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
	if vote > math.MaxUint8 {
		return m.fail(fmt.Errorf("wal: vote for node %d does not fit a slot", vote))
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
	encodeMeta(m.slot[:], seq, m.id, st)
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
