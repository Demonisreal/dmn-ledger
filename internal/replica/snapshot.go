package replica

import (
	"bufio"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"

	"github.com/Demonisreal/dmn-ledger/internal/client"
	"github.com/Demonisreal/dmn-ledger/internal/raft"
	"github.com/Demonisreal/dmn-ledger/internal/state"
	"github.com/Demonisreal/dmn-ledger/internal/u128"
	"github.com/Demonisreal/dmn-ledger/internal/wal"
)

const (
	snapMagic    = "DMNR"
	snapVersion  = 2
	snapHeadLen  = len(snapMagic) + 1 + 8 + 8 + sha256.Size + 8 + 8
	sessionLen   = client.IDSize + 8 + 8 + 8 + 1 + 1 + 8 + 4
	snapReadSize = 64 << 10
)

var (
	errBadSnapshot = errors.New("replica: bad snapshot")
	ErrMaxSessions = errors.New("replica: MaxSessions differs from the snapshot")
)

type snapHead struct {
	index, term uint64
	hash        [sha256.Size]byte
	limit       uint64
	sessions    uint64
}

func (n *Node) parseHead(b []byte) (snapHead, error) {
	if string(b[:len(snapMagic)]) != snapMagic || b[len(snapMagic)] != snapVersion {
		return snapHead{}, fmt.Errorf("%w: magic or version", errBadSnapshot)
	}
	b = b[len(snapMagic)+1:]
	h := snapHead{
		index:    binary.BigEndian.Uint64(b),
		term:     binary.BigEndian.Uint64(b[8:]),
		limit:    binary.BigEndian.Uint64(b[16+sha256.Size:]),
		sessions: binary.BigEndian.Uint64(b[24+sha256.Size:]),
	}
	copy(h.hash[:], b[16:])
	if h.limit != uint64(n.cfg.MaxSessions) { //nolint:gosec // G115: Open refuses a MaxSessions below 1
		return snapHead{}, fmt.Errorf("%w: the snapshot was written with %d, this node has %d", ErrMaxSessions, h.limit, n.cfg.MaxSessions)
	}
	return h, nil
}

func (n *Node) writeSnapshot(w io.Writer) error {
	b := make([]byte, 0, snapHeadLen+len(n.sessions)*sessionLen)
	b = append(b, snapMagic...)
	b = append(b, snapVersion)
	b = binary.BigEndian.AppendUint64(b, n.applied)
	b = binary.BigEndian.AppendUint64(b, n.appliedTerm)
	b = append(b, n.hash[:]...)
	b = binary.BigEndian.AppendUint64(b, uint64(n.cfg.MaxSessions)) //nolint:gosec // G115: Open refuses a MaxSessions below 1
	b = binary.BigEndian.AppendUint64(b, uint64(len(n.sessions)))
	for _, id := range slices.SortedFunc(maps.Keys(n.sessions), u128.Uint128.Cmp) {
		s := n.sessions[id]
		b = id.AppendBytes(b)
		b = binary.BigEndian.AppendUint64(b, s.since)
		b = binary.BigEndian.AppendUint64(b, s.last)
		b = binary.BigEndian.AppendUint64(b, s.access)
		b = append(b, byte(s.reply.Op), byte(s.reply.Status))
		b = binary.BigEndian.AppendUint64(b, s.reply.Time)
		b = binary.BigEndian.AppendUint32(b, uint32(len(s.reply.Body))) //nolint:gosec // G115: replies stay below MaxBody
		b = append(b, s.reply.Body...)
	}
	if _, err := w.Write(b); err != nil {
		return err
	}
	return n.sm.WriteSnapshot(w)
}

func (n *Node) load(r io.Reader) error {
	br := bufio.NewReaderSize(r, snapReadSize)
	var buf [snapHeadLen]byte
	if _, err := io.ReadFull(br, buf[:]); err != nil {
		return err
	}
	head, err := n.parseHead(buf[:])
	if err != nil {
		return err
	}
	sessions := make(map[u128.Uint128]*session)
	var rec [sessionLen]byte
	for range head.sessions {
		if _, err := io.ReadFull(br, rec[:]); err != nil {
			return err
		}
		id := u128.FromBytes(rec[:])
		s := &session{
			since:  binary.BigEndian.Uint64(rec[16:]),
			last:   binary.BigEndian.Uint64(rec[24:]),
			access: binary.BigEndian.Uint64(rec[32:]),
		}
		s.reply = client.Reply{Client: id, Number: s.last, Op: client.Op(rec[40]), Status: client.Status(rec[41]), Time: binary.BigEndian.Uint64(rec[42:])}
		size := binary.BigEndian.Uint32(rec[50:])
		if size > client.MaxBody {
			return fmt.Errorf("%w: reply of %d bytes", errBadSnapshot, size)
		}
		if size > 0 {
			s.reply.Body = make([]byte, size)
			if _, err := io.ReadFull(br, s.reply.Body); err != nil {
				return err
			}
		}
		if _, dup := sessions[id]; dup {
			return fmt.Errorf("%w: session %v twice", errBadSnapshot, id)
		}
		sessions[id] = s
	}
	sm, err := state.ReadSnapshot(br)
	if err != nil {
		return err
	}
	n.sm, n.sessions = sm, sessions
	n.applied, n.appliedTerm, n.hash = head.index, head.term, head.hash
	n.anchor, n.anchorTick = sm.Last(), n.ticks
	return nil
}

func (n *Node) install(s raft.Snapshot) error {
	src, ok := n.incoming[s]
	if !ok {
		return fmt.Errorf("replica: no payload for snapshot %d term %d", s.Index, s.Term)
	}
	var buf [snapHeadLen]byte
	if _, err := io.ReadFull(src, buf[:]); err != nil {
		return err
	}
	head, err := n.parseHead(buf[:])
	if err != nil {
		return err
	}
	if head.index != s.Index || head.term != s.Term {
		return fmt.Errorf("%w: payload holds %d/%d, raft wants %d/%d", errBadSnapshot, head.index, head.term, s.Index, s.Term)
	}
	err = wal.SaveSnapshot(n.cfg.FS, n.snapDir(), wal.SnapshotMeta(s), func(w io.Writer) error {
		if _, err := w.Write(buf[:]); err != nil {
			return err
		}
		_, err := io.Copy(w, src)
		return err
	})
	if err != nil {
		return err
	}
	if err := wal.ReadSnapshot(n.cfg.FS, n.snapDir(), wal.SnapshotMeta(s), n.load); err != nil {
		return err
	}
	if err := n.meta.Seal(s.Index); err != nil {
		return err
	}
	if err := n.log.TruncateAfter(0); err != nil {
		return err
	}
	n.snap = s
	return nil
}

func (n *Node) snapshot() error {
	s := raft.Snapshot{Index: n.applied, Term: n.appliedTerm}
	if err := wal.SaveSnapshot(n.cfg.FS, n.snapDir(), wal.SnapshotMeta(s), n.writeSnapshot); err != nil {
		return err
	}
	prev := n.snap.Index
	n.snap = s
	return n.log.CompactBefore(prev + 1)
}

func (n *Node) ReadSnapshot(s raft.Snapshot, read func(io.Reader) error) error {
	return wal.ReadSnapshot(n.cfg.FS, n.snapDir(), wal.SnapshotMeta(s), read)
}

func (n *Node) Digest() [sha256.Size]byte {
	h := sha256.New()
	_ = n.writeSnapshot(h)
	var d [sha256.Size]byte
	h.Sum(d[:0])
	return d
}
