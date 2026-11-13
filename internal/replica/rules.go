//go:build !simbug

package replica

import (
	"encoding/binary"

	"github.com/Demonisreal/dmn-ledger/internal/raft"
	"github.com/Demonisreal/dmn-ledger/internal/wal"
)

func (n *Node) persist(rd raft.Ready) error {
	if rd.HardState != nil {
		if err := n.meta.Save(rd.HardState.Term, rd.HardState.Vote, rd.HardState.Repair); err != nil {
			return err
		}
	}
	if rd.Snapshot != nil {
		if err := n.install(*rd.Snapshot); err != nil {
			return err
		}
	}
	if len(rd.Entries) == 0 {
		return nil
	}
	if err := n.store(rd.Entries); err != nil {
		return err
	}
	if err := n.log.Sync(); err != nil {
		return err
	}
	return n.meta.Seal(rd.Entries[len(rd.Entries)-1].Index)
}

func (n *Node) mend(hs *wal.Meta, damage error, broken []wal.SnapshotMeta, opts wal.Options) error {
	if err := n.meta.Settle(); err != nil {
		return err
	}
	if damage != nil && !hs.Repair {
		if len(n.cfg.Peers) == 1 {
			return damage
		}
		if err := n.meta.Save(hs.Term, hs.Vote, true); err != nil {
			return err
		}
		hs.Repair = true
	}
	if err := n.discard(broken, opts); err != nil {
		return err
	}
	if err := n.reconcile(hs.Repair); err != nil {
		return err
	}
	return n.meta.Seal(logView{n}.LastIndex())
}

func fresh(s *session, number uint64) bool {
	return number > s.last
}

func (n *Node) entryTime(body []byte) uint64 {
	return binary.BigEndian.Uint64(body[33:])
}
