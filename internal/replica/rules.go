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

func (n *Node) mend(hs wal.Meta, damage error, broken []wal.SnapshotMeta, opts wal.Options) (bool, error) {
	if err := n.meta.Settle(); err != nil {
		return false, err
	}
	repair, err := n.flag(hs, damage)
	if err != nil {
		return false, err
	}
	if err := n.discard(broken); err != nil {
		return false, err
	}
	if err := n.salvage(opts); err != nil {
		return false, err
	}
	if err := n.reconcile(repair); err != nil {
		return false, err
	}
	return repair, n.meta.Seal(logView{n}.LastIndex())
}

func fresh(s *session, number uint64) bool {
	return number > s.last
}

func (n *Node) entryTime(body []byte) uint64 {
	return binary.BigEndian.Uint64(body[33:])
}
