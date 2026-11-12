//go:build !simbug

package replica

import (
	"encoding/binary"

	"github.com/Demonisreal/dmn-ledger/internal/raft"
	"github.com/Demonisreal/dmn-ledger/internal/wal"
)

func (n *Node) persist(rd raft.Ready) error {
	if rd.HardState != nil {
		if err := n.hs.Save(wal.HardState{Term: rd.HardState.Term, Vote: rd.HardState.Vote}); err != nil {
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
	return n.log.Sync()
}

func fresh(s *session, number uint64) bool {
	return number > s.last
}

func (n *Node) entryTime(body []byte) uint64 {
	return binary.BigEndian.Uint64(body[33:])
}
