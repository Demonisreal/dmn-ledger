//go:build simbug

package replica

import (
	"encoding/binary"

	"github.com/Demonisreal/dmn-ledger/internal/raft"
	"github.com/Demonisreal/dmn-ledger/internal/simbug"
	"github.com/Demonisreal/dmn-ledger/internal/wal"
)

func (n *Node) persist(rd raft.Ready) error {
	if simbug.On("B4") {
		if err := n.log.Sync(); err != nil {
			return err
		}
	}
	if rd.HardState != nil && !simbug.On("B5") {
		if err := n.hs.Save(wal.HardState(*rd.HardState)); err != nil {
			return err
		}
	}
	if rd.Snapshot != nil {
		if err := n.install(*rd.Snapshot); err != nil {
			return err
		}
	}
	if len(rd.Entries) > 0 {
		if err := n.store(rd.Entries); err != nil {
			return err
		}
		if !simbug.On("B4") {
			if err := n.log.Sync(); err != nil {
				return err
			}
		}
	}
	if rd.HardState != nil && simbug.On("B5") {
		return n.hs.Save(wal.HardState(*rd.HardState))
	}
	return nil
}

func fresh(s *session, number uint64) bool {
	return number > s.last || simbug.On("B11") && number > 0
}

func (n *Node) entryTime(body []byte) uint64 {
	if simbug.On("B12") {
		return n.cfg.Clock()
	}
	return binary.BigEndian.Uint64(body[33:])
}
