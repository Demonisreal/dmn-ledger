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
		if err := n.meta.Save(rd.HardState.Term, rd.HardState.Vote, rd.HardState.Repair); err != nil {
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
		last := rd.Entries[len(rd.Entries)-1].Index
		if simbug.On("B14") {
			if err := n.meta.Seal(last); err != nil {
				return err
			}
		}
		if !simbug.On("B4") {
			if err := n.log.Sync(); err != nil {
				return err
			}
		}
		if !simbug.On("B13") && !simbug.On("B14") {
			if err := n.meta.Seal(last); err != nil {
				return err
			}
		}
	}
	if rd.HardState != nil && simbug.On("B5") {
		return n.meta.Save(rd.HardState.Term, rd.HardState.Vote, rd.HardState.Repair)
	}
	return nil
}

func (n *Node) mend(hs *wal.Meta, damage error, broken []wal.SnapshotMeta, opts wal.Options) error {
	tidy := func() error {
		if err := n.discard(broken, opts); err != nil {
			return err
		}
		if err := n.reconcile(hs.Repair || damage != nil); err != nil {
			return err
		}
		return n.meta.Seal(logView{n}.LastIndex())
	}
	early := simbug.On("B18") && damage != nil
	if early {
		if err := tidy(); err != nil {
			return err
		}
	}
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
	if early {
		return nil
	}
	return tidy()
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
