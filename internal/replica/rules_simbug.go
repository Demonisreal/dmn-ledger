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

func (n *Node) mend(hs wal.Meta, damage error, broken []wal.SnapshotMeta, opts wal.Options) (bool, error) {
	late := simbug.On("B18") && damage != nil
	repair := hs.Repair || late
	if !late {
		if err := n.meta.Settle(); err != nil {
			return false, err
		}
		var err error
		if repair, err = n.flag(hs, damage); err != nil {
			return false, err
		}
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
	if err := n.meta.Seal(logView{n}.LastIndex()); err != nil || !late {
		return repair, err
	}
	if err := n.meta.Settle(); err != nil {
		return false, err
	}
	return n.flag(hs, damage)
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
