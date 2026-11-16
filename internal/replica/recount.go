package replica

import (
	"errors"
	"fmt"
	"io"
	"math"

	"github.com/Demonisreal/dmn-ledger/internal/audit"
	"github.com/Demonisreal/dmn-ledger/internal/wal"
)

var (
	ErrBehind    = errors.New("replica: the directory ends before the head")
	ErrOtherHead = errors.New("replica: the directory leads to another head")
)

func Recount(cfg Config, want audit.Head) error {
	cfg.ID, cfg.Peers, cfg.SnapshotEvery = 1, []uint64{1}, math.MaxUint64
	n, err := recount(cfg, want)
	if err != nil {
		return err
	}
	first, _ := n.log.FirstIndex()
	if err := n.Close(); err != nil || n.snap.Index == 0 {
		return err
	}
	if err := wal.RemoveSnapshot(cfg.FS, n.snapDir(), wal.SnapshotMeta(n.snap)); err != nil {
		return err
	}
	older, _, err := wal.LoadSnapshot(cfg.FS, n.snapDir(), func(io.Reader) error { return nil })
	if err != nil && !errors.Is(err, wal.ErrNoSnapshot) {
		return err
	}
	if first > older.Index+1 {
		return nil
	}
	if n, err = recount(cfg, want); err != nil {
		return err
	}
	return n.Close()
}

func recount(cfg Config, want audit.Head) (*Node, error) {
	n, err := Open(cfg)
	if err != nil {
		return nil, err
	}
	if err := n.count(4*cfg.ElectionTicks, want); err != nil {
		return nil, errors.Join(err, n.Close())
	}
	return n, nil
}

func (n *Node) count(rounds int, want audit.Head) error {
	for st := n.Status(); st.Applied < st.Last; st = n.Status() {
		if rounds == 0 {
			return fmt.Errorf("replica: recount applied %d of %d entries", st.Applied, st.Last)
		}
		rounds--
		n.Tick()
		if _, err := n.Process(); err != nil {
			return err
		}
	}
	got, ok := n.sm.HeadAt(want.Seq)
	switch {
	case !ok:
		return fmt.Errorf("%w: %d links from snapshot %d, the head is link %d", ErrBehind, n.sm.Head().Seq, n.snap.Index, want.Seq)
	case got != want:
		return fmt.Errorf("%w: link %d from snapshot %d has head %x, want %x", ErrOtherHead, want.Seq, n.snap.Index, got.Hash[:4], want.Hash[:4])
	}
	return nil
}
