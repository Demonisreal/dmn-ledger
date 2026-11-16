package replica

import (
	"errors"
	"fmt"
	"io"
	"math"
	"path/filepath"

	"github.com/Demonisreal/dmn-ledger/internal/audit"
	"github.com/Demonisreal/dmn-ledger/internal/disk"
	"github.com/Demonisreal/dmn-ledger/internal/wal"
)

var (
	ErrBehind    = errors.New("replica: the directory ends before the head")
	ErrOtherHead = errors.New("replica: the directory leads to another head")
	ErrOtherLog  = errors.New("replica: the log leads to another head, an entry in it was changed or never committed")
)

type Run struct {
	Snapshot  uint64
	Committed uint64
	Links     uint64
}

func Recount(cfg Config, want audit.Head) ([]Run, error) {
	cfg.ID, cfg.Peers, cfg.SnapshotEvery = 1, []uint64{1}, math.MaxUint64
	dir := filepath.Join(cfg.Dir, "snap")
	if _, err := intact(cfg.FS, dir); err != nil {
		return nil, err
	}
	n, newest, err := recount(cfg, want, audit.Head{})
	if err != nil {
		return nil, err
	}
	first, _ := n.log.FirstIndex()
	sure, _ := n.sm.HeadAt(newest.Committed)
	if err := n.Close(); err != nil {
		return nil, err
	}
	if newest.Snapshot == 0 {
		return []Run{newest}, nil
	}
	if err := wal.RemoveSnapshot(cfg.FS, dir, wal.SnapshotMeta(n.snap)); err != nil {
		return nil, err
	}
	older, err := intact(cfg.FS, dir)
	if err != nil {
		return nil, err
	}
	if first == 0 || first > older.Index+1 {
		return []Run{newest}, nil
	}
	n, again, err := recount(cfg, want, sure)
	if err != nil {
		return nil, err
	}
	return []Run{newest, again}, n.Close()
}

func intact(fsys disk.FS, dir string) (wal.SnapshotMeta, error) {
	s, broken, err := wal.LoadSnapshot(fsys, dir, func(io.Reader) error { return nil })
	switch {
	case len(broken) > 0:
		return s, fmt.Errorf("replica: %w: snapshot at %d is broken", wal.ErrCorrupt, broken[0].Index)
	case errors.Is(err, wal.ErrNoSnapshot):
		return s, nil
	}
	return s, err
}

func recount(cfg Config, want, sure audit.Head) (*Node, Run, error) {
	n, err := Open(cfg)
	if err != nil {
		return nil, Run{}, err
	}
	run := Run{Snapshot: n.snap.Index, Committed: max(sure.Seq, n.sm.Head().Seq)}
	if err := n.count(4*cfg.ElectionTicks, want, sure, run.Committed); err != nil {
		return nil, Run{}, errors.Join(err, n.Close())
	}
	run.Links = n.sm.Head().Seq
	return n, run, nil
}

func (n *Node) count(rounds int, want, sure audit.Head, committed uint64) error {
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
	if got, ok := n.sm.HeadAt(sure.Seq); !ok || got != sure {
		return fmt.Errorf("%w: link %d from snapshot %d has head %x, the newer snapshot has %x", ErrOtherHead, sure.Seq, n.snap.Index, got.Hash[:4], sure.Hash[:4])
	}
	got, ok := n.sm.HeadAt(want.Seq)
	switch {
	case !ok:
		return fmt.Errorf("%w: %d links from snapshot %d, the head is link %d", ErrBehind, n.sm.Head().Seq, n.snap.Index, want.Seq)
	case got != want && want.Seq > committed:
		return fmt.Errorf("%w: link %d from snapshot %d has head %x, want %x, and the snapshots end at link %d", ErrOtherLog, want.Seq, n.snap.Index, got.Hash[:4], want.Hash[:4], committed)
	case got != want:
		return fmt.Errorf("%w: link %d from snapshot %d has head %x, want %x", ErrOtherHead, want.Seq, n.snap.Index, got.Hash[:4], want.Hash[:4])
	}
	return nil
}
