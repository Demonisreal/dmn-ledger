package check

import (
	"github.com/Demonisreal/dmn-ledger/internal/disk"
	"github.com/Demonisreal/dmn-ledger/internal/raft"
)

const syncSample = 20

type receipt struct {
	vote, voteTerm uint64
	term, acked    uint64
}

type watchedFS struct {
	disk.FS
	w  *watch
	id uint64
}

type watchedFile struct {
	disk.File
	fs *watchedFS
}

func (w *watch) disk() func(uint64, disk.FS) disk.FS {
	lie := lyingDisk()
	return func(id uint64, fsys disk.FS) disk.FS {
		if lie != nil {
			fsys = lie(id, fsys)
		}
		return &watchedFS{FS: fsys, w: w, id: id}
	}
}

func (f *watchedFS) Create(name string) (disk.File, error) {
	file, err := f.FS.Create(name)
	if err != nil {
		return nil, err
	}
	return watchedFile{file, f}, nil
}

func (f *watchedFS) Open(name string) (disk.File, error) {
	file, err := f.FS.Open(name)
	if err != nil {
		return nil, err
	}
	return watchedFile{file, f}, nil
}

func (f *watchedFS) SyncDir(dir string) error {
	err := f.FS.SyncDir(dir)
	if err == nil {
		f.w.synced(f.id)
	}
	return err
}

func (f watchedFile) Sync() error {
	err := f.File.Sync()
	if err == nil {
		f.fs.w.synced(f.fs.id)
	}
	return err
}

func (w *watch) note(id uint64, m raft.Message) {
	r := w.receipts[id]
	switch {
	case m.Type == raft.MsgVote:
		r.vote, r.voteTerm = id, m.Term
	case m.Type == raft.MsgVoteResp && !m.Reject:
		r.vote, r.voteTerm = m.To, m.Term
	case m.Type == raft.MsgAppResp && !m.Reject && m.Index > 0:
		if m.Term != r.term {
			r.term, r.acked = m.Term, 0
		}
		r.acked = max(r.acked, m.Index)
	default:
		return
	}
	w.receipts[id] = r
}

func (w *watch) synced(id uint64) {
	if w.c == nil || w.stuck != nil || w.c.Node(id) == nil {
		return
	}
	w.syncs++
	if w.syncs%w.sample != 0 {
		return
	}
	d := w.c.World().Disk(id).Clone(w.rng)
	d.WriteErrRate, d.SyncErrRate, d.MetaErrRate, d.ReadErrRate = 0, 0, 0, 0
	d.Crash()
	n, err := w.c.Open(id, d)
	if err != nil {
		w.stuck = w.fail("S5", id, "a power loss in the middle of a round leaves a disk that does not open: %v", err)
		return
	}
	st := n.Status()
	if err := n.Close(); err != nil {
		w.stuck = err
		return
	}
	r := w.receipts[id]
	switch {
	case st.Term < max(r.term, r.voteTerm):
		w.stuck = w.fail("S5", id, "promised in term %d, a power loss in the middle of a round leaves term %d", max(r.term, r.voteTerm), st.Term)
	case st.Term == r.voteTerm && st.Vote != r.vote:
		w.stuck = w.fail("S5", id, "voted for n%d in term %d, a power loss in the middle of a round leaves a vote for n%d", r.vote, r.voteTerm, st.Vote)
	case st.Term == r.term && st.Last < r.acked:
		w.stuck = w.fail("S5", id, "acknowledged up to %d in term %d, a power loss in the middle of a round leaves a log that ends at %d", r.acked, r.term, st.Last)
	}
}
