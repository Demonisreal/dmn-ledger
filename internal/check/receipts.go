package check

import (
	"github.com/Demonisreal/dmn-ledger/internal/disk"
	"github.com/Demonisreal/dmn-ledger/internal/raft"
	"github.com/Demonisreal/dmn-ledger/internal/replica"
)

const syncSample = 20

type receipt struct {
	vote, voteTerm uint64
	term, reach    uint64
	fed            uint64
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
	if m.Type == raft.MsgSnap || m.Type == raft.MsgApp && len(m.Entries) > 0 {
		to := w.receipts[m.To]
		to.fed = max(to.fed, m.Term)
		w.receipts[m.To] = to
	}
	r := w.receipts[id]
	switch {
	case m.Type == raft.MsgVote:
		r.vote, r.voteTerm = id, m.Term
	case m.Type == raft.MsgVoteResp && !m.Reject:
		r.vote, r.voteTerm = m.To, m.Term
	case m.Type == raft.MsgAppResp && !m.Reject && m.Index > 0:
		if m.Term != r.term {
			r.term, r.reach = m.Term, 0
		}
		r.reach = max(r.reach, m.Index)
	case m.Type == raft.MsgApp && len(m.Entries) > 0:
		if m.Term != r.term {
			r.term, r.reach = m.Term, 0
		}
		r.reach = max(r.reach, m.Entries[len(m.Entries)-1].Index)
	default:
		return
	}
	w.receipts[id] = r
}

func (w *watch) synced(id uint64) {
	if w.c == nil || w.stuck != nil {
		return
	}
	w.syncs++
	if w.syncs%w.sample != 0 {
		return
	}
	d := w.c.World().Disk(id).Clone(w.rng)
	d.WriteErrRate, d.SyncErrRate, d.MetaErrRate, d.ReadErrRate = 0, 0, 0, 0
	d.Crash()
	when := "a power loss in the middle of a round"
	if w.c.Node(id) == nil {
		when = "a power loss during the start"
	}
	n, err := w.c.Open(id, d)
	switch {
	case err != nil && w.unreadable(id, err):
		return
	case err != nil:
		w.stuck = w.fail("S5", id, "%s leaves a disk that does not open: %v", when, err)
		return
	}
	st := n.Status()
	if err := n.Close(); err != nil {
		w.stuck = err
		return
	}
	w.stuck = w.owes(id, st, when)
	if w.stuck == nil && w.groundless(id, st) {
		w.stuck = w.fail("F2", id, "%s leaves the node in repair, the plan never damaged its disk", when)
	}
}

func (w *watch) owes(id uint64, st replica.Status, when string) error {
	r := w.receipts[id]
	switch {
	case st.Term < max(r.term, r.voteTerm):
		return w.fail("S5", id, "promised in term %d, %s leaves term %d", max(r.term, r.voteTerm), when, st.Term)
	case st.Term == r.voteTerm && st.Vote != r.vote:
		return w.fail("S5", id, "voted for n%d in term %d, %s leaves a vote for n%d", r.vote, r.voteTerm, when, st.Vote)
	case (st.Term == r.term || r.fed <= r.term) && st.Last < r.reach && !st.Repair:
		return w.fail("S5", id, "promised its log up to %d in term %d, %s leaves a log that ends at %d and no repair flag", r.reach, r.term, when, st.Last)
	}
	return nil
}
