package check

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"syscall"

	"github.com/Demonisreal/dmn-ledger/internal/disk"
	"github.com/Demonisreal/dmn-ledger/internal/raft"
	"github.com/Demonisreal/dmn-ledger/internal/replica"
	"github.com/Demonisreal/dmn-ledger/internal/sim"
	"github.com/Demonisreal/dmn-ledger/internal/wal"
)

const (
	promiseSample  = 50
	invariantEvery = 64
	answerBound    = 2_000_000_000
	eventBudget    = 500_000
)

type Config struct {
	Seed    uint64
	Profile sim.Profile
	Nodes   uint64
	Trace   io.Writer
}

type Report struct {
	Calls   int
	Windows int
	Widest  int
	Trace   [sha256.Size]byte
}

type Violation struct {
	Seed   uint64
	At     uint64
	Check  string
	Node   uint64
	Reason string
}

func (v *Violation) Error() string {
	if v.Node == 0 {
		return fmt.Sprintf("seed %d at %d: %s: %s", v.Seed, v.At, v.Check, v.Reason)
	}
	return fmt.Sprintf("seed %d at %d: %s on n%d: %s", v.Seed, v.At, v.Check, v.Node, v.Reason)
}

type watch struct {
	cfg       Config
	c         *sim.Cluster
	over      bool
	rng       *rand.Rand
	leaders   map[uint64]uint64
	votes     map[[2]uint64]uint64
	hashes    map[uint64][sha256.Size]byte
	terms     []uint64
	commits   map[*replica.Node]uint64
	committed map[uint64]uint64
	entries   map[[2]uint64]raft.Entry
	checked   map[*replica.Node]uint64
	receipts  map[uint64]receipt
	promises  int
	sample    int
	syncs     int
	stuck     error
	events    int
}

func Run(cfg Config) (Report, error) {
	w := newWatch(cfg)
	return w.run(w.disk())
}

func newWatch(cfg Config) *watch {
	return &watch{
		cfg:       cfg,
		rng:       rand.New(rand.NewPCG(cfg.Seed, 2)), //nolint:gosec // G404: seeded on purpose, a run must replay
		leaders:   make(map[uint64]uint64),
		votes:     make(map[[2]uint64]uint64),
		hashes:    make(map[uint64][sha256.Size]byte),
		commits:   make(map[*replica.Node]uint64),
		committed: make(map[uint64]uint64),
		entries:   make(map[[2]uint64]raft.Entry),
		checked:   make(map[*replica.Node]uint64),
		receipts:  make(map[uint64]receipt),
		sample:    syncSample,
	}
}

func (w *watch) run(fsys func(uint64, disk.FS) disk.FS) (Report, error) {
	cc := sim.ClusterConfig{
		Seed:    w.cfg.Seed,
		Profile: w.cfg.Profile,
		Nodes:   w.cfg.Nodes,
		Trace:   w.cfg.Trace,
		Disk:    fsys,
		Said:    w.said,
		Check:   w.check,
		Failed:  w.failed,
	}
	c, err := sim.NewCluster(cc)
	if err != nil {
		return Report{}, err
	}
	w.c = c
	w.terms = make([]uint64, c.Nodes())
	err = c.Run()
	if err == nil {
		w.over = true
		err = w.end()
	}
	rep := report(c)
	if v, ok := errors.AsType[*Violation](err); ok {
		return rep, v
	}
	return rep, err
}

func report(c *sim.Cluster) Report {
	rep := Report{Calls: len(c.Calls()), Windows: len(c.Windows()) + 1, Trace: c.World().TraceHash()}
	lo := 0
	for _, hi := range append(c.Windows(), len(c.Calls())) {
		rep.Widest = max(rep.Widest, hi-lo)
		lo = hi
	}
	return rep
}

func (w *watch) fail(check string, node uint64, format string, args ...any) error {
	v := &Violation{Seed: w.cfg.Seed, Check: check, Node: node, Reason: fmt.Sprintf(format, args...)}
	switch {
	case w.over:
		v.At = w.c.World().End()
	case w.c != nil:
		v.At = w.c.World().Now()
	}
	return v
}

func (w *watch) failed(id uint64, err error) error {
	if errors.Is(err, syscall.EIO) {
		return nil
	}
	return w.fail("F1", id, "stopped without a disk error: %v", err)
}

func (w *watch) said(id uint64, msgs []raft.Message) error {
	for _, m := range msgs {
		if err := w.logged(id, m.Entries); err != nil {
			return err
		}
		w.note(id, m)
		promise := false
		switch {
		case m.Type == raft.MsgApp || m.Type == raft.MsgSnap:
			if lead, ok := w.leaders[m.Term]; ok && lead != id {
				return w.fail("S1", id, "n%d already led term %d", lead, m.Term)
			}
			w.leaders[m.Term] = id
		case m.Type == raft.MsgVoteResp && !m.Reject:
			key := [2]uint64{id, m.Term}
			if cand, ok := w.votes[key]; ok && cand != m.To {
				return w.fail("S2", id, "voted for n%d and n%d in term %d", cand, m.To, m.Term)
			}
			w.votes[key] = m.To
			promise = true
		case m.Type == raft.MsgVote, m.Type == raft.MsgAppResp && !m.Reject && m.Index > 0:
			promise = true
		}
		if !promise {
			continue
		}
		w.promises++
		if w.promises%promiseSample != 0 {
			continue
		}
		if err := w.keeps(id, m); err != nil {
			return err
		}
	}
	return nil
}

func (w *watch) keeps(id uint64, m raft.Message) error {
	d := w.c.World().Disk(id).Clone(w.rng)
	d.WriteErrRate, d.SyncErrRate, d.MetaErrRate, d.ReadErrRate = 0, 0, 0, 0
	d.Crash()
	n, err := w.c.Open(id, d)
	if err != nil {
		return w.fail("S5", id, "after %s to n%d a power loss leaves a disk that does not open: %v", m.Type, m.To, err)
	}
	st := n.Status()
	if err := n.Close(); err != nil {
		return err
	}
	switch {
	case st.Term != m.Term:
		return w.fail("S5", id, "sent %s in term %d, after a power loss the disk holds term %d", m.Type, m.Term, st.Term)
	case m.Type == raft.MsgVote && st.Vote != id:
		return w.fail("S5", id, "asked for votes in term %d, after a power loss the disk holds a vote for n%d", m.Term, st.Vote)
	case m.Type == raft.MsgVoteResp && st.Vote != m.To:
		return w.fail("S5", id, "granted n%d its vote in term %d, after a power loss the disk holds a vote for n%d", m.To, m.Term, st.Vote)
	case m.Type == raft.MsgAppResp && st.Last < m.Index:
		return w.fail("S5", id, "acknowledged up to %d in term %d, after a power loss the disk ends at %d", m.Index, m.Term, st.Last)
	}
	return nil
}

func (w *watch) check(c *sim.Cluster) error {
	if w.stuck != nil {
		return w.stuck
	}
	w.events++
	if w.events > eventBudget {
		return w.fail("L3", 0, "still running after %d events, messages pile up without progress", eventBudget)
	}
	for id := uint64(1); id <= c.Nodes(); id++ {
		n := c.Node(id)
		if n == nil {
			continue
		}
		st := n.Status()
		if h, ok := w.hashes[st.Applied]; !ok {
			w.hashes[st.Applied] = st.ApplyHash
		} else if h != st.ApplyHash {
			return w.fail("S3", id, "applied %d with hash %x, seen %x", st.Applied, st.ApplyHash[:4], h[:4])
		}
		if st.Term < w.terms[id-1] {
			return w.fail("S4", id, "term %d after %d", st.Term, w.terms[id-1])
		}
		w.terms[id-1] = st.Term
		if st.Commit < w.commits[n] {
			return w.fail("S4", id, "commit %d after %d", st.Commit, w.commits[n])
		}
		if err := w.sameTerms(id, n, w.commits[n]+1, st.Commit); err != nil {
			return err
		}
		w.commits[n] = st.Commit
		if last, ok := w.checked[n]; !ok || st.Applied >= last+invariantEvery {
			if err := n.CheckInvariants(); err != nil {
				return w.fail("S6", id, "at %d: %v", st.Applied, err)
			}
			w.checked[n] = st.Applied
		}
	}
	return nil
}

func (w *watch) sameTerms(id uint64, n *replica.Node, from, to uint64) error {
	for i := from; i <= to; i++ {
		term, err := n.EntryTerm(i)
		if errors.Is(err, wal.ErrCompacted) || errors.Is(err, wal.ErrUnavailable) {
			continue
		}
		if err != nil {
			return nil
		}
		if was, seen := w.committed[i]; seen && was != term {
			return w.fail("S3", id, "committed %d in term %d, an earlier commit holds term %d", i, term, was)
		}
		w.committed[i] = term
	}
	return nil
}
