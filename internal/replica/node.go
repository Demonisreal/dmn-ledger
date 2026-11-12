package replica

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"io"
	"math/rand/v2"
	"path/filepath"
	"slices"

	"github.com/Demonisreal/dmn-ledger/internal/client"
	"github.com/Demonisreal/dmn-ledger/internal/disk"
	"github.com/Demonisreal/dmn-ledger/internal/raft"
	"github.com/Demonisreal/dmn-ledger/internal/state"
	"github.com/Demonisreal/dmn-ledger/internal/u128"
	"github.com/Demonisreal/dmn-ledger/internal/wal"
)

const (
	applyBytes = 4 << 20
	stampLimit = 1 << 62
)

type Config struct {
	ID            uint64
	Peers         []uint64
	FS            disk.FS
	Dir           string
	Clock         func() uint64
	Rand          *rand.Rand
	Nonces        io.Reader
	MaxSessions   int
	SnapshotEvery uint64
	SegmentSize   int64
	ClockStep     uint64

	ElectionTicks  int
	HeartbeatTicks int
	SnapshotTicks  int
	MaxAppendBytes int64
	MaxUncommitted uint64
}

type Output struct {
	Messages []raft.Message
	Replies  []client.Reply
}

type Status struct {
	ID        uint64
	Role      raft.Role
	Term      uint64
	Vote      uint64
	Lead      uint64
	Commit    uint64
	Applied   uint64
	Last      uint64
	ApplyHash [sha256.Size]byte
	Sessions  int
}

type proposal struct {
	index uint64
	req   client.Request
}

type readBatch struct {
	tag  uint64
	reqs []client.Request
}

type Node struct {
	cfg  Config
	raft *raft.Raft
	hs   *wal.HardStateFile
	log  *wal.WAL
	snap raft.Snapshot

	sm          *state.Machine
	sessions    map[u128.Uint128]*session
	applied     uint64
	appliedTerm uint64
	hash        [sha256.Size]byte
	hasher      hash.Hash
	fails       []state.Failure

	ticks      uint64
	leading    uint64
	lead       uint64
	anchor     uint64
	anchorTick uint64

	inbox    []client.Request
	waiting  []proposal
	lookups  []client.Request
	reads    []readBatch
	tag      uint64
	incoming map[raft.Snapshot]io.Reader

	msgs    []raft.Message
	replies []client.Reply
	err     error
}

func Open(cfg Config) (_ *Node, err error) {
	switch {
	case cfg.MaxSessions < 1:
		return nil, errors.New("replica: MaxSessions must be at least 1")
	case cfg.SnapshotEvery == 0 || cfg.ClockStep == 0:
		return nil, errors.New("replica: SnapshotEvery and ClockStep must be positive")
	}
	n := &Node{
		cfg:      cfg,
		sessions: make(map[u128.Uint128]*session),
		hasher:   sha256.New(),
		incoming: make(map[raft.Snapshot]io.Reader),
	}
	var hs wal.HardState
	n.hs, hs, err = wal.OpenHardState(cfg.FS, cfg.Dir)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			_ = n.Close()
		}
	}()

	meta, _, err := wal.LoadSnapshot(cfg.FS, n.snapDir(), n.load)
	switch {
	case errors.Is(err, wal.ErrNoSnapshot):
		n.sm = state.New(0, 0)
	case err != nil:
		return nil, err
	case n.applied != meta.Index || n.appliedTerm != meta.Term:
		return nil, fmt.Errorf("replica: snapshot file %d/%d holds %d/%d", meta.Index, meta.Term, n.applied, n.appliedTerm)
	}
	n.snap = raft.Snapshot(meta)

	n.log, err = wal.Open(cfg.FS, filepath.Join(cfg.Dir, "wal"), wal.Options{SegmentSize: cfg.SegmentSize, Nonces: cfg.Nonces})
	if err != nil {
		return nil, err
	}
	if err := n.reconcile(); err != nil {
		return nil, err
	}
	n.raft, err = raft.New(raft.Config{
		ID:             cfg.ID,
		Peers:          cfg.Peers,
		ElectionTicks:  cfg.ElectionTicks,
		HeartbeatTicks: cfg.HeartbeatTicks,
		SnapshotTicks:  cfg.SnapshotTicks,
		MaxAppendBytes: cfg.MaxAppendBytes,
		MaxUncommitted: cfg.MaxUncommitted,
		Rand:           cfg.Rand,
		Log:            logView{n},
		HardState:      raft.HardState(hs),
	})
	if err != nil {
		return nil, err
	}
	return n, nil
}

func (n *Node) snapDir() string {
	return filepath.Join(n.cfg.Dir, "snap")
}

func (n *Node) reconcile() error {
	first, err := n.log.FirstIndex()
	if err != nil {
		return err
	}
	last, err := n.log.LastIndex()
	if err != nil {
		return err
	}
	s := n.snap
	switch {
	case last == 0:
		return nil
	case first > s.Index+1:
		return fmt.Errorf("replica: log starts at %d, after snapshot %d", first, s.Index)
	case last < s.Index:
		return n.log.TruncateAfter(0)
	case first > s.Index:
		return nil
	}
	term, err := n.log.Term(s.Index)
	if err != nil {
		return err
	}
	if term != s.Term {
		return n.log.TruncateAfter(0)
	}
	return nil
}

func (n *Node) Tick() {
	n.ticks++
	if n.err == nil {
		n.err = n.raft.Tick()
	}
}

func (n *Node) Receive(m raft.Message, snap io.Reader) {
	if n.err != nil {
		return
	}
	if m.Type == raft.MsgSnap && snap != nil {
		key := raft.Snapshot{Index: m.Index, Term: m.LogTerm}
		if _, ok := n.incoming[key]; !ok {
			n.incoming[key] = snap
		}
	}
	n.err = n.raft.Step(m)
}

func (n *Node) Request(req client.Request) {
	n.inbox = append(n.inbox, req)
}

func (n *Node) Process() (Output, error) {
	if n.err == nil {
		n.err = n.process()
	}
	clear(n.incoming)
	if n.err != nil {
		return Output{}, n.err
	}
	out := Output{Messages: n.msgs, Replies: n.replies}
	n.msgs, n.replies = nil, nil
	return out, nil
}

func (n *Node) process() error {
	n.follow()
	if err := n.accept(); err != nil {
		return err
	}
	if err := n.readIndex(); err != nil {
		return err
	}
	for n.raft.HasReady() {
		if err := n.round(); err != nil {
			return err
		}
	}
	if n.applied-n.snap.Index >= n.cfg.SnapshotEvery {
		return n.snapshot()
	}
	return nil
}

func (n *Node) round() error {
	rd := n.raft.Ready()
	if err := n.persist(rd); err != nil {
		return err
	}
	n.msgs = append(n.msgs, rd.Messages...)
	if err := n.applyTo(rd.Commit); err != nil {
		return err
	}
	if err := n.confirm(rd.Reads); err != nil {
		return err
	}
	n.raft.Advance()
	n.follow()
	return nil
}

func (n *Node) store(ents []raft.Entry) error {
	if err := n.log.TruncateAfter(ents[0].Index - 1); err != nil {
		return err
	}
	batch := make([]wal.Entry, len(ents))
	for i, e := range ents {
		batch[i] = wal.Entry{Index: e.Index, Term: e.Term, Kind: wal.Kind(e.Kind), Body: e.Body}
	}
	return n.log.Append(batch...)
}

func (n *Node) applyTo(commit uint64) error {
	for n.applied < commit {
		ents, err := n.log.Entries(n.applied+1, commit+1, applyBytes)
		if err != nil {
			return err
		}
		for _, e := range ents {
			n.apply(e)
		}
	}
	return nil
}

func (n *Node) follow() {
	st := n.raft.Status()
	n.lead = st.Lead
	if st.Role == raft.Leader && st.Term == n.leading {
		return
	}
	for _, p := range n.waiting {
		n.refuse(p.req, client.NotLeader)
	}
	n.waiting = nil
	for _, b := range n.reads {
		for _, req := range b.reqs {
			n.refuse(req, client.NotLeader)
		}
	}
	n.reads = nil
	n.leading = 0
	if st.Role == raft.Leader {
		n.leading = st.Term
	}
}

func (n *Node) accept() error {
	for _, req := range n.inbox {
		switch {
		case n.leading == 0:
			n.refuse(req, client.NotLeader)
		case !wellFormed(req):
			n.refuse(req, client.Malformed)
		case req.Op.Lookup():
			n.lookups = append(n.lookups, req)
		default:
			if err := n.propose(req); err != nil {
				return err
			}
		}
	}
	clear(n.inbox)
	n.inbox = n.inbox[:0]
	return nil
}

func (n *Node) propose(req client.Request) error {
	kind, body := kindRegister, req.Client.AppendBytes(nil)
	if req.Op != client.Register {
		if n.appliedTerm != n.leading {
			n.refuse(req, client.Busy)
			return nil
		}
		ts := n.stamp()
		if ts >= stampLimit {
			n.refuse(req, client.BadClock)
			return nil
		}
		kind, body = kindRequest, appendRequest(req, ts)
	}
	index, err := n.raft.Propose(kind, body)
	switch {
	case errors.Is(err, raft.ErrBusy):
		n.refuse(req, client.Busy)
		return nil
	case err != nil:
		return err
	}
	req.Body = nil
	n.waiting = append(n.waiting, proposal{index: index, req: req})
	return nil
}

func (n *Node) stamp() uint64 {
	clock := n.cfg.Clock()
	if n.anchor == 0 {
		if clock < stampLimit {
			n.anchor, n.anchorTick = clock, n.ticks
		}
		return clock
	}
	trusted := n.anchor + (n.ticks-n.anchorTick)*n.cfg.ClockStep
	if clock > trusted+n.cfg.ClockStep {
		return trusted + n.cfg.ClockStep
	}
	n.anchor, n.anchorTick = min(max(clock, n.sm.Last()), trusted), n.ticks
	return clock
}

func (n *Node) readIndex() error {
	if len(n.lookups) == 0 {
		return nil
	}
	reqs := n.lookups
	n.lookups = nil
	n.tag++
	err := n.raft.ReadIndex(n.tag)
	switch {
	case errors.Is(err, raft.ErrNotReady):
		for _, req := range reqs {
			n.refuse(req, client.Busy)
		}
		return nil
	case err != nil:
		return err
	}
	n.reads = append(n.reads, readBatch{tag: n.tag, reqs: reqs})
	return nil
}

func (n *Node) confirm(reads []raft.Read) error {
	for _, rd := range reads {
		if rd.Index > n.applied {
			return fmt.Errorf("%w: read %d confirmed at index %d, applied %d", raft.ErrSafety, rd.Tag, rd.Index, n.applied)
		}
		i := slices.IndexFunc(n.reads, func(b readBatch) bool { return b.tag == rd.Tag })
		if i < 0 {
			continue
		}
		for _, req := range n.reads[i].reqs {
			n.replies = append(n.replies, n.lookup(req))
		}
		n.reads = slices.Delete(n.reads, i, i+1)
	}
	return nil
}

func (n *Node) refuse(req client.Request, st client.Status) {
	n.replies = append(n.replies, client.Reply{Client: req.Client, Number: req.Number, Op: req.Op, Status: st, Leader: n.lead})
}

func (n *Node) Status() Status {
	st := n.raft.Status()
	return Status{
		ID:        st.ID,
		Role:      st.Role,
		Term:      st.Term,
		Vote:      st.Vote,
		Lead:      st.Lead,
		Commit:    st.Commit,
		Applied:   n.applied,
		Last:      st.Last,
		ApplyHash: n.hash,
		Sessions:  len(n.sessions),
	}
}

func (n *Node) EntryTerm(i uint64) (uint64, error) {
	return logView{n}.Term(i)
}

func (n *Node) CheckInvariants() error {
	return n.sm.CheckInvariants()
}

func (n *Node) LedgerDigest() [sha256.Size]byte {
	return n.sm.Digest()
}

func (n *Node) Session(id u128.Uint128) (uint64, bool) {
	s, ok := n.sessions[id]
	if !ok {
		return 0, false
	}
	return s.since, true
}

func (n *Node) Close() error {
	var errs []error
	if n.log != nil {
		errs = append(errs, n.log.Close())
	}
	if n.hs != nil {
		errs = append(errs, n.hs.Close())
	}
	return errors.Join(errs...)
}

type logView struct {
	n *Node
}

func (v logView) Snapshot() raft.Snapshot {
	return v.n.snap
}

func (v logView) FirstIndex() uint64 {
	if last, _ := v.n.log.LastIndex(); last == 0 {
		return v.n.snap.Index + 1
	}
	first, _ := v.n.log.FirstIndex()
	return first
}

func (v logView) LastIndex() uint64 {
	last, _ := v.n.log.LastIndex()
	return max(last, v.n.snap.Index)
}

func (v logView) Term(i uint64) (uint64, error) {
	if i == v.n.snap.Index {
		return v.n.snap.Term, nil
	}
	return v.n.log.Term(i)
}

func (v logView) Entries(lo, hi uint64, maxBytes int64) ([]raft.Entry, error) {
	ents, err := v.n.log.Entries(lo, hi, maxBytes)
	if err != nil {
		return nil, err
	}
	out := make([]raft.Entry, len(ents))
	for i, e := range ents {
		out[i] = raft.Entry{Index: e.Index, Term: e.Term, Kind: uint8(e.Kind), Body: e.Body}
	}
	return out, nil
}
