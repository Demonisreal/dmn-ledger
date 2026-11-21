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

	"github.com/Demonisreal/dmn-ledger/internal/audit"
	"github.com/Demonisreal/dmn-ledger/internal/client"
	"github.com/Demonisreal/dmn-ledger/internal/disk"
	"github.com/Demonisreal/dmn-ledger/internal/raft"
	"github.com/Demonisreal/dmn-ledger/internal/state"
	"github.com/Demonisreal/dmn-ledger/internal/u128"
	"github.com/Demonisreal/dmn-ledger/internal/wal"
)

const (
	applyBytes = 4 << 20
	readBudget = 8 << 20
	stampLimit = 1 << 62
)

type Config struct {
	ID            uint64
	Cluster       uint64
	Peers         []uint64
	FS            disk.FS
	Dir           string
	Create        bool
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

	recount bool
}

var ErrIdentity = errors.New("replica: the directory belongs to another node or cluster")

type Output struct {
	Messages []raft.Message
	Replies  []client.Reply
}

type Status struct {
	ID         uint64
	Role       raft.Role
	Term       uint64
	Vote       uint64
	Lead       uint64
	Commit     uint64
	Applied    uint64
	Last       uint64
	ApplyHash  [sha256.Size]byte
	Sessions   int
	Repair     bool
	Time       uint64
	NextExpiry uint64
	Head       audit.Head
}

type proposal struct {
	index uint64
	req   client.Request
}

type reqKey struct {
	client  u128.Uint128
	session uint64
	number  uint64
	op      client.Op
}

func keyOf(req client.Request) reqKey {
	return reqKey{client: req.Client, session: req.Session, number: req.Number, op: req.Op}
}

type readBatch struct {
	tag  uint64
	reqs []client.Request
}

type Node struct {
	cfg  Config
	raft *raft.Raft
	meta *wal.MetaFile
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
	proposed map[reqKey]bool
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
		proposed: make(map[reqKey]bool),
		incoming: make(map[raft.Snapshot]io.Reader),
	}
	var hs wal.Meta
	own := wal.Identity{Cluster: cfg.Cluster, Node: cfg.ID, Nodes: uint64(len(cfg.Peers))}
	if cfg.Create {
		n.meta, hs, err = wal.CreateMeta(cfg.FS, cfg.Dir, own)
	} else {
		n.meta, hs, err = wal.OpenMeta(cfg.FS, cfg.Dir)
	}
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			_ = n.Close()
		}
	}()
	held := n.meta.Identity()
	switch {
	case cfg.recount && held.Cluster != own.Cluster:
		return nil, fmt.Errorf("%w: %s holds %s, not cluster %d", ErrIdentity, cfg.Dir, held, own.Cluster)
	case !cfg.recount && held != own:
		return nil, fmt.Errorf("%w: %s holds %s, not %s", ErrIdentity, cfg.Dir, held, own)
	}

	base, broken, err := wal.LoadSnapshot(cfg.FS, n.snapDir(), n.load)
	switch {
	case errors.Is(err, wal.ErrNoSnapshot):
		n.sm = state.New(0, 0)
	case err != nil:
		return nil, err
	case n.applied != base.Index || n.appliedTerm != base.Term:
		return nil, fmt.Errorf("replica: snapshot file %d/%d holds %d/%d", base.Index, base.Term, n.applied, n.appliedTerm)
	}
	n.snap = raft.Snapshot(base)

	opts := wal.Options{SegmentSize: cfg.SegmentSize, Nonces: cfg.Nonces, Sealed: hs.Sealed, Strict: !n.meta.Intact()}
	n.log, err = wal.Open(cfg.FS, n.walDir(), opts)
	damage := err
	switch {
	case err == nil:
		damage = n.uncovered(hs.Sealed, broken)
	case !errors.Is(err, wal.ErrCorrupt):
		return nil, err
	}
	hs.Repair, err = n.mend(hs, damage, broken, opts)
	if err != nil {
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
		HardState:      raft.HardState{Term: hs.Term, Vote: hs.Vote, Repair: hs.Repair},
	})
	if err != nil {
		return nil, err
	}
	return n, nil
}

func (n *Node) snapDir() string {
	return filepath.Join(n.cfg.Dir, "snap")
}

func (n *Node) walDir() string {
	return filepath.Join(n.cfg.Dir, "wal")
}

func (n *Node) uncovered(sealed uint64, broken []wal.SnapshotMeta) error {
	first, _ := n.log.FirstIndex()
	last, _ := n.log.LastIndex()
	switch {
	case len(broken) > 0 && !n.meta.Intact():
		return fmt.Errorf("replica: %w: snapshot at %d is broken and the seal may be older than it", wal.ErrCorrupt, broken[0].Index)
	case sealed <= n.snap.Index, last >= sealed && first <= n.snap.Index+1:
		return nil
	}
	return fmt.Errorf("replica: %w: sealed up to %d, snapshot at %d, log from %d to %d", wal.ErrCorrupt, sealed, n.snap.Index, first, last)
}

func (n *Node) flag(hs wal.Meta, damage error) (bool, error) {
	switch {
	case damage == nil || hs.Repair:
		return hs.Repair, nil
	case len(n.cfg.Peers) == 1:
		return false, damage
	}
	return true, n.meta.Save(hs.Term, hs.Vote, true)
}

func (n *Node) discard(broken []wal.SnapshotMeta) error {
	for _, s := range broken {
		if err := wal.RemoveSnapshot(n.cfg.FS, n.snapDir(), s); err != nil {
			return err
		}
	}
	return nil
}

func (n *Node) salvage(opts wal.Options) error {
	if n.log != nil {
		return nil
	}
	opts.Salvage = true
	var err error
	n.log, err = wal.Open(n.cfg.FS, n.walDir(), opts)
	return err
}

func (n *Node) reconcile(repair bool) error {
	first, _ := n.log.FirstIndex()
	last, _ := n.log.LastIndex()
	s := n.snap
	switch {
	case last == 0:
		return nil
	case first > s.Index+1 && !repair:
		return fmt.Errorf("replica: log starts at %d, after snapshot %d", first, s.Index)
	case first > s.Index+1, last < s.Index:
		return n.empty()
	case first > s.Index:
		return nil
	}
	term, err := n.log.Term(s.Index)
	if err != nil {
		return err
	}
	if term != s.Term {
		return n.empty()
	}
	return nil
}

func (n *Node) empty() error {
	if err := n.lower(n.snap.Index); err != nil {
		return err
	}
	return n.log.TruncateAfter(0)
}

func (n *Node) lower(index uint64) error {
	if index >= n.meta.Sealed() {
		return nil
	}
	return n.meta.Seal(index)
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
	keep := ents[0].Index - 1
	if err := n.lower(keep); err != nil {
		return err
	}
	if err := n.log.TruncateAfter(keep); err != nil {
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
	clear(n.proposed)
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
	var reading int
	for _, b := range n.reads {
		for _, req := range b.reqs {
			reading += replyBound(req)
		}
	}
	for _, req := range n.inbox {
		switch {
		case n.leading == 0:
			n.refuse(req, client.NotLeader)
		case !wellFormed(req):
			n.refuse(req, client.Malformed)
		case req.Op.Lookup() && reading+replyBound(req) > readBudget:
			n.refuse(req, client.Busy)
		case req.Op.Lookup():
			n.lookups = append(n.lookups, req)
			reading += replyBound(req)
		case n.proposed[keyOf(req)]:
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
	n.proposed[keyOf(req)] = true
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
	var spent int
	for _, rd := range reads {
		if rd.Index > n.applied {
			return fmt.Errorf("%w: read %d confirmed at index %d, applied %d", raft.ErrSafety, rd.Tag, rd.Index, n.applied)
		}
		i := slices.IndexFunc(n.reads, func(b readBatch) bool { return b.tag == rd.Tag })
		if i < 0 {
			continue
		}
		for _, req := range n.reads[i].reqs {
			if spent >= readBudget {
				n.refuse(req, client.Busy)
				continue
			}
			r, work := n.lookup(req)
			n.replies = append(n.replies, r)
			spent += work
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
	next, _ := n.sm.NextExpiry()
	return Status{
		ID:         st.ID,
		Role:       st.Role,
		Term:       st.Term,
		Vote:       st.Vote,
		Lead:       st.Lead,
		Commit:     st.Commit,
		Applied:    n.applied,
		Last:       st.Last,
		ApplyHash:  n.hash,
		Sessions:   len(n.sessions),
		Repair:     st.Repair,
		Time:       n.sm.Last(),
		NextExpiry: next,
		Head:       n.sm.Head(),
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
	if n.meta != nil {
		errs = append(errs, n.meta.Close())
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
