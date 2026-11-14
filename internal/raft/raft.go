package raft

import (
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
)

const maxPeers = 5

type progress struct {
	match     uint64
	next      uint64
	seq       uint64
	inflight  bool
	active    bool
	snapping  bool
	snapIndex uint64
	snapTicks int
}

type pendingRead struct {
	tag   uint64
	index uint64
	seq   uint64
}

type Raft struct {
	id    uint64
	peers []uint64
	self  int

	electionTicks  int
	heartbeatTicks int
	snapshotTicks  int
	maxAppendBytes int64
	maxUncommitted uint64
	rand           *rand.Rand

	log  Log
	snap *Snapshot
	ents []Entry

	term      uint64
	vote      uint64
	role      Role
	lead      uint64
	commit    uint64
	termStart uint64

	prs   []progress
	votes []bool

	elapsed    int
	timeout    int
	sinceLead  int
	beat       int
	checkTicks int

	seq     uint64
	pending []pendingRead
	reads   []Read

	msgs        []Message
	hs          HardState
	readyCommit uint64

	repair     bool
	nonce      uint64
	probeTicks int
	goalTerm   uint64
	goal       uint64
	acked      uint64
}

func New(cfg Config) (*Raft, error) {
	switch n := len(cfg.Peers); {
	case n != 1 && n != 3 && n != maxPeers:
		return nil, fmt.Errorf("raft: %d peers, want 1, 3 or 5", n)
	case cfg.Peers[0] == 0 || !ascending(cfg.Peers):
		return nil, fmt.Errorf("raft: peers %v must be non-zero and ascending", cfg.Peers)
	case !slices.Contains(cfg.Peers, cfg.ID):
		return nil, fmt.Errorf("raft: id %d not in peers %v", cfg.ID, cfg.Peers)
	case cfg.HeartbeatTicks < 1 || cfg.ElectionTicks <= cfg.HeartbeatTicks || cfg.SnapshotTicks < 1:
		return nil, errors.New("raft: need 0 < HeartbeatTicks < ElectionTicks and SnapshotTicks > 0")
	case cfg.MaxAppendBytes < 0:
		return nil, errors.New("raft: negative MaxAppendBytes")
	case cfg.MaxUncommitted == 0 || cfg.Rand == nil || cfg.Log == nil:
		return nil, errors.New("raft: MaxUncommitted, Rand and Log are required")
	}
	r := &Raft{
		id:             cfg.ID,
		peers:          slices.Clone(cfg.Peers),
		self:           slices.Index(cfg.Peers, cfg.ID),
		electionTicks:  cfg.ElectionTicks,
		heartbeatTicks: cfg.HeartbeatTicks,
		snapshotTicks:  cfg.SnapshotTicks,
		maxAppendBytes: cfg.MaxAppendBytes,
		maxUncommitted: cfg.MaxUncommitted,
		rand:           cfg.Rand,
		log:            cfg.Log,
		term:           cfg.HardState.Term,
		vote:           cfg.HardState.Vote,
		hs:             cfg.HardState,
		prs:            make([]progress, len(cfg.Peers)),
		votes:          make([]bool, len(cfg.Peers)),
		sinceLead:      cfg.ElectionTicks,
		repair:         cfg.HardState.Repair,
	}
	if r.repair {
		r.nonce = r.rand.Uint64()
	}
	lastTerm, err := r.lastTerm()
	if err != nil {
		return nil, err
	}
	if r.term < lastTerm {
		return nil, fmt.Errorf("raft: hard state term %d below last entry term %d", r.term, lastTerm)
	}
	r.commit = r.log.Snapshot().Index
	r.readyCommit = r.commit
	r.resetTimer()
	return r, nil
}

func ascending(ids []uint64) bool {
	for i := 1; i < len(ids); i++ {
		if ids[i] <= ids[i-1] {
			return false
		}
	}
	return true
}

func (r *Raft) Tick() error {
	r.sinceLead++
	if r.role != Leader {
		r.elapsed++
		if r.repair {
			r.probe()
		}
		if r.elapsed >= r.timeout && r.voter() {
			return r.preVote()
		}
		return nil
	}
	r.checkTicks++
	if r.checkTicks >= r.electionTicks {
		r.checkTicks = 0
		if !r.checkQuorum() {
			r.becomeFollower(r.term, 0)
			return nil
		}
	}
	for i := range r.prs {
		p := &r.prs[i]
		if p.snapping {
			p.snapTicks++
			p.snapping = p.snapTicks < r.snapshotTicks
		}
	}
	r.beat++
	if r.beat < r.heartbeatTicks {
		return nil
	}
	r.beat = 0
	for i := range r.prs {
		r.prs[i].inflight = false
	}
	return r.broadcast(true)
}

func (r *Raft) Step(m Message) error {
	if !r.wellFormed(m) {
		return nil
	}
	switch {
	case m.Term > r.term:
		preVote := m.Type == MsgPreVote || (m.Type == MsgPreVoteResp && !m.Reject)
		if !preVote {
			var lead uint64
			if m.Type == MsgApp || m.Type == MsgSnap {
				lead = m.From
			}
			r.becomeFollower(m.Term, lead)
		}
	case m.Term < r.term:
		switch m.Type {
		case MsgApp, MsgSnap:
			r.send(Message{Type: MsgAppResp, To: m.From, Term: r.term, Reject: true})
		case MsgVote:
			r.send(Message{Type: MsgVoteResp, To: m.From, Term: r.term, Reject: true})
		case MsgPreVote:
			r.send(Message{Type: MsgPreVoteResp, To: m.From, Term: r.term, Reject: true})
		}
		return nil
	}
	switch m.Type {
	case MsgPreVote:
		return r.handlePreVote(m)
	case MsgPreVoteResp:
		if r.role == PreCandidate && !m.Reject && m.Term == r.term+1 && r.poll(m.From) {
			return r.campaign()
		}
	case MsgVote:
		return r.handleVote(m)
	case MsgVoteResp:
		if r.role == Candidate && !m.Reject && r.poll(m.From) {
			return r.becomeLeader()
		}
	case MsgApp, MsgSnap:
		if r.role == Leader || (r.lead != 0 && r.lead != m.From) {
			return fmt.Errorf("%w: second leader %d in term %d", ErrSafety, m.From, r.term)
		}
		if r.role != Follower {
			r.becomeFollower(r.term, m.From)
		}
		r.lead, r.sinceLead = m.From, 0
		r.resetTimer()
		if m.Type == MsgSnap {
			return r.handleSnap(m)
		}
		return r.handleApp(m)
	case MsgAppResp:
		if r.role == Leader {
			return r.handleAppResp(m)
		}
	case MsgRepairProbe:
		if r.role == Leader {
			r.send(Message{Type: MsgRepairInfo, To: m.From, Term: r.term, Index: r.repairGoal(), Seq: m.Seq})
		}
	case MsgRepairInfo:
		if r.repair && r.goalTerm != r.term && m.Seq == r.nonce {
			r.goalTerm, r.goal = r.term, m.Index
		}
	}
	return nil
}

func (r *Raft) wellFormed(m Message) bool {
	switch {
	case m.To != r.id, m.From == r.id, !slices.Contains(r.peers, m.From):
		return false
	case m.Type < MsgPreVote, m.Type > MsgRepairInfo:
		return false
	case m.Term == 0, m.Term == math.MaxUint64, m.LogTerm > m.Term:
		return false
	case m.Index == 0 && m.LogTerm != 0:
		return false
	}
	t := m.LogTerm
	for k, e := range m.Entries {
		if e.Index == 0 || e.Index != m.Index+1+uint64(k) || e.Term < t || e.Term > m.Term {
			return false
		}
		t = e.Term
	}
	return true
}

func (r *Raft) Propose(kind uint8, body []byte) (uint64, error) {
	switch {
	case r.role != Leader:
		return 0, ErrNotLeader
	case kind == 0:
		return 0, ErrKind
	case r.lastIndex()-r.commit >= r.maxUncommitted:
		return 0, ErrBusy
	}
	i := r.lastIndex() + 1
	r.ents = append(r.ents, Entry{Index: i, Term: r.term, Kind: kind, Body: body})
	return i, r.broadcast(false)
}

func (r *Raft) ReadIndex(tag uint64) error {
	switch {
	case r.role != Leader:
		return ErrNotLeader
	case r.commit < r.termStart:
		return ErrNotReady
	}
	r.seq++
	r.prs[r.self].seq = r.seq
	r.pending = append(r.pending, pendingRead{tag: tag, index: r.commit, seq: r.seq})
	r.releaseReads()
	return r.broadcast(true)
}

func (r *Raft) HasReady() bool {
	return r.hs != r.hardState() || r.snap != nil || len(r.ents) > 0 ||
		len(r.msgs) > 0 || r.commit != r.readyCommit || len(r.reads) > 0
}

func (r *Raft) Ready() Ready {
	rd := Ready{Snapshot: r.snap, Entries: r.ents, Messages: r.msgs, Commit: r.commit, Reads: r.reads}
	if hs := r.hardState(); hs != r.hs {
		rd.HardState = &hs
	}
	return rd
}

func (r *Raft) Advance() {
	r.hs = r.hardState()
	r.snap, r.ents, r.msgs, r.reads = nil, nil, nil, nil
	r.readyCommit = r.commit
	r.heal()
	if r.role == Leader {
		r.prs[r.self].match = r.lastIndex()
		r.maybeCommit()
	}
}

func (r *Raft) Status() Status {
	return Status{
		ID:     r.id,
		Role:   r.role,
		Term:   r.term,
		Vote:   r.vote,
		Lead:   r.lead,
		Commit: r.commit,
		Last:   r.lastIndex(),
		Repair: r.repair,
	}
}

func (r *Raft) hardState() HardState {
	return HardState{Term: r.term, Vote: r.vote, Repair: r.repair}
}

func (r *Raft) probe() {
	r.probeTicks++
	if r.probeTicks >= r.heartbeatTicks && r.lead != 0 && r.goalTerm != r.term {
		r.probeTicks = 0
		r.send(Message{Type: MsgRepairProbe, To: r.lead, Term: r.term, Seq: r.nonce})
	}
}

func (r *Raft) heal() {
	if r.repair && r.goalTerm == r.term && r.acked >= r.goal {
		r.repair = false
		r.resetTimer()
	}
}

func (r *Raft) send(m Message) {
	m.From = r.id
	r.msgs = append(r.msgs, m)
}

func (r *Raft) resetTimer() {
	r.elapsed = 0
	r.timeout = r.electionTicks + r.rand.IntN(r.electionTicks)
}

func (r *Raft) quorum() int {
	return len(r.peers)/2 + 1
}

func (r *Raft) poll(from uint64) bool {
	r.votes[slices.Index(r.peers, from)] = true
	n := 0
	for _, v := range r.votes {
		if v {
			n++
		}
	}
	return n >= r.quorum()
}

func (r *Raft) becomeFollower(term, lead uint64) {
	if term > r.term {
		r.term, r.vote, r.acked = term, 0, 0
		r.dropOlder()
	}
	r.role, r.lead = Follower, lead
	r.pending = nil
	r.resetTimer()
}

func (r *Raft) dropOlder() {
	r.msgs = slices.DeleteFunc(r.msgs, func(m Message) bool { return m.Term < r.term })
}

func (r *Raft) preVote() error {
	r.role, r.lead = PreCandidate, 0
	r.resetTimer()
	clear(r.votes)
	if r.poll(r.id) {
		return r.campaign()
	}
	return r.requestVotes(MsgPreVote, r.term+1)
}

func (r *Raft) campaign() error {
	r.role, r.lead = Candidate, 0
	r.term++
	r.vote, r.acked = r.id, 0
	r.dropOlder()
	r.resetTimer()
	clear(r.votes)
	if r.poll(r.id) {
		return r.becomeLeader()
	}
	return r.requestVotes(MsgVote, r.term)
}

func (r *Raft) requestVotes(t MsgType, term uint64) error {
	lastTerm, err := r.lastTerm()
	if err != nil {
		return err
	}
	for _, id := range r.peers {
		if id != r.id {
			r.send(Message{Type: t, To: id, Term: term, Index: r.lastIndex(), LogTerm: lastTerm})
		}
	}
	return nil
}

func (r *Raft) becomeLeader() error {
	r.role, r.lead = Leader, r.id
	last := r.lastIndex()
	for i := range r.prs {
		r.prs[i] = progress{next: last + 1}
	}
	r.termStart = last + 1
	r.beat, r.checkTicks = 0, 0
	r.ents = append(r.ents, Entry{Index: last + 1, Term: r.term})
	return r.broadcast(true)
}

func (r *Raft) checkQuorum() bool {
	n := 0
	for i := range r.prs {
		if i == r.self || r.prs[i].active {
			n++
		}
		r.prs[i].active = false
	}
	return n >= r.quorum()
}

func (r *Raft) handlePreVote(m Message) error {
	lastTerm, err := r.lastTerm()
	if err != nil {
		return err
	}
	grant := r.voter() && m.Term > r.term && r.role != Leader && r.sinceLead >= r.electionTicks &&
		upToDate(m.LogTerm, m.Index, lastTerm, r.lastIndex())
	if grant {
		r.send(Message{Type: MsgPreVoteResp, To: m.From, Term: m.Term})
	} else {
		r.send(Message{Type: MsgPreVoteResp, To: m.From, Term: r.term, Reject: true})
	}
	return nil
}

func (r *Raft) handleVote(m Message) error {
	lastTerm, err := r.lastTerm()
	if err != nil {
		return err
	}
	if r.voter() && (r.vote == 0 || r.vote == m.From) && upToDate(m.LogTerm, m.Index, lastTerm, r.lastIndex()) {
		r.vote = m.From
		r.resetTimer()
		r.send(Message{Type: MsgVoteResp, To: m.From, Term: r.term})
	} else {
		r.send(Message{Type: MsgVoteResp, To: m.From, Term: r.term, Reject: true})
	}
	return nil
}

func (r *Raft) handleApp(m Message) error {
	if m.Index < r.commit {
		r.ack(m, r.commit)
		return nil
	}
	term, ok, err := r.termAt(m.Index)
	if err != nil {
		return err
	}
	if !ok || term != m.LogTerm {
		if ok && m.Index == r.commit {
			return fmt.Errorf("%w: leader has term %d at committed index %d, log has %d", ErrSafety, m.LogTerm, m.Index, term)
		}
		hint, err := r.conflictHint(m.Index, term, ok)
		if err != nil {
			return err
		}
		r.send(Message{Type: MsgAppResp, To: m.From, Term: r.term, Index: hint, Reject: true, Seq: m.Seq})
		return nil
	}
	if err := r.merge(m.Entries); err != nil {
		return err
	}
	last := m.Index + uint64(len(m.Entries))
	r.followCommit(m.Commit, last)
	r.ack(m, last)
	return nil
}

func (r *Raft) conflictHint(i, term uint64, found bool) (uint64, error) {
	if !found {
		return r.lastIndex() + 1, nil
	}
	for i-1 > r.commit {
		t, ok, err := r.termAt(i - 1)
		if err != nil {
			return 0, err
		}
		if !ok || t != term {
			break
		}
		i--
	}
	return i, nil
}

func (r *Raft) handleSnap(m Message) error {
	if m.Index <= r.commit {
		r.ack(m, r.commit)
		return nil
	}
	term, ok, err := r.termAt(m.Index)
	if err != nil {
		return err
	}
	if !ok || term != m.LogTerm {
		r.snap = &Snapshot{Index: m.Index, Term: m.LogTerm}
		r.ents = nil
	}
	r.commit = m.Index
	r.ack(m, m.Index)
	return nil
}

func (r *Raft) handleAppResp(m Message) error {
	i := slices.Index(r.peers, m.From)
	p := &r.prs[i]
	p.active = true
	p.seq = max(p.seq, m.Seq)
	r.releaseReads()
	switch {
	case m.Reject && (m.Index == 0 || m.Index >= p.next):
	case m.Reject:
		p.next, p.match = m.Index, min(p.match, m.Index-1)
		p.inflight = false
		return r.sendApp(i, true)
	case m.Index > p.match && m.Index <= r.lastIndex():
		p.match, p.next = m.Index, m.Index+1
		p.inflight = false
		if p.snapping && p.match >= p.snapIndex {
			p.snapping = false
		}
		r.maybeCommit()
		return r.sendApp(i, false)
	}
	return nil
}

func (r *Raft) releaseReads() {
	s := r.confirmedSeq()
	k := 0
	for ; k < len(r.pending) && r.pending[k].seq <= s; k++ {
		r.reads = append(r.reads, Read{Tag: r.pending[k].tag, Index: r.pending[k].index})
	}
	r.pending = r.pending[k:]
}

func quorumValue(vals []uint64, q int) uint64 {
	slices.Sort(vals)
	return vals[len(vals)-q]
}

func (r *Raft) broadcast(beat bool) error {
	for i := range r.prs {
		if i == r.self {
			continue
		}
		if err := r.sendApp(i, beat); err != nil {
			return err
		}
	}
	return nil
}

func (r *Raft) sendApp(i int, beat bool) error {
	p := &r.prs[i]
	if p.snapping {
		if beat {
			r.send(Message{Type: MsgApp, To: r.peers[i], Term: r.term, Commit: r.commit, Seq: r.seq})
		}
		return nil
	}
	prev := p.next - 1
	term, ok, err := r.termAt(prev)
	if err != nil {
		return err
	}
	if !ok {
		s := r.log.Snapshot()
		if r.snap != nil {
			s = *r.snap
		}
		p.snapping, p.snapIndex, p.snapTicks = true, s.Index, 0
		p.inflight = false
		r.send(Message{Type: MsgSnap, To: r.peers[i], Term: r.term, Index: s.Index, LogTerm: s.Term, Commit: r.commit, Seq: r.seq})
		return nil
	}
	var ents []Entry
	if last := r.lastIndex(); !p.inflight && p.next <= last {
		if ents, err = r.entries(p.next, last+1); err != nil {
			return err
		}
		p.inflight = true
	} else if !beat {
		return nil
	}
	r.send(Message{Type: MsgApp, To: r.peers[i], Term: r.term, Index: prev, LogTerm: term, Commit: r.commit, Seq: r.seq, Entries: ents})
	return nil
}
