package raft

import (
	"errors"
	"fmt"
	"hash/fnv"
	"math/rand/v2"
	"slices"
	"testing"
)

var errRange = errors.New("memlog: index out of range")

type memLog struct {
	snap  Snapshot
	first uint64
	ents  []Entry
	err   error
}

func newMemLog(terms ...uint64) *memLog {
	l := &memLog{first: 1}
	for i, t := range terms {
		l.ents = append(l.ents, Entry{Index: uint64(i + 1), Term: t, Kind: 1, Body: []byte{byte(i), byte(t)}})
	}
	return l
}

func (l *memLog) Snapshot() Snapshot { return l.snap }
func (l *memLog) FirstIndex() uint64 { return l.first }
func (l *memLog) LastIndex() uint64  { return l.first - 1 + uint64(len(l.ents)) }

func (l *memLog) Term(i uint64) (uint64, error) {
	if l.err != nil {
		return 0, l.err
	}
	if i == l.snap.Index {
		return l.snap.Term, nil
	}
	if i < l.first || i > l.LastIndex() {
		return 0, fmt.Errorf("%w: term %d not in [%d, %d]", errRange, i, l.first, l.LastIndex())
	}
	return l.ents[i-l.first].Term, nil
}

func (l *memLog) Entries(lo, hi uint64, maxBytes int64) ([]Entry, error) {
	if l.err != nil {
		return nil, l.err
	}
	if lo < l.first || hi > l.LastIndex()+1 || lo > hi {
		return nil, fmt.Errorf("%w: entries [%d, %d) not in [%d, %d]", errRange, lo, hi, l.first, l.LastIndex())
	}
	ents := l.ents[lo-l.first : hi-l.first]
	var size int64
	for k, e := range ents {
		size += int64(len(e.Body))
		if k > 0 && size > maxBytes {
			return ents[:k], nil
		}
	}
	return ents, nil
}

func (l *memLog) append(ents []Entry) {
	k := ents[0].Index - l.first
	if k < uint64(len(l.ents)) {
		l.ents = slices.Clone(l.ents[:k])
	}
	l.ents = append(l.ents, ents...)
}

func (l *memLog) install(s Snapshot) {
	l.snap, l.first, l.ents = s, s.Index+1, nil
}

func (l *memLog) compact(snap, keep uint64) {
	t, err := l.Term(snap)
	if err != nil {
		panic(err)
	}
	l.snap = Snapshot{Index: snap, Term: t}
	if keep > l.first {
		l.ents = slices.Clone(l.ents[keep-l.first:])
		l.first = keep
	}
}

func (l *memLog) terms() []uint64 {
	var ts []uint64
	for _, e := range l.ents {
		ts = append(ts, e.Term)
	}
	return ts
}

type tuning struct {
	election, heartbeat, snapshot int
	maxBytes                      int64
	maxUncommitted                uint64
}

var defaultTuning = tuning{election: 10, heartbeat: 2, snapshot: 50, maxBytes: 4 << 20, maxUncommitted: 4096}

type node struct {
	id       uint64
	r        *Raft
	log      *memLog
	hs       HardState
	rng      *rand.Rand
	applied  uint64
	hash     uint64
	snapHash uint64
	offered  map[uint64]uint64
	commit   uint64
	reads    []Read
	healedIn uint64
}

type envelope struct {
	m    Message
	hash uint64
}

type cluster struct {
	t        testing.TB
	tune     tuning
	ids      []uint64
	nodes    []*node
	net      []envelope
	cut      [6][6]bool
	chk      *checker
	early    bool
	hold     func() bool
	crashAt  func(n *node) int
	installs int
	heals    int
}

func newCluster(t testing.TB, size int, tune tuning, seed uint64) *cluster {
	c := &cluster{t: t, tune: tune, chk: newChecker()}
	for i := 1; i <= size; i++ {
		c.ids = append(c.ids, uint64(i))
	}
	for _, id := range c.ids {
		n := &node{id: id, log: newMemLog(), rng: rand.New(rand.NewPCG(seed, id)), offered: map[uint64]uint64{}}
		c.nodes = append(c.nodes, n)
		c.boot(n)
	}
	return c
}

func (c *cluster) config(n *node) Config {
	return Config{
		ID:             n.id,
		Peers:          c.ids,
		ElectionTicks:  c.tune.election,
		HeartbeatTicks: c.tune.heartbeat,
		SnapshotTicks:  c.tune.snapshot,
		MaxAppendBytes: c.tune.maxBytes,
		MaxUncommitted: c.tune.maxUncommitted,
		Rand:           n.rng,
		Log:            n.log,
		HardState:      n.hs,
	}
}

func (c *cluster) boot(n *node) {
	c.t.Helper()
	r, err := New(c.config(n))
	if err != nil {
		c.t.Fatalf("node %d: New: %v", n.id, err)
	}
	n.r = r
	n.applied, n.hash, n.commit = n.log.snap.Index, n.snapHash, n.log.snap.Index
	n.healedIn = 0
	clear(n.offered)
}

func (c *cluster) node(id uint64) *node { return c.nodes[id-1] }

func (c *cluster) leader() *node {
	for _, n := range c.nodes {
		if n.r.role == Leader {
			return n
		}
	}
	return nil
}

func (c *cluster) must(n *node, err error) {
	c.t.Helper()
	if err != nil {
		c.t.Fatalf("node %d: %v", n.id, err)
	}
}

func (c *cluster) process(n *node) {
	for n.r.HasReady() {
		if c.chk.failed() {
			return
		}
		rd := n.r.Ready()
		at := -1
		if c.crashAt != nil {
			at = c.crashAt(n)
		}
		if c.early {
			c.emit(n, rd.Messages)
		}
		if at == 0 {
			c.crash(n)
			return
		}
		if rd.HardState != nil {
			c.chk.hardState(n, *rd.HardState)
			if n.hs.Repair && !rd.HardState.Repair {
				c.chk.healed(n)
				c.heals++
			}
			n.hs = *rd.HardState
		}
		if at == 1 {
			c.crash(n)
			return
		}
		if rd.Snapshot != nil {
			hash, ok := n.offered[rd.Snapshot.Index]
			if !ok {
				c.chk.fail("node %d: snapshot %d without payload", n.id, rd.Snapshot.Index)
				return
			}
			n.log.install(*rd.Snapshot)
			c.installs++
			n.snapHash, n.hash, n.applied = hash, hash, rd.Snapshot.Index
			c.chk.state(n, rd.Snapshot.Index, hash)
		}
		if at == 2 {
			c.crash(n)
			return
		}
		if len(rd.Entries) > 0 {
			if rd.Entries[0].Index <= n.commit {
				c.chk.fail("node %d: entries from %d overwrite commit %d", n.id, rd.Entries[0].Index, n.commit)
				return
			}
			n.log.append(rd.Entries)
			c.chk.stored(n, rd.Entries)
		}
		if at == 3 {
			c.crash(n)
			return
		}
		if !c.early {
			c.chk.promises(n, rd.Messages)
			c.emit(n, rd.Messages)
		}
		c.apply(n, rd.Commit)
		n.reads = append(n.reads, rd.Reads...)
		c.chk.reads(n, rd.Reads)
		n.r.Advance()
		c.watchRepair(n)
		c.chk.status(n)
	}
}

func (c *cluster) watchRepair(n *node) {
	if st := n.r.Status(); n.hs.Repair && !st.Repair && n.healedIn == 0 {
		n.healedIn = st.Term
	}
}

func (c *cluster) emit(n *node, msgs []Message) {
	c.chk.sent(n, msgs)
	for _, m := range msgs {
		env := envelope{m: cloneMsg(m)}
		if m.Type == MsgSnap {
			env.hash = n.snapHash
		}
		c.net = append(c.net, env)
	}
}

func cloneMsg(m Message) Message {
	if m.Entries != nil {
		m.Entries = slices.Clone(m.Entries)
		for i := range m.Entries {
			m.Entries[i].Body = slices.Clone(m.Entries[i].Body)
		}
	}
	return m
}

func (c *cluster) apply(n *node, commit uint64) {
	if commit < n.commit {
		c.chk.fail("node %d: commit fell from %d to %d", n.id, n.commit, commit)
		return
	}
	n.commit = commit
	if commit <= n.applied {
		return
	}
	ents, err := n.log.Entries(n.applied+1, commit+1, 1<<62)
	if err != nil {
		c.chk.fail("node %d: commit %d not on disk: %v", n.id, commit, err)
		return
	}
	for _, e := range ents {
		n.hash = chain(n.hash, e)
		n.applied = e.Index
		c.chk.committed(n, e)
		c.chk.state(n, e.Index, n.hash)
	}
}

func chain(h uint64, e Entry) uint64 {
	f := fnv.New64a()
	var b [25]byte
	for i := range 8 {
		b[i] = byte(h >> (8 * i))
		b[8+i] = byte(e.Index >> (8 * i))
		b[16+i] = byte(e.Term >> (8 * i))
	}
	b[24] = e.Kind
	_, _ = f.Write(b[:])
	_, _ = f.Write(e.Body)
	return f.Sum64()
}

func (c *cluster) crash(n *node) {
	c.chk.restart(n)
	c.boot(n)
}

func (c *cluster) damage(n *node, keep int) {
	n.log.ents = slices.Clone(n.log.ents[:keep])
	if n.log.LastIndex() < n.log.snap.Index {
		n.log.first, n.log.ents = n.log.snap.Index+1, nil
	}
	n.hs.Repair = true
	c.crash(n)
}

func (c *cluster) snapshot(n *node, keepOld bool) {
	c.process(n)
	if n.applied <= n.log.snap.Index {
		return
	}
	keep := n.applied + 1
	if keepOld {
		keep = n.log.snap.Index + 1
	}
	n.log.compact(n.applied, keep)
	n.snapHash = n.hash
}

func (c *cluster) deliver(env envelope) {
	m := env.m
	if c.cut[m.From][m.To] {
		return
	}
	n := c.node(m.To)
	if m.Type == MsgSnap {
		n.offered[m.Index] = env.hash
	}
	if err := n.r.Step(m); err != nil {
		c.chk.fail("node %d: Step %v: %v", n.id, m.Type, err)
		return
	}
	c.watchRepair(n)
	c.after(n)
}

func (c *cluster) after(n *node) {
	if c.hold == nil || !c.hold() {
		c.process(n)
	}
}

func (c *cluster) take(match func(Message) bool) []envelope {
	var got []envelope
	rest := c.net[:0]
	for _, env := range c.net {
		if match(env.m) {
			got = append(got, env)
		} else {
			rest = append(rest, env)
		}
	}
	c.net = rest
	return got
}

func (c *cluster) deliverAll(match func(Message) bool) {
	for _, env := range c.take(match) {
		c.deliver(env)
	}
	c.checkOK()
}

func (c *cluster) settle() {
	for range 1000 {
		if len(c.net) == 0 {
			return
		}
		c.deliverAll(func(Message) bool { return true })
	}
	c.t.Fatal("network did not settle")
}

func (c *cluster) drop(match func(Message) bool) { c.take(match) }

func (c *cluster) tick(n *node) {
	c.must(n, n.r.Tick())
	c.after(n)
}

func (c *cluster) timeout(n *node) {
	n.r.elapsed = n.r.timeout - 1
	for _, o := range c.nodes {
		o.r.sinceLead = o.r.electionTicks
	}
	c.tick(n)
}

func (c *cluster) elect(n *node) {
	c.timeout(n)
	c.settle()
	if n.r.role != Leader {
		c.t.Fatalf("node %d not elected: %v", n.id, n.r.Status())
	}
}

func (c *cluster) propose(n *node, body string) uint64 {
	i, err := n.r.Propose(1, []byte(body))
	c.must(n, err)
	c.process(n)
	return i
}

func (c *cluster) isolate(id uint64) {
	for _, o := range c.ids {
		if o != id {
			c.cut[id][o], c.cut[o][id] = true, true
		}
	}
}

func (c *cluster) heal() { c.cut = [6][6]bool{} }

func (c *cluster) checkOK() {
	c.t.Helper()
	if c.chk.failed() {
		c.t.Fatal(c.chk.violation)
	}
}

func is(t MsgType) func(Message) bool {
	return func(m Message) bool { return m.Type == t }
}

func to(id uint64) func(Message) bool {
	return func(m Message) bool { return m.To == id }
}

func from(id uint64) func(Message) bool {
	return func(m Message) bool { return m.From == id }
}

type entryKey struct{ index, term uint64 }

type voteKey struct{ node, term uint64 }

type entryID struct {
	kind     uint8
	body     string
	prevTerm uint64
}

type holding struct{ node, index, term uint64 }

type committedEntry struct {
	seenIn uint64
	term   uint64
	kind   uint8
	body   string
}

type checker struct {
	violation string
	leaders   map[uint64]uint64
	votes     map[voteKey]uint64
	ents      map[entryKey]entryID
	held      map[holding]bool
	commits   map[uint64]committedEntry
	hashes    map[uint64]uint64
	terms     map[uint64]uint64
	maxCommit uint64
	readFloor map[uint64]map[uint64]uint64
}

func newChecker() *checker {
	return &checker{
		leaders:   map[uint64]uint64{},
		votes:     map[voteKey]uint64{},
		ents:      map[entryKey]entryID{},
		held:      map[holding]bool{},
		commits:   map[uint64]committedEntry{},
		hashes:    map[uint64]uint64{},
		terms:     map[uint64]uint64{},
		readFloor: map[uint64]map[uint64]uint64{},
	}
}

func (chk *checker) failed() bool { return chk.violation != "" }

func (chk *checker) fail(format string, args ...any) {
	if chk.violation == "" {
		chk.violation = fmt.Sprintf(format, args...)
	}
}

func (chk *checker) sent(n *node, msgs []Message) {
	for _, m := range msgs {
		switch {
		case m.Type == MsgVoteResp && !m.Reject:
			key := voteKey{n.id, m.Term}
			if prev, ok := chk.votes[key]; ok && prev != m.To {
				chk.fail("election safety: node %d voted for %d and %d in term %d", n.id, prev, m.To, m.Term)
			}
			chk.votes[key] = m.To
		case m.Type == MsgApp || m.Type == MsgSnap:
			chk.leader(n.id, m.Term)
		}
	}
}

func (chk *checker) promises(n *node, msgs []Message) {
	for _, m := range msgs {
		if n.hs.Repair && votingOrLeading(m) {
			chk.fail("repair: node %d sends %v in term %d, its disk says repair", n.id, m.Type, m.Term)
		}
		switch {
		case m.Type == MsgAppResp && !m.Reject && (m.Term != n.hs.Term || m.Index > n.log.LastIndex()):
			chk.fail("promise: node %d acknowledged %d in term %d, disk has term %d and ends at %d", n.id, m.Index, m.Term, n.hs.Term, n.log.LastIndex())
		case m.Type == MsgVoteResp && !m.Reject && (n.hs.Term != m.Term || n.hs.Vote != m.To):
			chk.fail("promise: node %d granted %d in term %d, disk has %+v", n.id, m.To, m.Term, n.hs)
		case m.Type == MsgVote && (n.hs.Term != m.Term || n.hs.Vote != n.id):
			chk.fail("promise: node %d campaigns in term %d, disk has %+v", n.id, m.Term, n.hs)
		case (m.Type == MsgApp || m.Type == MsgSnap) && m.Term != n.hs.Term:
			chk.fail("promise: node %d leads term %d, disk has term %d", n.id, m.Term, n.hs.Term)
		case m.Type == MsgApp && m.Index+uint64(len(m.Entries)) > n.log.LastIndex():
			chk.fail("promise: node %d sends entries up to %d, disk ends at %d", n.id, m.Index+uint64(len(m.Entries)), n.log.LastIndex())
		}
	}
}

func votingOrLeading(m Message) bool {
	switch m.Type {
	case MsgPreVote, MsgVote, MsgApp, MsgSnap:
		return true
	case MsgPreVoteResp, MsgVoteResp:
		return !m.Reject
	}
	return false
}

func (chk *checker) leader(id, term uint64) {
	if prev, ok := chk.leaders[term]; ok && prev != id {
		chk.fail("election safety: leaders %d and %d in term %d", prev, id, term)
	}
	chk.leaders[term] = id
}

func (chk *checker) hardState(n *node, hs HardState) {
	if hs.Term < chk.terms[n.id] {
		chk.fail("node %d: term fell from %d to %d", n.id, chk.terms[n.id], hs.Term)
	}
	if hs.Repair && !n.hs.Repair {
		chk.fail("node %d: the core set repair on its own", n.id)
	}
	chk.terms[n.id] = hs.Term
}

func (chk *checker) healed(n *node) {
	for i := n.log.snap.Index + 1; i <= chk.maxCommit; i++ {
		ce, ok := chk.commits[i]
		if !ok || ce.seenIn > n.healedIn || !chk.held[holding{n.id, i, ce.term}] {
			continue
		}
		if t, err := n.log.Term(i); err != nil || t != ce.term {
			chk.fail("repair: node %d left repair in term %d without entry %d of term %d, which it once stored and which is committed", n.id, n.healedIn, i, ce.term)
			return
		}
	}
}

func (chk *checker) stored(n *node, ents []Entry) {
	for _, e := range ents {
		chk.held[holding{n.id, e.Index, e.Term}] = true
		prev, err := n.log.Term(e.Index - 1)
		if err != nil {
			prev = ^uint64(0)
		}
		id := entryID{kind: e.Kind, body: string(e.Body), prevTerm: prev}
		key := entryKey{e.Index, e.Term}
		if old, ok := chk.ents[key]; ok {
			if old.kind != id.kind || old.body != id.body {
				chk.fail("log matching: two entries at %d in term %d", e.Index, e.Term)
			}
			if old.prevTerm != id.prevTerm && old.prevTerm != ^uint64(0) && prev != ^uint64(0) {
				chk.fail("log matching: entry %d term %d follows terms %d and %d", e.Index, e.Term, old.prevTerm, prev)
			}
			continue
		}
		chk.ents[key] = id
	}
}

func (chk *checker) committed(n *node, e Entry) {
	ce := committedEntry{seenIn: n.r.term, term: e.Term, kind: e.Kind, body: string(e.Body)}
	if old, ok := chk.commits[e.Index]; ok && (old.term != ce.term || old.kind != ce.kind || old.body != ce.body) {
		chk.fail("state machine safety: node %d applied term %d at %d, committed was term %d", n.id, e.Term, e.Index, old.term)
		return
	}
	if _, ok := chk.commits[e.Index]; !ok {
		chk.commits[e.Index] = ce
	}
	chk.maxCommit = max(chk.maxCommit, e.Index)
}

func (chk *checker) state(n *node, i, hash uint64) {
	if old, ok := chk.hashes[i]; ok && old != hash {
		chk.fail("state machine safety: node %d has a different state at %d", n.id, i)
	}
	chk.hashes[i] = hash
}

func (chk *checker) status(n *node) {
	st := n.r.Status()
	if st.Role != Leader {
		return
	}
	if chk.leaders[st.Term] == n.id {
		return
	}
	chk.leader(n.id, st.Term)
	for i, ce := range chk.commits {
		if ce.seenIn >= st.Term {
			continue
		}
		t, ok, err := n.r.termAt(i)
		if err != nil || ok && t != ce.term || !ok && i > n.log.snap.Index {
			chk.fail("leader completeness: leader %d of term %d lacks committed entry %d", n.id, st.Term, i)
			return
		}
	}
}

func (chk *checker) readStarted(n *node, tag uint64) {
	if chk.readFloor[n.id] == nil {
		chk.readFloor[n.id] = map[uint64]uint64{}
	}
	chk.readFloor[n.id][tag] = chk.maxCommit
}

func (chk *checker) reads(n *node, rs []Read) {
	for _, rr := range rs {
		floor, ok := chk.readFloor[n.id][rr.Tag]
		if !ok {
			chk.fail("node %d: read %d never requested", n.id, rr.Tag)
			return
		}
		if rr.Index < floor {
			chk.fail("linearizable read: node %d read %d at index %d, %d was committed before", n.id, rr.Tag, rr.Index, floor)
			return
		}
		delete(chk.readFloor[n.id], rr.Tag)
	}
}

func (chk *checker) restart(n *node) {
	delete(chk.readFloor, n.id)
}

func (c *cluster) load(id uint64, hs HardState, terms ...uint64) *node {
	n := c.node(id)
	n.log, n.hs = newMemLog(terms...), hs
	c.boot(n)
	return n
}

func (c *cluster) step(n *node, m Message) {
	c.t.Helper()
	m.To = n.id
	c.deliver(envelope{m: m})
	c.checkOK()
}

func (c *cluster) sent(id uint64) []Message {
	var msgs []Message
	for _, env := range c.take(from(id)) {
		msgs = append(msgs, env.m)
	}
	return msgs
}

func (c *cluster) only(id uint64) Message {
	c.t.Helper()
	msgs := c.sent(id)
	if len(msgs) != 1 {
		c.t.Fatalf("node %d sent %d messages, want 1: %+v", id, len(msgs), msgs)
	}
	return msgs[0]
}

func (c *cluster) exchange(match func(Message) bool) {
	c.t.Helper()
	for range 100 {
		if len(c.net) == 0 || !slices.ContainsFunc(c.net, func(env envelope) bool { return match(env.m) }) {
			return
		}
		c.deliverAll(match)
	}
	c.t.Fatal("exchange did not settle")
}

func (c *cluster) read(n *node, tag uint64) error {
	c.chk.readStarted(n, tag)
	if err := n.r.ReadIndex(tag); err != nil {
		return err
	}
	c.after(n)
	return nil
}

func voting(m Message) bool {
	return m.Type == MsgPreVote || m.Type == MsgPreVoteResp || m.Type == MsgVote || m.Type == MsgVoteResp
}

func ent(i, t uint64) Entry {
	return Entry{Index: i, Term: t, Kind: 1, Body: []byte{byte(i - 1), byte(t)}}
}
