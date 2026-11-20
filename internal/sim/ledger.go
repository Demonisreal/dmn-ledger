package sim

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"math/rand/v2"
	"path/filepath"
	"slices"
	"strings"

	"github.com/Demonisreal/dmn-ledger/internal/audit"
	"github.com/Demonisreal/dmn-ledger/internal/client"
	"github.com/Demonisreal/dmn-ledger/internal/disk"
	"github.com/Demonisreal/dmn-ledger/internal/raft"
	"github.com/Demonisreal/dmn-ledger/internal/replica"
	"github.com/Demonisreal/dmn-ledger/internal/state"
	"github.com/Demonisreal/dmn-ledger/internal/u128"
	"github.com/Demonisreal/dmn-ledger/internal/wal"
	"github.com/Demonisreal/dmn-ledger/internal/wire"
	"github.com/Demonisreal/dmn-ledger/internal/workload"
)

const (
	ledgerDir     = "/ledger"
	clusterID     = 1
	windowSize    = 32
	thinkTicks    = 20
	lookupOdds    = 5
	cutLookups    = 2
	readKinds     = 16
	linkSpan      = 64
	maxSessions   = 64
	evictSessions = 2
	drainTime     = second
	joinTicks     = 200
	rejoinOdds    = 8
	rotTail       = 200
)

type ClusterConfig struct {
	Seed    uint64
	Profile Profile
	Nodes   uint64
	Trace   io.Writer
	Disk    func(id uint64, fsys disk.FS) disk.FS
	Said    func(id uint64, msgs []raft.Message) error
	Check   func(c *Cluster) error
	Failed  func(id uint64, err error) error
}

type Call struct {
	Client     uint64
	ID         u128.Uint128
	Session    uint64
	Op         client.Op
	Body       []byte
	Start, End uint64
	Reply      client.Reply
}

type Cluster struct {
	cfg       ClusterConfig
	w         *World
	gen       *workload.Gen
	sessions  int
	churn     bool
	snapEvery uint64
	segSize   int64
	appendMax int64
	retry     int
	addrs     map[u128.Uint128]uint64
	calls     []Call
	windows   []int
	started   int
	open      int
	stopped   error
}

func NewCluster(cfg ClusterConfig) (*Cluster, error) {
	rng := pcg(cfg.Seed, 1)
	c := &Cluster{
		cfg:       cfg,
		snapEvery: 4 + rng.Uint64N(37),
		segSize:   4<<10 + rng.Int64N(16<<10),
		addrs:     make(map[u128.Uint128]uint64),
		gen:       workload.New(fork(rng)),
	}
	c.gen.MaxBatch = 1 + rng.IntN(8)
	c.appendMax = rng.Int64N(8<<10 + 1)
	if cfg.Profile == ProfileLeaders {
		c.appendMax = 0
	}
	c.retry = 2 + rng.IntN(19)
	c.sessions = maxSessions
	if cfg.Profile == ProfileEvict {
		c.sessions, c.churn = evictSessions, true
	}
	nodes := cfg.Nodes
	if nodes == 0 {
		nodes = 3
		if cfg.Seed%5 == 4 {
			nodes = 5
		}
	}
	w, err := New(Config{
		Seed:      cfg.Seed,
		Profile:   cfg.Profile,
		Nodes:     nodes,
		Clients:   3 + cfg.Seed%4,
		Node:      c.startNode,
		Client:    c.startClient,
		Disk:      cfg.Disk,
		Said:      c.said,
		Check:     c.check,
		Leading:   c.leading,
		Repairing: c.repairing,
		Rot:       rotTarget,
		Trace:     cfg.Trace,
	})
	if err != nil {
		return nil, err
	}
	c.w = w
	return c, nil
}

func (c *Cluster) Run() error {
	return c.w.Run()
}

func (c *Cluster) World() *World {
	return c.w
}

func (c *Cluster) Nodes() uint64 {
	return c.w.cfg.Nodes
}

func (c *Cluster) Node(id uint64) *replica.Node {
	if p, ok := c.w.hosts[id-1].proc.(*ledgerNode); ok {
		return p.n
	}
	return nil
}

func (c *Cluster) MaxSessions() int {
	return c.sessions
}

func (c *Cluster) Calls() []Call {
	return c.calls
}

func (c *Cluster) Windows() []int {
	return c.windows
}

func (c *Cluster) said(id uint64, msgs []Msg) error {
	if c.cfg.Said == nil || id > c.Nodes() {
		return nil
	}
	var said []raft.Message
	for _, m := range msgs {
		if b, ok := m.Body.(*raftBody); ok {
			said = append(said, b.Message)
		}
	}
	return c.cfg.Said(id, said)
}

func (c *Cluster) check() error {
	if c.stopped != nil || c.cfg.Check == nil {
		return c.stopped
	}
	return c.cfg.Check(c)
}

func (c *Cluster) leading(id uint64) (uint64, bool) {
	n := c.Node(id)
	if n == nil {
		return 0, false
	}
	st := n.Status()
	return st.Commit, st.Role == raft.Leader
}

func (c *Cluster) repairing(id uint64) bool {
	n := c.Node(id)
	return n != nil && n.Status().Repair
}

func rotTarget(durable []disk.DurableFile, meta bool, rng *rand.Rand) (string, int64, bool) {
	var files []disk.DurableFile
	tail := -1
	for _, f := range durable {
		base := filepath.Base(f.Name)
		if f.Size == 0 || strings.HasSuffix(base, ".tmp") || (base == wal.MetaName) != meta {
			continue
		}
		if strings.HasPrefix(base, wal.SegmentPrefix) {
			tail = len(files)
		}
		files = append(files, f)
	}
	if len(files) == 0 {
		return "", 0, false
	}
	k := rng.IntN(len(files))
	if tail >= 0 && rng.IntN(2) == 0 {
		k = tail
	}
	f := files[k]
	switch {
	case meta:
		return f.Name, rng.Int64N(2)*wal.MetaStride + rng.Int64N(wal.MetaSlotLen), true
	case k == tail && rng.IntN(2) == 0:
		return f.Name, f.Size - 1 - rng.Int64N(min(f.Size, rotTail)), true
	}
	return f.Name, rng.Int64N(f.Size), true
}

func (c *Cluster) failed(id uint64, err error) {
	if c.cfg.Failed != nil && c.stopped == nil {
		c.stopped = c.cfg.Failed(id, err)
	}
}

func (c *Cluster) admit() bool {
	if c.started == windowSize {
		if c.open > 0 {
			return false
		}
		c.started = 0
		c.windows = append(c.windows, len(c.calls))
	}
	c.started++
	c.open++
	return true
}

func (c *Cluster) Open(id uint64, fsys disk.FS) (*replica.Node, error) {
	return c.openNode(NodeEnv{
		ID:     id,
		Peers:  slices.Clone(c.w.peers),
		FS:     fsys,
		Clock:  c.w.Now,
		Rand:   pcg(c.cfg.Seed, id),
		Nonces: nonces(pcg(c.cfg.Seed, id)),
	})
}

func (c *Cluster) startNode(env NodeEnv) (Process, error) {
	n, err := c.openNode(env)
	if err != nil {
		c.failed(env.ID, err)
		return nil, err
	}
	return &ledgerNode{c: c, id: env.ID, n: n}, nil
}

func (c *Cluster) openNode(env NodeEnv) (*replica.Node, error) {
	return replica.Open(replica.Config{
		ID:             env.ID,
		Cluster:        clusterID,
		Peers:          env.Peers,
		FS:             env.FS,
		Dir:            ledgerDir,
		Create:         true,
		Clock:          env.Clock,
		Rand:           env.Rand,
		Nonces:         env.Nonces,
		MaxSessions:    c.sessions,
		SnapshotEvery:  c.snapEvery,
		SegmentSize:    c.segSize,
		ClockStep:      tick + tick/4,
		ElectionTicks:  10,
		HeartbeatTicks: 2,
		SnapshotTicks:  50,
		MaxAppendBytes: c.appendMax,
		MaxUncommitted: 4096,
	})
}

func (c *Cluster) startClient(env ClientEnv) Process {
	p := &ledgerClient{c: c, addr: env.ID, nodes: env.Nodes, rng: env.Rand, now: env.Now, call: -1}
	p.join()
	p.left = 30 + env.Rand.IntN(31)
	if c.churn {
		p.wait = env.Rand.IntN(joinTicks)
	}
	return p
}

func (c *Cluster) cut() bool {
	for _, win := range c.w.plan.windows {
		switch win.kind {
		case split, oneway, isolate, bridge, pause:
			if win.start <= c.w.now && c.w.now < win.end {
				return true
			}
		}
	}
	return false
}

type ledgerNode struct {
	c  *Cluster
	id uint64
	n  *replica.Node
}

func (p *ledgerNode) Tick() {
	p.n.Tick()
}

func (p *ledgerNode) Receive(m Msg) {
	switch b := m.Body.(type) {
	case *raftBody:
		var snap io.Reader
		if b.Snap != nil {
			snap = bytes.NewReader(b.Snap)
		}
		p.n.Receive(b.Message, snap)
	case *requestBody:
		p.n.Request(b.Request)
	}
}

func (p *ledgerNode) Run() ([]Msg, error) {
	out, err := p.n.Process()
	if err != nil {
		p.c.failed(p.id, err)
		return nil, err
	}
	msgs := make([]Msg, 0, len(out.Messages)+len(out.Replies))
	for _, m := range out.Messages {
		b := &raftBody{Message: m}
		if m.Type == raft.MsgSnap {
			err := p.n.ReadSnapshot(raft.Snapshot{Index: m.Index, Term: m.LogTerm}, func(r io.Reader) error {
				var err error
				b.Snap, err = io.ReadAll(r)
				return err
			})
			if errors.Is(err, wal.ErrNoSnapshot) {
				continue
			}
			if err != nil {
				p.c.failed(p.id, err)
				return nil, err
			}
		}
		msgs = append(msgs, Msg{To: m.To, Body: b})
	}
	for _, r := range out.Replies {
		msgs = append(msgs, Msg{To: p.c.addrs[r.Client], Body: &replyBody{r}})
	}
	return msgs, nil
}

type ledgerClient struct {
	c       *Cluster
	addr    uint64
	nodes   []uint64
	id      u128.Uint128
	s       *client.Session
	rng     *rand.Rand
	now     func() uint64
	ready   bool
	session uint64
	left    int
	wait    int
	pause   int
	call    int
	batch   workload.Batch
	wrote   uint64
	held    u128.Uint128
	err     error
}

func (p *ledgerClient) join() {
	p.id = u128.Uint128{Hi: p.rng.Uint64(), Lo: p.rng.Uint64()}
	p.c.addrs[p.id] = p.addr
	p.s = client.New(p.id, p.nodes, p.c.retry)
	p.ready = false
}

func (p *ledgerClient) Tick() {
	if p.wait > 0 {
		p.wait--
		return
	}
	p.s.Tick()
	if p.pause > 0 {
		p.pause--
	}
	p.submit()
}

func (p *ledgerClient) submit() {
	c := p.c
	if !p.ready || p.call >= 0 || p.left == 0 || p.pause > 0 || p.now()+drainTime >= c.w.plan.end || !c.admit() {
		return
	}
	var op client.Op
	var body []byte
	odds := lookupOdds
	if c.cut() {
		odds = cutLookups
	}
	if p.rng.IntN(odds) == 0 {
		op, body = p.read()
		p.batch = workload.Batch{}
	} else {
		p.batch = c.gen.Batch()
		op, body = client.CreateTransfers, p.batch.Body
		if p.batch.Op == state.CreateAccounts {
			op = client.CreateAccounts
		}
	}
	if err := p.s.Submit(op, body); err != nil {
		p.err = err
		return
	}
	p.call = len(c.calls)
	c.calls = append(c.calls, Call{Client: p.addr - c.Nodes(), ID: p.id, Session: p.session, Op: op, Body: body, Start: p.now()})
	p.left--
}

func (p *ledgerClient) read() (client.Op, []byte) {
	pick := p.rng.IntN(readKinds)
	switch {
	case pick == 0 && p.rng.IntN(2) == 0:
		return client.Links, binary.BigEndian.AppendUint64(make([]byte, 8), math.MaxUint64)
	case pick == 0:
		from := p.rng.Uint64N(4 * linkSpan)
		return client.Links, binary.BigEndian.AppendUint64(binary.BigEndian.AppendUint64(nil, from), from+p.rng.Uint64N(linkSpan+1))
	case pick <= 2:
		return client.Head, nil
	case pick == 3 && p.wrote > 0:
		return client.Prove, binary.BigEndian.AppendUint64([]byte{audit.KindBatch}, p.wrote)
	}
	accounts, ids := p.c.gen.Lookup()
	if pick > 6 {
		if accounts {
			return client.LookupAccounts, client.AppendIDs(nil, ids)
		}
		return client.LookupTransfers, client.AppendIDs(nil, ids)
	}
	kind := byte(audit.KindAccount)
	switch {
	case !accounts && p.rng.IntN(3) == 0:
		kind = audit.KindExpiry
	case !accounts:
		kind = audit.KindTransfer
	}
	var id u128.Uint128
	switch {
	case kind == audit.KindExpiry && !p.held.IsZero():
		id = p.held
	case len(ids) > 0:
		id = ids[0]
	}
	return client.Prove, id.AppendBytes([]byte{kind})
}

func (p *ledgerClient) Receive(m Msg) {
	b, ok := m.Body.(*replyBody)
	if !ok {
		return
	}
	p.s.Step(b.Reply)
	r, done := p.s.Result()
	switch {
	case !done:
		return
	case p.call < 0:
		p.ready = r.Status == client.OK
		p.session = p.s.Since()
		return
	}
	call := &p.c.calls[p.call]
	call.End, call.Reply = p.now(), r
	p.call = -1
	p.c.open--
	switch {
	case r.Status == client.OK && !call.Op.Lookup():
		fails := client.DecodeFailures(r.Body)
		p.c.gen.Settle(p.batch, fails)
		p.wrote = r.Time
		if call.Op == client.CreateTransfers && p.held.IsZero() {
			p.held = pending(p.batch.Body, fails)
		}
	case call.Op == client.Prove && call.Body[0] == audit.KindExpiry:
		p.held = u128.Uint128{}
	}
	switch {
	case p.c.churn && (r.Status == client.SessionUnknown || p.rng.IntN(rejoinOdds) == 0):
		p.join()
	case r.Status == client.SessionUnknown:
		p.left = 0
	}
	p.pause = p.rng.IntN(thinkTicks)
}

func pending(body []byte, fails []state.Failure) u128.Uint128 {
	for i := 0; len(body) >= state.TransferSize; i, body = i+1, body[state.TransferSize:] {
		tr := state.DecodeTransfer(body)
		if tr.Flags&state.Pending != 0 && tr.Timeout > 0 && !slices.ContainsFunc(fails, func(f state.Failure) bool { return f.Index == i }) {
			return tr.ID
		}
	}
	return u128.Uint128{}
}

func (p *ledgerClient) Run() ([]Msg, error) {
	if p.err != nil || p.wait > 0 {
		return nil, p.err
	}
	out := p.s.Outbox()
	msgs := make([]Msg, len(out))
	for i, m := range out {
		msgs[i] = Msg{To: m.To, Body: &requestBody{m.Request}}
	}
	return msgs, nil
}

type raftBody struct {
	raft.Message
	Snap []byte
}

func (b *raftBody) Clone() Body {
	frames, err := wire.AppendMessage(nil, clusterID, b.Message)
	if err != nil {
		panic(err)
	}
	if b.Type == raft.MsgSnap {
		for chunk := range slices.Chunk(b.Snap, wire.ChunkSize) {
			frames = wire.AppendChunk(frames, clusterID, chunk)
		}
		crc := crc32.Checksum(b.Snap, crc32.MakeTable(crc32.Castagnoli))
		frames = wire.AppendSnapshotEnd(frames, clusterID, uint64(len(b.Snap)), crc)
	}
	f, frames, err := wire.Decode(frames, clusterID, wire.MaxPeerBody)
	if err != nil {
		panic(err)
	}
	c := &raftBody{}
	if c.Message, err = f.Message(); err != nil {
		panic(err)
	}
	if c.Type != raft.MsgSnap {
		return c
	}
	c.Snap = []byte{}
	for {
		if f, frames, err = wire.Decode(frames, clusterID, wire.MaxPeerBody); err != nil {
			panic(err)
		}
		if f.Type != wire.TypeSnapshotChunk {
			break
		}
		chunk, err := f.Chunk()
		if err != nil {
			panic(err)
		}
		c.Snap = append(c.Snap, chunk...)
	}
	size, crc, err := f.SnapshotEnd()
	if err != nil {
		panic(err)
	}
	if size != uint64(len(c.Snap)) || crc != crc32.Checksum(c.Snap, crc32.MakeTable(crc32.Castagnoli)) {
		panic(fmt.Sprintf("sim: snapshot of %d bytes arrived as %d", size, len(c.Snap)))
	}
	return c
}

func (b *raftBody) String() string {
	m := b.Message
	s := fmt.Sprintf("%s t%d i%d/%d c%d s%d", m.Type, m.Term, m.Index, m.LogTerm, m.Commit, m.Seq)
	if m.Reject {
		s += " reject"
	}
	if k := len(m.Entries); k > 0 {
		s += fmt.Sprintf(" e%d-%d", m.Entries[0].Index, m.Entries[k-1].Index)
	}
	if b.Snap != nil {
		s += fmt.Sprintf(" snap %dB", len(b.Snap))
	}
	return s
}

type requestBody struct {
	client.Request
}

func (b *requestBody) Clone() Body {
	f, _, err := wire.Decode(wire.AppendRequest(nil, clusterID, b.Request), clusterID, wire.MaxClientBody)
	if err != nil {
		panic(err)
	}
	req, err := f.Request()
	if err != nil {
		panic(err)
	}
	return &requestBody{req}
}

func (b *requestBody) String() string {
	return fmt.Sprintf("req %016x #%d %s %dB", b.Client.Lo, b.Number, b.Op, len(b.Body))
}

type replyBody struct {
	client.Reply
}

func (b *replyBody) Clone() Body {
	f, _, err := wire.Decode(wire.AppendReply(nil, clusterID, b.Reply), clusterID, wire.MaxClientBody)
	if err != nil {
		panic(err)
	}
	r, err := f.Reply()
	if err != nil {
		panic(err)
	}
	return &replyBody{r}
}

func (b *replyBody) String() string {
	return fmt.Sprintf("reply %016x #%d %s %s n%d t%d %dB", b.Client.Lo, b.Number, b.Op, b.Status, b.Leader, b.Time, len(b.Body))
}
