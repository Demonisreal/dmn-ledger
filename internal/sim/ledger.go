package sim

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"slices"

	"github.com/Demonisreal/dmn-ledger/internal/client"
	"github.com/Demonisreal/dmn-ledger/internal/disk"
	"github.com/Demonisreal/dmn-ledger/internal/raft"
	"github.com/Demonisreal/dmn-ledger/internal/replica"
	"github.com/Demonisreal/dmn-ledger/internal/state"
	"github.com/Demonisreal/dmn-ledger/internal/u128"
	"github.com/Demonisreal/dmn-ledger/internal/wal"
	"github.com/Demonisreal/dmn-ledger/internal/workload"
)

const (
	ledgerDir   = "/ledger"
	windowSize  = 32
	retryTicks  = 20
	thinkTicks  = 20
	lookupOdds  = 5
	maxSessions = 64
	drainTime   = second
)

type ClusterConfig struct {
	Seed    uint64
	Profile Profile
	Nodes   uint64
	Trace   io.Writer
	Disk    func(id uint64, fsys disk.FS) disk.FS
	Said    func(id uint64, msgs []raft.Message) error
	Check   func(c *Cluster) error
}

type Call struct {
	Client     uint64
	Op         client.Op
	Body       []byte
	Start, End uint64
	Reply      client.Reply
}

type Cluster struct {
	cfg       ClusterConfig
	w         *World
	gen       *workload.Gen
	snapEvery uint64
	segSize   int64
	addrs     map[u128.Uint128]uint64
	calls     []Call
	windows   []int
	started   int
	open      int
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
	nodes := cfg.Nodes
	if nodes == 0 {
		nodes = 3
		if cfg.Seed%5 == 4 {
			nodes = 5
		}
	}
	w, err := New(Config{
		Seed:    cfg.Seed,
		Profile: cfg.Profile,
		Nodes:   nodes,
		Clients: 3 + cfg.Seed%4,
		Node:    c.startNode,
		Client:  c.startClient,
		Disk:    cfg.Disk,
		Said:    c.said,
		Check:   c.check,
		Trace:   cfg.Trace,
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
	if c.cfg.Check == nil {
		return nil
	}
	return c.cfg.Check(c)
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

func (c *Cluster) startNode(env NodeEnv) (Process, error) {
	n, err := replica.Open(replica.Config{
		ID:             env.ID,
		Peers:          env.Peers,
		FS:             env.FS,
		Dir:            ledgerDir,
		Clock:          env.Clock,
		Rand:           env.Rand,
		Nonces:         env.Nonces,
		MaxSessions:    maxSessions,
		SnapshotEvery:  c.snapEvery,
		SegmentSize:    c.segSize,
		ClockStep:      2 * tick,
		ElectionTicks:  10,
		HeartbeatTicks: 2,
		SnapshotTicks:  50,
		MaxAppendBytes: 1 << 20,
		MaxUncommitted: 4096,
	})
	if err != nil {
		return nil, err
	}
	return &ledgerNode{c: c, n: n}, nil
}

func (c *Cluster) startClient(env ClientEnv) Process {
	id := u128.Uint128{Hi: env.Rand.Uint64(), Lo: env.Rand.Uint64()}
	c.addrs[id] = env.ID
	return &ledgerClient{
		c:    c,
		addr: env.ID,
		s:    client.New(id, env.Nodes, retryTicks),
		rng:  env.Rand,
		now:  env.Now,
		left: 30 + env.Rand.IntN(31),
		call: -1,
	}
}

type ledgerNode struct {
	c *Cluster
	n *replica.Node
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
	c     *Cluster
	addr  uint64
	s     *client.Session
	rng   *rand.Rand
	now   func() uint64
	ready bool
	left  int
	pause int
	call  int
	batch workload.Batch
	err   error
}

func (p *ledgerClient) Tick() {
	p.s.Tick()
	p.pause--
	p.submit()
}

func (p *ledgerClient) submit() {
	c := p.c
	if !p.ready || p.call >= 0 || p.left == 0 || p.pause > 0 || p.now()+drainTime >= c.w.plan.end || !c.admit() {
		return
	}
	var op client.Op
	var body []byte
	if p.rng.IntN(lookupOdds) == 0 {
		accounts, ids := c.gen.Lookup()
		op = client.LookupTransfers
		if accounts {
			op = client.LookupAccounts
		}
		for _, id := range ids {
			body = id.AppendBytes(body)
		}
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
	c.calls = append(c.calls, Call{Client: p.addr, Op: op, Body: body, Start: p.now()})
	p.left--
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
		return
	}
	call := &p.c.calls[p.call]
	call.End, call.Reply = p.now(), r
	p.call = -1
	p.c.open--
	if r.Status == client.OK && !call.Op.Lookup() {
		p.c.gen.Settle(p.batch, failures(r.Body))
	}
	if r.Status == client.SessionUnknown {
		p.left = 0
	}
	p.pause = p.rng.IntN(thinkTicks)
}

func (p *ledgerClient) Run() ([]Msg, error) {
	if p.err != nil {
		return nil, p.err
	}
	out := p.s.Outbox()
	msgs := make([]Msg, len(out))
	for i, m := range out {
		msgs[i] = Msg{To: m.To, Body: &requestBody{m.Request}}
	}
	return msgs, nil
}

func failures(body []byte) []state.Failure {
	var fails []state.Failure
	for ; len(body) >= 8; body = body[8:] {
		fails = append(fails, state.Failure{
			Index:  int(binary.BigEndian.Uint32(body)),
			Reason: state.Reason(binary.BigEndian.Uint32(body[4:])),
		})
	}
	return fails
}

type raftBody struct {
	raft.Message
	Snap []byte
}

func (b *raftBody) Clone() Body {
	c := *b
	if b.Entries != nil {
		c.Entries = make([]raft.Entry, len(b.Entries))
		for i, e := range b.Entries {
			e.Body = slices.Clone(e.Body)
			c.Entries[i] = e
		}
	}
	c.Snap = slices.Clone(b.Snap)
	return &c
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
	c := *b
	c.Body = slices.Clone(b.Body)
	return &c
}

func (b *requestBody) String() string {
	return fmt.Sprintf("req %016x #%d %s %dB", b.Client.Lo, b.Number, b.Op, len(b.Body))
}

type replyBody struct {
	client.Reply
}

func (b *replyBody) Clone() Body {
	c := *b
	c.Body = slices.Clone(b.Body)
	return &c
}

func (b *replyBody) String() string {
	return fmt.Sprintf("reply %016x #%d %s %s n%d t%d %dB", b.Client.Lo, b.Number, b.Op, b.Status, b.Leader, b.Time, len(b.Body))
}
