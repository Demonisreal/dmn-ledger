package replica

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"math/rand/v2"
	"testing"

	"github.com/Demonisreal/dmn-ledger/internal/client"
	"github.com/Demonisreal/dmn-ledger/internal/disk"
	"github.com/Demonisreal/dmn-ledger/internal/raft"
	"github.com/Demonisreal/dmn-ledger/internal/state"
	"github.com/Demonisreal/dmn-ledger/internal/u128"
	"github.com/Demonisreal/dmn-ledger/internal/wal"
)

const (
	tickNs = 10_000_000
	start  = 1_000_000_000_000
)

type envelope struct {
	m    raft.Message
	snap []byte
}

type testNet struct {
	t       testing.TB
	nodes   []*Node
	disks   []*disk.Sim
	cfgs    []Config
	clocks  []uint64
	queue   []envelope
	cut     map[uint64]bool
	drop    func(raft.Message) bool
	replies []client.Reply
	opens   uint64
	snaps   int
}

func newTestNet(t testing.TB, size int, tune func(*Config)) *testNet {
	t.Helper()
	tn := &testNet{t: t, cut: make(map[uint64]bool)}
	peers := make([]uint64, size)
	for i := range peers {
		peers[i] = uint64(i) + 1
	}
	for i := range size {
		cfg := Config{
			ID:             peers[i],
			Peers:          peers,
			Dir:            "/n",
			MaxSessions:    16,
			SnapshotEvery:  1000,
			ClockStep:      tickNs + tickNs/4,
			ElectionTicks:  10,
			HeartbeatTicks: 2,
			SnapshotTicks:  50,
			MaxAppendBytes: 1 << 20,
			MaxUncommitted: 4096,
		}
		if tune != nil {
			tune(&cfg)
		}
		tn.cfgs = append(tn.cfgs, cfg)
		tn.disks = append(tn.disks, disk.NewSim(rand.New(rand.NewPCG(uint64(i), 9))))
		tn.clocks = append(tn.clocks, start)
		tn.nodes = append(tn.nodes, nil)
		tn.open(i)
	}
	return tn
}

func (tn *testNet) open(i int) {
	tn.t.Helper()
	cfg := tn.cfgs[i]
	cfg.FS = tn.disks[i]
	cfg.Clock = func() uint64 { return tn.clocks[i] }
	tn.opens++
	cfg.Rand = rand.New(rand.NewPCG(cfg.ID, tn.opens))
	cfg.Nonces = rand.NewChaCha8([32]byte{byte(cfg.ID)})
	n, err := Open(cfg)
	if err != nil {
		tn.t.Fatalf("open n%d: %v", i+1, err)
	}
	tn.nodes[i] = n
}

func (tn *testNet) crash(i int) {
	tn.nodes[i] = nil
	tn.disks[i].Crash()
}

func (tn *testNet) run(i int) {
	tn.t.Helper()
	n := tn.nodes[i]
	out, err := n.Process()
	if err != nil {
		tn.t.Fatalf("n%d: %v", i+1, err)
	}
	for _, m := range out.Messages {
		e := envelope{m: m}
		if m.Type == raft.MsgSnap {
			err := n.ReadSnapshot(raft.Snapshot{Index: m.Index, Term: m.LogTerm}, func(r io.Reader) error {
				var err error
				e.snap, err = io.ReadAll(r)
				return err
			})
			if errors.Is(err, wal.ErrNoSnapshot) {
				continue
			}
			if err != nil {
				tn.t.Fatal(err)
			}
		}
		tn.queue = append(tn.queue, e)
	}
	tn.replies = append(tn.replies, out.Replies...)
}

func (tn *testNet) deliver() {
	tn.t.Helper()
	for len(tn.queue) > 0 {
		e := tn.queue[0]
		tn.queue = tn.queue[1:]
		to := tn.nodes[e.m.To-1]
		if tn.cut[e.m.To] || tn.cut[e.m.From] || to == nil || tn.drop != nil && tn.drop(e.m) {
			continue
		}
		var snap io.Reader
		if e.snap != nil {
			snap = bytes.NewReader(e.snap)
			tn.snaps++
		}
		to.Receive(e.m, snap)
		tn.run(int(e.m.To - 1))
	}
}

func (tn *testNet) tick(ids ...int) {
	tn.t.Helper()
	for _, i := range ids {
		if tn.nodes[i] == nil {
			continue
		}
		tn.clocks[i] += tickNs
		tn.nodes[i].Tick()
		tn.run(i)
	}
	tn.deliver()
}

func (tn *testNet) all() []int {
	ids := make([]int, len(tn.nodes))
	for i := range ids {
		ids[i] = i
	}
	return ids
}

func (tn *testNet) elect(i int) {
	tn.t.Helper()
	for range 200 {
		if tn.nodes[i].Status().Role == raft.Leader && tn.nodes[i].Status().Commit >= tn.nodes[i].Status().Last {
			return
		}
		tn.tick(i)
	}
	tn.t.Fatalf("n%d did not become leader: %+v", i+1, tn.nodes[i].Status())
}

func (tn *testNet) take() []client.Reply {
	r := tn.replies
	tn.replies = nil
	return r
}

func (tn *testNet) send(i int, req client.Request) {
	tn.t.Helper()
	tn.nodes[i].Request(req)
	tn.run(i)
	tn.deliver()
}

func (tn *testNet) call(i int, req client.Request) client.Reply {
	tn.t.Helper()
	tn.send(i, req)
	for range 20 {
		for _, r := range tn.take() {
			if r.Client == req.Client && r.Number == req.Number && r.Op == req.Op {
				return r
			}
		}
		tn.tick(i)
	}
	tn.t.Fatalf("no reply to %+v", req)
	return client.Reply{}
}

type tester struct {
	tn      *testNet
	node    int
	id      u128.Uint128
	session uint64
	number  uint64
}

func (tn *testNet) register(node int, id uint64) *tester {
	tn.t.Helper()
	c := &tester{tn: tn, node: node, id: u128.From64(id)}
	r := tn.call(node, client.Request{Client: c.id, Op: client.Register})
	if r.Status != client.OK || len(r.Body) != 8 {
		tn.t.Fatalf("register: %+v", r)
	}
	c.session = binary.BigEndian.Uint64(r.Body)
	return c
}

func (c *tester) request(op client.Op, body []byte) client.Request {
	c.number++
	return client.Request{Client: c.id, Session: c.session, Number: c.number, Op: op, Body: body}
}

func (c *tester) do(op client.Op, body []byte) client.Reply {
	c.tn.t.Helper()
	return c.tn.call(c.node, c.request(op, body))
}

func accounts(ids ...uint64) []byte {
	var b []byte
	for _, id := range ids {
		b = state.AppendAccount(b, state.Account{ID: u128.From64(id), Ledger: 1, Code: 1})
	}
	return b
}

func transfer(id, dr, cr, amount uint64) []byte {
	return state.AppendTransfer(nil, state.Transfer{
		ID:              u128.From64(id),
		DebitAccountID:  u128.From64(dr),
		CreditAccountID: u128.From64(cr),
		Amount:          u128.From64(amount),
		Ledger:          1,
		Code:            1,
	})
}

func ids(vals ...uint64) []byte {
	var b []byte
	for _, v := range vals {
		b = u128.From64(v).AppendBytes(b)
	}
	return b
}
