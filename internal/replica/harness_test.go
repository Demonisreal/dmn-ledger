package replica

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"strings"
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

type receipt struct {
	term, vote     uint64
	ackTerm, acked uint64
	commit         uint64
}

func (r receipt) kept(st Status) error {
	switch {
	case st.Term < r.term:
		return fmt.Errorf("term %d on disk, %d was promised", st.Term, r.term)
	case st.Term == r.term && r.vote != 0 && st.Vote != r.vote:
		return fmt.Errorf("vote for n%d on disk, n%d was promised in term %d", st.Vote, r.vote, r.term)
	case st.Repair:
		return nil
	case st.Term == r.ackTerm && st.Last < r.acked:
		return fmt.Errorf("log ends at %d, %d was acknowledged in term %d, and the node is not in repair", st.Last, r.acked, r.ackTerm)
	case st.Last < r.commit:
		return fmt.Errorf("log ends at %d, the node knew %d as committed, and it is not in repair", st.Last, r.commit)
	}
	return nil
}

type testNet struct {
	t       testing.TB
	nodes   []*Node
	disks   []*disk.Sim
	cfgs    []Config
	clocks  []uint64
	held    []receipt
	queue   []envelope
	cut     map[uint64]bool
	drop    func(raft.Message) bool
	replies []client.Reply
	opens   uint64
	snaps   int
	tap     func(i int, fsys disk.FS) disk.FS
	halt    func(i int, err error)
}

func newTestNet(t testing.TB, size int, tune func(*Config)) *testNet {
	t.Helper()
	tn := stoppedTestNet(t, size, tune)
	for i := range size {
		tn.open(i)
	}
	return tn
}

func stoppedTestNet(t testing.TB, size int, tune func(*Config)) *testNet {
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
		tn.held = append(tn.held, receipt{})
	}
	return tn
}

func (tn *testNet) config(i int) Config {
	cfg := tn.cfgs[i]
	if cfg.FS == nil {
		cfg.FS = tn.disks[i]
	}
	if tn.tap != nil {
		cfg.FS = tn.tap(i, cfg.FS)
	}
	cfg.Clock = func() uint64 { return tn.clocks[i] }
	tn.opens++
	cfg.Rand = rand.New(rand.NewPCG(cfg.ID, tn.opens))
	cfg.Nonces = rand.NewChaCha8([32]byte{byte(cfg.ID)})
	return cfg
}

func (tn *testNet) open(i int) {
	tn.t.Helper()
	n, err := Open(tn.config(i))
	if err != nil {
		tn.t.Fatalf("open n%d: %v", i+1, err)
	}
	tn.nodes[i] = n
}

func (tn *testNet) probe(i int, fsys disk.FS) (Status, error) {
	cfg := tn.cfgs[i]
	cfg.FS = fsys
	cfg.Clock = func() uint64 { return tn.clocks[i] }
	cfg.Rand = rand.New(rand.NewPCG(cfg.ID, 0))
	cfg.Nonces = rand.NewChaCha8([32]byte{byte(cfg.ID)})
	n, err := Open(cfg)
	if err != nil {
		return Status{}, err
	}
	st := n.Status()
	return st, n.Close()
}

func (tn *testNet) crash(i int) {
	tn.nodes[i] = nil
	tn.disks[i].Crash()
}

func (tn *testNet) stop(i int) {
	tn.t.Helper()
	if err := tn.nodes[i].Close(); err != nil {
		tn.t.Fatal(err)
	}
	tn.nodes[i] = nil
}

func (tn *testNet) note(i int, msgs []raft.Message) {
	st, r := tn.nodes[i].Status(), &tn.held[i]
	r.term, r.vote, r.commit = st.Term, st.Vote, max(r.commit, st.Commit)
	for _, m := range msgs {
		if m.Type != raft.MsgAppResp || m.Reject || m.Index == 0 {
			continue
		}
		if m.Term != r.ackTerm {
			r.ackTerm, r.acked = m.Term, 0
		}
		r.acked = max(r.acked, m.Index)
	}
	if st.Role == raft.Leader {
		r.ackTerm, r.acked = st.Term, st.Last
	}
}

func (tn *testNet) run(i int) {
	tn.t.Helper()
	n := tn.nodes[i]
	out, err := n.Process()
	if err != nil && tn.halt != nil {
		tn.halt(i, err)
		tn.stop(i)
		return
	}
	if err != nil {
		tn.t.Fatalf("n%d: %v", i+1, err)
	}
	tn.note(i, out.Messages)
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

func (tn *testNet) choose(ids ...int) int {
	tn.t.Helper()
	for range 400 {
		for _, i := range ids {
			if st := tn.nodes[i].Status(); st.Role == raft.Leader && st.Commit >= st.Last {
				return i
			}
		}
		tn.tick(ids...)
	}
	tn.t.Fatalf("no leader among %v", ids)
	return 0
}

func stored(d *disk.Sim, suffix string) []disk.DurableFile {
	var files []disk.DurableFile
	for _, f := range d.Durable() {
		if f.Size > 0 && strings.HasSuffix(f.Name, suffix) {
			files = append(files, f)
		}
	}
	return files
}

func (tn *testNet) flip(i int, name string, off int64) {
	tn.t.Helper()
	if err := tn.disks[i].Corrupt(name, off); err != nil {
		tn.t.Fatal(err)
	}
}

func (tn *testNet) spoilTail(i int) {
	tn.t.Helper()
	tn.crash(i)
	logs := stored(tn.disks[i], ".log")
	tail := logs[len(logs)-1]
	tn.flip(i, tail.Name, tail.Size-10)
	tn.open(i)
	if st := tn.nodes[i].Status(); !st.Repair {
		tn.t.Fatalf("n%d opened a log with a broken sealed frame as a voter: %+v", i+1, st)
	}
}

func (tn *testNet) converge(i int) {
	tn.t.Helper()
	lead := tn.choose(tn.all()...)
	for range 200 {
		tn.tick(tn.all()...)
		if st := tn.nodes[i].Status(); !st.Repair && tn.nodes[i].Digest() == tn.nodes[lead].Digest() {
			return
		}
	}
	tn.t.Fatalf("n%d did not catch up with n%d: %+v", i+1, lead+1, tn.nodes[i].Status())
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
