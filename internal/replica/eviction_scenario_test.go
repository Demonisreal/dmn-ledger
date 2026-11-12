package replica

import (
	"testing"

	"github.com/Demonisreal/dmn-ledger/internal/client"
	"github.com/Demonisreal/dmn-ledger/internal/raft"
)

func (tn *testNet) takeOver(i int, others ...int) {
	tn.t.Helper()
	id := uint64(i) + 1
	tn.drop = func(m raft.Message) bool {
		return m.From != id && (m.Type == raft.MsgPreVote || m.Type == raft.MsgVote)
	}
	defer func() { tn.drop = nil }()
	for range 400 {
		st := tn.nodes[i].Status()
		if st.Role == raft.Leader && st.Commit >= st.Last && st.Applied >= st.Last {
			return
		}
		tn.tick(append([]int{i}, others...)...)
	}
	tn.t.Fatalf("n%d did not become leader: %+v", i+1, tn.nodes[i].Status())
}

func TestEvictionSurvivesSnapshotAndFailover(t *testing.T) {
	tn := newTestNet(t, 3, func(cfg *Config) { cfg.MaxSessions, cfg.SnapshotEvery, cfg.SegmentSize = 2, 4, 512 })
	tn.elect(0)
	tn.cut[3] = true
	a, b := tn.register(0, 1), tn.register(0, 2)
	ok(t, a.do(client.CreateAccounts, accounts(1, 2)))
	applied := b.request(client.CreateTransfers, transfer(10, 1, 2, 5))
	ok(t, tn.call(0, applied))
	for i := range uint64(20) {
		ok(t, a.do(client.CreateTransfers, transfer(100+i, 1, 2, 1)))
	}
	tn.register(0, 3)
	tn.cut[3] = false
	for range 100 {
		tn.tick(tn.all()...)
	}
	if tn.snaps == 0 {
		t.Fatal("no snapshot reached n3")
	}
	tn.crash(0)
	tn.takeOver(2, 1)
	if r := tn.call(2, applied); r.Status != client.SessionUnknown {
		t.Fatalf("retry of an applied request after eviction: %+v", r)
	}
	late := tn.register(2, 2)
	if late.session == b.session {
		t.Fatal("a late register revived the old session")
	}
	if r := tn.call(2, applied); r.Status != client.SessionUnknown {
		t.Fatalf("retry of an applied request after a late register: %+v", r)
	}
	a.node = 2
	if got := debits(t, a, 1); got != 25 {
		t.Fatalf("debits %d, want 25", got)
	}
	if tn.nodes[1].Digest() != tn.nodes[2].Digest() {
		t.Fatal("n2 and n3 differ")
	}
}
