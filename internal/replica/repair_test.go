package replica

import (
	"errors"
	"slices"
	"testing"

	"github.com/Demonisreal/dmn-ledger/internal/client"
	"github.com/Demonisreal/dmn-ledger/internal/raft"
	"github.com/Demonisreal/dmn-ledger/internal/wal"
)

func written(t *testing.T, count uint64) (*testNet, *tester) {
	t.Helper()
	tn := newTestNet(t, 3, nil)
	tn.elect(0)
	c := tn.register(0, 1)
	for i := range count {
		ok(t, c.do(client.CreateAccounts, accounts(i+1)))
	}
	tn.tick(tn.all()...)
	return tn, c
}

func TestBitFlipInTheLastSealedBatchIsRepaired(t *testing.T) {
	tn, c := written(t, 5)
	term, last := tn.nodes[0].Status().Term, tn.nodes[2].Status().Last
	tn.spoilTail(2)
	if got := tn.nodes[2].Status().Last; got >= last {
		t.Fatalf("n3 still ends at %d of %d after losing a frame", got, last)
	}
	for i := range uint64(5) {
		if r := ok(t, c.do(client.CreateAccounts, accounts(i+10))); r.Leader != 1 {
			t.Fatalf("answer from n%d, the leader was n1", r.Leader)
		}
		tn.tick(tn.all()...)
	}
	tn.converge(2)
	if got := tn.nodes[0].Status(); got.Role != raft.Leader || got.Term != term {
		t.Fatalf("the repair cost n1 its term: %+v", got)
	}
	tn.crash(2)
	tn.open(2)
	if st := tn.nodes[2].Status(); st.Repair {
		t.Fatalf("n3 is in repair again after a restart: %+v", st)
	}
	tn.crash(0)
	lead := tn.choose(1, 2)
	c.node = lead
	ok(t, c.do(client.CreateAccounts, accounts(20)))
}

func TestDamagedLeaderDoesNotRunAgain(t *testing.T) {
	tn, c := written(t, 5)
	tn.spoilTail(0)
	tn.drop = func(m raft.Message) bool {
		if m.From == 1 && tn.nodes[0].Status().Repair && (m.Type == raft.MsgPreVote || m.Type == raft.MsgVote) {
			t.Errorf("n1 sent %s while in repair", m.Type)
		}
		return false
	}
	for range 40 {
		tn.tick(tn.all()...)
	}
	lead := tn.choose(tn.all()...)
	if lead == 0 {
		t.Fatalf("the damaged n1 leads again: %+v", tn.nodes[0].Status())
	}
	c.node = lead
	ok(t, c.do(client.CreateAccounts, accounts(10)))
	tn.converge(0)
}

func TestTwoDamagedNodesOfThreeElectNoOne(t *testing.T) {
	tn, c := written(t, 5)
	tn.spoilTail(1)
	tn.spoilTail(2)
	tn.crash(0)
	tn.open(0)
	for range 300 {
		tn.tick(tn.all()...)
		for i, n := range tn.nodes {
			if st := n.Status(); st.Role == raft.Leader {
				t.Fatalf("n%d leads with two nodes in repair: %+v", i+1, st)
			}
		}
	}
	req := c.request(client.CreateAccounts, accounts(10))
	for i := range tn.nodes {
		tn.send(i, req)
	}
	replies := tn.take()
	if len(replies) != 3 {
		t.Fatalf("replies %+v, want one refusal per node", replies)
	}
	for _, r := range replies {
		if r.Status != client.NotLeader || r.Leader != 0 {
			t.Fatalf("reply %+v, want NotLeader without a leader", r)
		}
	}
}

func TestRepairSurvivesALeaderChangeAndACrash(t *testing.T) {
	tn, c := written(t, 5)
	term := tn.nodes[0].Status().Term
	tn.spoilTail(2)
	goals := make(map[uint64]int)
	hold := true
	tn.drop = func(m raft.Message) bool {
		if m.To == 3 && m.Type == raft.MsgRepairInfo {
			goals[m.Term]++
		}
		return hold && m.To == 3 && m.Type == raft.MsgApp && len(m.Entries) > 0
	}
	for range 6 {
		tn.tick(tn.all()...)
	}
	tn.cut[1] = true
	for range 15 {
		tn.tick(tn.all()...)
	}
	tn.cut[1] = false
	lead := tn.choose(tn.all()...)
	if st := tn.nodes[lead].Status(); st.Term == term || !tn.nodes[2].Status().Repair || goals[term] == 0 {
		t.Fatalf("leader %+v after term %d, n3 %+v with %d goals: want a new term with n3 still in repair", st, term, tn.nodes[2].Status(), goals[term])
	}
	tn.crash(2)
	tn.open(2)
	if st := tn.nodes[2].Status(); !st.Repair {
		t.Fatalf("a restart ended the repair: %+v", st)
	}
	hold = false
	tn.converge(2)
	if len(goals) < 2 {
		t.Fatalf("n3 got goals in terms %v, want one from each leader", goals)
	}
	c.node = tn.choose(tn.all()...)
	ok(t, c.do(client.CreateAccounts, accounts(10)))
	tn.crash(2)
	tn.open(2)
	if st := tn.nodes[2].Status(); st.Repair {
		t.Fatalf("n3 is in repair again after it was done: %+v", st)
	}
}

func TestBitFlipFoundWhileApplyingStopsTheNode(t *testing.T) {
	tn, c := written(t, 3)
	target := tn.nodes[2].Status().Last + 1
	tn.drop = func(m raft.Message) bool { return m.To == 3 && m.Commit >= target }
	ok(t, c.do(client.CreateAccounts, accounts(10)))
	tn.tick(tn.all()...)
	if st := tn.nodes[2].Status(); st.Last != target || st.Applied >= target {
		t.Fatalf("n3 %+v, want entry %d stored and not applied", st, target)
	}
	logs := stored(tn.disks[2], ".log")
	tn.flip(2, logs[len(logs)-1].Name, logs[len(logs)-1].Size-10)
	var stopped error
	tn.halt = func(_ int, err error) { stopped = err }
	applied := tn.nodes[2].Status().Applied
	tn.drop = func(raft.Message) bool {
		if n := tn.nodes[2]; n != nil && n.Status().Applied != applied {
			t.Fatalf("n3 applied %d, entry %d is broken", n.Status().Applied, target)
		}
		return false
	}
	for range 5 {
		tn.tick(tn.all()...)
	}
	if !errors.Is(stopped, wal.ErrCorrupt) || tn.nodes[2] != nil {
		t.Fatalf("n3 read a broken entry and kept going: %v", stopped)
	}
	tn.drop = nil
	tn.open(2)
	if st := tn.nodes[2].Status(); !st.Repair || st.Last >= target {
		t.Fatalf("restart after a broken entry: %+v, want repair with a log before %d", st, target)
	}
	tn.converge(2)
}

func TestBitFlipFoundWhileSendingStopsTheLeader(t *testing.T) {
	tn, c := written(t, 3)
	tn.cut[3] = true
	for i := range uint64(3) {
		ok(t, c.do(client.CreateAccounts, accounts(i+10)))
	}
	logs := stored(tn.disks[0], ".log")
	tn.flip(0, logs[len(logs)-1].Name, logs[len(logs)-1].Size-10)
	broken := tn.nodes[0].Status().Last
	var stopped error
	tn.halt = func(_ int, err error) { stopped = err }
	tn.cut[3] = false
	sent := false
	tn.drop = func(m raft.Message) bool {
		sent = sent || m.From == 1 && slices.ContainsFunc(m.Entries, func(e raft.Entry) bool { return e.Index == broken })
		return false
	}
	for range 10 {
		tn.tick(tn.all()...)
	}
	if !errors.Is(stopped, wal.ErrCorrupt) || tn.nodes[0] != nil || sent {
		t.Fatalf("n1 with a broken entry: stopped with %v, sent it: %t", stopped, sent)
	}
	tn.drop = nil
	tn.open(0)
	if st := tn.nodes[0].Status(); !st.Repair {
		t.Fatalf("restart after a broken entry: %+v, want repair", st)
	}
	c.node = tn.choose(1, 2)
	ok(t, c.do(client.CreateAccounts, accounts(20)))
	tn.converge(0)
}
