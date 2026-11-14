package replica

import (
	"bytes"
	"encoding/binary"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/Demonisreal/dmn-ledger/internal/client"
	"github.com/Demonisreal/dmn-ledger/internal/raft"
	"github.com/Demonisreal/dmn-ledger/internal/state"
	"github.com/Demonisreal/dmn-ledger/internal/u128"
)

func hold(id uint64, timeout uint32) []byte {
	return state.AppendTransfer(nil, state.Transfer{
		ID:              u128.From64(id),
		DebitAccountID:  u128.From64(1),
		CreditAccountID: u128.From64(2),
		Amount:          u128.From64(5),
		Ledger:          1,
		Code:            1,
		Flags:           state.Pending,
		Timeout:         timeout,
	})
}

func TestStatusShowsTimeAndNextExpiry(t *testing.T) {
	tn := newTestNet(t, 3, nil)
	tn.elect(0)
	c := tn.register(0, 1)
	shows := func(step string, time, next uint64) {
		t.Helper()
		tn.tick(tn.all()...)
		tn.tick(tn.all()...)
		for i, n := range tn.nodes {
			if st := n.Status(); st.Time != time || st.NextExpiry != next {
				t.Fatalf("%s: n%d shows time %d and next expiry %d, want %d and %d", step, i+1, st.Time, st.NextExpiry, time, next)
			}
		}
	}
	shows("empty ledger", 0, 0)

	const sec = 1_000_000_000
	made := ok(t, c.do(client.CreateAccounts, accounts(1, 2)))
	shows("two accounts", made.Time+1, 0)
	long := ok(t, c.do(client.CreateTransfers, hold(10, 30)))
	shows("first hold", long.Time, long.Time+30*sec)
	short := ok(t, c.do(client.CreateTransfers, hold(11, 5)))
	shows("shorter hold", short.Time, short.Time+5*sec)
	void := ok(t, c.do(client.CreateTransfers, state.AppendTransfer(nil, state.Transfer{
		ID: u128.From64(12), PendingID: u128.From64(11), Flags: state.VoidPending,
	})))
	if len(void.Body) != 0 {
		t.Fatalf("void failed: %x", void.Body)
	}
	shows("shorter hold voided", void.Time, long.Time+30*sec)
}

func TestLookupOverTheReplyLimitIsMalformed(t *testing.T) {
	tn := newTestNet(t, 1, nil)
	tn.elect(0)
	c := tn.register(0, 1)
	ok(t, c.do(client.CreateAccounts, accounts(1, 2)))
	ok(t, c.do(client.CreateTransfers, transfer(10, 1, 2, 5)))
	for _, tc := range []struct {
		op    client.Op
		id    uint64
		size  int
		limit int
	}{
		{client.LookupAccounts, 1, state.AccountSize, 10_922},
		{client.LookupTransfers, 10, state.TransferSize, 10_485},
	} {
		most := bytes.Repeat(ids(tc.id), tc.limit)
		r := ok(t, c.do(tc.op, most))
		if len(r.Body) != tc.limit*tc.size || len(r.Body) > client.MaxBody {
			t.Fatalf("%s of %d ids: reply of %d bytes, want %d and at most %d", tc.op, tc.limit, len(r.Body), tc.limit*tc.size, client.MaxBody)
		}
		if r := c.do(tc.op, append(most, ids(tc.id)...)); r.Status != client.Malformed {
			t.Fatalf("%s of %d ids: %s with %d bytes, want Malformed", tc.op, tc.limit+1, r.Status, len(r.Body))
		}
	}
}

func TestTicksInARowBeforeProcess(t *testing.T) {
	const electionTicks = 50
	tn := newTestNet(t, 3, func(cfg *Config) { cfg.ElectionTicks, cfg.HeartbeatTicks = electionTicks, 1 })
	r := rand.New(rand.NewPCG(7, electionTicks))
	burst := func() {
		t.Helper()
		for i, n := range tn.nodes {
			for range 1 + r.IntN(electionTicks-1) {
				tn.clocks[i] += tickNs
				n.Tick()
			}
			tn.run(i)
		}
		tn.deliver()
	}
	lead := -1
	for round := 0; lead < 0; round++ {
		if round == electionTicks {
			t.Fatalf("no leader after %d rounds", round)
		}
		burst()
		lead = slices.IndexFunc(tn.nodes, func(n *Node) bool { return n.Status().Role == raft.Leader })
	}
	term := tn.nodes[lead].Status().Term

	c := &tester{tn: tn, node: lead, id: u128.From64(1)}
	call := func(req client.Request) client.Reply {
		t.Helper()
		tn.nodes[lead].Request(req)
		burst()
		got := tn.take()
		if len(got) != 1 || got[0].Status != client.OK || got[0].Number != req.Number {
			t.Fatalf("%s #%d: replies %+v, want OK in the round it was sent", req.Op, req.Number, got)
		}
		return got[0]
	}
	write := func(op client.Op, body []byte) {
		t.Helper()
		reply := call(c.request(op, body))
		if len(reply.Body) != 0 || reply.Time != tn.clocks[lead] {
			t.Fatalf("%s #%d: failures %x at %d, want none at the leader's clock %d", op, reply.Number, reply.Body, reply.Time, tn.clocks[lead])
		}
	}
	c.session = binary.BigEndian.Uint64(call(client.Request{Client: c.id, Op: client.Register}).Body)
	write(client.CreateAccounts, accounts(1, 2))
	const transfers = 40
	for i := range uint64(transfers) {
		write(client.CreateTransfers, transfer(10+i, 1, 2, 1))
	}

	burst()
	for i, n := range tn.nodes {
		if st := n.Status(); st.Term != term || st.Lead != uint64(lead)+1 {
			t.Fatalf("n%d follows n%d in term %d, want n%d in term %d", i+1, st.Lead, st.Term, lead+1, term)
		}
		if n.Digest() != tn.nodes[lead].Digest() {
			t.Fatalf("n%d and n%d differ: %+v, %+v", i+1, lead+1, n.Status(), tn.nodes[lead].Status())
		}
		found := n.sm.LookupAccounts([]u128.Uint128{u128.From64(1)}, nil)
		if len(found) != 1 || found[0].DebitsPosted != u128.From64(transfers) {
			t.Fatalf("n%d holds %+v, want one account with %d posted debits", i+1, found, transfers)
		}
	}
}
