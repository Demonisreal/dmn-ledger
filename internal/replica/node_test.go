package replica

import (
	"bytes"
	"math/rand/v2"
	"testing"

	"github.com/Demonisreal/dmn-ledger/internal/client"
	"github.com/Demonisreal/dmn-ledger/internal/raft"
	"github.com/Demonisreal/dmn-ledger/internal/state"
	"github.com/Demonisreal/dmn-ledger/internal/u128"
	"github.com/Demonisreal/dmn-ledger/internal/wal"
)

func ok(t *testing.T, r client.Reply) client.Reply {
	t.Helper()
	if r.Status != client.OK {
		t.Fatalf("reply %+v, want OK", r)
	}
	return r
}

func debits(t *testing.T, c *tester, id uint64) uint64 {
	t.Helper()
	r := ok(t, c.do(client.LookupAccounts, ids(id)))
	if len(r.Body) != state.AccountSize {
		t.Fatalf("lookup of %d returned %d bytes", id, len(r.Body))
	}
	return state.DecodeAccount(r.Body).DebitsPosted.Lo
}

func TestWriteThenRead(t *testing.T) {
	tn := newTestNet(t, 1, nil)
	tn.elect(0)
	c := tn.register(0, 1)
	r := ok(t, c.do(client.CreateAccounts, accounts(1, 2)))
	if len(r.Body) != 0 || r.Time < start || r.Leader != 1 {
		t.Fatalf("create accounts: %+v", r)
	}
	ok(t, c.do(client.CreateTransfers, transfer(10, 1, 2, 5)))
	r = ok(t, c.do(client.LookupAccounts, ids(1, 2, 3)))
	if len(r.Body) != 2*state.AccountSize || state.DecodeAccount(r.Body).DebitsPosted != u128.From64(5) {
		t.Fatalf("lookup: %d bytes", len(r.Body))
	}
	r = ok(t, c.do(client.CreateTransfers, transfer(11, 1, 9, 5)))
	if len(r.Body) != 8 {
		t.Fatalf("failed transfer: body %x, want one failure", r.Body)
	}
}

func TestRetryGetsTheStoredReply(t *testing.T) {
	tn := newTestNet(t, 1, nil)
	tn.elect(0)
	c := tn.register(0, 1)
	ok(t, c.do(client.CreateAccounts, accounts(1, 2)))
	req := c.request(client.CreateTransfers, append(transfer(10, 1, 2, 5), transfer(11, 1, 2, 7)...))
	first := ok(t, tn.call(0, req))
	again := ok(t, tn.call(0, req))
	if first.Time != again.Time || !bytes.Equal(first.Body, again.Body) {
		t.Fatalf("retry %+v differs from %+v", again, first)
	}
	if got := debits(t, c, 1); got != 12 {
		t.Fatalf("debits %d after a retried batch, want 12", got)
	}
}

func TestStaleAndUnknownSessions(t *testing.T) {
	tn := newTestNet(t, 1, nil)
	tn.elect(0)
	c := tn.register(0, 1)
	old := c.request(client.CreateAccounts, accounts(1))
	ok(t, tn.call(0, old))
	ok(t, c.do(client.CreateAccounts, accounts(2)))
	if r := tn.call(0, old); r.Status != client.Stale {
		t.Fatalf("old number: %+v, want Stale", r)
	}
	stranger := client.Request{Client: u128.From64(99), Session: c.session, Number: 1, Op: client.CreateAccounts, Body: accounts(3)}
	if r := tn.call(0, stranger); r.Status != client.SessionUnknown {
		t.Fatalf("unregistered client: %+v", r)
	}
	forged := c.request(client.CreateAccounts, accounts(3))
	forged.Session++
	if r := tn.call(0, forged); r.Status != client.SessionUnknown {
		t.Fatalf("wrong session number: %+v", r)
	}
}

func TestEvictionAndLateRegister(t *testing.T) {
	tn := newTestNet(t, 1, func(cfg *Config) { cfg.MaxSessions = 2 })
	tn.elect(0)
	a, b := tn.register(0, 1), tn.register(0, 2)
	ok(t, a.do(client.CreateAccounts, accounts(1, 2)))
	pending := b.request(client.CreateTransfers, transfer(10, 1, 2, 5))
	tn.register(0, 3)
	if n := tn.nodes[0].Status().Sessions; n != 2 {
		t.Fatalf("%d sessions, want 2", n)
	}
	if r := tn.call(0, pending); r.Status != client.SessionUnknown {
		t.Fatalf("evicted client: %+v", r)
	}
	late := tn.register(0, 2)
	if late.session == b.session {
		t.Fatal("a late register revived the evicted session")
	}
	if r := tn.call(0, pending); r.Status != client.SessionUnknown {
		t.Fatalf("request of the evicted session after a late register: %+v", r)
	}
	if got := debits(t, a, 1); got != 0 {
		t.Fatalf("debits %d, want 0", got)
	}
}

func TestMalformedRequestsWriteNothing(t *testing.T) {
	tn := newTestNet(t, 1, nil)
	tn.elect(0)
	c := tn.register(0, 1)
	last := tn.nodes[0].Status().Last
	for _, req := range []client.Request{
		c.request(client.CreateAccounts, accounts(1)[:95]),
		c.request(client.CreateTransfers, accounts(1)),
		c.request(client.LookupAccounts, make([]byte, 15)),
		c.request(client.CreateAccounts, make([]byte, (client.MaxBody/state.AccountSize+1)*state.AccountSize)),
		c.request(0, nil),
		c.request(client.LookupTransfers+1, nil),
		{Client: c.id, Op: client.Register, Body: []byte{1}},
	} {
		if r := tn.call(0, req); r.Status != client.Malformed {
			t.Fatalf("%s with %d bytes: %+v", req.Op, len(req.Body), r)
		}
	}
	if got := tn.nodes[0].Status().Last; got != last {
		t.Fatalf("log grew from %d to %d", last, got)
	}
}

func TestFollowerPointsToTheLeader(t *testing.T) {
	tn := newTestNet(t, 3, nil)
	tn.elect(0)
	tn.send(1, client.Request{Client: u128.From64(1), Op: client.Register})
	r := tn.take()
	if len(r) != 1 || r[0].Status != client.NotLeader || r[0].Leader != 1 {
		t.Fatalf("follower replied %+v", r)
	}
}

func TestLostLeadershipFailsWaitingRequests(t *testing.T) {
	tn := newTestNet(t, 3, nil)
	tn.elect(0)
	c := tn.register(0, 1)
	tn.cut[2], tn.cut[3] = true, true
	write, read := c.request(client.CreateAccounts, accounts(1)), c.request(client.LookupAccounts, ids(1))
	tn.send(0, write)
	tn.send(0, read)
	if r := tn.take(); len(r) != 0 {
		t.Fatalf("answered without a majority: %+v", r)
	}
	term := tn.nodes[0].Status().Term
	tn.nodes[0].Receive(raft.Message{Type: raft.MsgApp, From: 2, To: 1, Term: term + 1}, nil)
	tn.run(0)
	got := tn.take()
	if len(got) != 2 {
		t.Fatalf("replies %+v, want two", got)
	}
	for _, r := range got {
		if r.Status != client.NotLeader || r.Leader != 2 {
			t.Fatalf("reply %+v, want NotLeader pointing at n2", r)
		}
	}
}

func TestBusyWhenTheCoreIsFull(t *testing.T) {
	tn := newTestNet(t, 3, func(cfg *Config) { cfg.MaxUncommitted = 1 })
	tn.elect(0)
	c := tn.register(0, 1)
	tn.cut[2], tn.cut[3] = true, true
	tn.send(0, c.request(client.CreateAccounts, accounts(1)))
	busy := c.request(client.CreateAccounts, accounts(2))
	tn.send(0, busy)
	if r := tn.take(); len(r) != 1 || r[0].Status != client.Busy || r[0].Number != busy.Number {
		t.Fatalf("replies %+v, want Busy", r)
	}
}

func TestReadBeforeOwnCommitIsBusy(t *testing.T) {
	tn := newTestNet(t, 3, nil)
	tn.drop = func(m raft.Message) bool { return m.Type == raft.MsgApp }
	for tn.nodes[0].Status().Role != raft.Leader {
		tn.tick(0)
	}
	if st := tn.nodes[0].Status(); st.Commit >= st.Last {
		t.Fatalf("leader committed its entry without appends: %+v", st)
	}
	tn.send(0, client.Request{Client: u128.From64(1), Number: 1, Op: client.LookupAccounts})
	if r := tn.take(); len(r) != 1 || r[0].Status != client.Busy {
		t.Fatalf("replies %+v, want Busy", r)
	}
}

func TestRestartRebuildsSessions(t *testing.T) {
	for _, every := range []uint64{1000, 3} {
		tn := newTestNet(t, 1, func(cfg *Config) { cfg.SnapshotEvery = every })
		tn.elect(0)
		c := tn.register(0, 1)
		ok(t, c.do(client.CreateAccounts, accounts(1, 2)))
		req := c.request(client.CreateTransfers, transfer(10, 1, 2, 5))
		first := ok(t, tn.call(0, req))
		tn.crash(0)
		tn.open(0)
		if every == 3 && tn.nodes[0].Status().Applied == 0 {
			t.Fatal("restart did not load a snapshot")
		}
		tn.elect(0)
		again := ok(t, tn.call(0, req))
		if again.Time != first.Time {
			t.Fatalf("every %d: retry after restart got %+v, want %+v", every, again, first)
		}
		if got := debits(t, c, 1); got != 5 {
			t.Fatalf("every %d: debits %d, want 5", every, got)
		}
	}
}

func filled(t *testing.T) (*testNet, raft.Snapshot) {
	t.Helper()
	tn := newTestNet(t, 1, func(cfg *Config) { cfg.SnapshotEvery = 4 })
	tn.elect(0)
	c := tn.register(0, 1)
	for i := range uint64(5) {
		ok(t, c.do(client.CreateAccounts, accounts(i+1)))
	}
	s := tn.nodes[0].snap
	if s.Index == 0 || tn.nodes[0].Status().Last <= s.Index {
		t.Fatalf("snapshot %+v, last %d", s, tn.nodes[0].Status().Last)
	}
	if err := tn.nodes[0].Close(); err != nil {
		t.Fatal(err)
	}
	return tn, s
}

func editLog(t *testing.T, tn *testNet, edit func(*wal.WAL) error) {
	t.Helper()
	w, err := wal.Open(tn.disks[0], "/n/wal", wal.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := edit(w); err != nil {
		t.Fatal(err)
	}
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestOpenReconcilesLogWithSnapshot(t *testing.T) {
	t.Run("log ends before the snapshot", func(t *testing.T) {
		tn, s := filled(t)
		editLog(t, tn, func(w *wal.WAL) error { return w.TruncateAfter(s.Index - 1) })
		tn.open(0)
		if st := tn.nodes[0].Status(); st.Last != s.Index || st.Applied != s.Index {
			t.Fatalf("status %+v, want log emptied at snapshot %d", st, s.Index)
		}
	})
	t.Run("other term at the snapshot index", func(t *testing.T) {
		tn, s := filled(t)
		editLog(t, tn, func(w *wal.WAL) error {
			if err := w.TruncateAfter(s.Index - 1); err != nil {
				return err
			}
			return w.Append(wal.Entry{Index: s.Index, Term: s.Term + 1}, wal.Entry{Index: s.Index + 1, Term: s.Term + 1})
		})
		tn.open(0)
		if last, _ := tn.nodes[0].log.LastIndex(); last != 0 {
			t.Fatalf("log still ends at %d", last)
		}
	})
	t.Run("log starts after the snapshot", func(t *testing.T) {
		tn, s := filled(t)
		editLog(t, tn, func(w *wal.WAL) error {
			if err := w.TruncateAfter(0); err != nil {
				return err
			}
			return w.Append(wal.Entry{Index: s.Index + 2, Term: s.Term})
		})
		cfg := tn.cfgs[0]
		cfg.FS, cfg.Clock, cfg.Rand = tn.disks[0], func() uint64 { return start }, rand.New(rand.NewPCG(1, 1))
		if _, err := Open(cfg); err == nil {
			t.Fatal("opened a log with a gap after the snapshot")
		}
	})
}

func TestLaggingFollowerGetsASnapshot(t *testing.T) {
	tn := newTestNet(t, 3, func(cfg *Config) { cfg.SnapshotEvery, cfg.SegmentSize = 4, 512 })
	tn.elect(0)
	c := tn.register(0, 1)
	tn.cut[3] = true
	for i := range uint64(30) {
		ok(t, c.do(client.CreateAccounts, accounts(i+1)))
	}
	if first := (logView{tn.nodes[0]}).FirstIndex(); first <= 2 {
		t.Fatalf("leader log still starts at %d", first)
	}
	tn.cut[3] = false
	for range 100 {
		tn.tick(tn.all()...)
	}
	if tn.snaps == 0 || tn.nodes[2].snap.Index == 0 {
		t.Fatalf("no snapshot reached n3: %d delivered, n3 at %+v", tn.snaps, tn.nodes[2].snap)
	}
	ok(t, c.do(client.CreateAccounts, accounts(100)))
	for range 5 {
		tn.tick(tn.all()...)
	}
	want := tn.nodes[0].Digest()
	for i, n := range tn.nodes {
		if n.Digest() != want || n.Status().Applied != tn.nodes[0].Status().Applied {
			t.Fatalf("n%d differs after catching up: %+v", i+1, n.Status())
		}
	}
	tn.crash(2)
	tn.open(2)
	for range 20 {
		tn.tick(tn.all()...)
	}
	if tn.nodes[2].Digest() != tn.nodes[0].Digest() {
		t.Fatal("n3 differs after restarting from the installed snapshot")
	}
}

func TestClockJumpIsBounded(t *testing.T) {
	tn := newTestNet(t, 1, nil)
	tn.elect(0)
	c := tn.register(0, 1)
	first := ok(t, c.do(client.CreateAccounts, accounts(1)))
	if first.Time != tn.clocks[0] {
		t.Fatalf("first time %d, clock %d", first.Time, tn.clocks[0])
	}
	tn.clocks[0] += 3600 * 1_000_000_000
	r := ok(t, c.do(client.CreateAccounts, accounts(2)))
	if r.Time-first.Time > 4*tn.cfgs[0].ClockStep {
		t.Fatalf("time jumped by %d ns with the clock", r.Time-first.Time)
	}
	tn.clocks[0] -= 4000 * 1_000_000_000
	back := ok(t, c.do(client.CreateAccounts, accounts(3)))
	if back.Time != r.Time+1 {
		t.Fatalf("clock behind: time %d after %d", back.Time, r.Time)
	}
}
