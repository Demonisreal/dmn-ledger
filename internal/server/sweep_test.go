package server

import (
	"encoding/binary"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Demonisreal/dmn-ledger/internal/client"
	"github.com/Demonisreal/dmn-ledger/internal/raft"
	"github.com/Demonisreal/dmn-ledger/internal/u128"
)

func sweeps(f *fakePeer, by u128.Uint128) (registers int, numbers []uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	seen := make(map[uint64]bool)
	for _, e := range f.logged {
		if seen[e.Index] {
			continue
		}
		seen[e.Index] = true
		switch {
		case e.Kind == 1 && u128.FromBytes(e.Body) == by:
			registers++
		case e.Kind == 2 && u128.FromBytes(e.Body) == by:
			numbers = append(numbers, binary.BigEndian.Uint64(e.Body[24:]))
		}
	}
	return registers, numbers
}

func TestSweepResumesWithTheSameNumberAfterLeading(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pn := newPipeNet()
		fakes := []*fakePeer{pn.fake(t, 2, 1, following), pn.fake(t, 3, 1, following)}
		cfg := pn.config(1, 3, t.TempDir())
		cfg.Sweep = true
		n := start(t, cfg)
		c := pn.connect(t, clientAddr(1), 1)
		c.leading()
		c.register()
		c.ok(client.CreateAccounts, accounts(1, 2))
		c.ok(client.CreateTransfers, hold(10, 1))
		time.Sleep(2 * time.Second)
		held(t, c, 0)

		var by u128.Uint128
		fakes[0].mu.Lock()
		for _, e := range fakes[0].logged {
			if e.Kind == 1 && u128.FromBytes(e.Body) != c.id {
				by = u128.FromBytes(e.Body)
			}
		}
		fakes[0].mu.Unlock()
		if _, numbers := sweeps(fakes[0], by); by.IsZero() || len(numbers) != 1 || numbers[0] != 1 {
			t.Fatalf("first sweep: sweeper %v with numbers %v, want one batch #1", by, numbers)
		}
		for _, f := range fakes {
			f.mu.Lock()
			f.stopAt = func(e raft.Entry) bool {
				return e.Kind == 2 && u128.FromBytes(e.Body) == by
			}
			f.mu.Unlock()
		}
		c.ok(client.CreateTransfers, hold(11, 1))
		for range 500 {
			if c.status().Role != raft.Leader {
				break
			}
			time.Sleep(testTick)
		}
		if st := c.status(); st.Role == raft.Leader {
			t.Fatal("the node still leads without acknowledgements")
		}
		_, before := sweeps(fakes[0], by)
		if len(before) != 2 || before[1] != 2 {
			t.Fatalf("before the node lost the lead the sweeper sent %v, want #1 and #2", before)
		}
		time.Sleep(2 * time.Second)
		if _, idle := sweeps(fakes[0], by); len(idle) != len(before) {
			t.Fatalf("the sweeper sent %v while the node did not lead", idle[len(before):])
		}
		for _, f := range fakes {
			f.mu.Lock()
			f.stopAt, f.mode = nil, following
			f.mu.Unlock()
		}
		c.leading()
		for range 300 {
			if pending(t, c, 1).IsZero() {
				break
			}
			time.Sleep(testTick)
		}
		held(t, c, 0)
		var registers, resent int
		var numbers []uint64
		for range 200 {
			registers, numbers = sweeps(fakes[0], by)
			if resent = len(numbers) - len(slices.DeleteFunc(slices.Clone(numbers), func(k uint64) bool { return k == 2 })); resent >= 2 {
				break
			}
			time.Sleep(testTick)
		}
		if registers != 1 || slices.Max(numbers) != 2 || resent < 2 {
			t.Fatalf("sweeper %v registered %d times and sent batches %v, want one session and #2 resent", by, registers, numbers)
		}
		if err := n.halt(); err != nil {
			t.Fatal(err)
		}
	})
}

func sweeping(t *testing.T, fsys *hookFS) (*pipeNet, *testNode, *testClient) {
	t.Helper()
	pn := newPipeNet()
	cfg := pn.config(1, 1, t.TempDir())
	cfg.Sweep = true
	if fsys != nil {
		cfg.FS = fsys
	}
	n := start(t, cfg)
	c := pn.connect(t, clientAddr(1), 1)
	c.leading()
	c.register()
	c.ok(client.CreateAccounts, accounts(1, 2))
	return pn, n, c
}

func held(t *testing.T, c *testClient, want uint64) {
	t.Helper()
	if got := pending(t, c, 1); got != u128.From64(want) {
		t.Fatalf("account 1 holds %v pending, want %d", got, want)
	}
}

func TestSweeperReleasesAnExpiredReservation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		_, n, c := sweeping(t, nil)
		c.ok(client.CreateTransfers, hold(10, 1))
		held(t, c, 5)
		time.Sleep(2 * time.Second)
		held(t, c, 0)
		if err := n.halt(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestSweeperIsIdleWithoutDeadlines(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		_, n, c := sweeping(t, nil)
		c.ok(client.CreateTransfers, transfer(10, 1, 2, 5))
		c.ok(client.CreateTransfers, hold(11, 3600))
		before := c.status().Last
		time.Sleep(10 * time.Second)
		if after := c.status().Last; after != before {
			t.Fatalf("the log grew from %d to %d in 10s without a deadline due", before, after)
		}
		if n.srv.sweeper.sess != nil {
			t.Fatal("the sweeper registered a session without a deadline due")
		}
		if err := n.halt(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestOnlyTheLeaderSweeps(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pn := newPipeNet()
		nodes := make([]*testNode, 3)
		for i := range nodes {
			cfg := pn.config(uint64(i)+1, 3, t.TempDir())
			cfg.Sweep = true
			nodes[i] = start(t, cfg)
		}
		lead := leaderOf(t, pn, 3)
		c := pn.connect(t, clientAddr(lead), 1)
		c.register()
		c.ok(client.CreateAccounts, accounts(1, 2))
		c.ok(client.CreateTransfers, hold(10, 1))
		time.Sleep(3 * time.Second)
		held(t, c, 0)
		if got := c.status(); got.Role.String() != "leader" || got.ID != lead {
			t.Fatalf("node %d no longer leads: %+v", lead, got)
		}
		for i, n := range nodes {
			if err := n.halt(); err != nil {
				t.Fatal(err)
			}
			if swept := n.srv.sweeper.sess != nil; swept != (uint64(i)+1 == lead) {
				t.Errorf("node %d, leader %d: sweeper session %v", i+1, lead, swept)
			}
		}
	})
}

func TestSweeperPausesWhileLedgerTimeTrails(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fsys := newHookFS()
		_, n, c := sweeping(t, fsys)
		c.ok(client.CreateTransfers, hold(10, 3))
		stall := make(chan time.Duration, 1)
		fsys.hook(func(name string) error {
			if isWAL(name) {
				select {
				case d := <-stall:
					time.Sleep(d)
				default:
				}
			}
			return nil
		})
		stall <- 4500 * time.Millisecond
		c.ok(client.CreateTransfers, transfer(11, 1, 2, 1))
		before := c.status()
		if before.NextExpiry == 0 || before.NextExpiry > wallClock() || before.Time >= before.NextExpiry {
			t.Fatalf("after the stall: ledger time %d, next expiry %d, clock %d; want the clock past the expiry and the ledger before it", before.Time, before.NextExpiry, wallClock())
		}
		time.Sleep(1500 * time.Millisecond)
		after := c.status()
		if swept := after.Last - before.Last; swept > 6 {
			t.Fatalf("%d entries in 1.5s while the ledger time trailed, want the register, its batch and at most two a second", swept)
		}
		time.Sleep(5 * time.Second)
		held(t, c, 0)
		if err := n.halt(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestSweeperSurvivesEviction(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pn := newPipeNet()
		cfg := pn.config(1, 1, t.TempDir())
		cfg.Sweep = true
		cfg.MaxSessions = 3
		n := start(t, cfg)
		c := pn.connect(t, clientAddr(1), 1)
		c.leading()
		c.register()
		c.ok(client.CreateAccounts, accounts(1, 2))
		c.ok(client.CreateTransfers, hold(10, 1))
		time.Sleep(2 * time.Second)
		held(t, c, 0)
		first := n.srv.sweeper.id
		c.ok(client.CreateAccounts, accounts(3))
		for id := uint64(2); id <= 3; id++ {
			pn.connect(t, clientAddr(1), id).register()
		}
		c.ok(client.CreateTransfers, hold(11, 1))
		held(t, c, 5)
		time.Sleep(3 * time.Second)
		held(t, c, 0)
		if err := n.halt(); err != nil {
			t.Fatal(err)
		}
		if n.srv.sweeper.id == first {
			t.Fatal("the sweeper kept its evicted session")
		}
	})
}

func TestSweepOff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pn, cfg := single(t, nil)
		n := start(t, cfg)
		c := pn.connect(t, clientAddr(1), 1)
		c.leading()
		c.register()
		c.ok(client.CreateAccounts, accounts(1, 2))
		c.ok(client.CreateTransfers, hold(10, 1))
		before := c.status().Last
		time.Sleep(5 * time.Second)
		held(t, c, 5)
		if after := c.status().Last; after != before {
			t.Fatalf("the log grew from %d to %d with the sweeper off", before, after)
		}
		if err := n.halt(); err != nil {
			t.Fatal(err)
		}
	})
}
