package replica

import (
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/Demonisreal/dmn-ledger/internal/client"
	"github.com/Demonisreal/dmn-ledger/internal/raft"
	"github.com/Demonisreal/dmn-ledger/internal/state"
	"github.com/Demonisreal/dmn-ledger/internal/u128"
)

const hour = 3600 * 1_000_000_000

type line struct {
	tn     *testNet
	from   uint64
	since  uint64
	writes uint64
}

func lineFrom(tn *testNet, r client.Reply) *line {
	return &line{tn: tn, from: r.Time, since: tn.nodes[0].ticks}
}

func (l *line) hold(r client.Reply) {
	l.tn.t.Helper()
	l.writes++
	ticks := l.tn.nodes[0].ticks - l.since
	if most := (ticks+1)*l.tn.cfgs[0].ClockStep + l.writes; r.Time-l.from > most {
		l.tn.t.Fatalf("time moved %d ns in %d ticks and %d writes, want at most %d", r.Time-l.from, ticks, l.writes, most)
	}
}

func TestClockJumpMovesOneStepPerTick(t *testing.T) {
	for _, size := range []int{1, 3} {
		t.Run(fmt.Sprintf("%d nodes", size), func(t *testing.T) {
			tn := newTestNet(t, size, nil)
			tn.elect(0)
			c := tn.register(0, 1)
			l := lineFrom(tn, ok(t, c.do(client.CreateAccounts, accounts(1))))
			tn.clocks[0] += hour
			for k := range uint64(5) {
				for i := range uint64(20) {
					l.hold(ok(t, c.do(client.CreateAccounts, accounts(100*k+i+10))))
				}
				tn.tick(tn.all()...)
			}
		})
	}
}

func TestHonestClockKeepsUpUnderCommitLatency(t *testing.T) {
	for _, delay := range []uint64{0, 1, 2, 4} {
		tn := newTestNet(t, 3, nil)
		tn.elect(0)
		c := tn.register(0, 1)
		ok(t, c.do(client.CreateAccounts, accounts(1)))
		wire := make(map[uint64][]envelope)
		post := func(at uint64) {
			wire[at] = append(wire[at], tn.queue...)
			tn.queue = nil
		}
		var newest uint64
		answered := 0
		sent := make(map[uint64]uint64)
		for k := range uint64(500) {
			for i := range tn.nodes {
				tn.clocks[i] += tickNs
				tn.nodes[i].Tick()
				tn.run(i)
			}
			req := c.request(client.CreateAccounts, accounts(100+k))
			sent[req.Number] = tn.clocks[0]
			tn.nodes[0].Request(req)
			tn.run(0)
			post(k + delay)
			for len(wire[k]) > 0 {
				due := wire[k]
				delete(wire, k)
				for _, e := range due {
					tn.nodes[e.m.To-1].Receive(e.m, nil)
					tn.run(int(e.m.To - 1))
				}
				post(k + delay)
			}
			for _, r := range tn.take() {
				if r.Status == client.OK && r.Op == client.CreateAccounts {
					if r.Time != sent[r.Number] {
						t.Fatalf("delay %d ticks: write %d stamped %d, the honest clock read %d", delay, r.Number, r.Time, sent[r.Number])
					}
					newest = max(newest, r.Time)
					answered++
				}
			}
		}
		if answered < 400 {
			t.Fatalf("delay %d: only %d of 500 writes answered", delay, answered)
		}
		if behind, most := tn.clocks[0]-newest, (2*delay+2)*tickNs; behind > most {
			t.Fatalf("delay %d ticks: ledger time %d ns behind an honest clock, want at most %d", delay, behind, most)
		}
	}
}

func TestDeadlineAfterIdleRestartDoesNotCollapse(t *testing.T) {
	tn := newTestNet(t, 1, nil)
	tn.elect(0)
	c := tn.register(0, 1)
	ok(t, c.do(client.CreateAccounts, accounts(1, 2)))
	for range 3000 {
		tn.tick(0)
	}
	tn.crash(0)
	tn.open(0)
	tn.elect(0)
	const timeout = 10
	hold := ok(t, c.do(client.CreateTransfers, state.AppendTransfer(nil, state.Transfer{
		ID: u128.From64(100), DebitAccountID: u128.From64(1), CreditAccountID: u128.From64(2),
		Amount: u128.From64(5), Ledger: 1, Code: 1, Flags: state.Pending, Timeout: timeout,
	})))
	if len(hold.Body) != 0 {
		t.Fatalf("pending transfer failed: %x", hold.Body)
	}
	for i := range uint64(1000) {
		ok(t, c.do(client.CreateAccounts, accounts(1000+i)))
	}
	quiet := timeout*1_000_000_000/tn.cfgs[0].ClockStep - 2
	for i := range quiet {
		tn.tick(0)
		ok(t, c.do(client.CreateAccounts, accounts(5000+i)))
	}
	post := ok(t, c.do(client.CreateTransfers, state.AppendTransfer(nil, state.Transfer{
		ID: u128.From64(101), PendingID: u128.From64(100), Flags: state.PostPending,
	})))
	if len(post.Body) != 0 {
		t.Fatalf("post %d ticks after a %d s hold failed with reason %d", quiet, timeout, binary.BigEndian.Uint32(post.Body[4:]))
	}
}

func TestInstalledSnapshotAnchorsTheClock(t *testing.T) {
	tn := newTestNet(t, 3, func(cfg *Config) { cfg.SnapshotEvery, cfg.SegmentSize = 4, 512 })
	tn.elect(0)
	c := tn.register(0, 1)
	tn.cut[3] = true
	for range 500 {
		tn.tick(tn.all()...)
	}
	for i := uint64(1); i < 30 || tn.nodes[0].snap.Index != tn.nodes[0].Status().Applied; i++ {
		ok(t, c.do(client.CreateAccounts, accounts(i)))
	}
	tn.cut[3] = false
	for i := 0; tn.nodes[2].snap.Index == 0; i++ {
		if i == 200 {
			t.Fatal("no snapshot reached n3")
		}
		tn.tick(tn.all()...)
	}
	since := tn.nodes[2].ticks
	last := tn.nodes[2].sm.Last()
	if tn.nodes[2].Status().Applied != tn.nodes[0].Status().Applied {
		t.Fatalf("n3 applied %d, leader %d", tn.nodes[2].Status().Applied, tn.nodes[0].Status().Applied)
	}
	tn.crash(0)
	tn.takeOver(2, 1)
	if tn.nodes[2].Status().Role != raft.Leader {
		t.Fatal("n3 is not leader")
	}
	tn.clocks[2] += hour
	c.node = 2
	r := ok(t, c.do(client.CreateAccounts, accounts(100)))
	if most := (tn.nodes[2].ticks - since + 1) * tn.cfgs[2].ClockStep; r.Time-last > most {
		t.Fatalf("first stamp after the install moved time by %d ns, want at most %d", r.Time-last, most)
	}
}

func TestCreepingClockIsHeldToOneStepPerTick(t *testing.T) {
	tn := newTestNet(t, 1, nil)
	tn.elect(0)
	c := tn.register(0, 1)
	l := lineFrom(tn, ok(t, c.do(client.CreateAccounts, accounts(1))))
	for i := range uint64(100) {
		tn.clocks[0] += tn.cfgs[0].ClockStep
		l.hold(ok(t, c.do(client.CreateAccounts, accounts(10+i))))
	}
}

func TestFastClockIsHeldToOneStepPerTick(t *testing.T) {
	for _, perTick := range []uint64{1, 4, 50} {
		for _, factor := range []uint64{2, 10} {
			t.Run(fmt.Sprintf("%dx with %d writes per tick", factor, perTick), func(t *testing.T) {
				tn := newTestNet(t, 1, nil)
				tn.elect(0)
				c := tn.register(0, 1)
				l := lineFrom(tn, ok(t, c.do(client.CreateAccounts, accounts(1))))
				for range 200 {
					tn.tick(0)
					for range perTick {
						tn.clocks[0] += (factor - 1) * tickNs / perTick
						l.hold(ok(t, c.do(client.CreateAccounts, accounts(10+l.writes))))
					}
				}
			})
		}
	}
}

func TestStampsNeverRiseFasterThanOneStepPerTick(t *testing.T) {
	seeds := uint64(300)
	if testing.Short() {
		seeds = 50
	}
	for seed := range seeds {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			rng := rand.New(rand.NewPCG(seed, 5))
			tn := newTestNet(t, 1, nil)
			tn.elect(0)
			c := tn.register(0, 1)
			l := lineFrom(tn, ok(t, c.do(client.CreateAccounts, accounts(1))))
			step := tn.cfgs[0].ClockStep
			for i := range uint64(400) {
				switch rng.IntN(6) {
				case 0:
					tn.tick(0)
				case 1:
					tn.clocks[0] += rng.Uint64N(3 * step)
				case 2:
					tn.clocks[0] -= rng.Uint64N(3 * step)
				case 3:
					tn.clocks[0] += rng.Uint64N(hour)
				case 4:
					tn.clocks[0] = l.from + rng.Uint64N((tn.nodes[0].ticks-l.since+2)*step)
				}
				l.hold(ok(t, c.do(client.CreateAccounts, accounts(10+i))))
			}
		})
	}
}

func TestClockComesBackAfterJumpingBack(t *testing.T) {
	tn := newTestNet(t, 1, nil)
	tn.elect(0)
	c := tn.register(0, 1)
	ok(t, c.do(client.CreateAccounts, accounts(1)))
	honest := tn.clocks[0]
	tn.clocks[0] -= 500 * 1_000_000_000
	ok(t, c.do(client.CreateAccounts, accounts(2)))
	tn.clocks[0] = honest + tickNs
	tn.tick(0)
	if r := ok(t, c.do(client.CreateAccounts, accounts(3))); r.Time != tn.clocks[0] {
		t.Fatalf("clock back at %d, stamp %d", tn.clocks[0], r.Time)
	}
}
