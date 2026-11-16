package ledger

import (
	"context"
	"encoding/binary"
	"errors"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Demonisreal/dmn-ledger/internal/client"
	"github.com/Demonisreal/dmn-ledger/internal/state"
	"github.com/Demonisreal/dmn-ledger/internal/u128"
)

func TestDialRegisters(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFake(t)
		c := dialed(t, f)
		defer c.Close()
		done := goCreate(context.Background(), c, transfers(5))
		g := f.next(t)
		want := client.Request{Client: u128.Uint128(alice), Session: session, Number: 1, Op: client.CreateTransfers}
		if g.req.Client != want.Client || g.req.Session != want.Session || g.req.Number != want.Number || g.req.Op != want.Op {
			t.Fatalf("first request after the register: %+v, want %+v", g.req, want)
		}
		g.ok(t)
		if r := await(t, done); r.err != nil {
			t.Fatal(r.err)
		}
	})
	synctest.Test(t, func(t *testing.T) {
		f := newFake(t)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		c, err := open(ctx, f.config(), alice, f.dial)
		if c != nil || !errors.Is(err, context.DeadlineExceeded) || errors.Is(err, ErrUncertain) {
			t.Fatalf("dial without an answer: %v, %v", c, err)
		}
	})
}

func TestCreateAndLookup(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFake(t)
		c := dialed(t, f)
		defer c.Close()
		sm := state.New(8, 8)
		clock := uint64(1_000)
		apply := func() {
			g := f.next(t)
			r := client.Reply{Status: client.OK, Leader: g.node}
			switch g.req.Op {
			case client.CreateAccounts, client.CreateTransfers:
				clock += 1_000
				r.Time = state.StartTime(clock, sm.Last())
				op := state.CreateAccounts
				if g.req.Op == client.CreateTransfers {
					op = state.CreateTransfers
				}
				fails, err := sm.Apply(op, clock, g.req.Body, nil)
				if err != nil {
					t.Fatal(err)
				}
				r.Body = client.AppendFailures(nil, fails)
			case client.LookupAccounts:
				for _, a := range sm.LookupAccounts(client.DecodeIDs(g.req.Body), nil) {
					r.Body = state.AppendAccount(r.Body, a)
				}
			case client.LookupTransfers:
				for _, tr := range sm.LookupTransfers(client.DecodeIDs(g.req.Body), nil) {
					r.Body = state.AppendTransfer(r.Body, tr)
				}
			}
			g.reply(t, r)
		}
		ctx := context.Background()

		done := goCreateAccounts(ctx, c, []Account{
			{ID: From64(1), Ledger: 1, Code: 10, Flags: DebitsMustNotExceedCredits},
			{ID: From64(2), Ledger: 1, Code: 10},
			{ID: From64(3), Ledger: 0, Code: 10},
		})
		apply()
		r := await(t, done)
		if r.err != nil || r.res.Timestamp != 2_000 || !slices.Equal(r.res.Failures, []Failure{{Index: 2, Reason: LedgerMustNotBeZero}}) {
			t.Fatalf("create accounts: %+v, %v", r.res, r.err)
		}

		pay := []Transfer{
			{ID: From64(10), DebitAccountID: From64(2), CreditAccountID: From64(1), Amount: From64(500), Ledger: 1, Code: 1},
			{ID: From64(11), DebitAccountID: From64(1), CreditAccountID: From64(2), Amount: From64(501), Ledger: 1, Code: 1},
		}
		done = goCreate(ctx, c, pay)
		apply()
		r = await(t, done)
		if r.err != nil || r.res.Timestamp != 3_000 || !slices.Equal(r.res.Failures, []Failure{{Index: 1, Reason: ExceedsCredits}}) {
			t.Fatalf("create transfers: %+v, %v", r.res, r.err)
		}

		found := make(chan []Account, 1)
		go func() {
			accounts, err := c.LookupAccounts(ctx, []Uint128{From64(1), From64(3), From64(2)})
			if err != nil {
				t.Error(err)
			}
			found <- accounts
		}()
		apply()
		want := []Account{
			{ID: From64(1), CreditsPosted: From64(500), Ledger: 1, Code: 10, Flags: DebitsMustNotExceedCredits, Timestamp: 2_000},
			{ID: From64(2), DebitsPosted: From64(500), Ledger: 1, Code: 10, Timestamp: 2_001},
		}
		if accounts := await(t, found); !slices.Equal(accounts, want) {
			t.Fatalf("lookup accounts: %+v, want %+v", accounts, want)
		}

		booked := make(chan []Transfer, 1)
		go func() {
			trs, err := c.LookupTransfers(ctx, []Uint128{From64(11), From64(10)})
			if err != nil {
				t.Error(err)
			}
			booked <- trs
		}()
		apply()
		wantTr := pay[0]
		wantTr.Timestamp = 3_000
		if trs := await(t, booked); !slices.Equal(trs, []Transfer{wantTr}) {
			t.Fatalf("lookup transfers: %+v, want %+v", trs, wantTr)
		}
	})
}

func TestFollowsTheLeaderHint(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFake(t)
		c := dialed(t, f)
		defer c.Close()
		done := goCreate(context.Background(), c, transfers(5))
		first := f.next(t)
		first.reply(t, client.Reply{Status: client.NotLeader, Leader: 3})
		g := f.next(t)
		if g.node != 3 || g.at.Sub(first.at) > tickEvery {
			t.Fatalf("after the hint: node %d after %v, want node 3 within a tick", g.node, g.at.Sub(first.at))
		}
		g.ok(t)
		if r := await(t, done); r.err != nil {
			t.Fatal(r.err)
		}
	})
}

func TestTimeoutMovesToTheNextNode(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFake(t)
		c := dialed(t, f)
		defer c.Close()
		done := goCreate(context.Background(), c, transfers(5))
		prev := f.next(t)
		for _, node := range []uint64{2, 3, 1} {
			g := f.next(t)
			if wait := g.at.Sub(prev.at); g.node != node || wait < time.Second || wait > time.Second+tickEvery {
				t.Fatalf("next attempt at node %d after %v, want node %d after the timeout of 1s", g.node, wait, node)
			}
			synctest.Wait()
			if _, err := prev.nc.Write([]byte{0}); err == nil {
				t.Fatalf("connection to node %d still open after its timeout", prev.node)
			}
			prev = g
		}
		prev.ok(t)
		if r := await(t, done); r.err != nil {
			t.Fatal(r.err)
		}
	})
}

func TestRetryKeepsTheNumber(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFake(t)
		c := dialed(t, f)
		defer c.Close()
		done := goCreate(context.Background(), c, transfers(5, 6))
		first := f.next(t)
		first.reply(t, client.Reply{Status: client.NotLeader, Leader: 2})
		g := f.next(t)
		_ = g.nc.Close()
		g = f.next(t)
		g.reply(t, client.Reply{Status: client.Busy})
		copies := []request{first}
		for range 4 {
			g = f.next(t)
			copies = append(copies, g)
		}
		for _, again := range copies {
			req := again.req
			if req.Client != first.req.Client || req.Session != session || req.Number != 1 || req.Op != client.CreateTransfers || !slices.Equal(req.Body, first.req.Body) {
				t.Fatalf("copy at node %d: %+v, want %+v", again.node, req, first.req)
			}
		}
		g.ok(t)
		if r := await(t, done); r.err != nil {
			t.Fatal(r.err)
		}
		_ = goCreate(context.Background(), c, transfers(7))
		if g := f.next(t); g.req.Number != 2 {
			t.Fatalf("next request #%d, want #2", g.req.Number)
		}
	})
}

func TestDeadConnectionMovesOnAtOnce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFake(t)
		c := dialed(t, f)
		defer c.Close()
		start := time.Now()
		done := goCreate(context.Background(), c, transfers(5))
		for _, node := range []uint64{1, 2} {
			g := f.next(t)
			if g.node != node {
				t.Fatalf("attempt at node %d, want node %d", g.node, node)
			}
			_ = g.nc.Close()
		}
		g := f.next(t)
		if g.node != 3 || time.Since(start) > 2*tickEvery {
			t.Fatalf("third attempt at node %d after %v, want node 3 within two ticks", g.node, time.Since(start))
		}
		g.ok(t)
		if r := await(t, done); r.err != nil {
			t.Fatal(r.err)
		}
	})
}

func TestForeignReplyClosesTheConnection(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFake(t)
		c := dialed(t, f)
		defer c.Close()
		done := goCreate(context.Background(), c, transfers(5))
		g := f.next(t)
		g.req.Client = u128.Uint128{Lo: 99}
		g.ok(t)
		next := f.next(t)
		if next.node != 2 || next.at.Sub(g.at) > tickEvery {
			t.Fatalf("after a reply for another client: node %d after %v, want node 2 within a tick", next.node, next.at.Sub(g.at))
		}
		synctest.Wait()
		if _, err := g.nc.Write([]byte{0}); err == nil {
			t.Fatal("connection that carried a foreign reply is still open")
		}
		next.ok(t)
		if r := await(t, done); r.err != nil {
			t.Fatal(r.err)
		}
	})
}

func TestFailedDialIsNotRepeatedAtOnce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFake(t)
		start := time.Now()
		f.refuse = func(uint64) bool { return time.Since(start) < 290*time.Millisecond }
		opened := make(chan error, 1)
		go func() {
			c, err := open(context.Background(), f.config(), alice, f.dial)
			if err == nil {
				defer c.Close()
			}
			opened <- err
		}()
		g := f.next(t)
		g.reply(t, client.Reply{Status: client.OK, Leader: g.node, Body: binary.BigEndian.AppendUint64(nil, session)})
		if err := await(t, opened); err != nil {
			t.Fatal(err)
		}
		if took := g.at.Sub(start); took > 400*time.Millisecond {
			t.Fatalf("registered after %v, want soon after the nodes came back at 290ms", took)
		}
		last := map[uint64]time.Time{}
		for len(f.dials) > 0 {
			d := <-f.dials
			if at, ok := last[d.node]; ok && d.at.Sub(at) < 250*time.Millisecond {
				t.Fatalf("node %d dialed again %v after a failed dial", d.node, d.at.Sub(at))
			}
			last[d.node] = d.at
		}
	})
}
