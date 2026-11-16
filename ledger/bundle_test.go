package ledger

import (
	"context"
	"slices"
	"testing"
	"testing/synctest"

	"github.com/Demonisreal/dmn-ledger/internal/client"
	"github.com/Demonisreal/dmn-ledger/internal/state"
)

func inFlight(t *testing.T, f *fake, c *Client) (request, <-chan created) {
	t.Helper()
	done := goCreate(context.Background(), c, transfers(1))
	return f.next(t), done
}

func TestConcurrentCallsShareARequest(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFake(t)
		c := dialed(t, f)
		defer c.Close()
		g, first := inFlight(t, f, c)
		var calls []<-chan created
		for id := uint64(10); id < 15; id++ {
			calls = append(calls, goCreate(context.Background(), c, transfers(id, id+100)))
		}
		g.ok(t)
		await(t, first)
		g = f.next(t)
		want := []uint64{10, 110, 11, 111, 12, 112, 13, 113, 14, 114}
		if !slices.Equal(g.transfers(), want) || g.req.Number != 2 {
			t.Fatalf("request #%d carried %v, want #2 with %v", g.req.Number, g.transfers(), want)
		}
		g.ok(t)
		for k, done := range calls {
			if r := await(t, done); r.err != nil || len(r.res.Failures) != 0 {
				t.Fatalf("call %d: %+v, %v", k, r.res, r.err)
			}
		}
		f.quiet(t, 0)
	})
}

func TestFailuresGoBackToTheirCaller(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFake(t)
		c := dialed(t, f)
		defer c.Close()
		g, first := inFlight(t, f, c)
		ctx := context.Background()
		calls := []<-chan created{
			goCreate(ctx, c, transfers(10, 11)),
			goCreate(ctx, c, transfers(20, 21, 22)),
			goCreate(ctx, c, nil),
			goCreate(ctx, c, transfers(30)),
			goCreate(ctx, c, transfers(40, 41)),
		}
		g.ok(t)
		await(t, first)
		g = f.next(t)
		g.reply(t, client.Reply{Status: client.OK, Leader: 1, Time: 1_000, Body: client.AppendFailures(nil, []state.Failure{
			{Index: 1, Reason: state.Exists},
			{Index: 2, Reason: state.DebitAccountNotFound},
			{Index: 4, Reason: state.ExceedsCredits},
			{Index: 5, Reason: state.AmountMustNotBeZero},
		})})
		want := [][]Failure{
			{{Index: 1, Reason: Exists}},
			{{Index: 0, Reason: DebitAccountNotFound}, {Index: 2, Reason: ExceedsCredits}},
			nil,
			{{Index: 0, Reason: AmountMustNotBeZero}},
			nil,
		}
		for k, done := range calls {
			if r := await(t, done); r.err != nil || !slices.Equal(r.res.Failures, want[k]) {
				t.Fatalf("call %d: %+v, %v, want failures %v", k, r.res, r.err, want[k])
			}
		}
	})
}

func TestTimestampOfABundledCall(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFake(t)
		c := dialed(t, f)
		defer c.Close()
		g, first := inFlight(t, f, c)
		ctx := context.Background()
		calls := []<-chan created{
			goCreate(ctx, c, transfers(10, 11)),
			goCreate(ctx, c, transfers(20, 21, 22)),
			goCreate(ctx, c, nil),
			goCreate(ctx, c, transfers(30)),
		}
		g.ok(t)
		await(t, first)
		f.next(t).reply(t, client.Reply{Status: client.OK, Leader: 1, Time: 1_000})
		for k, want := range []uint64{1_000, 1_002, 1_005, 1_005} {
			if r := await(t, calls[k]); r.err != nil || r.res.Timestamp != want || r.res.Batch != 1_000 {
				t.Fatalf("call %d: %+v, %v, want timestamp %d in batch 1000", k, r.res, r.err, want)
			}
		}

		g, first = inFlight(t, f, c)
		accounts := []<-chan created{
			goCreateAccounts(ctx, c, make([]Account, 4)),
			goCreateAccounts(ctx, c, make([]Account, 1)),
		}
		g.ok(t)
		await(t, first)
		f.next(t).reply(t, client.Reply{Status: client.OK, Leader: 1, Time: 7_000})
		for k, want := range []uint64{7_000, 7_004} {
			if r := await(t, accounts[k]); r.err != nil || r.res.Timestamp != want || r.res.Batch != 7_000 {
				t.Fatalf("account call %d: %+v, %v, want timestamp %d in batch 7000", k, r.res, r.err, want)
			}
		}
	})
}

func TestOpenChainEndsTheBatch(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFake(t)
		c := dialed(t, f)
		defer c.Close()
		g, first := inFlight(t, f, c)
		ctx := context.Background()
		calls := []<-chan created{
			goCreate(ctx, c, []Transfer{transfer(10, 0), transfer(11, Linked)}),
			goCreate(ctx, c, transfers(20)),
			goCreate(ctx, c, []Transfer{transfer(30, Linked), transfer(31, 0)}),
			goCreate(ctx, c, transfers(40)),
		}
		g.ok(t)
		await(t, first)
		for _, want := range [][]uint64{{10, 11}, {20, 30, 31, 40}} {
			g := f.next(t)
			if !slices.Equal(g.transfers(), want) {
				t.Fatalf("request carried %v, want %v", g.transfers(), want)
			}
			g.ok(t)
		}
		for k, done := range calls {
			if r := await(t, done); r.err != nil {
				t.Fatalf("call %d: %v", k, r.err)
			}
		}
	})
}

func TestLookupsAreNotBundled(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFake(t)
		c := dialed(t, f)
		defer c.Close()
		g, first := inFlight(t, f, c)
		ctx := context.Background()
		looked := []<-chan error{goLookup(ctx, c, 1, 2), goLookup(ctx, c, 3)}
		written := goCreate(ctx, c, transfers(10))
		g.ok(t)
		await(t, first)
		for _, want := range [][]uint64{{1, 2}, {3}} {
			g := f.next(t)
			if g.req.Op != client.LookupAccounts || !slices.Equal(ids(client.DecodeIDs(g.req.Body)), want) {
				t.Fatalf("request %v for %v, want a lookup of %v", g.req.Op, ids(client.DecodeIDs(g.req.Body)), want)
			}
			g.ok(t)
		}
		g = f.next(t)
		if g.req.Op != client.CreateTransfers || !slices.Equal(g.transfers(), []uint64{10}) {
			t.Fatalf("request %v with %v, want transfer 10", g.req.Op, g.transfers())
		}
		g.ok(t)
		for _, done := range looked {
			if err := await(t, done); err != nil {
				t.Fatal(err)
			}
		}
		if r := await(t, written); r.err != nil {
			t.Fatal(r.err)
		}
	})
}

func TestOrderOfCallsIsKept(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFake(t)
		c := dialed(t, f)
		defer c.Close()
		g, first := inFlight(t, f, c)
		ctx := context.Background()
		calls := []<-chan created{
			goCreate(ctx, c, transfers(10)),
			goCreateAccounts(ctx, c, []Account{{ID: From64(20), Ledger: 1, Code: 1}}),
			goCreate(ctx, c, transfers(30)),
		}
		g.ok(t)
		await(t, first)
		for _, op := range []client.Op{client.CreateTransfers, client.CreateAccounts, client.CreateTransfers} {
			g := f.next(t)
			if g.req.Op != op {
				t.Fatalf("request %v, want %v", g.req.Op, op)
			}
			g.ok(t)
		}
		for k, done := range calls {
			if r := await(t, done); r.err != nil {
				t.Fatalf("call %d: %v", k, r.err)
			}
		}
	})
}

func TestBundleStaysUnderTheLimit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFake(t)
		c := dialed(t, f)
		defer c.Close()
		g, first := inFlight(t, f, c)
		ctx := context.Background()
		full := goCreate(ctx, c, make([]Transfer, maxTransfers))
		one := goCreate(ctx, c, transfers(9))
		g.ok(t)
		await(t, first)
		for _, want := range []int{maxTransfers, 1} {
			g := f.next(t)
			if n := len(g.req.Body) / state.TransferSize; n != want {
				t.Fatalf("request carried %d transfers, want %d", n, want)
			}
			g.ok(t)
		}
		for _, done := range []<-chan created{full, one} {
			if r := await(t, done); r.err != nil {
				t.Fatal(r.err)
			}
		}
	})
}
