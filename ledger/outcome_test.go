package ledger

import (
	"context"
	"errors"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Demonisreal/dmn-ledger/internal/client"
	"github.com/Demonisreal/dmn-ledger/internal/state"
)

func TestEvictedClientStaysDead(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFake(t)
		c := dialed(t, f)
		defer c.Close()
		ctx := context.Background()
		sent := goCreate(ctx, c, transfers(5))
		g := f.next(t)
		waiting := goCreate(ctx, c, transfers(6))
		g.reply(t, client.Reply{Status: client.SessionUnknown, Leader: 1})
		if r := await(t, sent); !errors.Is(r.err, ErrEvicted) || !errors.Is(r.err, ErrUncertain) {
			t.Fatalf("request in flight: %v, want ErrEvicted and ErrUncertain", r.err)
		}
		if r := await(t, waiting); !errors.Is(r.err, ErrEvicted) || errors.Is(r.err, ErrUncertain) {
			t.Fatalf("request that waited: %v, want ErrEvicted alone", r.err)
		}
		for len(f.dials) > 0 {
			<-f.dials
		}
		for range 3 {
			if _, err := c.CreateTransfers(ctx, transfers(7)); !errors.Is(err, ErrEvicted) || errors.Is(err, ErrUncertain) {
				t.Fatalf("write after the eviction: %v, want ErrEvicted alone", err)
			}
			if _, err := c.LookupAccounts(ctx, []Uint128{From64(1)}); !errors.Is(err, ErrEvicted) {
				t.Fatalf("lookup after the eviction: %v, want ErrEvicted", err)
			}
		}
		f.quiet(t, 10*time.Second)
		if len(f.dials) > 0 {
			t.Fatalf("%d dials after the eviction", len(f.dials))
		}
	})
}

func TestContextBeforeSendingIsNotUncertain(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFake(t)
		c := dialed(t, f)
		defer c.Close()
		sent := goCreate(context.Background(), c, transfers(5))
		g := f.next(t)

		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer cancel()
		waiting := goCreate(ctx, c, transfers(6))
		if r := await(t, waiting); !errors.Is(r.err, context.DeadlineExceeded) || errors.Is(r.err, ErrUncertain) {
			t.Fatalf("call whose context ended in the queue: %v, want the context's error alone", r.err)
		}
		gone, stop := context.WithCancel(context.Background())
		stop()
		if _, err := c.CreateTransfers(gone, transfers(7)); err != context.Canceled {
			t.Fatalf("call with a context that had already ended: %v", err)
		}

		g.ok(t)
		if r := await(t, sent); r.err != nil {
			t.Fatal(r.err)
		}
		for range 20 {
			if _, err := c.CreateTransfers(gone, transfers(8)); err != context.Canceled {
				t.Fatalf("idle client, call with a context that had already ended: %v", err)
			}
		}
		f.quiet(t, 5*time.Second)
	})
}

func TestContextAfterSendingIsUncertain(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFake(t)
		c := dialed(t, f)
		defer c.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer cancel()
		start := time.Now()
		sent := goCreate(ctx, c, transfers(5))
		first := f.next(t)
		r := await(t, sent)
		if !errors.Is(r.err, ErrUncertain) || !errors.Is(r.err, context.DeadlineExceeded) {
			t.Fatalf("call whose context ended after sending: %v, want ErrUncertain and DeadlineExceeded", r.err)
		}
		if took := time.Since(start); took != 300*time.Millisecond {
			t.Fatalf("returned after %v, want at the deadline of 300ms", took)
		}
		again := f.next(t)
		if again.node != 2 || again.req.Number != first.req.Number {
			t.Fatalf("retry in the background: %v #%d to node %d, want #%d to node 2", again.req.Op, again.req.Number, again.node, first.req.Number)
		}
		again.ok(t)

		lookup, stop := context.WithCancel(context.Background())
		looked := goLookup(lookup, c, 1)
		g := f.next(t)
		stop()
		if err := await(t, looked); err != context.Canceled {
			t.Fatalf("lookup whose context ended after sending: %v, want context.Canceled alone", err)
		}
		g.reply(t, client.Reply{Status: client.OK, Leader: g.node})
	})
}

func TestCallsWaitBehindAnAbandonedRequest(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFake(t)
		c := dialed(t, f)
		defer c.Close()
		ctx, cancel := context.WithCancel(context.Background())
		abandoned := goCreate(ctx, c, transfers(5))
		first := f.next(t)
		cancel()
		if r := await(t, abandoned); !errors.Is(r.err, ErrUncertain) {
			t.Fatalf("abandoned call: %v, want ErrUncertain", r.err)
		}
		later := goCreate(context.Background(), c, transfers(6))
		again := f.next(t)
		if again.req.Number != first.req.Number || again.node != 2 {
			t.Fatalf("after the abandoned call: %v #%d to node %d, want its retry #%d to node 2", again.req.Op, again.req.Number, again.node, first.req.Number)
		}
		select {
		case r := <-later:
			t.Fatalf("later call returned %+v, %v before the abandoned request was answered", r.res, r.err)
		default:
		}
		again.reply(t, client.Reply{Status: client.OK, Leader: 2, Time: 50, Body: client.AppendFailures(nil, []state.Failure{{Index: 0, Reason: state.Exists}})})
		g := f.next(t)
		if g.req.Number != first.req.Number+1 || !slices.Equal(g.transfers(), []uint64{6}) {
			t.Fatalf("later call went out as #%d with %v, want #%d with transfer 6", g.req.Number, g.transfers(), first.req.Number+1)
		}
		g.reply(t, client.Reply{Status: client.OK, Leader: 2, Time: 99})
		if r := await(t, later); r.err != nil || r.res.Timestamp != 99 || len(r.res.Failures) != 0 {
			t.Fatalf("later call: %+v, %v, want its own result at time 99", r.res, r.err)
		}
	})
}

func TestCloseWhileInFlight(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFake(t)
		c := dialed(t, f)
		ctx := context.Background()
		sent := goCreate(ctx, c, transfers(5))
		f.next(t)
		waiting := goCreate(ctx, c, transfers(6))
		looked := goLookup(ctx, c, 1)
		if err := c.Close(); err != nil {
			t.Fatal(err)
		}
		if r := await(t, sent); !errors.Is(r.err, ErrClosed) || !errors.Is(r.err, ErrUncertain) {
			t.Fatalf("request in flight: %v, want ErrClosed and ErrUncertain", r.err)
		}
		if r := await(t, waiting); !errors.Is(r.err, ErrClosed) || errors.Is(r.err, ErrUncertain) {
			t.Fatalf("request that waited: %v, want ErrClosed alone", r.err)
		}
		if err := await(t, looked); !errors.Is(err, ErrClosed) {
			t.Fatalf("lookup that waited: %v, want ErrClosed", err)
		}
		if _, err := c.CreateTransfers(ctx, transfers(7)); !errors.Is(err, ErrClosed) || errors.Is(err, ErrUncertain) {
			t.Fatalf("write after Close: %v, want ErrClosed alone", err)
		}
		if err := c.Close(); err != nil {
			t.Fatalf("second Close: %v", err)
		}

		c = dialed(t, f)
		looked = goLookup(ctx, c, 1)
		f.next(t)
		if err := c.Close(); err != nil {
			t.Fatal(err)
		}
		if err := await(t, looked); !errors.Is(err, ErrClosed) || errors.Is(err, ErrUncertain) {
			t.Fatalf("lookup in flight: %v, want ErrClosed alone", err)
		}
	})
}

func TestTooLarge(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFake(t)
		c := dialed(t, f)
		defer c.Close()
		for _, tc := range []struct {
			name  string
			limit int
			size  int
			call  func(ctx context.Context, n int) error
		}{
			{"accounts", 10_922, 96, func(ctx context.Context, n int) error {
				_, err := c.CreateAccounts(ctx, make([]Account, n))
				return err
			}},
			{"transfers", 10_485, 100, func(ctx context.Context, n int) error {
				_, err := c.CreateTransfers(ctx, make([]Transfer, n))
				return err
			}},
			{"account lookup", 10_922, 16, func(ctx context.Context, n int) error {
				_, err := c.LookupAccounts(ctx, make([]Uint128, n))
				return err
			}},
			{"transfer lookup", 10_485, 16, func(ctx context.Context, n int) error {
				_, err := c.LookupTransfers(ctx, make([]Uint128, n))
				return err
			}},
		} {
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			err := tc.call(ctx, tc.limit+1)
			cancel()
			if !errors.Is(err, ErrTooLarge) {
				t.Fatalf("%s, %d: %v, want ErrTooLarge", tc.name, tc.limit+1, err)
			}
			f.quiet(t, time.Second)
			done := make(chan error, 1)
			go func() { done <- tc.call(context.Background(), tc.limit) }()
			g := f.next(t)
			if len(g.req.Body) != tc.limit*tc.size {
				t.Fatalf("%s, %d: body of %d bytes, want %d", tc.name, tc.limit, len(g.req.Body), tc.limit*tc.size)
			}
			g.ok(t)
			if err := await(t, done); err != nil {
				t.Fatalf("%s, %d: %v", tc.name, tc.limit, err)
			}
		}
	})
}

func TestUnexpectedRepliesAreNeverSuccess(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFake(t)
		c := dialed(t, f)
		defer c.Close()
		ctx := context.Background()
		for _, tc := range []struct {
			status client.Status
			want   error
		}{
			{client.Malformed, ErrRejected},
			{client.Stale, ErrUncertain},
			{client.Status(0), ErrUncertain},
			{client.Status(9), ErrUncertain},
		} {
			sent := goCreate(ctx, c, transfers(5))
			f.next(t).reply(t, client.Reply{Status: tc.status, Leader: 1})
			if r := await(t, sent); !errors.Is(r.err, tc.want) || errors.Is(r.err, ErrUncertain) != (tc.want == ErrUncertain) {
				t.Fatalf("reply %v: %v, want %v", tc.status, r.err, tc.want)
			}
		}
	})
}

func TestAbandonedCallIsAnsweredOnce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFake(t)
		c := dialed(t, f)
		ctx, cancel := context.WithCancel(context.Background())
		cl := &call{op: client.CreateTransfers, records: 1, body: appendTransfer(nil, transfer(5, 0))}
		abandoned := make(chan outcome, 1)
		go func() { abandoned <- c.do(ctx, cl) }()
		synctest.Wait()
		f.next(t)
		cancel()
		if o := await(t, abandoned); !errors.Is(o.err, ErrUncertain) {
			t.Fatalf("abandoned call: %v, want ErrUncertain", o.err)
		}
		f.next(t).ok(t)
		synctest.Wait()
		if len(cl.done) != 0 {
			t.Fatalf("abandoned call got a second answer after the reply: %+v", <-cl.done)
		}
		if err := c.Close(); err != nil {
			t.Fatal(err)
		}
		if len(cl.done) != 0 {
			t.Fatalf("abandoned call got a second answer at Close: %+v", <-cl.done)
		}
	})
}
