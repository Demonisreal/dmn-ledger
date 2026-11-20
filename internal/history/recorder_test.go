package history

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Demonisreal/dmn-ledger/internal/check"
	"github.com/Demonisreal/dmn-ledger/internal/client"
	"github.com/Demonisreal/dmn-ledger/internal/sim"
	"github.com/Demonisreal/dmn-ledger/internal/state"
	"github.com/Demonisreal/dmn-ledger/internal/u128"
)

var answered = client.Reply{Status: client.OK}

func since() func() uint64 {
	began := time.Now()
	return func() uint64 { return uint64(time.Since(began)) }
}

func ids(of ...uint64) []byte {
	var list []u128.Uint128
	for _, id := range of {
		list = append(list, u128.From64(id))
	}
	return client.AppendIDs(nil, list)
}

func lookup() (client.Op, []byte) {
	return client.LookupAccounts, ids(9)
}

func opening() (client.Op, []byte) {
	return client.CreateAccounts, state.AppendAccount(nil, state.Account{ID: u128.From64(1), Ledger: 1, Code: 1})
}

func begin(ctx context.Context, t *testing.T, rec *Recorder, pick func() (client.Op, []byte)) Turn {
	t.Helper()
	tr, ok := rec.Begin(ctx, 1, pick)
	if !ok {
		t.Fatal("Begin gave up")
	}
	return tr
}

func later(ctx context.Context, rec *Recorder) chan Turn {
	next := make(chan Turn, 1)
	go func() {
		if tr, ok := rec.Begin(ctx, 2, lookup); ok {
			next <- tr
		}
	}()
	return next
}

type sink struct{ windows [][]sim.Call }

func (s *sink) take(calls []sim.Call) error {
	s.windows = append(s.windows, calls)
	return nil
}

func (s *sink) sizes() string {
	var sizes []int
	for _, w := range s.windows {
		sizes = append(sizes, len(w))
	}
	return fmt.Sprint(sizes)
}

func TestWindowClosesAfterItsCalls(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		var s sink
		rec := NewRecorder(Options{Window: 16, Backlog: 4, Since: since(), Sink: s.take}, check.NewReplayer())
		var turns []Turn
		for range 16 {
			turns = append(turns, begin(ctx, t, rec, lookup))
		}
		next := later(ctx, rec)
		for k, tr := range turns[:15] {
			rec.End(tr, answered)
			synctest.Wait()
			if len(next) > 0 {
				t.Fatalf("call 17 began with %d of 16 still open", 15-k)
			}
		}
		rec.End(turns[15], answered)
		synctest.Wait()
		select {
		case tr := <-next:
			if tr.N != 16 {
				t.Fatalf("call 17 is number %d", tr.N)
			}
		default:
			t.Fatal("call 17 did not begin after all 16 ended")
		}
		if got := s.sizes(); got != "[16]" {
			t.Fatalf("windows %s, want [16]", got)
		}
	})
}

func TestBacklogHoldsTheLoad(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		const backlog, most = 4, 100
		rec := NewRecorder(Options{Window: 1, Backlog: backlog, Since: since()}, check.NewReplayer())
		begun := make(chan Turn, most)
		go func() {
			for range most {
				tr, ok := rec.Begin(ctx, 1, lookup)
				if !ok {
					return
				}
				rec.End(tr, answered)
				begun <- tr
			}
		}()
		synctest.Wait()
		if n := len(begun); n != backlog+1 {
			t.Fatalf("%d calls began while nobody judged, want %d", n, backlog+1)
		}
		go func() { _ = rec.Judge(ctx) }()
		synctest.Wait()
		if n := len(begun); n != most {
			t.Fatalf("%d calls began once the windows were judged, want %d", n, most)
		}
	})
}

func TestHoldWaitsForOpenCalls(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		var s sink
		rec := NewRecorder(Options{Window: 16, Backlog: 4, Since: since(), Sink: s.take}, check.NewReplayer())
		first, second := begin(ctx, t, rec, lookup), begin(ctx, t, rec, lookup)
		held := make(chan error, 1)
		go func() { held <- rec.Hold(ctx, 10*time.Second) }()
		synctest.Wait()
		next := later(ctx, rec)
		rec.End(first, answered)
		synctest.Wait()
		if len(held) > 0 {
			t.Fatalf("Hold returned %v with a call open", <-held)
		}
		rec.End(second, answered)
		synctest.Wait()
		select {
		case err := <-held:
			if err != nil {
				t.Fatal(err)
			}
		default:
			t.Fatal("Hold still waits after the last call ended")
		}
		if got := s.sizes(); got != "[2]" {
			t.Fatalf("windows %s after Hold, want [2]", got)
		}
		if len(next) > 0 {
			t.Fatal("a call began while the load was held")
		}
		rec.Resume()
		synctest.Wait()
		if tr, ok := <-next; !ok || tr.N != 2 {
			t.Fatalf("after Resume call %d began, want 2", tr.N)
		}
	})
}

func TestHoldGivesUp(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		var s sink
		rec := NewRecorder(Options{Window: 16, Backlog: 4, Since: since(), Sink: s.take}, check.NewReplayer())
		open := begin(ctx, t, rec, lookup)
		began := time.Now()
		if err := rec.Hold(ctx, time.Second); !errors.Is(err, ErrOpen) {
			t.Fatalf("Hold with an open call: %v, want ErrOpen", err)
		}
		if waited := time.Since(began); waited != time.Second {
			t.Fatalf("Hold gave up after %v, want 1s", waited)
		}
		next := later(ctx, rec)
		rec.End(open, answered)
		synctest.Wait()
		if len(next) > 0 {
			t.Fatal("a call began after Hold gave up")
		}
		rec.Resume()
		synctest.Wait()
		if len(next) == 0 {
			t.Fatal("no call began after Resume")
		}
		if len(s.windows) > 0 {
			t.Fatalf("windows %s, the open one must stay open", s.sizes())
		}
	})
}

func TestPushedWindowIsJudgedInOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		rec := NewRecorder(Options{Window: 16, Backlog: 4, Since: since()}, check.NewReplayer())
		tr := begin(ctx, t, rec, opening)
		rec.End(tr, client.Reply{Status: client.OK, Time: 100})
		if err := rec.Hold(ctx, time.Second); err != nil {
			t.Fatal(err)
		}
		shown := state.AppendAccount(nil, state.Account{ID: u128.From64(1), Ledger: 1, Code: 1, Timestamp: 100})
		rec.Push([]sim.Call{{Client: 2, Op: client.LookupAccounts, Body: ids(1), Reply: client.Reply{Status: client.OK, Body: shown}}})
		rec.Resume()
		rec.Close()
		if err := rec.Judge(ctx); err != nil {
			t.Fatal(err)
		}
		if calls, windows := rec.Judged(); calls != 2 || windows != 2 {
			t.Fatalf("judged %d calls in %d windows, want 2 in 2", calls, windows)
		}
	})
}

func TestJudgeReturnsTheViolation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		rec := NewRecorder(Options{Window: 16, Backlog: 4, Since: since()}, check.NewReplayer())
		tr := begin(ctx, t, rec, opening)
		rec.End(tr, client.Reply{Status: client.OK, Time: 100, Body: client.AppendFailures(nil, []state.Failure{{Reason: state.Exists}})})
		if err := rec.Hold(ctx, time.Second); err != nil {
			t.Fatal(err)
		}
		rec.Push([]sim.Call{{Client: 2, Op: client.LookupAccounts, Body: ids(9), Reply: answered}})
		rec.Close()
		err := rec.Judge(ctx)
		if v, ok := errors.AsType[*check.Violation](err); !ok || v.Check != "S8" {
			t.Fatalf("Judge returned %v, want a violation of S8", err)
		}
		if calls, windows := rec.Judged(); calls != 0 || windows != 0 {
			t.Fatalf("judged %d calls in %d windows after the violation", calls, windows)
		}
	})
}

func TestSinkSeesEveryWindow(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		var s sink
		rec := NewRecorder(Options{Window: 4, Backlog: 8, Since: since(), Sink: s.take}, check.NewReplayer())
		for range 10 {
			rec.End(begin(ctx, t, rec, lookup), answered)
		}
		if err := rec.Hold(ctx, time.Second); err != nil {
			t.Fatal(err)
		}
		rec.Push([]sim.Call{{Client: 2, Op: client.LookupAccounts, Body: ids(9), Reply: answered}})
		rec.Close()
		if err := rec.Judge(ctx); err != nil {
			t.Fatal(err)
		}
		if got := s.sizes(); got != "[4 4 2 1]" {
			t.Fatalf("windows %s, want [4 4 2 1]", got)
		}
		for _, w := range s.windows[:3] {
			for _, call := range w {
				if call.Reply.Status != client.OK {
					t.Fatalf("the sink saw a call without its reply: %+v", call)
				}
			}
		}
		if calls, windows := rec.Judged(); calls != 11 || windows != 4 {
			t.Fatalf("judged %d calls in %d windows, want 11 in 4", calls, windows)
		}
	})
}

func TestSinkErrorStopsJudge(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		full := errors.New("disk full")
		var seen int
		rec := NewRecorder(Options{Window: 1, Backlog: 8, Since: since(), Sink: func([]sim.Call) error {
			seen++
			if seen == 2 {
				return full
			}
			return nil
		}}, check.NewReplayer())
		for range 4 {
			rec.End(begin(ctx, t, rec, lookup), answered)
		}
		rec.Close()
		if err := rec.Judge(ctx); !errors.Is(err, full) {
			t.Fatalf("Judge returned %v, want the error of the sink", err)
		}
		if seen != 2 {
			t.Fatalf("the sink saw %d windows, want none after its error", seen)
		}
	})
}
