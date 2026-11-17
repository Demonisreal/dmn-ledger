package pg

import (
	"context"
	"slices"
	"testing"
	"testing/synctest"

	"github.com/Demonisreal/dmn-ledger/bench/load"
	"github.com/Demonisreal/dmn-ledger/ledger"
)

type fakeDB struct {
	batches chan []load.Op
	gate    chan struct{}
}

func (f *fakeDB) send(_ context.Context, ops []load.Op) []answer {
	f.batches <- ops
	<-f.gate
	answers := make([]answer, len(ops))
	for i, op := range ops {
		answers[i].out = load.Outcome{Existed: op.Amount%2 == 0, Retries: int(op.Debit)} //nolint:gosec // G115: small test numbers
	}
	return answers
}

type done struct {
	out load.Outcome
	err error
}

func startBundler(t *testing.T) (*fakeDB, *bundler) {
	t.Helper()
	f := &fakeDB{batches: make(chan []load.Op), gate: make(chan struct{})}
	b := newBundler(f.send)
	ctx, stop := context.WithCancel(context.Background())
	quit := make(chan struct{})
	go func() {
		defer close(quit)
		b.run(ctx)
	}()
	t.Cleanup(func() {
		stop()
		<-quit
	})
	return f, b
}

func goDo(b *bundler, op load.Op) <-chan done {
	ch := make(chan done, 1)
	go func() {
		out, err := b.do(context.Background(), op)
		ch <- done{out, err}
	}()
	synctest.Wait()
	return ch
}

func single(id uint64) load.Op {
	return load.Op{Kind: load.Single, ID: ledger.From64(id), Debit: id % 5, Credit: 9, Amount: id}
}

func ids(ops []load.Op) []uint64 {
	var got []uint64
	for _, op := range ops {
		got = append(got, op.ID.Lo)
	}
	return got
}

func TestBundlerKeepsOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, b := startBundler(t)
		first := goDo(b, single(1))
		if got := ids(<-f.batches); !slices.Equal(got, []uint64{1}) {
			t.Fatalf("first batch %v, want [1]", got)
		}
		var calls []<-chan done
		for id := uint64(10); id < 16; id++ {
			calls = append(calls, goDo(b, single(id)))
		}
		f.gate <- struct{}{}
		<-first
		if got, want := ids(<-f.batches), []uint64{10, 11, 12, 13, 14, 15}; !slices.Equal(got, want) {
			t.Fatalf("second batch %v, want %v", got, want)
		}
		f.gate <- struct{}{}
		for _, ch := range calls {
			if d := <-ch; d.err != nil {
				t.Fatal(d.err)
			}
		}
	})
}

func TestBundlerSplitsResults(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, b := startBundler(t)
		first := goDo(b, single(1))
		<-f.batches
		calls := map[uint64]<-chan done{}
		for id := uint64(20); id < 28; id++ {
			calls[id] = goDo(b, single(id))
		}
		f.gate <- struct{}{}
		<-first
		<-f.batches
		f.gate <- struct{}{}
		for id, ch := range calls {
			d := <-ch
			want := load.Outcome{Existed: id%2 == 0, Retries: int(id % 5)} //nolint:gosec // G115: small test numbers
			if d.err != nil || d.out != want {
				t.Fatalf("transfer %d got %+v, %v, want %+v", id, d.out, d.err, want)
			}
		}
	})
}

func TestBundlerWaitsForTheBatch(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, b := startBundler(t)
		first := goDo(b, single(1))
		<-f.batches
		synctest.Wait()
		select {
		case d := <-first:
			t.Fatalf("answered %+v before the batch came back", d)
		default:
		}
		f.gate <- struct{}{}
		<-first
	})
}

func TestBundleStaysUnderTheLimit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, b := startBundler(t)
		if b.limit != 10_485 {
			t.Fatalf("a batch carries up to %d transfers, want 10485 like a request of the SDK", b.limit)
		}
		b.limit = 4
		first := goDo(b, single(1))
		<-f.batches
		var calls []<-chan done
		for id := range uint64(11) {
			calls = append(calls, goDo(b, single(100+id)))
		}
		f.gate <- struct{}{}
		<-first
		for _, want := range []int{4, 4, 3} {
			if got := <-f.batches; len(got) != want {
				t.Fatalf("batch of %d, want %d", len(got), want)
			}
			f.gate <- struct{}{}
		}
		for _, ch := range calls {
			<-ch
		}
	})
}

func TestBundlerKeepsTwoPhaseApart(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, b := startBundler(t)
		first := goDo(b, single(1))
		<-f.batches
		ops := []load.Op{
			{Kind: load.Reserve, ID: ledger.From64(2), Amount: 1},
			{Kind: load.Post, ID: ledger.From64(3), Amount: 1},
			single(4),
			single(5),
			{Kind: load.Void, ID: ledger.From64(6), Amount: 1},
		}
		var calls []<-chan done
		for _, op := range ops {
			calls = append(calls, goDo(b, op))
		}
		f.gate <- struct{}{}
		<-first
		for _, want := range [][]uint64{{2, 3}, {4, 5}, {6}} {
			if got := ids(<-f.batches); !slices.Equal(got, want) {
				t.Fatalf("batch %v, want %v", got, want)
			}
			f.gate <- struct{}{}
		}
		for _, ch := range calls {
			<-ch
		}
	})
}
