package wal

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"go.uber.org/goleak"

	"github.com/Demonisreal/dmn-ledger/internal/disk"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

type slowSyncFS struct {
	*disk.Sim
	delay time.Duration
	syncs *atomic.Int64
}

type slowSyncFile struct {
	disk.File
	fs slowSyncFS
}

func (s slowSyncFS) Create(name string) (disk.File, error) {
	f, err := s.Sim.Create(name)
	if err != nil {
		return nil, err
	}
	return slowSyncFile{f, s}, nil
}

func (f slowSyncFile) Sync() error {
	f.fs.syncs.Add(1)
	time.Sleep(f.fs.delay)
	return f.File.Sync()
}

func startCommitter(t *testing.T, c *Committer) (stop func() error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- c.Run(ctx) }()
	return func() error {
		cancel()
		return <-errc
	}
}

func TestCommitterGroupsSyncs(t *testing.T) {
	syncs := new(atomic.Int64)
	fsys := slowSyncFS{Sim: newSim(1), delay: 2 * time.Millisecond, syncs: syncs}
	w := mustOpen(t, fsys, Options{})
	defer func() { _ = w.Close() }()
	c := NewCommitter(w, 1024, 1<<20)
	stop := startCommitter(t, c)

	const writers, perWriter = 8, 40
	var mu sync.Mutex
	next := uint64(1)
	var wg sync.WaitGroup
	for range writers {
		wg.Go(func() {
			for range perWriter {
				mu.Lock()
				ents := span(next, next+3, 1)
				next += 3
				errc, err := c.Submit(context.Background(), ents...)
				mu.Unlock()
				if err == nil {
					err = <-errc
				}
				if err != nil {
					t.Error(err)
					return
				}
			}
		})
	}
	wg.Wait()
	if err := stop(); err != nil {
		t.Fatal(err)
	}

	checkTerms(t, w, 1, repeat(1, writers*perWriter*3)...)
	if n := syncs.Load(); n > writers*perWriter/2 {
		t.Fatalf("%d syncs for %d submissions", n, writers*perWriter)
	}
}

func TestCommitterRoundLimits(t *testing.T) {
	fixed := func(i uint64) Entry { return Entry{Index: i, Term: 1, Body: make([]byte, 50)} }
	cases := []struct {
		name                 string
		maxEntries, maxBytes int
		subs, perSub         int
		wantSyncs            int64
	}{
		{"entries", 6, 1 << 20, 6, 2, 2},
		{"bytes", 100, 3 * frameSize(fixed(1)), 9, 1, 3},
		{"oversized submission", 4, 1 << 20, 2, 9, 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			syncs := new(atomic.Int64)
			w := mustOpen(t, slowSyncFS{Sim: newSim(1), syncs: syncs}, Options{})
			defer func() { _ = w.Close() }()
			c := NewCommitter(w, tc.maxEntries, tc.maxBytes)

			var waits []<-chan error
			next := uint64(1)
			for range tc.subs {
				var ents []Entry
				for range tc.perSub {
					ents = append(ents, fixed(next))
					next++
				}
				errc, err := c.Submit(context.Background(), ents...)
				if err != nil {
					t.Fatal(err)
				}
				waits = append(waits, errc)
			}
			stop := startCommitter(t, c)
			for _, errc := range waits {
				if err := <-errc; err != nil {
					t.Fatal(err)
				}
			}
			if err := stop(); err != nil {
				t.Fatal(err)
			}
			if got := syncs.Load(); got != tc.wantSyncs {
				t.Fatalf("%d syncs, want %d", got, tc.wantSyncs)
			}
		})
	}
}

func TestCommitterFailStop(t *testing.T) {
	sim := newSim(1)
	w := mustOpen(t, sim, Options{})
	defer func() { _ = w.Close() }()
	c := NewCommitter(w, 64, 1<<20)
	stop := startCommitter(t, c)

	errc, err := c.Submit(context.Background(), ent(1, 1))
	if err != nil || <-errc != nil {
		t.Fatalf("first commit: %v", err)
	}
	sim.SyncErrRate = 1
	errc, err = c.Submit(context.Background(), ent(2, 1))
	if err != nil {
		t.Fatal(err)
	}
	if err := <-errc; !errors.Is(err, syscall.EIO) {
		t.Fatalf("commit with failing sync = %v", err)
	}
	if err := stop(); !errors.Is(err, syscall.EIO) {
		t.Fatalf("Run = %v", err)
	}
	if _, err := c.Submit(context.Background(), ent(3, 1)); !errors.Is(err, syscall.EIO) {
		t.Fatalf("submit after failure = %v", err)
	}
}

func TestCommitterRejectsBadSubmissionOnly(t *testing.T) {
	w := mustOpen(t, newSim(1), Options{})
	defer func() { _ = w.Close() }()
	c := NewCommitter(w, 64, 1<<20)

	good, _ := c.Submit(context.Background(), ent(1, 1))
	bad, _ := c.Submit(context.Background(), ent(5, 1))
	after, _ := c.Submit(context.Background(), ent(2, 1))
	stop := startCommitter(t, c)
	if err := <-good; err != nil {
		t.Fatal(err)
	}
	if err := <-bad; !errors.Is(err, ErrOutOfOrder) {
		t.Fatalf("gap = %v", err)
	}
	if err := <-after; err != nil {
		t.Fatal(err)
	}
	if err := stop(); err != nil {
		t.Fatal(err)
	}
	checkTerms(t, w, 1, 1, 1)
}

func TestCommitterStopAnswersEveryone(t *testing.T) {
	w := mustOpen(t, newSim(1), Options{})
	defer func() { _ = w.Close() }()
	c := NewCommitter(w, 16, 1<<20)

	var waits []<-chan error
	for i := uint64(1); i <= 16; i++ {
		errc, err := c.Submit(context.Background(), ent(i, 1))
		if err != nil {
			t.Fatal(err)
		}
		waits = append(waits, errc)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := c.Run(ctx); err != nil {
		t.Fatal(err)
	}
	for _, errc := range waits {
		select {
		case err := <-errc:
			if err != nil && !errors.Is(err, ErrStopped) {
				t.Fatal(err)
			}
		case <-time.After(time.Second):
			t.Fatal("submission never answered")
		}
	}
	if _, err := c.Submit(context.Background(), ent(17, 1)); !errors.Is(err, ErrStopped) {
		t.Fatalf("submit after stop = %v", err)
	}
}

func TestCommitterSubmitHonoursContext(t *testing.T) {
	w := mustOpen(t, newSim(1), Options{})
	defer func() { _ = w.Close() }()
	c := NewCommitter(w, 1, 1<<20)
	if _, err := c.Submit(context.Background(), ent(1, 1)); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, err := c.Submit(ctx, ent(2, 1)); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("submit into full queue = %v", err)
	}
}
