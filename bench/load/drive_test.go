package load

import (
	"context"
	"errors"
	"reflect"
	"runtime"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

type recorder struct {
	Null
	inner Target
	mu    sync.Mutex
	at    []time.Time
	lanes []int
	ops   []Op
	open  int
	most  int
}

func (r *recorder) Do(ctx context.Context, lane int, op Op) (Outcome, error) {
	r.mu.Lock()
	r.at = append(r.at, time.Now())
	r.lanes = append(r.lanes, lane)
	r.ops = append(r.ops, op)
	r.open++
	r.most = max(r.most, r.open)
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		r.open--
		r.mu.Unlock()
	}()
	return r.inner.Do(ctx, lane, op)
}

type answering func(ctx context.Context) error

func (f answering) Setup(context.Context, Workload) error        { return nil }
func (f answering) Sample(context.Context) (Inside, error)       { return nil, nil }
func (f answering) Verify(context.Context, Books) (Tally, error) { return Tally{}, nil }
func (f answering) Close() error                                 { return nil }
func (f answering) Do(ctx context.Context, _ int, _ Op) (Outcome, error) {
	return Outcome{}, f(ctx)
}

func sleep(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func drive(t *testing.T, target Target, p Plan) *Run {
	t.Helper()
	run, err := Drive(context.Background(), target, NewStream(workloads[0], 7), p)
	if err != nil {
		t.Fatal(err)
	}
	return run
}

func near(got, want, slack time.Duration) bool {
	return got >= want-slack && got <= want+slack
}

func TestSelftest(t *testing.T) {
	if raceOn {
		t.Skip("the race detector slows the dispatcher past the floor the selftest demands")
	}
	st, err := RunSelftest(context.Background())
	t.Logf("floor at %d/s: p50 %v, p99 %v, dispatcher p99 %v", FloorRate, st.FloorP50, st.FloorP99, st.FloorLate)
	t.Logf("stall at %d/s: %d due, %d open at its end, p50 %v, p99 %v, max %v; p99 %v the second before",
		StallRate, st.StallDue, st.StallOpen, st.StallP50, st.StallP99, st.StallMax, st.CalmP99)
	if err != nil {
		t.Fatal(err)
	}
}

func TestStallCountsForEveryArrival(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		from := time.Now().Add(2 * time.Second)
		rec := &recorder{inner: delayed{answer: time.Millisecond, from: from, until: from.Add(time.Second)}}
		run := drive(t, rec, Plan{Rate: 1000, Lanes: 32, Warmup: time.Second, Window: 4 * time.Second, Drain: 2 * time.Second})
		before, hung, after := run.Seconds[1], run.Seconds[2], run.Seconds[3]
		if hung.Due != 1000 || hung.Latency.Finished() != 1000 || hung.Latency.Unfinished() != 0 {
			t.Fatalf("stalled second: %d due, %d answered, %d unfinished, want 1000 answered", hung.Due, hung.Latency.Finished(), hung.Latency.Unfinished())
		}
		for k := 1; k <= 9; k++ {
			q := float64(k) / 10
			want := time.Second - time.Duration(1000-k*100)*time.Millisecond + time.Millisecond
			if got := hung.Latency.Quantile(q); !near(got, want, want/30+time.Millisecond) {
				t.Errorf("stalled second p%d = %v, want about %v: waits must spread evenly over the stall", k*10, got, want)
			}
		}
		if got := hung.Latency.Max(); got < 990*time.Millisecond || got > 1040*time.Millisecond {
			t.Errorf("stalled second max %v, want about 1s", got)
		}
		if hung.Open < 1000 || rec.most < 1000 {
			t.Errorf("%d open at the end of the stall, %d at once in the target, want 1000: the dispatcher waited for answers", hung.Open, rec.most)
		}
		if got := hung.Late.Max(); got > time.Microsecond {
			t.Errorf("dispatcher late by %v during the stall", got)
		}
		for _, sec := range []Second{before, after} {
			if got := sec.Latency.Max(); got > 1040*time.Microsecond {
				t.Errorf("second next to the stall: max %v, want 1ms", got)
			}
		}
	})
}

func TestPlanIsFixed(t *testing.T) {
	for _, rate := range []int{3, 100, 7000} {
		synctest.Test(t, func(t *testing.T) {
			rec := &recorder{inner: answering(func(ctx context.Context) error { return sleep(ctx, 300*time.Millisecond) })}
			run := drive(t, rec, Plan{Rate: rate, Lanes: 4, Window: 2 * time.Second, Drain: time.Second})
			if len(rec.at) != 2*rate {
				t.Fatalf("rate %d: %d calls in 2s, want %d", rate, len(rec.at), 2*rate)
			}
			s := NewStream(workloads[0], 7)
			for i, at := range rec.at {
				due := time.Duration(int64(i) * int64(time.Second) / int64(rate))
				if want := run.Started.Add(due); !at.Equal(want) {
					t.Fatalf("rate %d: call %d at %v, want %v", rate, i, at.Sub(run.Started), due)
				}
				if rec.lanes[i] != i%4 {
					t.Fatalf("rate %d: call %d on lane %d, want %d", rate, i, rec.lanes[i], i%4)
				}
				if a := s.next(due); rec.ops[i] != a.First {
					t.Fatalf("rate %d: call %d sent %+v, want %+v", rate, i, rec.ops[i], a.First)
				}
			}
		})
	}
}

func TestLatencyCountsFromPlan(t *testing.T) {
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(1))
	var once sync.Once
	var start time.Time
	hog := answering(func(context.Context) error {
		if time.Since(start) > 500*time.Millisecond {
			once.Do(func() {
				for t0 := time.Now(); time.Since(t0) < 300*time.Millisecond; {
				}
			})
		}
		return nil
	})
	start = time.Now()
	run := drive(t, hog, Plan{Rate: 2000, Lanes: 4, Window: 2 * time.Second, Drain: time.Second})
	lat, late := merge(run, Window, false), merge(run, Window, true)
	if lat.Finished() != 4000 || late.Finished() != 4000 {
		t.Fatalf("%d answered, %d started, want all 4000 planned: arrivals the dispatcher was late for still count", lat.Finished(), late.Finished())
	}
	if late.Quantile(0.99) < time.Millisecond {
		t.Fatalf("dispatcher late by only %v at p99, the test could not hold it up", late.Quantile(0.99))
	}
	for _, q := range []float64{0.5, 0.9, 0.99, 0.999, 1} {
		if lat.Quantile(q) < late.Quantile(q) {
			t.Errorf("q %v: latency %v below the dispatcher's own delay %v: latency must count from the planned time", q, lat.Quantile(q), late.Quantile(q))
		}
	}
}

func TestWindowCountsByPlannedTime(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		target := answering(func(ctx context.Context) error {
			at := time.Since(start)
			switch {
			case at >= 2900*time.Millisecond:
				<-ctx.Done()
				return ctx.Err()
			case at >= 2*time.Second:
				return sleep(ctx, 2*time.Second)
			}
			return nil
		})
		run := drive(t, target, Plan{Rate: 1000, Lanes: 8, Warmup: time.Second, Window: 2 * time.Second, Drain: 3 * time.Second})
		if run.Window != 2 || len(run.Seconds) != 6 || run.Horizon != 3*time.Second {
			t.Fatalf("window %ds, %d seconds, horizon %v, want 2s, 6 and 3s", run.Window, len(run.Seconds), run.Horizon)
		}
		last := run.Seconds[2]
		if last.Phase != Window || last.Latency.Finished() != 900 || last.Latency.Unfinished() != 100 {
			t.Fatalf("last second of the window: %v, %d answered, %d unfinished, want 900 and 100", last.Phase, last.Latency.Finished(), last.Latency.Unfinished())
		}
		if got := last.Latency.Quantile(0.85); !near(got, 2*time.Second, 70*time.Millisecond) {
			t.Errorf("p85 %v, want 2s: answers in the drain count for the second they were due", got)
		}
		if got := last.Latency.Quantile(0.95); got != Never {
			t.Errorf("p95 %v, want Never: arrivals that never finished count as unfinished", got)
		}
		var due uint64
		for _, sec := range run.Seconds[3:] {
			due += sec.Due
			if sec.Phase != Drain || sec.Latency.Finished()+sec.Latency.Unfinished() != 0 {
				t.Errorf("drain second %v with %d latencies", sec.Phase, sec.Latency.Finished()+sec.Latency.Unfinished())
			}
		}
		if due != 0 {
			t.Errorf("%d arrivals due in the drain", due)
		}
	})
}

func TestSaturationIsDetected(t *testing.T) {
	for _, c := range []struct {
		per       time.Duration
		saturated bool
	}{{2 * time.Millisecond, true}, {500 * time.Microsecond, false}} {
		synctest.Test(t, func(t *testing.T) {
			one := make(chan struct{}, 1)
			server := answering(func(ctx context.Context) error {
				select {
				case one <- struct{}{}:
				case <-ctx.Done():
					return ctx.Err()
				}
				defer func() { <-one }()
				return sleep(ctx, c.per)
			})
			run := drive(t, server, Plan{Rate: 1000, Lanes: 4, Warmup: time.Second, Window: 4 * time.Second, Drain: time.Second})
			if run.Saturated != c.saturated || run.Capped {
				t.Fatalf("%v per call at 1000/s: saturated %v, capped %v, %d open at the end of the window", c.per, run.Saturated, run.Capped, run.OpenAtEnd)
			}
			if !c.saturated {
				return
			}
			if run.OpenAtEnd < 2400 {
				t.Errorf("%d open at the end of the window, want about 2500", run.OpenAtEnd)
			}
			for _, sec := range run.Seconds[1:5] {
				if sec.Booked < 499 || sec.Booked > 501 {
					t.Errorf("%d booked in a second, want 500 under overload", sec.Booked)
				}
			}
		})
	}
}

func TestOpenCallsAreCapped(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rec := &recorder{inner: answering(func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() })}
		run := drive(t, rec, Plan{Rate: 1000, Lanes: 4, Warmup: time.Second, Window: 4 * time.Second, Drain: time.Second, OpenLimit: 500})
		if !run.Capped || !run.Saturated || len(rec.at) != 500 || rec.most != 500 {
			t.Fatalf("capped %v, saturated %v, %d calls, %d at once, want no call past 500 open", run.Capped, run.Saturated, len(rec.at), rec.most)
		}
		if run.Window != 4 {
			t.Fatalf("window %ds, want all 4: the cap holds new calls back, it does not end the stage", run.Window)
		}
		for i, sec := range run.Seconds[:5] {
			if sec.Due != 1000 || sec.Latency.Finished() != 0 || sec.Latency.Unfinished() != 1000 {
				t.Errorf("second %d: %d due, %d answered, %d unfinished, want 1000 due and unfinished: a skipped arrival counts as never answered", i, sec.Due, sec.Latency.Finished(), sec.Latency.Unfinished())
			}
		}
		if started := merge(run, Warmup, true).Finished() + merge(run, Window, true).Finished(); started != 500 {
			t.Errorf("dispatcher delay of %d calls, want only the 500 started", started)
		}
	})
	synctest.Test(t, func(t *testing.T) {
		one := make(chan struct{}, 1)
		server := answering(func(ctx context.Context) error {
			select {
			case one <- struct{}{}:
			case <-ctx.Done():
				return ctx.Err()
			}
			defer func() { <-one }()
			return sleep(ctx, 2*time.Millisecond)
		})
		run := drive(t, server, Plan{Rate: 1000, Lanes: 4, Warmup: time.Second, Window: 4 * time.Second, Drain: time.Second, OpenLimit: 300})
		if !run.Capped || !run.Saturated || run.Window != 4 || run.OpenAtEnd > 300 {
			t.Fatalf("capped %v, saturated %v, window %ds, %d open at the end, want a full window under the cap", run.Capped, run.Saturated, run.Window, run.OpenAtEnd)
		}
		for i, sec := range run.Seconds[1:5] {
			if sec.Booked < 499 || sec.Booked > 501 {
				t.Errorf("window second %d: %d booked, want 500 under overload", i, sec.Booked)
			}
			if n := sec.Latency.Finished() + sec.Latency.Unfinished(); n != sec.Due || sec.Due != 1000 || sec.Latency.Unfinished() < 450 {
				t.Errorf("window second %d: %d due, %d latencies, %d unfinished, want every arrival counted and about 500 skipped", i, sec.Due, n, sec.Latency.Unfinished())
			}
		}
	})
}

func TestWindowStretches(t *testing.T) {
	for _, c := range []struct {
		settles, window int
	}{{-1, 6}, {0, 3}, {3, 3}, {4, 4}, {5, 5}, {100, 6}} {
		synctest.Test(t, func(t *testing.T) {
			var ticks []int
			tick := func(sec int) bool {
				ticks = append(ticks, sec)
				return c.settles >= 0 && sec >= c.settles
			}
			run := drive(t, Null{}, Plan{Rate: 100, Lanes: 1, Warmup: time.Second, Window: 3 * time.Second, MaxWindow: 6 * time.Second, Tick: tick})
			if run.Window != c.window {
				t.Fatalf("settles at %d: window %ds, want %ds", c.settles, run.Window, c.window)
			}
			var due uint64
			for _, sec := range run.Seconds {
				if sec.Phase != Drain {
					due += sec.Due
				}
			}
			if due != uint64(100*(1+c.window)) {
				t.Errorf("settles at %d: %d due, want %d", c.settles, due, 100*(1+c.window))
			}
			if len(ticks) != len(run.Seconds) {
				t.Errorf("%d ticks for %d seconds", len(ticks), len(run.Seconds))
			}
		})
	}
}

func TestFailuresAreCounted(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		broken := errors.New("node gone")
		var n int
		var mu sync.Mutex
		target := answering(func(context.Context) error {
			mu.Lock()
			defer mu.Unlock()
			n++
			if n%10 == 0 {
				return broken
			}
			return nil
		})
		run := drive(t, target, Plan{Rate: 100, Lanes: 2, Window: 2 * time.Second})
		var failed uint64
		for _, sec := range run.Seconds {
			failed += sec.Failed
		}
		if failed != 20 || run.Books.Confirmed != 180 || len(run.Problems) != 1 || run.Problems[0] != "node gone" {
			t.Fatalf("%d failed, %d confirmed, problems %q", failed, run.Books.Confirmed, run.Problems)
		}
		if u := run.Books.Unsure; u.Single.Calls != 20 || u.Calls() != 20 || u.Single.Amount < 20 || u.Single.Amount > 2000 {
			t.Errorf("unsure %+v, want the 20 failed calls with their amounts", u)
		}
		if got := merge(run, Window, false).Unfinished(); got != 20 {
			t.Errorf("%d failed arrivals without latency, want 20 counted as unfinished", got)
		}
	})
}

func TestBooks(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		w, _ := Named("twophase")
		run, err := Drive(context.Background(), Null{}, NewStream(w, 3), Plan{Rate: 500, Lanes: 4, Window: 4 * time.Second})
		if err != nil {
			t.Fatal(err)
		}
		s := NewStream(w, 3)
		var posted uint64
		for i := range 2000 {
			if a := s.next(time.Duration(i) * 2 * time.Millisecond); a.Then.Kind == Post {
				posted += a.Then.Amount
			}
		}
		b := run.Books
		if b.Confirmed != 4000 || b.Posted != posted || b.Pending != 0 || len(b.Sample) != 1000 {
			t.Fatalf("books %d confirmed, %d posted, %d pending, %d sampled, want 4000, %d, 0, 1000", b.Confirmed, b.Posted, b.Pending, len(b.Sample), posted)
		}
		for _, sec := range run.Seconds {
			if sec.Reserve == nil || sec.Reserve.Finished() != sec.Latency.Finished() {
				t.Fatalf("two-phase second without its reserve latencies")
			}
		}
	})
}

func TestCutCallsAreUnsure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		w, _ := Named("twophase")
		target := answering(func(ctx context.Context) error {
			if time.Since(start) < 1500*time.Millisecond {
				return nil
			}
			<-ctx.Done()
			return ctx.Err()
		})
		run, err := Drive(context.Background(), target, NewStream(w, 5), Plan{Rate: 100, Lanes: 2, Window: 2 * time.Second})
		if err != nil {
			t.Fatal(err)
		}
		s := NewStream(w, 5)
		var want Books
		for i := range 200 {
			a := s.next(time.Duration(i) * 10 * time.Millisecond)
			if i < 150 {
				want.Count(a.First, Outcome{}, nil)
				want.Count(a.Then, Outcome{}, nil)
				continue
			}
			want.Count(a.First, Outcome{}, context.Canceled)
		}
		b := run.Books
		if b.Confirmed != want.Confirmed || b.Posted != want.Posted || b.Pending != want.Pending || b.Unsure != want.Unsure || b.Unsure.Reserve.Calls != 50 {
			t.Fatalf("books %d confirmed, %d posted, %d pending, unsure %+v, want %d, %d, %d and %+v: a call the end of the run cut off counts as unsure",
				b.Confirmed, b.Posted, b.Pending, b.Unsure, want.Confirmed, want.Posted, want.Pending, want.Unsure)
		}
	})
}

func TestBooksCount(t *testing.T) {
	var b Books
	failed := errors.New("outcome unknown")
	for _, c := range []struct {
		op  Op
		out Outcome
		err error
	}{
		{Op{Kind: Single, Amount: 3}, Outcome{}, nil},
		{Op{Kind: Single, Amount: 4}, Outcome{Existed: true}, nil},
		{Op{Kind: Reserve, Amount: 10}, Outcome{}, nil},
		{Op{Kind: Reserve, Amount: 20}, Outcome{}, nil},
		{Op{Kind: Post, Amount: 10}, Outcome{}, nil},
		{Op{Kind: Void, Amount: 20}, Outcome{}, failed},
		{Op{Kind: Reserve, Amount: 30}, Outcome{}, nil},
		{Op{Kind: Void, Amount: 30}, Outcome{}, nil},
		{Op{Kind: Single, Amount: 5}, Outcome{}, failed},
		{Op{Kind: Reserve, Amount: 6}, Outcome{}, failed},
		{Op{Kind: Post, Amount: 20}, Outcome{}, failed},
	} {
		b.Count(c.op, c.out, c.err)
	}
	want := Books{Confirmed: 7, Existed: 1, Posted: 13, Pending: 20,
		Unsure: Unsure{Single: Doubt{1, 5}, Reserve: Doubt{1, 6}, Post: Doubt{1, 20}, Void: Doubt{1, 20}}}
	if !reflect.DeepEqual(b, want) || b.Unsure.Calls() != 4 {
		t.Fatalf("books %+v, want %+v", b, want)
	}
}

func TestDriveStopsWithItsContext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
		defer cancel()
		hang := answering(func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() })
		run, err := Drive(ctx, hang, NewStream(workloads[0], 1), Plan{Rate: 100, Lanes: 2, Window: 5 * time.Second})
		if run != nil || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("drive after its context ended: %v, %v", run, err)
		}
	})
}

func TestDriveRefusesBadPlans(t *testing.T) {
	for _, p := range []Plan{
		{Rate: 0, Lanes: 1, Window: time.Second},
		{Rate: 1, Lanes: 0, Window: time.Second},
		{Rate: 1, Lanes: 1},
		{Rate: 1, Lanes: 1, Window: 1500 * time.Millisecond},
		{Rate: 1, Lanes: 1, Window: 2 * time.Second, MaxWindow: time.Second},
		{Rate: 1, Lanes: 1, Window: time.Second, Drain: 6 * time.Second},
		{Rate: 1, Lanes: 1, Window: time.Second, OpenLimit: MaxOpen + 1},
	} {
		if _, err := Drive(context.Background(), Null{}, NewStream(workloads[0], 1), p); !errors.Is(err, ErrPlan) {
			t.Errorf("%+v: %v, want ErrPlan", p, err)
		}
	}
}
