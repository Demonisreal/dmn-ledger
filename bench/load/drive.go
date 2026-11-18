package load

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/Demonisreal/dmn-ledger/ledger"
)

var (
	ErrPlan = errors.New("load: bad plan")
	ErrShed = errors.New("load: call given up")
)

const (
	MaxOpen     = 100_000
	MaxDrain    = 5 * time.Second
	sampleSize  = 1000
	maxProblems = 10
	coarse      = 2 * time.Millisecond
)

type Plan struct {
	Rate      int
	Lanes     int
	Warmup    time.Duration
	Window    time.Duration
	MaxWindow time.Duration
	Drain     time.Duration
	OpenLimit int
	Tick      func(sec int) (enough bool)
}

type Phase uint8

const (
	Warmup Phase = iota
	Window
	Drain
)

func (p Phase) String() string {
	switch p {
	case Warmup:
		return "warmup"
	case Window:
		return "window"
	}
	return "drain"
}

type Second struct {
	Phase   Phase
	Due     uint64
	Booked  uint64
	Open    uint64
	Retries uint64
	Failed  uint64
	Shed    uint64
	Latency *Histogram
	Reserve *Histogram
	Late    *Histogram
	Inside  Inside
	Sampled bool
}

type Run struct {
	Plan      Plan
	Started   time.Time
	Warmup    int
	Window    int
	Seconds   []Second
	Before    Inside
	Saturated bool
	Capped    bool
	OpenAtEnd uint64
	Horizon   time.Duration
	Books     Books
	Problems  []string
}

type slot struct {
	due, done, booked, retries, failed, shed atomic.Uint64
	latency, late                            Histogram
	reserve                                  *Histogram
	open                                     uint64
	inside                                   Inside
	sampled                                  bool
}

type driver struct {
	t      Target
	plan   Plan
	start  time.Time
	slots  []slot
	open   atomic.Int64
	until  atomic.Int64
	capped atomic.Bool
	calls  sync.WaitGroup
	frozen bool

	confirmed, existed atomic.Uint64
	posted, pending    atomic.Int64
	stride             int

	mu       sync.Mutex
	sample   []ledger.Uint128
	problems []string
	unsure   Unsure
}

func (p Plan) check() error {
	whole := func(d time.Duration) bool { return d >= 0 && d%time.Second == 0 }
	switch {
	case p.Rate < 1 || p.Lanes < 1:
		return fmt.Errorf("%w: rate %d, lanes %d", ErrPlan, p.Rate, p.Lanes)
	case !whole(p.Warmup) || !whole(p.Window) || !whole(p.MaxWindow) || !whole(p.Drain):
		return fmt.Errorf("%w: warmup, window and drain must be whole seconds", ErrPlan)
	case p.Window < time.Second || p.MaxWindow != 0 && p.MaxWindow < p.Window:
		return fmt.Errorf("%w: window %v, max %v", ErrPlan, p.Window, p.MaxWindow)
	case p.Drain > MaxDrain:
		return fmt.Errorf("%w: drain %v over %v", ErrPlan, p.Drain, MaxDrain)
	case p.OpenLimit < 0 || p.OpenLimit > MaxOpen:
		return fmt.Errorf("%w: open limit %d", ErrPlan, p.OpenLimit)
	}
	return nil
}

func Drive(ctx context.Context, t Target, s *Stream, p Plan) (*Run, error) {
	if err := p.check(); err != nil {
		return nil, err
	}
	if p.MaxWindow == 0 || p.Tick == nil {
		p.MaxWindow = p.Window
	}
	if p.OpenLimit == 0 {
		p.OpenLimit = MaxOpen
	}
	warm, win, longest, drain := int(p.Warmup/time.Second), int(p.Window/time.Second), int(p.MaxWindow/time.Second), int(p.Drain/time.Second)
	d := &driver{t: t, plan: p, slots: make([]slot, warm+longest+drain+1)}
	if s.w.TwoPhase {
		for i := range d.slots {
			d.slots[i].reserve = new(Histogram)
		}
	}
	d.stride = max(1, p.Rate*(warm+win)/sampleSize)
	d.until.Store(int64(time.Duration(warm+longest) * time.Second))

	run := &Run{Plan: p, Warmup: warm}
	before, err := t.Sample(ctx)
	if err != nil {
		d.problem(fmt.Errorf("sample: %w", err))
	}
	run.Before = before

	calls, stop := context.WithCancel(ctx)
	defer stop()
	run.Started = time.Now()
	d.start = run.Started
	dispatched := make(chan struct{})
	go func() {
		defer close(dispatched)
		d.dispatch(calls, s)
	}()

	end, total, err := d.tick(ctx, run, warm, win, longest, drain)
	stop()
	<-dispatched
	d.calls.Wait()
	if err != nil {
		return nil, err
	}

	run.Window = max(end-warm, 0)
	var due, shed uint64
	for i := warm; i < end; i++ {
		due, shed = due+d.slots[i].due.Load(), shed+d.slots[i].shed.Load()
	}
	run.Saturated = run.Capped || run.OpenAtEnd > uint64(p.Rate) || 100*shed > due //nolint:gosec // G115: Rate is at least 1
	run.Horizon = time.Duration(total-end) * time.Second
	run.Seconds = make([]Second, total)
	for i := range run.Seconds {
		sl := &d.slots[i]
		sec := &run.Seconds[i]
		sec.Phase = Drain
		switch {
		case i < warm:
			sec.Phase = Warmup
		case i < end:
			sec.Phase = Window
		}
		sec.Due, sec.Booked, sec.Retries, sec.Failed, sec.Shed = sl.due.Load(), sl.booked.Load(), sl.retries.Load(), sl.failed.Load(), sl.shed.Load()
		sec.Open, sec.Inside, sec.Sampled = sl.open, sl.inside, sl.sampled
		sec.Latency, sec.Late, sec.Reserve = &sl.latency, &sl.late, sl.reserve
	}
	run.Books = Books{
		Confirmed: d.confirmed.Load(),
		Existed:   d.existed.Load(),
		Posted:    uint64(d.posted.Load()),  //nolint:gosec // G115: a sum of positive amounts
		Pending:   uint64(d.pending.Load()), //nolint:gosec // G115: every release follows its reserve
		Unsure:    d.unsure,
		Sample:    d.sample,
	}
	run.Problems = d.problems
	return run, nil
}

func (d *driver) tick(ctx context.Context, run *Run, warm, win, longest, drain int) (end, total int, err error) {
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	end = -1
	for sec := 0; ; sec++ {
		timer.Reset(time.Until(d.start.Add(time.Duration(sec+1) * time.Second)))
		select {
		case <-ctx.Done():
			return 0, 0, ctx.Err()
		case <-timer.C:
		}
		var open uint64
		for i := range d.slots[:sec+1] {
			done := d.slots[i].done.Load()
			open += d.slots[i].due.Load() - done
		}
		enough := true
		if d.plan.Tick != nil {
			enough = d.plan.Tick(sec)
		}
		if end < 0 {
			n := sec + 1 - warm
			if n >= longest || n >= win && enough {
				end = sec + 1
				d.until.Store(min(d.until.Load(), int64(time.Duration(end)*time.Second)))
				run.Capped = d.capped.Load()
				run.OpenAtEnd = open
			}
		}
		inside, err := d.t.Sample(ctx)
		if err != nil {
			d.problem(fmt.Errorf("sample: %w", err))
		}
		sl := &d.slots[sec]
		sl.open, sl.inside, sl.sampled = open, inside, err == nil
		if end >= 0 && (d.open.Load() == 0 || sec+1-end >= drain) {
			return end, sec + 1, nil
		}
	}
}

func (d *driver) dispatch(ctx context.Context, s *Stream) {
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	var last time.Duration
	defer func() { s.clock += uint64(last/time.Millisecond) + 1 }() //nolint:gosec // G115: last is never negative
	rate := int64(d.plan.Rate)
	for i := int64(0); ; i++ {
		due := time.Duration(i * int64(time.Second) / rate)
		if int64(due) >= d.until.Load() {
			return
		}
		if time.Until(d.start.Add(due)) > 0 {
			if !d.await(ctx, timer, d.start.Add(due)) {
				return
			}
			if int64(due) >= d.until.Load() {
				return
			}
		}
		a := s.next(due)
		last = due
		sl := &d.slots[due/time.Second]
		sl.due.Add(1)
		if d.open.Load() >= int64(d.plan.OpenLimit) {
			d.capped.Store(true)
			sl.latency.AddNever()
			if sl.reserve != nil {
				sl.reserve.AddNever()
			}
			sl.done.Add(1)
			continue
		}
		d.open.Add(1)
		d.calls.Add(1)
		go d.call(ctx, int(i%int64(d.plan.Lanes)), int(i), due, a)
	}
}

func (d *driver) await(ctx context.Context, timer *time.Timer, due time.Time) bool {
	for {
		wait := time.Until(due)
		if wait <= 0 {
			return true
		}
		if wait <= coarse && !d.frozen {
			before := time.Now()
			ts := syscall.NsecToTimespec(int64(wait))
			_ = syscall.Nanosleep(&ts, nil)
			if d.frozen = time.Now().Equal(before); !d.frozen {
				continue
			}
		}
		if wait > coarse {
			wait -= coarse
		}
		timer.Reset(wait)
		select {
		case <-ctx.Done():
			return false
		case <-timer.C:
		}
	}
}

func (d *driver) call(ctx context.Context, lane, i int, due time.Duration, a Arrival) {
	defer d.calls.Done()
	sl := &d.slots[due/time.Second]
	defer d.open.Add(-1)
	defer sl.done.Add(1)
	planned := d.start.Add(due)
	sl.late.Add(time.Since(planned))
	if !d.do(ctx, lane, a.First, sl) {
		sl.latency.AddNever()
		if sl.reserve != nil {
			sl.reserve.AddNever()
		}
		return
	}
	last := a.First.ID
	if a.Then.Kind != 0 {
		sl.reserve.Add(time.Since(planned))
		if !d.do(ctx, lane, a.Then, sl) {
			sl.latency.AddNever()
			return
		}
		last = a.Then.ID
	}
	sl.latency.Add(time.Since(planned))
	if i%d.stride == 0 {
		d.mu.Lock()
		if len(d.sample) < sampleSize {
			d.sample = append(d.sample, last)
		}
		d.mu.Unlock()
	}
}

func (d *driver) do(ctx context.Context, lane int, op Op, sl *slot) bool {
	out, err := d.t.Do(ctx, lane, op)
	if errors.Is(err, ErrShed) {
		sl.shed.Add(1)
		return false
	}
	if err != nil {
		if ctx.Err() == nil {
			sl.failed.Add(1)
			d.problem(err)
		}
		d.mu.Lock()
		d.unsure.add(op)
		d.mu.Unlock()
		return false
	}
	at := min(int(time.Since(d.start)/time.Second), len(d.slots)-1)
	d.slots[at].booked.Add(1)
	sl.retries.Add(uint64(out.Retries)) //nolint:gosec // G115: a count of retries, never negative
	d.confirmed.Add(1)
	if out.Existed {
		d.existed.Add(1)
		return true
	}
	amount := int64(op.Amount) //nolint:gosec // G115: an amount from 1 to 100
	switch op.Kind {
	case Single:
		d.posted.Add(amount)
	case Reserve:
		d.pending.Add(amount)
	case Post:
		d.pending.Add(-amount)
		d.posted.Add(amount)
	case Void:
		d.pending.Add(-amount)
	}
	return true
}

func (d *driver) problem(err error) {
	msg := err.Error()
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.problems) < maxProblems && !slices.Contains(d.problems, msg) {
		d.problems = append(d.problems, msg)
	}
}
