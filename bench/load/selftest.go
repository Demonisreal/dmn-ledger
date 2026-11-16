package load

import (
	"context"
	"errors"
	"fmt"
	"time"
)

var ErrSelftest = errors.New("load: selftest failed")

const (
	FloorRate = 200_000
	StallRate = 1_000
)

type Selftest struct {
	FloorP50, FloorP99, FloorLate time.Duration
	CalmP99                       time.Duration
	StallDue, StallOpen           uint64
	StallP50, StallP99, StallMax  time.Duration
}

type delayed struct {
	Null
	answer      time.Duration
	from, until time.Time
}

func (t delayed) Do(ctx context.Context, _ int, _ Op) (Outcome, error) {
	wait := t.answer
	if now := time.Now(); !now.Before(t.from) && now.Before(t.until) {
		wait += t.until.Sub(now)
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return Outcome{}, ctx.Err()
	case <-timer.C:
		return Outcome{}, nil
	}
}

func RunSelftest(ctx context.Context) (Selftest, error) {
	var st Selftest
	floor, err := Drive(ctx, Null{}, NewStream(workloads[0], 1), Plan{Rate: FloorRate, Lanes: 32, Warmup: time.Second, Window: 2 * time.Second, Drain: time.Second})
	if err != nil {
		return st, err
	}
	lat, late := merge(floor, Window, false), merge(floor, Window, true)
	st.FloorP50, st.FloorP99, st.FloorLate = lat.Quantile(0.5), lat.Quantile(0.99), late.Quantile(0.99)

	from := time.Now().Add(2 * time.Second)
	stuck := delayed{answer: time.Millisecond, from: from, until: from.Add(time.Second)}
	stall, err := Drive(ctx, stuck, NewStream(workloads[0], 2), Plan{Rate: StallRate, Lanes: 32, Warmup: time.Second, Window: 3 * time.Second, Drain: 2 * time.Second})
	if err != nil {
		return st, err
	}
	calm, hung := stall.Seconds[1], stall.Seconds[2]
	st.CalmP99 = calm.Latency.Quantile(0.99)
	st.StallDue, st.StallOpen = hung.Due, hung.Open
	st.StallP50, st.StallP99, st.StallMax = hung.Latency.Quantile(0.5), hung.Latency.Quantile(0.99), hung.Latency.Max()

	switch {
	case st.FloorP99 >= time.Millisecond || st.FloorLate >= time.Millisecond:
		return st, fmt.Errorf("%w: floor p99 %v, dispatcher late by %v at %d/s", ErrSelftest, st.FloorP99, st.FloorLate, FloorRate)
	case st.StallDue < StallRate*95/100 || st.StallOpen < st.StallDue*9/10:
		return st, fmt.Errorf("%w: %d arrivals due in the stall, %d open at its end", ErrSelftest, st.StallDue, st.StallOpen)
	case st.StallP50 < 400*time.Millisecond || st.StallP50 > 600*time.Millisecond || st.StallP99 < 900*time.Millisecond || st.StallMax > 1100*time.Millisecond:
		return st, fmt.Errorf("%w: a one second stall gave p50 %v, p99 %v, max %v", ErrSelftest, st.StallP50, st.StallP99, st.StallMax)
	case st.CalmP99 > 50*time.Millisecond:
		return st, fmt.Errorf("%w: p99 %v before the stall", ErrSelftest, st.CalmP99)
	}
	return st, nil
}

func merge(r *Run, phase Phase, late bool) *Histogram {
	h := new(Histogram)
	for _, sec := range r.Seconds {
		switch {
		case sec.Phase != phase:
		case late:
			h.Merge(sec.Late)
		default:
			h.Merge(sec.Latency)
		}
	}
	return h
}
