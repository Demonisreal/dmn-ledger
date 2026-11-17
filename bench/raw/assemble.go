package raw

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/Demonisreal/dmn-ledger/bench/control"
	"github.com/Demonisreal/dmn-ledger/bench/load"
)

var ErrMismatch = errors.New("raw: run and control do not line up")

const (
	WhyNoSample  = "no sample"
	WhyRestless  = "restless"
	WhyFailed    = "calls failed"
	WhyGenerator = "generator at its limit"
	WhyTerm      = "term changed"
	WhyVerify    = "verify failed"
	WhyStale     = "stale status"
	WhyStalled   = "target stalled"

	LimitTarget    = "target"
	LimitGenerator = "generator"

	LateLimit = time.Millisecond
	CPULimit  = 0.8
)

type delta struct {
	syncs, requests uint64
	parts           map[string]uint64
}

func Assemble(h Head, run *load.Run, ctl []control.Second) (*File, error) {
	if h.Load.Need < 1 {
		return nil, fmt.Errorf("raw: a speed needs at least one valid second, not %d", h.Load.Need)
	}
	if len(ctl) < len(run.Seconds) {
		return nil, fmt.Errorf("%w: %d seconds driven, %d measured", ErrMismatch, len(run.Seconds), len(ctl))
	}
	h.Schema = Schema
	h.Started = run.Started.UTC()
	marks := h.Thresholds.Judge(ctl[:len(run.Seconds)])
	f := &File{Head: h, Before: parts(run.Before), Seconds: make([]Second, len(run.Seconds))}
	deltas := make([]delta, len(run.Seconds))
	prev := run.Before
	for i, sec := range run.Seconds {
		c, m := ctl[i], marks[i]
		if !sec.Sampled || i > 0 && !run.Seconds[i-1].Sampled {
			m.Valid, m.Why = false, append(m.Why, WhyNoSample)
		}
		if stale(sec.Inside) || i > 0 && stale(run.Seconds[i-1].Inside) {
			m.Valid, m.Why = false, append(m.Why, WhyStale)
		}
		f.Seconds[i] = Second{
			Phase:      sec.Phase.String(),
			Due:        sec.Due,
			Booked:     sec.Booked,
			Open:       sec.Open,
			Retries:    sec.Retries,
			Failed:     sec.Failed,
			Count:      sec.Latency.Finished() + sec.Latency.Unfinished(),
			P50:        sec.Latency.Quantile(0.5),
			P99:        sec.Latency.Quantile(0.99),
			LateP99:    sec.Late.Quantile(0.99),
			Flushes:    c.Flushes,
			FlushTime:  c.FlushTime,
			Writes:     c.Writes,
			WriteTime:  c.WriteTime,
			CPU:        CPU{Generator: c.Generator, Target: c.Target, Foreign: c.Foreign},
			MHz:        c.MHz,
			Throttled:  c.Throttled,
			Available:  c.Available,
			SwappedOut: c.SwappedOut,
			Speed:      m.Speed,
			Valid:      m.Valid,
			Why:        m.Why,
			Parts:      parts(sec.Inside),
		}
		deltas[i] = since(sec.Inside, prev)
		prev = sec.Inside
	}

	var window []control.Mark
	late := new(load.Histogram)
	var booked uint64
	var generator, target float64
	stalled := false
	for i, sec := range run.Seconds {
		if sec.Phase != load.Window {
			continue
		}
		window = append(window, control.Mark{Speed: f.Seconds[i].Speed, Valid: f.Seconds[i].Valid})
		stalled = stalled || slices.ContainsFunc(f.Seconds[i].Why, func(why string) bool {
			return why == WhyNoSample || why == WhyStale || why == control.WhyFewFlushes
		})
		late.Merge(sec.Late)
		booked += sec.Booked
		generator += ctl[i].Generator
		target += ctl[i].Target
	}
	f.Bands.Slow = band(run, f.Seconds, deltas, control.Slow)
	f.Bands.Fast = band(run, f.Seconds, deltas, control.Fast)

	v := &f.Verdict
	v.Speed = control.Settle(window, h.Load.Need)
	v.Saturated, v.Capped = run.Saturated, run.Capped
	v.OpenAtEnd, v.Horizon = run.OpenAtEnd, run.Horizon
	v.LateP99 = late.Quantile(0.99)
	for i, sec := range f.Seconds {
		if run.Seconds[i].Phase == load.Window && (!sec.Valid || sec.Speed != v.Speed) {
			v.Excluded++
			v.ExcludedP99 = max(v.ExcludedP99, sec.P99)
		}
	}
	if n := len(window); n > 0 {
		v.Throughput = float64(booked) / float64(n)
		v.GeneratorShare = generator / float64(n*max(h.Machine.GoMaxProcs, 1))
		cores := cpuCount(h.Target.Cores)
		if cores == 0 {
			cores = h.Machine.Threads
		}
		v.TargetShare = target / float64(n*max(cores, 1))
	}
	limited := v.LateP99 > LateLimit || v.GeneratorShare > CPULimit
	switch {
	case v.Saturated && limited:
		v.Limit = LimitGenerator
	case v.Saturated:
		v.Limit = LimitTarget
	case limited:
		v.Reasons = append(v.Reasons, WhyGenerator)
	}
	if v.Speed == control.Restless {
		v.Reasons = append(v.Reasons, WhyRestless)
	}
	if stalled && !h.AllowStalls {
		v.Reasons = append(v.Reasons, WhyStalled)
	}
	var failed uint64
	for _, sec := range run.Seconds {
		failed += sec.Failed
	}
	if failed > 0 && !v.Saturated {
		v.Reasons = append(v.Reasons, WhyFailed)
	}
	v.Problems = run.Problems
	v.Valid = len(v.Reasons) == 0

	f.Books = Books{
		Confirmed: run.Books.Confirmed,
		Existed:   run.Books.Existed,
		Posted:    run.Books.Posted,
		Pending:   run.Books.Pending,
		Unsure:    run.Books.Unsure,
		Sampled:   len(run.Books.Sample),
	}
	return f, nil
}

func band(run *load.Run, secs []Second, deltas []delta, speed control.Speed) Band {
	var b Band
	lat, reserve := new(load.Histogram), new(load.Histogram)
	var booked, syncs, requests, flushes uint64
	var flushTime time.Duration
	partSyncs := make(map[string]uint64)
	for i, sec := range run.Seconds {
		if sec.Phase != load.Window || !secs[i].Valid || secs[i].Speed != speed {
			continue
		}
		b.Seconds++
		lat.Merge(sec.Latency)
		if sec.Reserve != nil {
			reserve.Merge(sec.Reserve)
		}
		booked += sec.Booked
		syncs += deltas[i].syncs
		requests += deltas[i].requests
		for name, n := range deltas[i].parts {
			partSyncs[name] += n
		}
		flushes += secs[i].Flushes
		flushTime += secs[i].FlushTime
	}
	b.Histogram = histogram(lat)
	b.P50, b.P99, b.P999, b.Max = lat.Quantile(0.5), lat.Quantile(0.99), lat.Quantile(0.999), lat.Max()
	b.Unfinished = lat.Unfinished()
	if run.Seconds[0].Reserve != nil {
		h := histogram(reserve)
		b.Reserve = &h
		b.ReserveP50, b.ReserveP99 = reserve.Quantile(0.5), reserve.Quantile(0.99)
	}
	if b.Seconds == 0 {
		return b
	}
	b.Throughput = float64(booked) / float64(b.Seconds)
	if booked > 0 {
		b.Syncs = float64(syncs) / float64(booked)
		b.PartSyncs = make(map[string]float64, len(partSyncs))
		for name, n := range partSyncs {
			b.PartSyncs[name] = float64(n) / float64(booked)
		}
	}
	if requests > 0 {
		b.PerRequest = float64(booked) / float64(requests)
	}
	if flushes > 0 {
		b.Flush = flushTime / time.Duration(flushes) //nolint:gosec // G115: flushes in a window
		if b.P50 != load.Never {
			b.P50Flushes = float64(b.P50) / float64(b.Flush)
		}
	}
	return b
}

func histogram(h *load.Histogram) Histogram {
	out := Histogram{Never: h.Unfinished(), Buckets: [][2]int64{}}
	for _, b := range h.Occupied() {
		out.Buckets = append(out.Buckets, [2]int64{int64(b.Upper), int64(b.N)}) //nolint:gosec // G115: a count of arrivals
	}
	return out
}

func parts(in load.Inside) []Part {
	out := make([]Part, 0, len(in))
	for _, p := range in {
		out = append(out, Part(p))
	}
	return out
}

func cpuCount(list string) int {
	n := 0
	for _, span := range strings.FieldsFunc(list, func(r rune) bool { return r == ',' || r == ' ' }) {
		if lo, hi, ok := strings.Cut(span, "-"); ok {
			a, errA := strconv.Atoi(lo)
			b, errB := strconv.Atoi(hi)
			if errA == nil && errB == nil && b >= a {
				n += b - a + 1
				continue
			}
		}
		n++
	}
	return n
}

func stale(in load.Inside) bool {
	return slices.ContainsFunc(in, func(p load.Part) bool { return p.Stale })
}

func since(now, prev load.Inside) delta {
	d := delta{parts: make(map[string]uint64, len(now))}
	for _, p := range now {
		i := slices.IndexFunc(prev, func(q load.Part) bool { return q.Name == p.Name })
		if i < 0 {
			continue
		}
		d.parts[p.Name] = p.Syncs - prev[i].Syncs
		d.syncs += p.Syncs - prev[i].Syncs
	}
	if was, is := applied(prev), applied(now); is > was {
		d.requests = is - was
	}
	return d
}

func applied(in load.Inside) uint64 {
	var most uint64
	for _, p := range in {
		most = max(most, p.Requests, p.Counters["applied"])
	}
	return most
}

func (f *File) Refuse(reason string) {
	f.Verdict.Valid = false
	if !slices.Contains(f.Verdict.Reasons, reason) {
		f.Verdict.Reasons = append(f.Verdict.Reasons, reason)
	}
}

func (f *File) Verified(t load.Tally, err error) {
	f.Books.Verified = err == nil
	f.Books.Found = t.Found
	f.Books.DebitsPosted, f.Books.CreditsPosted = t.DebitsPosted, t.CreditsPosted
	f.Books.DebitsPending, f.Books.CreditsPending = t.DebitsPending, t.CreditsPending
	if err != nil {
		f.Refuse(fmt.Sprintf("%s: %v", WhyVerify, err))
	}
}

func (f *File) TermChanged() bool {
	terms := make(map[string]uint64)
	window := func(i int) bool { return i >= 0 && i < len(f.Seconds) && f.Seconds[i].Phase == load.Window.String() }
	for i, s := range f.Seconds {
		if !window(i) && !window(i+1) && !window(i-1) {
			continue
		}
		for _, p := range s.Parts {
			if t, ok := terms[p.Name]; ok && t != p.Term {
				return true
			}
			terms[p.Name] = p.Term
		}
	}
	return false
}
