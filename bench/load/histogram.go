package load

import (
	"math"
	"math/bits"
	"sync/atomic"
	"time"
)

const (
	perDoubling = 32
	doublings   = 36
	Buckets     = 1 + doublings*perDoubling
)

const Never = time.Duration(math.MaxInt64)

const top = time.Duration(1<<doublings) * time.Microsecond

type Histogram struct {
	counts [Buckets]atomic.Uint64
	never  atomic.Uint64
}

type Bucket struct {
	Upper time.Duration
	N     uint64
}

func bucketOf(d time.Duration) int {
	d = min(max(d, 0), top)
	x := uint64(d) * perDoubling / 1000
	if x < perDoubling {
		return 0
	}
	k := bits.Len64(x) - 6
	return min(1+k*perDoubling+int(x>>k)-perDoubling, Buckets-1) //nolint:gosec // G115: x>>k is below 64
}

func Upper(i int) time.Duration {
	if i == 0 {
		return time.Microsecond
	}
	k, sub := (i-1)/perDoubling, (i-1)%perDoubling
	return time.Duration(((perDoubling+sub+1)<<k*1000 + perDoubling - 1) / perDoubling)
}

func (h *Histogram) Add(d time.Duration) {
	h.counts[bucketOf(d)].Add(1)
}

func (h *Histogram) AddNever() {
	h.never.Add(1)
}

func (h *Histogram) Merge(o *Histogram) {
	for i := range o.counts {
		if n := o.counts[i].Load(); n > 0 {
			h.counts[i].Add(n)
		}
	}
	h.never.Add(o.never.Load())
}

func (h *Histogram) Finished() uint64 {
	var n uint64
	for i := range h.counts {
		n += h.counts[i].Load()
	}
	return n
}

func (h *Histogram) Unfinished() uint64 {
	return h.never.Load()
}

func (h *Histogram) Quantile(q float64) time.Duration {
	done := h.Finished()
	total := done + h.never.Load()
	if total == 0 {
		return 0
	}
	rank := min(max(uint64(math.Ceil(q*float64(total))), 1), total)
	if rank > done {
		return Never
	}
	var cum uint64
	for i := range h.counts {
		cum += h.counts[i].Load()
		if cum >= rank {
			return Upper(i)
		}
	}
	return Never
}

func (h *Histogram) Max() time.Duration {
	return h.Quantile(1)
}

func (h *Histogram) Occupied() []Bucket {
	var occ []Bucket
	for i := range h.counts {
		if n := h.counts[i].Load(); n > 0 {
			occ = append(occ, Bucket{Upper: Upper(i), N: n})
		}
	}
	return occ
}
