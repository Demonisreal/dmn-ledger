package load

import (
	"math"
	"math/rand/v2"
	"slices"
	"testing"
	"time"
)

func TestHistogram(t *testing.T) {
	t.Run("edges", func(t *testing.T) {
		for i := range Buckets - 1 {
			up := Upper(i)
			if bucketOf(up-1) != i || bucketOf(up) != i+1 {
				t.Fatalf("bucket %d ends at %v: %v lands in %d, %v in %d", i, up, up-1, bucketOf(up-1), up, bucketOf(up))
			}
			if i > 0 && float64(up-Upper(i-1)) > 0.032*float64(Upper(i-1))+1 {
				t.Fatalf("bucket %d from %v to %v is wider than 3.2%%", i, Upper(i-1), up)
			}
		}
		if bucketOf(-time.Second) != 0 || bucketOf(999) != 0 || bucketOf(1000) != 1 {
			t.Fatal("below 1µs must land in the first bucket, 1µs in the second")
		}
		if bucketOf(top) != Buckets-1 || bucketOf(Never) != Buckets-1 || Upper(Buckets-1) != top {
			t.Fatalf("the last bucket ends at %v, want %v", Upper(Buckets-1), top)
		}
	})
	t.Run("against a sorted list", func(t *testing.T) {
		r := rand.New(rand.NewPCG(1, 2))
		var h Histogram
		var all []time.Duration
		for range 50_000 {
			d := time.Duration(math.Exp2(r.Float64()*30) * 100)
			h.Add(d)
			all = append(all, d)
		}
		slices.Sort(all)
		for _, q := range []float64{0, 0.001, 0.1, 0.5, 0.9, 0.99, 0.999, 0.9999, 1} {
			rank := min(max(int(math.Ceil(q*float64(len(all)))), 1), len(all))
			exact, got := all[rank-1], h.Quantile(q)
			if got <= exact || float64(got) > float64(max(exact, time.Microsecond))*1.032+1 {
				t.Errorf("q %v: %v for %v, want the top of its bucket within 3.2%%", q, got, exact)
			}
		}
	})
	t.Run("unfinished", func(t *testing.T) {
		var h Histogram
		for i := range 98 {
			h.Add(time.Duration(i+1) * time.Millisecond)
		}
		h.AddNever()
		h.AddNever()
		if got := h.Quantile(0.98); got < 98*time.Millisecond || got == Never {
			t.Errorf("p98 %v, want the last finished one", got)
		}
		if h.Quantile(0.99) != Never || h.Max() != Never {
			t.Errorf("p99 %v, max %v, want Never with two of 100 unfinished", h.Quantile(0.99), h.Max())
		}
		var empty Histogram
		if empty.Quantile(0.5) != 0 || empty.Max() != 0 {
			t.Error("an empty histogram has no latency")
		}
	})
	t.Run("merge", func(t *testing.T) {
		var a, b, sum Histogram
		for i := range 1000 {
			d := time.Duration(i) * 37 * time.Microsecond
			sum.Add(d)
			if i%3 == 0 {
				a.Add(d)
			} else {
				b.Add(d)
			}
		}
		b.AddNever()
		sum.AddNever()
		a.Merge(&b)
		if !slices.Equal(a.Occupied(), sum.Occupied()) || a.Unfinished() != 1 || a.Finished() != 1000 {
			t.Fatal("merged histogram differs from the one that saw everything")
		}
		var n uint64
		for _, b := range a.Occupied() {
			n += b.N
		}
		if n != 1000 {
			t.Fatalf("occupied buckets hold %d, want 1000", n)
		}
	})
}

func BenchmarkHistogramAdd(b *testing.B) {
	var h Histogram
	b.RunParallel(func(pb *testing.PB) {
		d := time.Duration(0)
		for pb.Next() {
			d += 977
			h.Add(d % (50 * time.Millisecond))
		}
	})
}
