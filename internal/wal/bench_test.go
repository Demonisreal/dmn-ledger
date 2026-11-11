package wal

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/Demonisreal/dmn-ledger/internal/disk"
)

const benchBody = 128

func BenchmarkAppendSync(b *testing.B) {
	for _, n := range []int{1, 64, 1024, 8190} {
		b.Run(fmt.Sprintf("batch=%d", n), func(b *testing.B) {
			w, err := Open(disk.OS{}, b.TempDir(), Options{})
			if err != nil {
				b.Fatal(err)
			}
			defer func() { _ = w.Close() }()
			batch := make([]Entry, n)
			body := make([]byte, benchBody)
			next := uint64(1)
			b.SetBytes(int64(n * benchBody))
			b.ReportAllocs()
			ops := 0
			for b.Loop() {
				for k := range batch {
					batch[k] = Entry{Index: next, Term: 1, Body: body}
					next++
				}
				if err := w.Append(batch...); err != nil {
					b.Fatal(err)
				}
				if err := w.Sync(); err != nil {
					b.Fatal(err)
				}
				ops++
				if next-w.FirstIndex() > 1<<20 {
					b.StopTimer()
					if err := w.CompactBefore(next); err != nil {
						b.Fatal(err)
					}
					b.StartTimer()
				}
			}
			b.ReportMetric(float64(ops*n)/b.Elapsed().Seconds(), "entries/s")
		})
	}
}

func BenchmarkCommitter(b *testing.B) {
	for _, writers := range []int{1, 16, 256} {
		b.Run(fmt.Sprintf("writers=%d", writers), func(b *testing.B) {
			w, err := Open(disk.OS{}, b.TempDir(), Options{})
			if err != nil {
				b.Fatal(err)
			}
			defer func() { _ = w.Close() }()
			c := NewCommitter(w, 8190, 8<<20)
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- c.Run(ctx) }()

			body := make([]byte, benchBody)
			var mu sync.Mutex
			next := uint64(1)
			var left atomic.Int64
			left.Store(int64(b.N))
			b.ReportAllocs()
			b.ResetTimer()
			var wg sync.WaitGroup
			for range writers {
				wg.Go(func() {
					for left.Add(-1) >= 0 {
						mu.Lock()
						errc, err := c.Submit(ctx, Entry{Index: next, Term: 1, Body: body})
						next++
						mu.Unlock()
						if err == nil {
							err = <-errc
						}
						if err != nil {
							b.Error(err)
							return
						}
					}
				})
			}
			wg.Wait()
			b.ReportMetric(float64(next-1)/b.Elapsed().Seconds(), "entries/s")
			cancel()
			if err := <-done; err != nil {
				b.Fatal(err)
			}
		})
	}
}
