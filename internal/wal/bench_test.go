package wal

import (
	"fmt"
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
				if first, _ := w.FirstIndex(); next-first > 1<<20 {
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
