package audit_test

import (
	"fmt"
	"testing"

	"github.com/Demonisreal/dmn-ledger/internal/audit"
)

func BenchmarkProve(b *testing.B) {
	for _, n := range []int{1, 128, 8000} {
		b.Run(fmt.Sprintf("leaves=%d", n), func(b *testing.B) {
			records := make([][]byte, n)
			for i := range records {
				records[i] = record(100, byte(i))
			}
			leaves := make([]audit.Hash, n)
			for i, rec := range records {
				leaves[i] = audit.Leaf(audit.KindTransfer, rec)
			}
			link := audit.Link{Leaves: uint32(n), Root: treeOf(leaves).Root()}
			var buf []byte
			b.ReportAllocs()
			for range b.N {
				for i, rec := range records {
					leaves[i] = audit.Leaf(audit.KindTransfer, rec)
				}
				i := n / 2
				buf = audit.AppendReceipt(buf[:0], audit.Receipt{
					Kind:   audit.KindTransfer,
					Record: records[i],
					Index:  uint32(i),
					Path:   audit.Path(leaves, i),
					Seq:    1,
					Link:   link,
				})
			}
		})
	}
}

func BenchmarkDecode(b *testing.B) {
	kinds := make([]byte, 128)
	records := make([][]byte, 128)
	for i := range records {
		kinds[i], records[i] = audit.KindTransfer, record(100, byte(i))
	}
	proof := audit.AppendReceipt(nil, seal(audit.Head{}, 1e9, 2, 128, nil, kinds, records).receipt(77))
	b.ReportAllocs()
	b.SetBytes(int64(len(proof)))
	for range b.N {
		if _, _, _, err := audit.Decode(proof); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkConnect(b *testing.B) {
	links := make([]audit.Link, 1000)
	for i := range links {
		links[i] = sampleLink(uint64(i))
	}
	var h audit.Head
	b.ReportAllocs()
	for range b.N {
		h = audit.Connect(h, links)
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*len(links)), "ns/link")
}
