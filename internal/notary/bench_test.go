package notary_test

import (
	"crypto/ed25519"
	"testing"

	"github.com/Demonisreal/dmn-ledger/internal/audit"
	"github.com/Demonisreal/dmn-ledger/internal/notary"
)

func BenchmarkSign(b *testing.B) {
	h := audit.Head{Seq: 7, Hash: audit.Hash{1}}
	b.ReportAllocs()
	for range b.N {
		notary.Sign(key, cluster, h)
	}
}

func BenchmarkVerify(b *testing.B) {
	proof := decodeHex(b, vectors(b)[0].Proof)
	trusted := []ed25519.PublicKey{public(otherKey), public(key)}
	b.ReportAllocs()
	for range b.N {
		if _, _, _, err := notary.Verify(proof, cluster, trusted); err != nil {
			b.Fatal(err)
		}
	}
}
