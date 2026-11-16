package notary_test

import (
	"bytes"
	"crypto/ed25519"
	"errors"
	"slices"
	"testing"

	"github.com/Demonisreal/dmn-ledger/internal/audit"
	"github.com/Demonisreal/dmn-ledger/internal/notary"
)

var refusals = []error{audit.ErrBadReceipt, notary.ErrUnsigned, notary.ErrCluster, notary.ErrUntrusted, notary.ErrSignature}

func TestSignIsDeterministic(t *testing.T) {
	h := audit.Head{Seq: 7, Hash: audit.Hash{1, 2, 3}}
	stamp := notary.Sign(key, cluster, h)
	if len(stamp) != notary.StampSize || notary.StampSize != 104 {
		t.Fatalf("stamp is %d bytes, StampSize %d", len(stamp), notary.StampSize)
	}
	if again := notary.Sign(key, cluster, h); !bytes.Equal(again, stamp) {
		t.Fatal("two stamps over the same head differ")
	}
	c, signer, ok := notary.Signer(stamp, h)
	if !ok || c != cluster || !signer.Equal(public(key)) {
		t.Fatalf("Signer gives cluster %#x, key %x, %v", c, signer, ok)
	}

	others := map[string][]byte{
		"cluster": notary.Sign(key, otherCluster, h),
		"number":  notary.Sign(key, cluster, audit.Head{Seq: 8, Hash: h.Hash}),
		"hash":    notary.Sign(key, cluster, audit.Head{Seq: 7, Hash: audit.Hash{1, 2, 4}}),
		"key":     notary.Sign(otherKey, cluster, h),
	}
	for name, other := range others {
		if bytes.Equal(other[8+ed25519.PublicKeySize:], stamp[8+ed25519.PublicKeySize:]) {
			t.Errorf("another %s gives the same signature", name)
		}
	}
	if _, _, ok := notary.Signer(stamp, audit.Head{Seq: 8, Hash: h.Hash}); ok {
		t.Error("stamp of head 7 passes for head 8")
	}
	if _, _, ok := notary.Signer(stamp[:notary.StampSize-1], h); ok {
		t.Error("short stamp passes")
	}
}

func TestEveryFlippedBitIsRefused(t *testing.T) {
	var flips int
	for _, v := range vectors(t)[:6] {
		proof := decodeHex(t, v.Proof)
		for bit := range 8 * len(proof) {
			flipped := slices.Clone(proof)
			flipped[bit/8] ^= 1 << (bit % 8)
			flips++
			if _, _, _, err := notary.Verify(flipped, cluster, []ed25519.PublicKey{public(key)}); err == nil {
				t.Errorf("%s: bit %d flipped, still verifies", v.Name, bit)
			}
		}
	}
	t.Logf("%d flips over six stamped proofs, none verifies", flips)
}

func FuzzVerify(f *testing.F) {
	for _, v := range vectors(f) {
		f.Add(decodeHex(f, v.Proof), uint64(cluster))
	}
	stranger := public(keyFromSeed("a key that never signed"))
	f.Fuzz(func(t *testing.T, proof []byte, c uint64) {
		if _, _, _, err := notary.Verify(proof, c, []ed25519.PublicKey{stranger}); err == nil {
			t.Fatal("verified against a key that never signed")
		}
		r, h, signer, err := notary.Verify(proof, c, []ed25519.PublicKey{stranger, public(key)})
		if err != nil {
			if !slices.ContainsFunc(refusals, func(e error) bool { return errors.Is(err, e) }) {
				t.Fatalf("unexpected error %v", err)
			}
			return
		}
		if !signer.Equal(public(key)) {
			t.Fatalf("signed by %x", signer)
		}
		dr, dh, stamp, err := audit.Decode(proof)
		if err != nil || dh != h || dr.Seq != r.Seq || dr.Kind != r.Kind {
			t.Fatalf("Verify and Decode disagree: %v", err)
		}
		if signedFor, _, ok := notary.Signer(stamp, h); !ok || signedFor != c {
			t.Fatalf("Signer gives %#x %v for cluster %#x", signedFor, ok, c)
		}
	})
}
