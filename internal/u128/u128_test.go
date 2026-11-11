package u128

import (
	"math/big"
	"math/rand/v2"
	"testing"
)

var two128 = new(big.Int).Lsh(big.NewInt(1), 128)

func toBig(u Uint128) *big.Int {
	return new(big.Int).SetBytes(u.AppendBytes(nil))
}

func fromBig(n *big.Int) Uint128 {
	var b [16]byte
	return FromBytes(new(big.Int).Mod(n, two128).FillBytes(b[:]))
}

func TestString(t *testing.T) {
	t.Parallel()
	tests := []struct {
		u    Uint128
		want string
	}{
		{Uint128{}, "0"},
		{From64(42), "42"},
		{Uint128{Lo: 1<<64 - 1}, "18446744073709551615"},
		{Uint128{Hi: 1}, "18446744073709551616"},
		{Uint128{Hi: 1, Lo: 0x158e460913d00000}, "20000000000000000000"},
		{Max, "340282366920938463463374607431768211455"},
	}
	for _, tt := range tests {
		if got := tt.u.String(); got != tt.want {
			t.Errorf("%#v.String() = %s, want %s", tt.u, got, tt.want)
		}
	}
}

func TestOverflowEdges(t *testing.T) {
	t.Parallel()
	one := From64(1)
	if sum, o := Max.AddOverflow(one); !o || !sum.IsZero() {
		t.Errorf("Max+1 = %v, %v", sum, o)
	}
	if sum, o := Max.AddOverflow(Uint128{}); o || sum != Max {
		t.Errorf("Max+0 = %v, %v", sum, o)
	}
	if diff, o := (Uint128{}).SubUnderflow(one); !o || diff != Max {
		t.Errorf("0-1 = %v, %v", diff, o)
	}
	if sum, o := (Uint128{Lo: 1<<64 - 1}).AddOverflow(one); o || sum != (Uint128{Hi: 1}) {
		t.Errorf("carry into Hi: %#v, %v", sum, o)
	}
	if diff, o := (Uint128{Hi: 1}).SubUnderflow(one); o || diff != (Uint128{Lo: 1<<64 - 1}) {
		t.Errorf("borrow from Hi: %#v, %v", diff, o)
	}
}

func randUint128(r *rand.Rand) Uint128 {
	switch r.IntN(4) {
	case 0:
		return From64(r.Uint64N(1000))
	case 1:
		return Max.Sub(From64(r.Uint64N(1000)))
	case 2:
		return Uint128{Lo: r.Uint64()}
	}
	return Uint128{Hi: r.Uint64(), Lo: r.Uint64()}
}

func TestAgainstBig(t *testing.T) {
	t.Parallel()
	r := rand.New(rand.NewPCG(1, 2))
	for range 100_000 {
		a, b := randUint128(r), randUint128(r)
		ba, bb := toBig(a), toBig(b)

		sum, o := a.AddOverflow(b)
		bsum := new(big.Int).Add(ba, bb)
		if sum != fromBig(bsum) || o != (bsum.Cmp(two128) >= 0) {
			t.Fatalf("%v + %v = %v, %v; want %v", a, b, sum, o, bsum)
		}
		diff, u := a.SubUnderflow(b)
		bdiff := new(big.Int).Sub(ba, bb)
		if diff != fromBig(bdiff) || u != (bdiff.Sign() < 0) {
			t.Fatalf("%v - %v = %v, %v; want %v", a, b, diff, u, bdiff)
		}
		if got, want := a.Cmp(b), ba.Cmp(bb); got != want {
			t.Fatalf("Cmp(%v, %v) = %d, want %d", a, b, got, want)
		}
		if got, want := a.String(), ba.String(); got != want {
			t.Fatalf("String() = %s, want %s", got, want)
		}
		if FromBytes(a.AppendBytes(nil)) != a {
			t.Fatalf("bytes round trip of %v", a)
		}
	}
}
