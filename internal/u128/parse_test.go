package u128

import (
	"errors"
	"math/rand/v2"
	"testing"
)

func TestParse(t *testing.T) {
	t.Parallel()
	tests := []struct {
		in   string
		want Uint128
		err  error
	}{
		{"0", Uint128{}, nil},
		{"42", From64(42), nil},
		{"007", From64(7), nil},
		{"18446744073709551615", Uint128{Lo: 1<<64 - 1}, nil},
		{"18446744073709551616", Uint128{Hi: 1}, nil},
		{"340282366920938463463374607431768211455", Max(), nil},
		{"340282366920938463463374607431768211456", Uint128{}, ErrRange},
		{"340282366920938463463374607431768211460", Uint128{}, ErrRange},
		{"3402823669209384634633746074317682114550", Uint128{}, ErrRange},
		{"0x0", Uint128{}, nil},
		{"0x2a", From64(42), nil},
		{"0x2A", From64(42), nil},
		{"0xffffffffffffffff", Uint128{Lo: 1<<64 - 1}, nil},
		{"0x10000000000000000", Uint128{Hi: 1}, nil},
		{"0x0123456789abcdef0123456789ABCDEF", Uint128{Hi: 0x0123456789abcdef, Lo: 0x0123456789abcdef}, nil},
		{"0xffffffffffffffffffffffffffffffff", Max(), nil},
		{"0x000ffffffffffffffffffffffffffffffff", Max(), nil},
		{"0x100000000000000000000000000000000", Uint128{}, ErrRange},
		{"", Uint128{}, ErrSyntax},
		{"0x", Uint128{}, ErrSyntax},
		{"0X2a", Uint128{}, ErrSyntax},
		{"2a", Uint128{}, ErrSyntax},
		{"0xg", Uint128{}, ErrSyntax},
		{"-1", Uint128{}, ErrSyntax},
		{"+1", Uint128{}, ErrSyntax},
		{" 1", Uint128{}, ErrSyntax},
		{"1_000", Uint128{}, ErrSyntax},
		{"0x1:", Uint128{}, ErrSyntax},
	}
	for _, tt := range tests {
		got, err := Parse(tt.in)
		if got != tt.want || !errors.Is(err, tt.err) {
			t.Errorf("Parse(%q) = %v, %v; want %v, %v", tt.in, got, err, tt.want, tt.err)
		}
	}

	r := rand.New(rand.NewPCG(3, 4))
	for range 100_000 {
		u := randUint128(r)
		for _, s := range []string{u.String(), "0x" + toBig(u).Text(16)} {
			if got, err := Parse(s); got != u || err != nil {
				t.Fatalf("Parse(%q) = %v, %v", s, got, err)
			}
		}
		over := toBig(u)
		over.Add(over, two128)
		for _, s := range []string{over.String(), "0x" + over.Text(16)} {
			if _, err := Parse(s); !errors.Is(err, ErrRange) {
				t.Fatalf("Parse(%q): %v, want ErrRange", s, err)
			}
		}
	}
}
