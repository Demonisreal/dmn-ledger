package u128

import (
	"cmp"
	"encoding/binary"
	"fmt"
	"math"
	"math/bits"
	"strconv"
)

type Uint128 struct {
	Hi, Lo uint64
}

func Max() Uint128 {
	return Uint128{Hi: math.MaxUint64, Lo: math.MaxUint64}
}

func From64(v uint64) Uint128 {
	return Uint128{Lo: v}
}

func FromBytes(b []byte) Uint128 {
	return Uint128{Hi: binary.BigEndian.Uint64(b), Lo: binary.BigEndian.Uint64(b[8:])}
}

func (u Uint128) IsZero() bool {
	return u.Hi == 0 && u.Lo == 0
}

func (u Uint128) Cmp(v Uint128) int {
	if u.Hi != v.Hi {
		return cmp.Compare(u.Hi, v.Hi)
	}
	return cmp.Compare(u.Lo, v.Lo)
}

func (u Uint128) AddWrap(v Uint128) Uint128 {
	sum, _ := u.AddOverflow(v)
	return sum
}

func (u Uint128) SubWrap(v Uint128) Uint128 {
	diff, _ := u.SubUnderflow(v)
	return diff
}

func (u Uint128) AddOverflow(v Uint128) (sum Uint128, overflow bool) {
	lo, carry := bits.Add64(u.Lo, v.Lo, 0)
	hi, carry := bits.Add64(u.Hi, v.Hi, carry)
	return Uint128{Hi: hi, Lo: lo}, carry != 0
}

func (u Uint128) SubUnderflow(v Uint128) (diff Uint128, underflow bool) {
	lo, borrow := bits.Sub64(u.Lo, v.Lo, 0)
	hi, borrow := bits.Sub64(u.Hi, v.Hi, borrow)
	return Uint128{Hi: hi, Lo: lo}, borrow != 0
}

func (u Uint128) PutBytes(b []byte) {
	binary.BigEndian.PutUint64(b, u.Hi)
	binary.BigEndian.PutUint64(b[8:], u.Lo)
}

func (u Uint128) AppendBytes(b []byte) []byte {
	b = binary.BigEndian.AppendUint64(b, u.Hi)
	return binary.BigEndian.AppendUint64(b, u.Lo)
}

func (u Uint128) String() string {
	if u.Hi == 0 {
		return strconv.FormatUint(u.Lo, 10)
	}
	const e19 = 10_000_000_000_000_000_000
	q := Uint128{Hi: u.Hi / e19}
	var r uint64
	q.Lo, r = bits.Div64(u.Hi%e19, u.Lo, e19)
	return fmt.Sprintf("%s%019d", q, r)
}
