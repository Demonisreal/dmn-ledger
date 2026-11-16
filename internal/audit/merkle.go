package audit

import (
	"crypto/sha256"
	"math/bits"
)

const (
	HashSize = sha256.Size
	MaxHops  = 32

	KindBatch    = 0
	KindAccount  = 1
	KindTransfer = 2
	KindExpiry   = 3
	KindHead     = 4
)

const (
	prefixLeaf  = 0x00
	prefixNode  = 0x01
	prefixHead  = 0x02
	prefixBody  = 0x03
	prefixReply = 0x04
)

const (
	accountSize  = 96
	transferSize = 100
)

type Hash = [HashSize]byte

var emptyRoot = sha256.Sum256(nil)

func Leaf(kind byte, record []byte) Hash {
	var buf [2 + transferSize]byte
	return sha256.Sum256(append(append(buf[:0], prefixLeaf, kind), record...))
}

func node(left, right Hash) Hash {
	var buf [1 + 2*HashSize]byte
	buf[0] = prefixNode
	copy(buf[1:], left[:])
	copy(buf[1+HashSize:], right[:])
	return sha256.Sum256(buf[:])
}

type Tree struct {
	peaks [MaxHops + 1]Hash
	n     uint32
}

func (t *Tree) Reset() {
	t.n = 0
}

func (t *Tree) Add(leaf Hash) {
	height := 0
	for n := t.n; n&1 == 1; n >>= 1 {
		leaf = node(t.peaks[height], leaf)
		height++
	}
	t.peaks[height] = leaf
	t.n++
}

func (t *Tree) Len() uint32 {
	return t.n
}

func (t *Tree) Root() Hash {
	if t.n == 0 {
		return emptyRoot
	}
	height := bits.TrailingZeros32(t.n)
	root := t.peaks[height]
	for height++; height < bits.Len32(t.n); height++ {
		if t.n>>height&1 == 1 {
			root = node(t.peaks[height], root)
		}
	}
	return root
}

func rootOf(leaves []Hash) Hash {
	var t Tree
	for _, l := range leaves {
		t.Add(l)
	}
	return t.Root()
}

func Path(leaves []Hash, index int) []Hash {
	if len(leaves) <= 1 {
		return nil
	}
	k := 1 << (bits.Len(uint(len(leaves)-1)) - 1)
	if index < k {
		return append(Path(leaves[:k], index), rootOf(leaves[k:]))
	}
	return append(Path(leaves[k:], index-k), rootOf(leaves[:k]))
}

func RootFromPath(leaf Hash, index, size uint32, path []Hash) (Hash, bool) {
	if index >= size {
		return Hash{}, false
	}
	fn, sn := index, size-1
	r := leaf
	for _, p := range path {
		if sn == 0 {
			return Hash{}, false
		}
		if fn&1 == 1 || fn == sn {
			r = node(p, r)
			for fn&1 == 0 && fn != 0 {
				fn >>= 1
				sn >>= 1
			}
		} else {
			r = node(r, p)
		}
		fn >>= 1
		sn >>= 1
	}
	if sn != 0 {
		return Hash{}, false
	}
	return r, true
}
