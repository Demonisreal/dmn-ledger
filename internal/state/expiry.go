package state

import "github.com/Demonisreal/dmn-ledger/internal/u128"

type expiry struct {
	at uint64
	id u128.Uint128
}

type expiryQueue []expiry

func (q expiryQueue) less(i, j int) bool {
	if q[i].at != q[j].at {
		return q[i].at < q[j].at
	}
	return q[i].id.Cmp(q[j].id) < 0
}

func (q *expiryQueue) push(e expiry) {
	h := append(*q, e)
	for i := len(h) - 1; i > 0; {
		parent := (i - 1) / 2
		if !h.less(i, parent) {
			break
		}
		h[i], h[parent] = h[parent], h[i]
		i = parent
	}
	*q = h
}

func (q *expiryQueue) pop() expiry {
	h := *q
	top := h[0]
	last := len(h) - 1
	h[0] = h[last]
	h = h[:last]
	for i := 0; ; {
		next := i
		if l := 2*i + 1; l < len(h) && h.less(l, next) {
			next = l
		}
		if r := 2*i + 2; r < len(h) && h.less(r, next) {
			next = r
		}
		if next == i {
			break
		}
		h[i], h[next] = h[next], h[i]
		i = next
	}
	*q = h
	return top
}
