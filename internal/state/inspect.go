package state

import (
	"crypto/sha256"
	"slices"

	"github.com/Demonisreal/dmn-ledger/internal/u128"
)

func (m *Machine) LookupAccounts(ids []u128.Uint128, dst []Account) []Account {
	for _, id := range ids {
		if i, ok := m.accountIdx[id]; ok {
			dst = append(dst, m.accounts[i])
		}
	}
	return dst
}

func (m *Machine) LookupTransfers(ids []u128.Uint128, dst []Transfer) []Transfer {
	for _, id := range ids {
		if i, ok := m.transferIdx[id]; ok {
			dst = append(dst, m.transfer(i).Transfer)
		}
	}
	return dst
}

func (m *Machine) Last() uint64 {
	return m.last
}

func (m *Machine) Digest() [sha256.Size]byte {
	m.hash.Reset()
	m.hashw.Reset(m.hash)
	_ = m.encode(m.hashw)
	var d [sha256.Size]byte
	m.hash.Sum(d[:0])
	return d
}

type order struct {
	idx, spare, fresh []int
}

func (o *order) sync(n int, cmp func(i, j int) int) []int {
	if len(o.idx) == n {
		return o.idx
	}
	o.fresh = o.fresh[:0]
	for i := len(o.idx); i < n; i++ {
		o.fresh = append(o.fresh, i)
	}
	slices.SortFunc(o.fresh, cmp)

	merged, a, b := o.spare[:0], o.idx, o.fresh
	for len(a) > 0 && len(b) > 0 {
		if cmp(a[0], b[0]) < 0 {
			merged, a = append(merged, a[0]), a[1:]
		} else {
			merged, b = append(merged, b[0]), b[1:]
		}
	}
	merged = append(append(merged, a...), b...)
	o.idx, o.spare = merged, o.idx
	return o.idx
}

func (m *Machine) sortedAccounts() []int {
	return m.accountOrder.sync(len(m.accounts), func(i, j int) int {
		return m.accounts[i].ID.Cmp(m.accounts[j].ID)
	})
}

func (m *Machine) sortedTransfers() []int {
	return m.transferOrder.sync(m.ntransfers, func(i, j int) int {
		return m.transfer(i).ID.Cmp(m.transfer(j).ID)
	})
}
