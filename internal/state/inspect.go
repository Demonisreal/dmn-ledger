package state

import (
	"crypto/sha256"

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
