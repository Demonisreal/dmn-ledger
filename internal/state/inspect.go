package state

import (
	"crypto/sha256"
	"fmt"
	"maps"
	"slices"

	"github.com/Demonisreal/dmn-ledger/internal/u128"
)

func (m *Machine) LookupAccounts(ids []u128.Uint128) []Account {
	var found []Account
	for _, id := range ids {
		if i, ok := m.accountIdx[id]; ok {
			found = append(found, m.accounts[i])
		}
	}
	return found
}

func (m *Machine) LookupTransfers(ids []u128.Uint128) []Transfer {
	var found []Transfer
	for _, id := range ids {
		if i, ok := m.transferIdx[id]; ok {
			found = append(found, m.transfer(i).Transfer)
		}
	}
	return found
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

type total struct {
	carry uint64
	sum   u128.Uint128
}

func (t *total) add(v u128.Uint128) {
	var overflow bool
	if t.sum, overflow = t.sum.AddOverflow(v); overflow {
		t.carry++
	}
}

type ledgerTotals struct {
	debitsPending, debitsPosted, creditsPending, creditsPosted total
}

func (m *Machine) CheckInvariants() error {
	open := make([]struct{ debits, credits u128.Uint128 }, len(m.accounts))
	for i := range m.ntransfers {
		tr := m.transfer(i)
		if tr.status != statusPending {
			continue
		}
		open[tr.dr].debits = open[tr.dr].debits.AddWrap(tr.Amount)
		open[tr.cr].credits = open[tr.cr].credits.AddWrap(tr.Amount)
	}

	ledgers := make(map[uint32]*ledgerTotals)
	for i := range m.accounts {
		a := &m.accounts[i]
		if a.DebitsPending != open[i].debits || a.CreditsPending != open[i].credits {
			return fmt.Errorf("account %v: pending %v/%v, open transfers sum to %v/%v",
				a.ID, a.DebitsPending, a.CreditsPending, open[i].debits, open[i].credits)
		}
		debits, dOverflow := a.DebitsPosted.AddOverflow(a.DebitsPending)
		credits, cOverflow := a.CreditsPosted.AddOverflow(a.CreditsPending)
		switch {
		case dOverflow || cOverflow:
			return fmt.Errorf("account %v: posted plus pending overflows", a.ID)
		case a.Flags&DebitsMustNotExceedCredits != 0 && debits.Cmp(a.CreditsPosted) > 0:
			return fmt.Errorf("account %v: debits %v exceed credits posted %v", a.ID, debits, a.CreditsPosted)
		case a.Flags&CreditsMustNotExceedDebits != 0 && credits.Cmp(a.DebitsPosted) > 0:
			return fmt.Errorf("account %v: credits %v exceed debits posted %v", a.ID, credits, a.DebitsPosted)
		}

		lt := ledgers[a.Ledger]
		if lt == nil {
			lt = new(ledgerTotals)
			ledgers[a.Ledger] = lt
		}
		lt.debitsPending.add(a.DebitsPending)
		lt.debitsPosted.add(a.DebitsPosted)
		lt.creditsPending.add(a.CreditsPending)
		lt.creditsPosted.add(a.CreditsPosted)
	}

	for _, ledger := range slices.Sorted(maps.Keys(ledgers)) {
		lt := ledgers[ledger]
		if lt.debitsPosted != lt.creditsPosted {
			return fmt.Errorf("ledger %d: debits posted and credits posted differ", ledger)
		}
		if lt.debitsPending != lt.creditsPending {
			return fmt.Errorf("ledger %d: debits pending and credits pending differ", ledger)
		}
	}
	return nil
}
