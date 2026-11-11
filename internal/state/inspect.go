package state

import (
	"crypto/sha256"
	"encoding/binary"
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
			found = append(found, m.transfers[i].Transfer)
		}
	}
	return found
}

func (m *Machine) Digest() [sha256.Size]byte {
	accts := slices.SortedFunc(slices.Values(m.accounts), func(a, b Account) int {
		return a.ID.Cmp(b.ID)
	})
	trs := slices.SortedFunc(slices.Values(m.transfers), func(a, b stored) int {
		return a.ID.Cmp(b.ID)
	})

	h := sha256.New()
	buf := make([]byte, 0, TransferSize+1)
	h.Write(binary.BigEndian.AppendUint64(buf, uint64(len(accts))))
	for _, a := range accts {
		h.Write(AppendAccount(buf, a))
	}
	h.Write(binary.BigEndian.AppendUint64(buf, uint64(len(trs))))
	for _, tr := range trs {
		h.Write(append(AppendTransfer(buf, tr.Transfer), byte(tr.status)))
	}
	return [sha256.Size]byte(h.Sum(nil))
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
	for i := range m.transfers {
		tr := &m.transfers[i]
		if tr.status != statusPending {
			continue
		}
		dr, cr := &open[m.accountIdx[tr.DebitAccountID]], &open[m.accountIdx[tr.CreditAccountID]]
		dr.debits = dr.debits.Add(tr.Amount)
		cr.credits = cr.credits.Add(tr.Amount)
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
