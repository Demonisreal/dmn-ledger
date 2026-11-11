package state

import (
	"errors"
	"fmt"
	"maps"
	"math/bits"
	"slices"

	"github.com/Demonisreal/dmn-ledger/internal/u128"
)

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

type sides struct {
	debitsPending, debitsPosted, creditsPending, creditsPosted total
}

func (s *sides) matches(a *Account) bool {
	return s.debitsPending == total{sum: a.DebitsPending} && s.debitsPosted == total{sum: a.DebitsPosted} &&
		s.creditsPending == total{sum: a.CreditsPending} && s.creditsPosted == total{sum: a.CreditsPosted}
}

func (m *Machine) CheckInvariants() error {
	switch {
	case m.last >= tsLimit:
		return fmt.Errorf("last timestamp %d out of range", m.last)
	case len(m.undo) > 0:
		return fmt.Errorf("%d undo entries left after the batch", len(m.undo))
	case len(m.accountIdx) != len(m.accounts) || len(m.transferIdx) != m.ntransfers:
		return fmt.Errorf("indexes hold %d accounts and %d transfers, stored are %d and %d",
			len(m.accountIdx), len(m.transferIdx), len(m.accounts), m.ntransfers)
	}

	stamps := make([]uint64, 0, len(m.accounts)+m.ntransfers)
	ledgers := make(map[uint32]*sides)
	for i := range m.accounts {
		a := &m.accounts[i]
		if j, ok := m.accountIdx[a.ID]; !ok || j != i {
			return fmt.Errorf("account %v: not indexed", a.ID)
		}
		if err := checkAccount(a); err != nil {
			return fmt.Errorf("account %v: %w", a.ID, err)
		}
		stamps = append(stamps, a.Timestamp)

		lt := ledgers[a.Ledger]
		if lt == nil {
			lt = new(sides)
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

	booked := make([]sides, len(m.accounts))
	claimed := make([]bool, m.ntransfers)
	var resolvers, resolved, queued int
	for i := range m.ntransfers {
		tr := m.transfer(i)
		if j, ok := m.transferIdx[tr.ID]; !ok || j != i {
			return fmt.Errorf("transfer %v: not indexed", tr.ID)
		}
		if err := m.checkTransfer(i, booked, claimed); err != nil {
			return fmt.Errorf("transfer %v: %w", tr.ID, err)
		}
		stamps = append(stamps, tr.Timestamp)
		if tr.Flags&(PostPending|VoidPending) != 0 {
			resolvers++
		}
		if tr.status == statusPosted || tr.status == statusVoided {
			resolved++
		}
		if tr.heap >= 0 {
			queued++
		}
	}
	if resolvers != resolved {
		return fmt.Errorf("%d posts and voids for %d posted or voided pending transfers", resolvers, resolved)
	}
	if queued != len(m.expiries) {
		return fmt.Errorf("expiry queue holds %d entries for %d waiting transfers", len(m.expiries), queued)
	}
	for i := 1; i < len(m.expiries); i++ {
		if m.expiresBefore(i, (i-1)/2) {
			return fmt.Errorf("expiry queue out of order at %d", i)
		}
	}

	slices.Sort(stamps)
	for i, ts := range stamps {
		if ts == 0 || ts > m.last || i > 0 && ts == stamps[i-1] {
			return fmt.Errorf("timestamp %d is 0, repeated or after last %d", ts, m.last)
		}
	}

	for i := range m.accounts {
		if a := &m.accounts[i]; !booked[i].matches(a) {
			return fmt.Errorf("account %v: balances do not match its transfers", a.ID)
		}
	}
	return nil
}

func checkAccount(a *Account) error {
	debits, dOverflow := a.DebitsPosted.AddOverflow(a.DebitsPending)
	credits, cOverflow := a.CreditsPosted.AddOverflow(a.CreditsPending)
	switch {
	case a.ID.IsZero() || a.ID == u128.Max():
		return errors.New("id out of range")
	case a.Flags&^accountFlagsKnown != 0 || a.Flags == accountFlagsKnown:
		return fmt.Errorf("flags %#x", uint16(a.Flags))
	case a.Ledger == 0 || a.Code == 0:
		return errors.New("ledger or code is 0")
	case dOverflow || cOverflow:
		return errors.New("posted plus pending overflows")
	case a.Flags&DebitsMustNotExceedCredits != 0 && debits.Cmp(a.CreditsPosted) > 0:
		return fmt.Errorf("debits %v exceed credits posted %v", debits, a.CreditsPosted)
	case a.Flags&CreditsMustNotExceedDebits != 0 && credits.Cmp(a.DebitsPosted) > 0:
		return fmt.Errorf("credits %v exceed debits posted %v", credits, a.DebitsPosted)
	}
	return nil
}

func (m *Machine) checkTransfer(i int, booked []sides, claimed []bool) error {
	tr := m.transfer(i)
	dr, cr := &m.accounts[tr.dr], &m.accounts[tr.cr]
	kind := tr.Flags & (Pending | PostPending | VoidPending)
	switch {
	case tr.ID.IsZero() || tr.ID == u128.Max():
		return errors.New("id out of range")
	case tr.Flags&^transferFlagsKnown != 0 || bits.OnesCount16(uint16(kind)) > 1:
		return fmt.Errorf("flags %#x", uint16(tr.Flags))
	case tr.dr == tr.cr || dr.ID != tr.DebitAccountID || cr.ID != tr.CreditAccountID:
		return errors.New("accounts do not match")
	case tr.Ledger != dr.Ledger || tr.Ledger != cr.Ledger || tr.Code == 0:
		return fmt.Errorf("ledger %d, code %d", tr.Ledger, tr.Code)
	case tr.Amount.IsZero():
		return errors.New("amount is 0")
	case tr.Timeout != 0 && kind != Pending:
		return errors.New("timeout without pending")
	case tr.PendingID.IsZero() == (kind&(PostPending|VoidPending) != 0):
		return fmt.Errorf("pending id %v", tr.PendingID)
	case tr.status > statusExpired || (tr.status != statusNone) != (kind == Pending):
		return fmt.Errorf("status %d", tr.status)
	case tr.status == statusExpired && (tr.Timeout == 0 || tr.expiresAt() > m.last):
		return fmt.Errorf("expired before its deadline, last %d", m.last)
	}

	waiting := tr.status == statusPending && tr.Timeout > 0
	if waiting != (tr.heap >= 0) ||
		waiting && (tr.heap >= len(m.expiries) || m.expiries[tr.heap] != expiry{at: tr.expiresAt(), tr: i}) {
		return fmt.Errorf("expiry queue position %d", tr.heap)
	}

	switch kind {
	case 0:
		booked[tr.dr].debitsPosted.add(tr.Amount)
		booked[tr.cr].creditsPosted.add(tr.Amount)
		return nil
	case Pending:
		if tr.status == statusPending {
			booked[tr.dr].debitsPending.add(tr.Amount)
			booked[tr.cr].creditsPending.add(tr.Amount)
		}
		return nil
	}

	pi, ok := m.transferIdx[tr.PendingID]
	if !ok {
		return fmt.Errorf("pending transfer %v missing", tr.PendingID)
	}
	p := m.transfer(pi)
	want := statusVoided
	if kind == PostPending {
		want = statusPosted
	}
	switch {
	case claimed[pi]:
		return fmt.Errorf("pending transfer %v resolved twice", p.ID)
	case p.status != want:
		return fmt.Errorf("pending transfer %v has status %d", p.ID, p.status)
	case p.dr != tr.dr || p.cr != tr.cr || p.Code != tr.Code:
		return fmt.Errorf("accounts or code differ from pending transfer %v", p.ID)
	case tr.Amount.Cmp(p.Amount) > 0 || want == statusVoided && tr.Amount != p.Amount:
		return fmt.Errorf("amount %v, pending transfer %v holds %v", tr.Amount, p.ID, p.Amount)
	case tr.Timestamp <= p.Timestamp || p.Timeout > 0 && tr.Timestamp >= p.expiresAt():
		return fmt.Errorf("timestamp %d outside the lifetime of pending transfer %v", tr.Timestamp, p.ID)
	}
	claimed[pi] = true
	if want == statusPosted {
		booked[tr.dr].debitsPosted.add(tr.Amount)
		booked[tr.cr].creditsPosted.add(tr.Amount)
	}
	return nil
}
