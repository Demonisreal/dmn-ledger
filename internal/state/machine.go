package state

import (
	"errors"
	"fmt"
	"math/bits"

	"github.com/Demonisreal/dmn-ledger/internal/u128"
)

var ErrMalformedBatch = errors.New("malformed batch")

type status uint8

const (
	statusNone status = iota
	statusPending
	statusPosted
	statusVoided
	statusExpired
)

type stored struct {
	Transfer
	status status
}

type undo struct {
	dr, cr         int
	drPrev, crPrev Account
	pending        int
}

type Machine struct {
	accounts    []Account
	accountIdx  map[u128.Uint128]int
	transfers   []stored
	transferIdx map[u128.Uint128]int
	expiries    expiryQueue
	undo        []undo
	last        uint64
}

func New() *Machine {
	return &Machine{
		accountIdx:  make(map[u128.Uint128]int),
		transferIdx: make(map[u128.Uint128]int),
	}
}

func (m *Machine) Apply(op Op, ts uint64, body []byte) ([]Result, error) {
	switch {
	case op == CreateAccounts && len(body)%AccountSize == 0:
	case op == CreateTransfers && len(body)%TransferSize == 0:
	default:
		return nil, fmt.Errorf("%w: op %d with %d bytes", ErrMalformedBatch, op, len(body))
	}
	m.expire(ts)
	ts = max(ts, m.last+1)
	if op == CreateAccounts {
		return m.createAccounts(ts, body), nil
	}
	return m.createTransfers(ts, body), nil
}

func (m *Machine) createAccounts(ts uint64, body []byte) []Result {
	var failed []Result
	for i := range len(body) / AccountSize {
		a := DecodeAccount(body[i*AccountSize:])
		if code := m.createAccount(&a, ts); code != OK {
			failed = append(failed, Result{Index: i, Code: code})
		}
		m.last = ts
		ts++
	}
	return failed
}

func (m *Machine) createAccount(a *Account, ts uint64) Code {
	switch {
	case a.ID.IsZero():
		return IDMustNotBeZero
	case a.ID == u128.Max:
		return IDMustNotBeMax
	case a.Flags&^accountFlagsKnown != 0:
		return FlagsUnknown
	case a.Flags == DebitsMustNotExceedCredits|CreditsMustNotExceedDebits:
		return FlagsAreMutuallyExclusive
	case a.Ledger == 0:
		return LedgerMustNotBeZero
	case a.Code == 0:
		return CodeMustNotBeZero
	case !a.DebitsPending.IsZero() || !a.DebitsPosted.IsZero() ||
		!a.CreditsPending.IsZero() || !a.CreditsPosted.IsZero():
		return BalancesMustBeZero
	case a.Timestamp != 0:
		return TimestampMustBeZero
	}
	if i, ok := m.accountIdx[a.ID]; ok {
		e := &m.accounts[i]
		if e.Ledger != a.Ledger || e.Code != a.Code || e.Flags != a.Flags {
			return ExistsWithDifferentFields
		}
		return Exists
	}
	a.Timestamp = ts
	m.accountIdx[a.ID] = len(m.accounts)
	m.accounts = append(m.accounts, *a)
	return OK
}

func (m *Machine) createTransfers(ts uint64, body []byte) []Result {
	n := len(body) / TransferSize
	open := n
	for open > 0 && DecodeTransfer(body[(open-1)*TransferSize:]).Flags&Linked != 0 {
		open--
	}

	var failed []Result
	first, broken, code := 0, -1, OK
	for i := range open {
		tr := DecodeTransfer(body[i*TransferSize:])
		if broken < 0 {
			if code = m.createTransfer(&tr, ts); code != OK {
				m.rollback()
				broken = i
			}
		}
		m.last = ts
		ts++
		if tr.Flags&Linked != 0 {
			continue
		}
		if broken >= 0 {
			for j := first; j <= i; j++ {
				c := LinkedEventFailed
				if j == broken {
					c = code
				}
				failed = append(failed, Result{Index: j, Code: c})
			}
		}
		m.undo = m.undo[:0]
		first, broken = i+1, -1
	}
	for i := open; i < n; i++ {
		failed = append(failed, Result{Index: i, Code: LinkedEventChainOpen})
		m.last = ts
		ts++
	}
	return failed
}

func (m *Machine) createTransfer(tr *Transfer, ts uint64) Code {
	switch {
	case tr.ID.IsZero():
		return IDMustNotBeZero
	case tr.ID == u128.Max:
		return IDMustNotBeMax
	case tr.Flags&^transferFlagsKnown != 0:
		return FlagsUnknown
	case bits.OnesCount16(uint16(tr.Flags&(Pending|PostPending|VoidPending))) > 1:
		return FlagsAreMutuallyExclusive
	case tr.Flags&(PostPending|VoidPending) != 0:
		return m.resolvePending(tr, ts)
	case tr.DebitAccountID.IsZero():
		return DebitAccountIDMustNotBeZero
	case tr.CreditAccountID.IsZero():
		return CreditAccountIDMustNotBeZero
	case tr.DebitAccountID == tr.CreditAccountID:
		return AccountsMustBeDifferent
	case !tr.PendingID.IsZero():
		return PendingIDMustBeZero
	case tr.Timeout != 0 && tr.Flags&Pending == 0:
		return TimeoutRequiresPending
	case tr.Ledger == 0:
		return LedgerMustNotBeZero
	case tr.Code == 0:
		return CodeMustNotBeZero
	case tr.Amount.IsZero():
		return AmountMustNotBeZero
	case tr.Timestamp != 0:
		return TimestampMustBeZero
	}

	dri, ok := m.accountIdx[tr.DebitAccountID]
	if !ok {
		return DebitAccountNotFound
	}
	cri, ok := m.accountIdx[tr.CreditAccountID]
	if !ok {
		return CreditAccountNotFound
	}
	dr, cr := &m.accounts[dri], &m.accounts[cri]
	switch {
	case dr.Ledger != cr.Ledger:
		return AccountsMustHaveSameLedger
	case tr.Ledger != dr.Ledger:
		return TransferMustHaveSameLedgerAsAccounts
	}
	if i, ok := m.transferIdx[tr.ID]; ok {
		return exists(&m.transfers[i].Transfer, *tr)
	}

	debits, overflow := dr.DebitsPosted.Add(dr.DebitsPending).AddOverflow(tr.Amount)
	if overflow {
		return OverflowsDebits
	}
	credits, overflow := cr.CreditsPosted.Add(cr.CreditsPending).AddOverflow(tr.Amount)
	if overflow {
		return OverflowsCredits
	}
	if dr.Flags&DebitsMustNotExceedCredits != 0 && debits.Cmp(dr.CreditsPosted) > 0 {
		return ExceedsCredits
	}
	if cr.Flags&CreditsMustNotExceedDebits != 0 && credits.Cmp(cr.DebitsPosted) > 0 {
		return ExceedsDebits
	}

	m.undo = append(m.undo, undo{dr: dri, cr: cri, drPrev: *dr, crPrev: *cr, pending: -1})
	st := statusNone
	if tr.Flags&Pending != 0 {
		dr.DebitsPending = dr.DebitsPending.Add(tr.Amount)
		cr.CreditsPending = cr.CreditsPending.Add(tr.Amount)
		st = statusPending
	} else {
		dr.DebitsPosted = dr.DebitsPosted.Add(tr.Amount)
		cr.CreditsPosted = cr.CreditsPosted.Add(tr.Amount)
	}
	tr.Timestamp = ts
	m.transferIdx[tr.ID] = len(m.transfers)
	m.transfers = append(m.transfers, stored{Transfer: *tr, status: st})
	if st == statusPending && tr.Timeout > 0 {
		m.expiries.push(expiry{at: tr.expiresAt(), id: tr.ID})
	}
	return OK
}

func (m *Machine) resolvePending(tr *Transfer, ts uint64) Code {
	switch {
	case tr.PendingID.IsZero():
		return PendingIDMustNotBeZero
	case tr.Timeout != 0:
		return TimeoutRequiresPending
	case tr.Timestamp != 0:
		return TimestampMustBeZero
	}
	if i, ok := m.transferIdx[tr.ID]; ok {
		e := &m.transfers[i].Transfer
		if e.PendingID != tr.PendingID {
			return ExistsWithDifferentFields
		}
		return exists(e, inherit(*tr, &m.transfers[m.transferIdx[e.PendingID]].Transfer))
	}

	pi, ok := m.transferIdx[tr.PendingID]
	if !ok {
		return PendingTransferNotFound
	}
	p := &m.transfers[pi]
	post := tr.Flags&PostPending != 0
	switch {
	case p.Flags&Pending == 0:
		return PendingTransferNotPending
	case !tr.DebitAccountID.IsZero() && tr.DebitAccountID != p.DebitAccountID:
		return PendingTransferHasDifferentDebitAccount
	case !tr.CreditAccountID.IsZero() && tr.CreditAccountID != p.CreditAccountID:
		return PendingTransferHasDifferentCreditAccount
	case tr.Ledger != 0 && tr.Ledger != p.Ledger:
		return PendingTransferHasDifferentLedger
	case tr.Code != 0 && tr.Code != p.Code:
		return PendingTransferHasDifferentCode
	case p.status == statusPosted:
		return PendingTransferAlreadyPosted
	case p.status == statusVoided:
		return PendingTransferAlreadyVoided
	case p.status == statusExpired || p.Timeout > 0 && p.expiresAt() <= ts:
		return PendingTransferExpired
	case post && tr.Amount.Cmp(p.Amount) > 0:
		return ExceedsPendingTransferAmount
	case !post && !tr.Amount.IsZero() && tr.Amount != p.Amount:
		return PendingTransferHasDifferentAmount
	}

	*tr = inherit(*tr, &p.Transfer)
	dri, cri := m.accountIdx[p.DebitAccountID], m.accountIdx[p.CreditAccountID]
	dr, cr := &m.accounts[dri], &m.accounts[cri]
	m.undo = append(m.undo, undo{dr: dri, cr: cri, drPrev: *dr, crPrev: *cr, pending: pi})
	dr.DebitsPending = dr.DebitsPending.Sub(p.Amount)
	cr.CreditsPending = cr.CreditsPending.Sub(p.Amount)
	if post {
		dr.DebitsPosted = dr.DebitsPosted.Add(tr.Amount)
		cr.CreditsPosted = cr.CreditsPosted.Add(tr.Amount)
		p.status = statusPosted
	} else {
		p.status = statusVoided
	}
	tr.Timestamp = ts
	m.transferIdx[tr.ID] = len(m.transfers)
	m.transfers = append(m.transfers, stored{Transfer: *tr})
	return OK
}

func inherit(tr Transfer, p *Transfer) Transfer {
	if tr.DebitAccountID.IsZero() {
		tr.DebitAccountID = p.DebitAccountID
	}
	if tr.CreditAccountID.IsZero() {
		tr.CreditAccountID = p.CreditAccountID
	}
	if tr.Ledger == 0 {
		tr.Ledger = p.Ledger
	}
	if tr.Code == 0 {
		tr.Code = p.Code
	}
	if tr.Amount.IsZero() {
		tr.Amount = p.Amount
	}
	return tr
}

func exists(e *Transfer, tr Transfer) Code {
	tr.Timestamp = e.Timestamp
	if tr != *e {
		return ExistsWithDifferentFields
	}
	return Exists
}

func (m *Machine) rollback() {
	for i := len(m.undo) - 1; i >= 0; i-- {
		u := &m.undo[i]
		last := len(m.transfers) - 1
		delete(m.transferIdx, m.transfers[last].ID)
		m.transfers = m.transfers[:last]
		if u.pending >= 0 {
			m.transfers[u.pending].status = statusPending
		}
		m.accounts[u.dr] = u.drPrev
		m.accounts[u.cr] = u.crPrev
	}
	m.undo = m.undo[:0]
}

func (tr *Transfer) expiresAt() uint64 {
	return tr.Timestamp + uint64(tr.Timeout)*1e9
}

func (m *Machine) expire(now uint64) {
	for len(m.expiries) > 0 && m.expiries[0].at <= now {
		e := m.expiries.pop()
		i, ok := m.transferIdx[e.id]
		if !ok {
			continue
		}
		p := &m.transfers[i]
		if p.status != statusPending || p.Timeout == 0 || p.expiresAt() != e.at {
			continue
		}
		dr := &m.accounts[m.accountIdx[p.DebitAccountID]]
		cr := &m.accounts[m.accountIdx[p.CreditAccountID]]
		dr.DebitsPending = dr.DebitsPending.Sub(p.Amount)
		cr.CreditsPending = cr.CreditsPending.Sub(p.Amount)
		p.status = statusExpired
	}
}
