package state

import (
	"bufio"
	"crypto/sha256"
	"errors"
	"fmt"
	"hash"
	"math/bits"

	"github.com/Demonisreal/dmn-ledger/internal/u128"
)

var ErrMalformedBatch = errors.New("malformed batch")

const (
	tsLimit   = 1 << 63
	blockBits = 14
	blockSize = 1 << blockBits
)

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
	dr, cr int
	heap   int
	status status
}

type undo struct {
	dr, cr, pending               int
	debitsPending, debitsPosted   u128.Uint128
	creditsPending, creditsPosted u128.Uint128
}

type Machine struct {
	accounts    []Account
	accountIdx  map[u128.Uint128]int
	blocks      []*[blockSize]stored
	ntransfers  int
	transferIdx map[u128.Uint128]int
	expiries    []expiry
	undo        []undo
	last        uint64

	accountOrder, transferOrder order
	hash                        hash.Hash
	hashw                       *bufio.Writer
}

func New(accounts, transfers int) *Machine {
	m := &Machine{
		accounts:    make([]Account, 0, accounts),
		accountIdx:  make(map[u128.Uint128]int, accounts),
		transferIdx: make(map[u128.Uint128]int, transfers),
		hash:        sha256.New(),
	}
	m.hashw = bufio.NewWriter(m.hash)
	for range (transfers + blockSize - 1) / blockSize {
		m.blocks = append(m.blocks, new([blockSize]stored))
	}
	return m
}

func (m *Machine) transfer(i int) *stored {
	return &m.blocks[i>>blockBits][i&(blockSize-1)]
}

func (m *Machine) store(s stored) int {
	i := m.ntransfers
	if i>>blockBits == len(m.blocks) {
		m.blocks = append(m.blocks, new([blockSize]stored))
	}
	*m.transfer(i) = s
	m.transferIdx[s.ID] = i
	m.ntransfers++
	return i
}

func (m *Machine) Apply(op Op, ts uint64, body []byte, dst []Failure) ([]Failure, error) {
	var size int
	switch op {
	case CreateAccounts:
		size = AccountSize
	case CreateTransfers:
		size = TransferSize
	default:
		return dst, fmt.Errorf("%w: unknown op %d", ErrMalformedBatch, op)
	}
	if len(body)%size != 0 {
		return dst, fmt.Errorf("%w: %d bytes is not a multiple of %d", ErrMalformedBatch, len(body), size)
	}
	start := max(ts, m.last+1)
	if ts >= tsLimit || start+max(uint64(len(body)/size), 1) > tsLimit {
		return dst, fmt.Errorf("%w: timestamp %d out of range, last %d", ErrMalformedBatch, ts, m.last)
	}

	m.last = start
	m.expire(start)
	if op == CreateAccounts {
		return m.createAccounts(start, body, dst), nil
	}
	return m.createTransfers(start, body, dst), nil
}

func (m *Machine) createAccounts(ts uint64, body []byte, dst []Failure) []Failure {
	for i := range len(body) / AccountSize {
		a := DecodeAccount(body[i*AccountSize:])
		if reason := m.createAccount(&a, ts); reason != OK {
			dst = append(dst, Failure{Index: i, Reason: reason})
		}
		m.last = ts
		ts++
	}
	return dst
}

func (m *Machine) createAccount(a *Account, ts uint64) Reason {
	switch {
	case a.ID.IsZero():
		return IDMustNotBeZero
	case a.ID == u128.Max():
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

func (m *Machine) createTransfers(ts uint64, body []byte, dst []Failure) []Failure {
	n := len(body) / TransferSize
	open := n
	for open > 0 && transferFlags(body[(open-1)*TransferSize:])&Linked != 0 {
		open--
	}

	first, broken, reason := 0, -1, OK
	for i := range open {
		tr := DecodeTransfer(body[i*TransferSize:])
		if broken < 0 {
			if reason = m.createTransfer(&tr, ts); reason != OK {
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
				r := LinkedEventFailed
				if j == broken {
					r = reason
				}
				dst = append(dst, Failure{Index: j, Reason: r})
			}
		}
		m.undo = m.undo[:0]
		first, broken = i+1, -1
	}
	for i := open; i < n; i++ {
		dst = append(dst, Failure{Index: i, Reason: LinkedEventChainOpen})
		m.last = ts
		ts++
	}
	return dst
}

func (m *Machine) createTransfer(tr *Transfer, ts uint64) Reason {
	switch {
	case tr.ID.IsZero():
		return IDMustNotBeZero
	case tr.ID == u128.Max():
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
		return exists(&m.transfer(i).Transfer, *tr)
	}

	debits, overflow := dr.DebitsPosted.AddWrap(dr.DebitsPending).AddOverflow(tr.Amount)
	if overflow {
		return OverflowsDebits
	}
	credits, overflow := cr.CreditsPosted.AddWrap(cr.CreditsPending).AddOverflow(tr.Amount)
	if overflow {
		return OverflowsCredits
	}
	if dr.Flags&DebitsMustNotExceedCredits != 0 && debits.Cmp(dr.CreditsPosted) > 0 {
		return ExceedsCredits
	}
	if cr.Flags&CreditsMustNotExceedDebits != 0 && credits.Cmp(cr.DebitsPosted) > 0 {
		return ExceedsDebits
	}

	m.saveBalances(dri, cri, -1)
	st := statusNone
	if tr.Flags&Pending != 0 {
		dr.DebitsPending = dr.DebitsPending.AddWrap(tr.Amount)
		cr.CreditsPending = cr.CreditsPending.AddWrap(tr.Amount)
		st = statusPending
	} else {
		dr.DebitsPosted = dr.DebitsPosted.AddWrap(tr.Amount)
		cr.CreditsPosted = cr.CreditsPosted.AddWrap(tr.Amount)
	}
	tr.Timestamp = ts
	i := m.store(stored{Transfer: *tr, dr: dri, cr: cri, heap: -1, status: st})
	if st == statusPending && tr.Timeout > 0 {
		m.scheduleExpiry(i)
	}
	return OK
}

func (m *Machine) resolvePending(tr *Transfer, ts uint64) Reason {
	switch {
	case tr.PendingID.IsZero():
		return PendingIDMustNotBeZero
	case tr.Timeout != 0:
		return TimeoutRequiresPending
	case tr.Timestamp != 0:
		return TimestampMustBeZero
	}
	if i, ok := m.transferIdx[tr.ID]; ok {
		e := &m.transfer(i).Transfer
		if e.PendingID != tr.PendingID {
			return ExistsWithDifferentFields
		}
		return exists(e, inherit(*tr, &m.transfer(m.transferIdx[e.PendingID]).Transfer))
	}

	pi, ok := m.transferIdx[tr.PendingID]
	if !ok {
		return PendingTransferNotFound
	}
	p := m.transfer(pi)
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
	dr, cr := &m.accounts[p.dr], &m.accounts[p.cr]
	m.saveBalances(p.dr, p.cr, pi)
	dr.DebitsPending = dr.DebitsPending.SubWrap(p.Amount)
	cr.CreditsPending = cr.CreditsPending.SubWrap(p.Amount)
	if post {
		dr.DebitsPosted = dr.DebitsPosted.AddWrap(tr.Amount)
		cr.CreditsPosted = cr.CreditsPosted.AddWrap(tr.Amount)
		p.status = statusPosted
	} else {
		p.status = statusVoided
	}
	if p.heap >= 0 {
		m.cancelExpiry(p.heap)
	}
	tr.Timestamp = ts
	m.store(stored{Transfer: *tr, dr: p.dr, cr: p.cr, heap: -1})
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

func exists(e *Transfer, tr Transfer) Reason {
	tr.Timestamp = e.Timestamp
	if tr != *e {
		return ExistsWithDifferentFields
	}
	return Exists
}

func (m *Machine) saveBalances(dr, cr, pending int) {
	d, c := &m.accounts[dr], &m.accounts[cr]
	m.undo = append(m.undo, undo{
		dr: dr, cr: cr, pending: pending,
		debitsPending: d.DebitsPending, debitsPosted: d.DebitsPosted,
		creditsPending: c.CreditsPending, creditsPosted: c.CreditsPosted,
	})
}

func (m *Machine) rollback() {
	for i := len(m.undo) - 1; i >= 0; i-- {
		u := &m.undo[i]
		last := m.ntransfers - 1
		if t := m.transfer(last); t.heap >= 0 {
			m.cancelExpiry(t.heap)
		}
		delete(m.transferIdx, m.transfer(last).ID)
		m.ntransfers = last
		if u.pending >= 0 {
			p := m.transfer(u.pending)
			p.status = statusPending
			if p.Timeout > 0 {
				m.scheduleExpiry(u.pending)
			}
		}
		dr, cr := &m.accounts[u.dr], &m.accounts[u.cr]
		dr.DebitsPending, dr.DebitsPosted = u.debitsPending, u.debitsPosted
		cr.CreditsPending, cr.CreditsPosted = u.creditsPending, u.creditsPosted
	}
	m.undo = m.undo[:0]
}

func (tr *Transfer) expiresAt() uint64 {
	return tr.Timestamp + uint64(tr.Timeout)*1e9
}

func (m *Machine) expire(now uint64) {
	for len(m.expiries) > 0 && m.expiries[0].at <= now {
		p := m.transfer(m.expiries[0].tr)
		m.cancelExpiry(0)
		dr, cr := &m.accounts[p.dr], &m.accounts[p.cr]
		dr.DebitsPending = dr.DebitsPending.SubWrap(p.Amount)
		cr.CreditsPending = cr.CreditsPending.SubWrap(p.Amount)
		p.status = statusExpired
	}
}
