package oracle

import (
	"cmp"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"maps"
	"math/big"
	"slices"

	"github.com/Demonisreal/dmn-ledger/internal/state"
	"github.com/Demonisreal/dmn-ledger/internal/u128"
)

const (
	statusPending uint8 = iota + 1
	statusPosted
	statusVoided
	statusExpired
)

var limit = new(big.Int).Lsh(big.NewInt(1), 128)

type account struct {
	id        u128.Uint128
	ledger    uint32
	code      uint16
	flags     state.AccountFlags
	timestamp uint64

	debitsPending, debitsPosted, creditsPending, creditsPosted *big.Int
}

type transfer struct {
	state.Transfer
	status uint8
}

type Ledger struct {
	accounts  map[u128.Uint128]*account
	transfers map[u128.Uint128]*transfer
	last      uint64
}

func New() *Ledger {
	return &Ledger{
		accounts:  map[u128.Uint128]*account{},
		transfers: map[u128.Uint128]*transfer{},
	}
}

func (l *Ledger) Apply(op state.Op, ts uint64, body []byte) ([]state.Result, error) {
	var size int
	switch op {
	case state.CreateAccounts:
		size = state.AccountSize
	case state.CreateTransfers:
		size = state.TransferSize
	default:
		return nil, fmt.Errorf("%w: op %d", state.ErrMalformedBatch, op)
	}
	if len(body)%size != 0 {
		return nil, fmt.Errorf("%w: %d bytes", state.ErrMalformedBatch, len(body))
	}

	l.expire(ts)
	n := len(body) / size
	stamps := make([]uint64, n)
	for i := range stamps {
		stamps[i] = max(ts, l.last+1)
		l.last = stamps[i]
	}

	var failed []state.Result
	if op == state.CreateAccounts {
		for i := range n {
			code := l.createAccount(state.DecodeAccount(body[i*size:]), stamps[i])
			if code != state.OK {
				failed = append(failed, state.Result{Index: i, Code: code})
			}
		}
		return failed, nil
	}

	trs := make([]state.Transfer, n)
	for i := range trs {
		trs[i] = state.DecodeTransfer(body[i*size:])
	}
	for first := 0; first < n; {
		last := first
		for last < n && trs[last].Flags&state.Linked != 0 {
			last++
		}
		if last == n {
			for i := first; i < n; i++ {
				failed = append(failed, state.Result{Index: i, Code: state.LinkedEventChainOpen})
			}
			break
		}

		x := &tx{l: l, accounts: map[u128.Uint128]*account{}, transfers: map[u128.Uint128]*transfer{}}
		ok := true
		for i := first; i <= last && ok; i++ {
			code := x.createTransfer(trs[i], stamps[i])
			if code == state.OK {
				continue
			}
			ok = false
			for j := first; j <= last; j++ {
				c := state.LinkedEventFailed
				if j == i {
					c = code
				}
				failed = append(failed, state.Result{Index: j, Code: c})
			}
		}
		if ok {
			maps.Copy(l.accounts, x.accounts)
			maps.Copy(l.transfers, x.transfers)
		}
		first = last + 1
	}
	return failed, nil
}

func (l *Ledger) createAccount(a state.Account, ts uint64) state.Code {
	if a.ID.IsZero() {
		return state.IDMustNotBeZero
	}
	if a.ID == u128.Max {
		return state.IDMustNotBeMax
	}
	if a.Flags > state.DebitsMustNotExceedCredits|state.CreditsMustNotExceedDebits {
		return state.FlagsUnknown
	}
	if a.Flags&state.DebitsMustNotExceedCredits != 0 && a.Flags&state.CreditsMustNotExceedDebits != 0 {
		return state.FlagsAreMutuallyExclusive
	}
	if a.Ledger == 0 {
		return state.LedgerMustNotBeZero
	}
	if a.Code == 0 {
		return state.CodeMustNotBeZero
	}
	zero := u128.Uint128{}
	if a.DebitsPending != zero || a.DebitsPosted != zero || a.CreditsPending != zero || a.CreditsPosted != zero {
		return state.BalancesMustBeZero
	}
	if a.Timestamp != 0 {
		return state.TimestampMustBeZero
	}
	if e, ok := l.accounts[a.ID]; ok {
		if e.ledger == a.Ledger && e.code == a.Code && e.flags == a.Flags {
			return state.Exists
		}
		return state.ExistsWithDifferentFields
	}
	l.accounts[a.ID] = &account{
		id:             a.ID,
		ledger:         a.Ledger,
		code:           a.Code,
		flags:          a.Flags,
		timestamp:      ts,
		debitsPending:  new(big.Int),
		debitsPosted:   new(big.Int),
		creditsPending: new(big.Int),
		creditsPosted:  new(big.Int),
	}
	return state.OK
}

type tx struct {
	l         *Ledger
	accounts  map[u128.Uint128]*account
	transfers map[u128.Uint128]*transfer
}

func (x *tx) account(id u128.Uint128) *account {
	if a, ok := x.accounts[id]; ok {
		return a
	}
	a, ok := x.l.accounts[id]
	if !ok {
		return nil
	}
	cp := *a
	x.accounts[id] = &cp
	return &cp
}

func (x *tx) transfer(id u128.Uint128) *transfer {
	if t, ok := x.transfers[id]; ok {
		return t
	}
	t, ok := x.l.transfers[id]
	if !ok {
		return nil
	}
	cp := *t
	x.transfers[id] = &cp
	return &cp
}

func (x *tx) createTransfer(tr state.Transfer, ts uint64) state.Code {
	kinds := 0
	for _, f := range []state.TransferFlags{state.Pending, state.PostPending, state.VoidPending} {
		if tr.Flags&f != 0 {
			kinds++
		}
	}
	if tr.ID.IsZero() {
		return state.IDMustNotBeZero
	}
	if tr.ID == u128.Max {
		return state.IDMustNotBeMax
	}
	if tr.Flags >= state.VoidPending<<1 {
		return state.FlagsUnknown
	}
	if kinds > 1 {
		return state.FlagsAreMutuallyExclusive
	}
	if tr.Flags&(state.PostPending|state.VoidPending) != 0 {
		return x.finishPending(tr, ts)
	}

	zero := u128.Uint128{}
	if tr.DebitAccountID == zero {
		return state.DebitAccountIDMustNotBeZero
	}
	if tr.CreditAccountID == zero {
		return state.CreditAccountIDMustNotBeZero
	}
	if tr.DebitAccountID == tr.CreditAccountID {
		return state.AccountsMustBeDifferent
	}
	if tr.PendingID != zero {
		return state.PendingIDMustBeZero
	}
	if tr.Timeout > 0 && tr.Flags&state.Pending == 0 {
		return state.TimeoutRequiresPending
	}
	if tr.Ledger == 0 {
		return state.LedgerMustNotBeZero
	}
	if tr.Code == 0 {
		return state.CodeMustNotBeZero
	}
	if tr.Amount == zero {
		return state.AmountMustNotBeZero
	}
	if tr.Timestamp != 0 {
		return state.TimestampMustBeZero
	}
	dr := x.account(tr.DebitAccountID)
	if dr == nil {
		return state.DebitAccountNotFound
	}
	cr := x.account(tr.CreditAccountID)
	if cr == nil {
		return state.CreditAccountNotFound
	}
	if dr.ledger != cr.ledger {
		return state.AccountsMustHaveSameLedger
	}
	if tr.Ledger != dr.ledger {
		return state.TransferMustHaveSameLedgerAsAccounts
	}
	if e := x.transfer(tr.ID); e != nil {
		return compare(e.Transfer, tr)
	}

	amount := toBig(tr.Amount)
	debits := sum(dr.debitsPosted, dr.debitsPending, amount)
	if debits.Cmp(limit) >= 0 {
		return state.OverflowsDebits
	}
	credits := sum(cr.creditsPosted, cr.creditsPending, amount)
	if credits.Cmp(limit) >= 0 {
		return state.OverflowsCredits
	}
	if dr.flags&state.DebitsMustNotExceedCredits != 0 && debits.Cmp(dr.creditsPosted) > 0 {
		return state.ExceedsCredits
	}
	if cr.flags&state.CreditsMustNotExceedDebits != 0 && credits.Cmp(cr.debitsPosted) > 0 {
		return state.ExceedsDebits
	}

	stored := &transfer{Transfer: tr}
	stored.Timestamp = ts
	if tr.Flags&state.Pending != 0 {
		dr.debitsPending = sum(dr.debitsPending, amount)
		cr.creditsPending = sum(cr.creditsPending, amount)
		stored.status = statusPending
	} else {
		dr.debitsPosted = sum(dr.debitsPosted, amount)
		cr.creditsPosted = sum(cr.creditsPosted, amount)
	}
	x.transfers[tr.ID] = stored
	return state.OK
}

func (x *tx) finishPending(tr state.Transfer, ts uint64) state.Code {
	zero := u128.Uint128{}
	if tr.PendingID == zero {
		return state.PendingIDMustNotBeZero
	}
	if tr.Timeout > 0 {
		return state.TimeoutRequiresPending
	}
	if tr.Timestamp != 0 {
		return state.TimestampMustBeZero
	}
	if e := x.transfer(tr.ID); e != nil {
		if e.PendingID != tr.PendingID {
			return state.ExistsWithDifferentFields
		}
		return compare(e.Transfer, fill(tr, x.transfer(tr.PendingID).Transfer))
	}

	p := x.transfer(tr.PendingID)
	if p == nil {
		return state.PendingTransferNotFound
	}
	if p.Flags&state.Pending == 0 {
		return state.PendingTransferNotPending
	}
	if tr.DebitAccountID != zero && tr.DebitAccountID != p.DebitAccountID {
		return state.PendingTransferHasDifferentDebitAccount
	}
	if tr.CreditAccountID != zero && tr.CreditAccountID != p.CreditAccountID {
		return state.PendingTransferHasDifferentCreditAccount
	}
	if tr.Ledger != 0 && tr.Ledger != p.Ledger {
		return state.PendingTransferHasDifferentLedger
	}
	if tr.Code != 0 && tr.Code != p.Code {
		return state.PendingTransferHasDifferentCode
	}
	switch p.status {
	case statusPosted:
		return state.PendingTransferAlreadyPosted
	case statusVoided:
		return state.PendingTransferAlreadyVoided
	case statusExpired:
		return state.PendingTransferExpired
	}
	if p.Timeout > 0 && p.Timestamp+uint64(p.Timeout)*1_000_000_000 <= ts {
		return state.PendingTransferExpired
	}
	post := tr.Flags&state.PostPending != 0
	if post && tr.Amount.Cmp(p.Amount) > 0 {
		return state.ExceedsPendingTransferAmount
	}
	if !post && tr.Amount != zero && tr.Amount != p.Amount {
		return state.PendingTransferHasDifferentAmount
	}

	stored := &transfer{Transfer: fill(tr, p.Transfer)}
	stored.Timestamp = ts
	dr, cr := x.account(p.DebitAccountID), x.account(p.CreditAccountID)
	held := toBig(p.Amount)
	dr.debitsPending = new(big.Int).Sub(dr.debitsPending, held)
	cr.creditsPending = new(big.Int).Sub(cr.creditsPending, held)
	if post {
		amount := toBig(stored.Amount)
		dr.debitsPosted = sum(dr.debitsPosted, amount)
		cr.creditsPosted = sum(cr.creditsPosted, amount)
		p.status = statusPosted
	} else {
		p.status = statusVoided
	}
	x.transfers[tr.ID] = stored
	return state.OK
}

func fill(tr, p state.Transfer) state.Transfer {
	zero := u128.Uint128{}
	if tr.DebitAccountID == zero {
		tr.DebitAccountID = p.DebitAccountID
	}
	if tr.CreditAccountID == zero {
		tr.CreditAccountID = p.CreditAccountID
	}
	if tr.Ledger == 0 {
		tr.Ledger = p.Ledger
	}
	if tr.Code == 0 {
		tr.Code = p.Code
	}
	if tr.Amount == zero {
		tr.Amount = p.Amount
	}
	return tr
}

func compare(e, tr state.Transfer) state.Code {
	e.Timestamp = 0
	if e == tr {
		return state.Exists
	}
	return state.ExistsWithDifferentFields
}

func (l *Ledger) expire(now uint64) {
	var due []*transfer
	for _, t := range l.transfers {
		if t.status == statusPending && t.Timeout > 0 && expiresAt(t) <= now {
			due = append(due, t)
		}
	}
	slices.SortFunc(due, func(a, b *transfer) int {
		return cmp.Or(cmp.Compare(expiresAt(a), expiresAt(b)), a.ID.Cmp(b.ID))
	})
	for _, t := range due {
		held := toBig(t.Amount)
		dr, cr := l.accounts[t.DebitAccountID], l.accounts[t.CreditAccountID]
		dr.debitsPending = new(big.Int).Sub(dr.debitsPending, held)
		cr.creditsPending = new(big.Int).Sub(cr.creditsPending, held)
		t.status = statusExpired
	}
}

func expiresAt(t *transfer) uint64 {
	return t.Timestamp + uint64(t.Timeout)*1_000_000_000
}

func (l *Ledger) LookupAccounts(ids []u128.Uint128) []state.Account {
	var found []state.Account
	for _, id := range ids {
		if a, ok := l.accounts[id]; ok {
			found = append(found, a.export())
		}
	}
	return found
}

func (l *Ledger) LookupTransfers(ids []u128.Uint128) []state.Transfer {
	var found []state.Transfer
	for _, id := range ids {
		if t, ok := l.transfers[id]; ok {
			found = append(found, t.Transfer)
		}
	}
	return found
}

func (l *Ledger) Digest() [sha256.Size]byte {
	h := sha256.New()
	ids := slices.SortedFunc(maps.Keys(l.accounts), u128.Uint128.Cmp)
	_ = binary.Write(h, binary.BigEndian, uint64(len(ids)))
	for _, id := range ids {
		_ = binary.Write(h, binary.BigEndian, l.accounts[id].export())
	}
	ids = slices.SortedFunc(maps.Keys(l.transfers), u128.Uint128.Cmp)
	_ = binary.Write(h, binary.BigEndian, uint64(len(ids)))
	for _, id := range ids {
		t := l.transfers[id]
		_ = binary.Write(h, binary.BigEndian, t.Transfer)
		_ = binary.Write(h, binary.BigEndian, t.status)
	}
	return [sha256.Size]byte(h.Sum(nil))
}

func (l *Ledger) CheckInvariants() error {
	type sides struct{ debitsPending, debitsPosted, creditsPending, creditsPosted big.Int }
	ledgers := map[uint32]*sides{}
	for _, a := range l.accounts {
		s := ledgers[a.ledger]
		if s == nil {
			s = &sides{}
			ledgers[a.ledger] = s
		}
		s.debitsPending.Add(&s.debitsPending, a.debitsPending)
		s.debitsPosted.Add(&s.debitsPosted, a.debitsPosted)
		s.creditsPending.Add(&s.creditsPending, a.creditsPending)
		s.creditsPosted.Add(&s.creditsPosted, a.creditsPosted)
	}
	for _, ledger := range slices.Sorted(maps.Keys(ledgers)) {
		s := ledgers[ledger]
		if s.debitsPosted.Cmp(&s.creditsPosted) != 0 || s.debitsPending.Cmp(&s.creditsPending) != 0 {
			return fmt.Errorf("ledger %d: debits and credits differ", ledger)
		}
	}
	return nil
}

func (a *account) export() state.Account {
	return state.Account{
		ID:             a.id,
		DebitsPending:  fromBig(a.debitsPending),
		DebitsPosted:   fromBig(a.debitsPosted),
		CreditsPending: fromBig(a.creditsPending),
		CreditsPosted:  fromBig(a.creditsPosted),
		Ledger:         a.ledger,
		Code:           a.code,
		Flags:          a.flags,
		Timestamp:      a.timestamp,
	}
}

func toBig(u u128.Uint128) *big.Int {
	return new(big.Int).SetBytes(u.AppendBytes(nil))
}

func fromBig(n *big.Int) u128.Uint128 {
	var b [16]byte
	return u128.FromBytes(n.FillBytes(b[:]))
}

func sum(terms ...*big.Int) *big.Int {
	s := new(big.Int)
	for _, t := range terms {
		s.Add(s, t)
	}
	return s
}
