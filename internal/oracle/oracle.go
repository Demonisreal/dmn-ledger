package oracle

import (
	"bytes"
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

var (
	limit = new(big.Int).Lsh(big.NewInt(1), 128)
	zero  u128.Uint128
)

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
	heads     [][32]byte
	links     [][]byte
}

func New() *Ledger {
	return &Ledger{
		accounts:  map[u128.Uint128]*account{},
		transfers: map[u128.Uint128]*transfer{},
	}
}

func (l *Ledger) Clone() *Ledger {
	c := &Ledger{
		accounts:  make(map[u128.Uint128]*account, len(l.accounts)),
		transfers: make(map[u128.Uint128]*transfer, len(l.transfers)),
		last:      l.last,
		heads:     l.heads[:len(l.heads):len(l.heads)],
		links:     l.links[:len(l.links):len(l.links)],
	}
	for id, a := range l.accounts {
		cp := *a
		c.accounts[id] = &cp
	}
	for id, t := range l.transfers {
		cp := *t
		c.transfers[id] = &cp
	}
	return c
}

func (l *Ledger) Last() uint64 {
	return l.last
}

func (l *Ledger) Apply(op state.Op, ts uint64, body []byte) ([]state.Failure, error) {
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

	if ts >= 1<<63 {
		return nil, fmt.Errorf("%w: timestamp %d", state.ErrMalformedBatch, ts)
	}
	n := len(body) / size
	stamps := make([]uint64, n)
	now := max(ts, l.last+1)
	next := now
	for i := range stamps {
		stamps[i] = next
		next++
	}
	if now >= 1<<63 || next > 1<<63 {
		return nil, fmt.Errorf("%w: timestamp %d", state.ErrMalformedBatch, ts)
	}
	var leaves [][32]byte
	for _, t := range l.expire(now) {
		leaves = append(leaves, leaf(3, t.Transfer))
	}
	l.last = now
	if n > 0 {
		l.last = stamps[n-1]
	}

	var failed []state.Failure
	if op == state.CreateAccounts {
		for i := range n {
			a := decode[state.Account](body[i*size:])
			reason := l.createAccount(a, stamps[i])
			if reason != state.OK {
				failed = append(failed, state.Failure{Index: i, Reason: reason})
				continue
			}
			leaves = append(leaves, leaf(1, l.accounts[a.ID].export()))
		}
		l.chain(op, now, n, body, failed, leaves)
		return failed, nil
	}

	trs := make([]state.Transfer, n)
	for i := range trs {
		trs[i] = decode[state.Transfer](body[i*size:])
	}
	for first := 0; first < n; {
		last := first
		for last < n && trs[last].Flags&state.Linked != 0 {
			last++
		}
		if last == n {
			for i := first; i < n; i++ {
				failed = append(failed, state.Failure{Index: i, Reason: state.LinkedEventChainOpen})
			}
			break
		}

		x := &tx{l: l, accounts: map[u128.Uint128]*account{}, transfers: map[u128.Uint128]*transfer{}}
		ok := true
		for i := first; i <= last && ok; i++ {
			reason := x.createTransfer(trs[i], stamps[i])
			if reason == state.OK {
				continue
			}
			ok = false
			for j := first; j <= last; j++ {
				c := state.LinkedEventFailed
				if j == i {
					c = reason
				}
				failed = append(failed, state.Failure{Index: j, Reason: c})
			}
		}
		if ok {
			maps.Copy(l.accounts, x.accounts)
			maps.Copy(l.transfers, x.transfers)
			for _, tr := range trs[first : last+1] {
				leaves = append(leaves, leaf(2, l.transfers[tr.ID].Transfer))
			}
		}
		first = last + 1
	}
	l.chain(op, now, n, body, failed, leaves)
	return failed, nil
}

func (l *Ledger) chain(op state.Op, now uint64, events int, body []byte, failed []state.Failure, leaves [][32]byte) {
	var reply []uint32
	for _, f := range failed {
		reply = append(reply, uint32(f.Index), uint32(f.Reason)) //nolint:gosec // G115: an index into the batch
	}
	var link bytes.Buffer
	_ = binary.Write(&link, binary.BigEndian, struct {
		Time              uint64
		Op                uint8
		Events, Leaves    uint32
		Body, Reply, Root [32]byte
	}{now, uint8(op), uint32(events), uint32(len(leaves)), hashOf(3, body), hashOf(4, reply), root(leaves)}) //nolint:gosec // G115: a body is far below 4 GiB
	l.heads = append(l.heads, hashOf(2, l.Head(), uint64(len(l.links)+1), link.Bytes()))
	l.links = append(l.links, link.Bytes())
}

func hashOf(prefix uint8, parts ...any) [32]byte {
	var buf bytes.Buffer
	buf.WriteByte(prefix)
	for _, p := range parts {
		_ = binary.Write(&buf, binary.BigEndian, p)
	}
	return sha256.Sum256(buf.Bytes())
}

func leaf(kind uint8, record any) [32]byte {
	return hashOf(0, kind, record)
}

func root(leaves [][32]byte) [32]byte {
	switch len(leaves) {
	case 0:
		return sha256.Sum256(nil)
	case 1:
		return leaves[0]
	}
	k := 1
	for k*2 < len(leaves) {
		k *= 2
	}
	return hashOf(1, root(leaves[:k]), root(leaves[k:]))
}

func (l *Ledger) Head() [32]byte {
	return l.HeadAt(len(l.heads))
}

func (l *Ledger) HeadAt(seq int) [32]byte {
	if seq == 0 {
		return [32]byte{}
	}
	return l.heads[seq-1]
}

func (l *Ledger) Links() int {
	return len(l.links)
}

func (l *Ledger) Link(seq int) []byte {
	return l.links[seq-1]
}

func (l *Ledger) Expired(id u128.Uint128) bool {
	t, ok := l.transfers[id]
	return ok && t.status == statusExpired
}

func (l *Ledger) createAccount(a state.Account, ts uint64) state.Reason {
	if a.ID.IsZero() {
		return state.IDMustNotBeZero
	}
	if a.ID == u128.Max() {
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

func (x *tx) createTransfer(tr state.Transfer, ts uint64) state.Reason {
	kinds := 0
	for _, f := range []state.TransferFlags{state.Pending, state.PostPending, state.VoidPending} {
		if tr.Flags&f != 0 {
			kinds++
		}
	}
	if tr.ID.IsZero() {
		return state.IDMustNotBeZero
	}
	if tr.ID == u128.Max() {
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

func (x *tx) finishPending(tr state.Transfer, ts uint64) state.Reason {
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

func compare(e, tr state.Transfer) state.Reason {
	e.Timestamp = 0
	if e == tr {
		return state.Exists
	}
	return state.ExistsWithDifferentFields
}

func (l *Ledger) expire(now uint64) []*transfer {
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
	return due
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
	_ = binary.Write(h, binary.BigEndian, uint8(1))
	_ = binary.Write(h, binary.BigEndian, l.last)
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

func decode[T state.Account | state.Transfer](rec []byte) T {
	var v T
	_ = binary.Read(bytes.NewReader(rec), binary.BigEndian, &v)
	return v
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
