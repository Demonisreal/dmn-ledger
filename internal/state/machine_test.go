package state_test

import (
	"bytes"
	"errors"
	"math"
	"runtime/debug"
	"slices"
	"testing"

	"github.com/Demonisreal/dmn-ledger/internal/audit"
	"github.com/Demonisreal/dmn-ledger/internal/state"
	"github.com/Demonisreal/dmn-ledger/internal/u128"
)

var id128 = u128.From64

func encodeAccounts(as ...state.Account) []byte {
	var b []byte
	for _, a := range as {
		b = state.AppendAccount(b, a)
	}
	return b
}

func encodeTransfers(trs ...state.Transfer) []byte {
	var b []byte
	for _, tr := range trs {
		b = state.AppendTransfer(b, tr)
	}
	return b
}

func mustApply(t *testing.T, m *state.Machine, ts uint64, trs ...state.Transfer) {
	t.Helper()
	fails, err := m.Apply(state.CreateTransfers, ts, encodeTransfers(trs...), nil)
	if err != nil || len(fails) > 0 {
		t.Fatalf("Apply at %d: %v %v", ts, fails, err)
	}
}

func account(t *testing.T, m *state.Machine, id uint64) state.Account {
	t.Helper()
	found := m.LookupAccounts([]u128.Uint128{id128(id)}, nil)
	if len(found) != 1 {
		t.Fatalf("account %d not found", id)
	}
	return found[0]
}

type batchSpec struct {
	op   state.Op
	ts   uint64
	body []byte
}

var setup = []batchSpec{
	{state.CreateAccounts, 1, encodeAccounts(
		state.Account{ID: id128(1), Ledger: 1, Code: 1},
		state.Account{ID: id128(2), Ledger: 1, Code: 1},
		state.Account{ID: id128(3), Ledger: 2, Code: 1},
		state.Account{ID: id128(4), Ledger: 1, Code: 1, Flags: state.DebitsMustNotExceedCredits},
		state.Account{ID: id128(5), Ledger: 1, Code: 1, Flags: state.CreditsMustNotExceedDebits},
		state.Account{ID: id128(6), Ledger: 1, Code: 1},
		state.Account{ID: id128(7), Ledger: 1, Code: 1},
	)},
	{state.CreateTransfers, 10, encodeTransfers(
		state.Transfer{ID: id128(100), DebitAccountID: id128(1), CreditAccountID: id128(2), Amount: id128(10), Ledger: 1, Code: 1},
		state.Transfer{ID: id128(101), DebitAccountID: id128(6), CreditAccountID: id128(7), Amount: u128.Max().SubWrap(id128(1)), Ledger: 1, Code: 1},
		state.Transfer{ID: id128(200), DebitAccountID: id128(1), CreditAccountID: id128(2), Amount: id128(10), Ledger: 1, Code: 1, Flags: state.Pending},
		state.Transfer{ID: id128(201), DebitAccountID: id128(1), CreditAccountID: id128(2), Amount: id128(10), Ledger: 1, Code: 1, Flags: state.Pending},
		state.Transfer{ID: id128(202), PendingID: id128(201), Flags: state.PostPending},
		state.Transfer{ID: id128(203), DebitAccountID: id128(1), CreditAccountID: id128(2), Amount: id128(10), Ledger: 1, Code: 1, Flags: state.Pending},
		state.Transfer{ID: id128(204), PendingID: id128(203), Flags: state.VoidPending},
		state.Transfer{ID: id128(205), DebitAccountID: id128(7), CreditAccountID: id128(6), Amount: id128(3), Ledger: 1, Code: 1, Flags: state.Pending, Timeout: 30},
	)},
}

func newLedger(t *testing.T) *state.Machine {
	t.Helper()
	m := state.New(0, 0)
	for _, b := range setup {
		if fails, err := m.Apply(b.op, b.ts, b.body, nil); err != nil || len(fails) > 0 {
			t.Fatalf("setup at %d: %v %v", b.ts, fails, err)
		}
	}
	return m
}

func records(t *testing.T, m *state.Machine) []byte {
	t.Helper()
	snap := snapshot(t, m)
	return snap[:len(snap)-8-int(m.Head().Seq)*audit.LinkSize]
}

func burned(t *testing.T, events int) *state.Machine {
	t.Helper()
	m := newLedger(t)
	if _, err := m.Apply(state.CreateTransfers, 20, make([]byte, events*state.TransferSize), nil); err != nil {
		t.Fatal(err)
	}
	return m
}

func TestCreateAccounts(t *testing.T) {
	t.Parallel()
	valid := state.Account{ID: id128(50), Ledger: 1, Code: 1}
	tests := []struct {
		name string
		edit func(*state.Account)
		want state.Reason
	}{
		{"ok", func(*state.Account) {}, state.OK},
		{"id zero", func(a *state.Account) { a.ID = u128.Uint128{} }, state.IDMustNotBeZero},
		{"id max", func(a *state.Account) { a.ID = u128.Max() }, state.IDMustNotBeMax},
		{"unknown flag", func(a *state.Account) { a.Flags = 1 << 2 }, state.FlagsUnknown},
		{"both limits", func(a *state.Account) {
			a.Flags = state.DebitsMustNotExceedCredits | state.CreditsMustNotExceedDebits
		}, state.FlagsAreMutuallyExclusive},
		{"ledger zero", func(a *state.Account) { a.Ledger = 0 }, state.LedgerMustNotBeZero},
		{"code zero", func(a *state.Account) { a.Code = 0 }, state.CodeMustNotBeZero},
		{"balance set", func(a *state.Account) { a.CreditsPending = id128(1) }, state.BalancesMustBeZero},
		{"timestamp set", func(a *state.Account) { a.Timestamp = 5 }, state.TimestampMustBeZero},
		{"exists", func(a *state.Account) { a.ID = id128(1) }, state.Exists},
		{"exists with other code", func(a *state.Account) { a.ID, a.Code = id128(1), 2 }, state.ExistsWithDifferentFields},
		{"ledger zero beats exists", func(a *state.Account) { a.ID, a.Ledger = id128(1), 0 }, state.LedgerMustNotBeZero},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			m := newLedger(t)
			a := valid
			tt.edit(&a)
			fails, err := m.Apply(state.CreateAccounts, 20, encodeAccounts(a), nil)
			if err != nil {
				t.Fatal(err)
			}
			var got state.Reason
			if len(fails) > 0 {
				got = fails[0].Reason
			}
			if got != tt.want {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCreateTransfers(t *testing.T) {
	t.Parallel()
	valid := state.Transfer{ID: id128(1000), DebitAccountID: id128(1), CreditAccountID: id128(2), Amount: id128(5), Ledger: 1, Code: 1}
	post := func(pending uint64) state.Transfer {
		return state.Transfer{ID: id128(1000), PendingID: id128(pending), Flags: state.PostPending}
	}
	tests := []struct {
		name string
		tr   state.Transfer
		edit func(*state.Transfer)
		want state.Reason
	}{
		{"ok", valid, nil, state.OK},
		{"id zero", valid, func(tr *state.Transfer) { tr.ID = u128.Uint128{} }, state.IDMustNotBeZero},
		{"id max", valid, func(tr *state.Transfer) { tr.ID = u128.Max() }, state.IDMustNotBeMax},
		{"unknown flag", valid, func(tr *state.Transfer) { tr.Flags = 1 << 4 }, state.FlagsUnknown},
		{"pending and post", valid, func(tr *state.Transfer) {
			tr.Flags = state.Pending | state.PostPending
		}, state.FlagsAreMutuallyExclusive},
		{"debit zero", valid, func(tr *state.Transfer) { tr.DebitAccountID = u128.Uint128{} }, state.DebitAccountIDMustNotBeZero},
		{"credit zero", valid, func(tr *state.Transfer) { tr.CreditAccountID = u128.Uint128{} }, state.CreditAccountIDMustNotBeZero},
		{"same accounts", valid, func(tr *state.Transfer) { tr.CreditAccountID = id128(1) }, state.AccountsMustBeDifferent},
		{"pending id on normal", valid, func(tr *state.Transfer) { tr.PendingID = id128(200) }, state.PendingIDMustBeZero},
		{"timeout on normal", valid, func(tr *state.Transfer) { tr.Timeout = 1 }, state.TimeoutRequiresPending},
		{"ledger zero", valid, func(tr *state.Transfer) { tr.Ledger = 0 }, state.LedgerMustNotBeZero},
		{"code zero", valid, func(tr *state.Transfer) { tr.Code = 0 }, state.CodeMustNotBeZero},
		{"amount zero", valid, func(tr *state.Transfer) { tr.Amount = u128.Uint128{} }, state.AmountMustNotBeZero},
		{"timestamp set", valid, func(tr *state.Transfer) { tr.Timestamp = 1 }, state.TimestampMustBeZero},
		{"debit missing", valid, func(tr *state.Transfer) { tr.DebitAccountID = id128(99) }, state.DebitAccountNotFound},
		{"credit missing", valid, func(tr *state.Transfer) { tr.CreditAccountID = id128(99) }, state.CreditAccountNotFound},
		{"accounts on two ledgers", valid, func(tr *state.Transfer) { tr.CreditAccountID = id128(3) }, state.AccountsMustHaveSameLedger},
		{"transfer on other ledger", valid, func(tr *state.Transfer) { tr.Ledger = 2 }, state.TransferMustHaveSameLedgerAsAccounts},
		{"exists", valid, func(tr *state.Transfer) { tr.ID, tr.Amount = id128(100), id128(10) }, state.Exists},
		{"exists with other amount", valid, func(tr *state.Transfer) { tr.ID = id128(100) }, state.ExistsWithDifferentFields},
		{"overflows debits", valid, func(tr *state.Transfer) { tr.DebitAccountID, tr.Amount = id128(6), id128(2) }, state.OverflowsDebits},
		{"overflows credits", valid, func(tr *state.Transfer) { tr.CreditAccountID, tr.Amount = id128(7), id128(2) }, state.OverflowsCredits},
		{"exceeds credits", valid, func(tr *state.Transfer) { tr.DebitAccountID = id128(4) }, state.ExceedsCredits},
		{"exceeds debits", valid, func(tr *state.Transfer) { tr.CreditAccountID = id128(5) }, state.ExceedsDebits},
		{"pending exceeds credits", valid, func(tr *state.Transfer) {
			tr.DebitAccountID, tr.Flags = id128(4), state.Pending
		}, state.ExceedsCredits},

		{"post full", post(200), nil, state.OK},
		{"post partial", post(200), func(tr *state.Transfer) { tr.Amount = id128(3) }, state.OK},
		{"void", post(200), func(tr *state.Transfer) { tr.Flags = state.VoidPending }, state.OK},
		{"void exact amount", post(200), func(tr *state.Transfer) {
			tr.Flags, tr.Amount = state.VoidPending, id128(10)
		}, state.OK},
		{"post with matching fields", post(200), func(tr *state.Transfer) {
			tr.DebitAccountID, tr.CreditAccountID, tr.Ledger, tr.Code = id128(1), id128(2), 1, 1
		}, state.OK},
		{"pending id zero", post(0), nil, state.PendingIDMustNotBeZero},
		{"post with timeout", post(200), func(tr *state.Transfer) { tr.Timeout = 1 }, state.TimeoutRequiresPending},
		{"post with timestamp", post(200), func(tr *state.Transfer) { tr.Timestamp = 1 }, state.TimestampMustBeZero},
		{"post exists", post(201), func(tr *state.Transfer) { tr.ID = id128(202) }, state.Exists},
		{"post exists resolved", post(201), func(tr *state.Transfer) {
			tr.ID, tr.Amount, tr.Ledger = id128(202), id128(10), 1
		}, state.Exists},
		{"post exists other pending", post(200), func(tr *state.Transfer) { tr.ID = id128(202) }, state.ExistsWithDifferentFields},
		{"post exists other amount", post(201), func(tr *state.Transfer) {
			tr.ID, tr.Amount = id128(202), id128(4)
		}, state.ExistsWithDifferentFields},
		{"pending missing", post(999), nil, state.PendingTransferNotFound},
		{"pending is posted transfer", post(100), nil, state.PendingTransferNotPending},
		{"other debit", post(200), func(tr *state.Transfer) { tr.DebitAccountID = id128(6) }, state.PendingTransferHasDifferentDebitAccount},
		{"other credit", post(200), func(tr *state.Transfer) { tr.CreditAccountID = id128(6) }, state.PendingTransferHasDifferentCreditAccount},
		{"other ledger", post(200), func(tr *state.Transfer) { tr.Ledger = 2 }, state.PendingTransferHasDifferentLedger},
		{"other code", post(200), func(tr *state.Transfer) { tr.Code = 2 }, state.PendingTransferHasDifferentCode},
		{"already posted", post(201), nil, state.PendingTransferAlreadyPosted},
		{"already voided", post(203), nil, state.PendingTransferAlreadyVoided},
		{"post too much", post(200), func(tr *state.Transfer) { tr.Amount = id128(11) }, state.ExceedsPendingTransferAmount},
		{"void partial", post(200), func(tr *state.Transfer) {
			tr.Flags, tr.Amount = state.VoidPending, id128(9)
		}, state.PendingTransferHasDifferentAmount},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			m := newLedger(t)
			tr := tt.tr
			if tt.edit != nil {
				tt.edit(&tr)
			}
			fails, err := m.Apply(state.CreateTransfers, 20, encodeTransfers(tr), nil)
			if err != nil {
				t.Fatal(err)
			}
			var got state.Reason
			if len(fails) > 0 {
				got = fails[0].Reason
			}
			if got != tt.want {
				t.Errorf("got %v, want %v", got, tt.want)
			}
			if err := m.CheckInvariants(); err != nil {
				t.Error(err)
			}
		})
	}
}

func TestPostPartial(t *testing.T) {
	t.Parallel()
	m := newLedger(t)
	before := account(t, m, 1)
	mustApply(t, m, 20, state.Transfer{ID: id128(1000), PendingID: id128(200), Amount: id128(4), Flags: state.PostPending})

	dr := account(t, m, 1)
	if want := before.DebitsPending.SubWrap(id128(10)); dr.DebitsPending != want {
		t.Errorf("debits pending %v, want %v", dr.DebitsPending, want)
	}
	if want := before.DebitsPosted.AddWrap(id128(4)); dr.DebitsPosted != want {
		t.Errorf("debits posted %v, want %v", dr.DebitsPosted, want)
	}
	stored := m.LookupTransfers([]u128.Uint128{id128(1000)}, nil)
	want := state.Transfer{
		ID: id128(1000), DebitAccountID: id128(1), CreditAccountID: id128(2), Amount: id128(4), PendingID: id128(200),
		Ledger: 1, Code: 1, Flags: state.PostPending, Timestamp: 20,
	}
	if len(stored) != 1 || stored[0] != want {
		t.Errorf("stored %+v, want %+v", stored, want)
	}
}

func TestLinked(t *testing.T) {
	t.Parallel()
	tr := func(id, dr, cr, amount uint64, flags state.TransferFlags) state.Transfer {
		return state.Transfer{ID: id128(id), DebitAccountID: id128(dr), CreditAccountID: id128(cr), Amount: id128(amount),
			Ledger: 1, Code: 1, Flags: flags}
	}
	tests := []struct {
		name string
		trs  []state.Transfer
		want []state.Failure
	}{
		{"chain commits", []state.Transfer{
			tr(1000, 1, 2, 1, state.Linked),
			tr(1001, 2, 1, 1, 0),
		}, nil},
		{"failure rolls back the chain", []state.Transfer{
			tr(1000, 1, 2, 1, 0),
			tr(1001, 1, 2, 1, state.Linked|state.Pending),
			tr(1002, 4, 2, 1, state.Linked),
			tr(1003, 2, 1, 1, 0),
			tr(1004, 1, 2, 1, 0),
		}, []state.Failure{{Index: 1, Reason: state.LinkedEventFailed}, {Index: 2, Reason: state.ExceedsCredits}, {Index: 3, Reason: state.LinkedEventFailed}}},
		{"chain sees its own effects", []state.Transfer{
			tr(1000, 2, 4, 3, state.Linked),
			tr(1001, 4, 2, 3, 0),
		}, nil},
		{"duplicate inside chain", []state.Transfer{
			tr(1000, 1, 2, 1, state.Linked),
			tr(1000, 1, 2, 1, state.Linked),
			tr(1001, 1, 2, 1, 0),
		}, []state.Failure{
			{Index: 0, Reason: state.LinkedEventFailed},
			{Index: 1, Reason: state.Exists},
			{Index: 2, Reason: state.LinkedEventFailed},
		}},
		{"open chain", []state.Transfer{
			tr(1000, 1, 2, 1, 0),
			tr(1001, 1, 2, 1, state.Linked),
			tr(1002, 99, 2, 1, state.Linked),
		}, []state.Failure{{Index: 1, Reason: state.LinkedEventChainOpen}, {Index: 2, Reason: state.LinkedEventChainOpen}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			m := newLedger(t)
			fails, err := m.Apply(state.CreateTransfers, 20, encodeTransfers(tt.trs...), nil)
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(fails, tt.want) {
				t.Fatalf("got %v, want %v", fails, tt.want)
			}
			if err := m.CheckInvariants(); err != nil {
				t.Fatal(err)
			}
			for i, tr := range tt.trs {
				failed := slices.ContainsFunc(fails, func(f state.Failure) bool { return f.Index == i })
				if found := len(m.LookupTransfers([]u128.Uint128{tr.ID}, nil)) == 1; found == failed {
					t.Errorf("transfer %v: found %v after failed=%v", tr.ID, found, failed)
				}
			}
			if len(tt.want) == len(tt.trs) && !bytes.Equal(records(t, m), records(t, burned(t, len(tt.trs)))) {
				t.Error("state changed although every event failed")
			}
		})
	}
}

func TestRollbackRestoresState(t *testing.T) {
	t.Parallel()
	m := newLedger(t)
	fails, err := m.Apply(state.CreateTransfers, 20, encodeTransfers(
		state.Transfer{ID: id128(1000), PendingID: id128(200), Amount: id128(4), Flags: state.PostPending | state.Linked},
		state.Transfer{ID: id128(1001), DebitAccountID: id128(2), CreditAccountID: id128(1), Amount: id128(7), Ledger: 1, Code: 1,
			Flags: state.Pending | state.Linked, Timeout: 1},
		state.Transfer{ID: id128(1002), PendingID: id128(1001), Flags: state.VoidPending | state.Linked},
		state.Transfer{ID: id128(1003), DebitAccountID: id128(1), CreditAccountID: id128(2), Amount: id128(1), Ledger: 3, Code: 1},
	), nil)
	if err != nil || len(fails) != 4 || fails[3].Reason != state.TransferMustHaveSameLedgerAsAccounts {
		t.Fatalf("fails %v, err %v", fails, err)
	}
	if !bytes.Equal(records(t, m), records(t, burned(t, 4))) {
		t.Fatal("rolled back chain left traces")
	}
	mustApply(t, m, 3e9, state.Transfer{ID: id128(1004), PendingID: id128(200), Flags: state.PostPending})
}

func TestTimestamps(t *testing.T) {
	t.Parallel()
	m := state.New(0, 0)
	steps := []struct {
		batch uint64
		count int
		first uint64
	}{
		{100, 3, 100},
		{50, 2, 103},
		{105, 1, 105},
		{1000, 0, 0},
		{900, 1, 1001},
		{2000, 2, 2000},
	}
	var id uint64
	for _, s := range steps {
		var as []state.Account
		for range s.count {
			id++
			as = append(as, state.Account{ID: id128(id), Ledger: 1, Code: 1})
		}
		if _, err := m.Apply(state.CreateAccounts, s.batch, encodeAccounts(as...), nil); err != nil {
			t.Fatal(err)
		}
		for i, a := range as {
			if got, want := account(t, m, a.ID.Lo).Timestamp, s.first+uint64(i); got != want {
				t.Errorf("batch at %d, event %d: timestamp %d, want %d", s.batch, i, got, want)
			}
		}
	}
}

func TestExpiry(t *testing.T) {
	t.Parallel()
	m := newLedger(t)
	pending := func(id uint64, timeout uint32) state.Transfer {
		return state.Transfer{ID: id128(id), DebitAccountID: id128(2), CreditAccountID: id128(1), Amount: id128(5),
			Ledger: 1, Code: 1, Flags: state.Pending, Timeout: timeout}
	}
	mustApply(t, m, 1e9, pending(300, 1), pending(301, 1), pending(302, 0))

	fails, err := m.Apply(state.CreateTransfers, 2e9-1, encodeTransfers(
		state.Transfer{},
		state.Transfer{ID: id128(400), PendingID: id128(300), Flags: state.PostPending},
	), nil)
	if err != nil {
		t.Fatal(err)
	}
	want := []state.Failure{
		{Index: 0, Reason: state.IDMustNotBeZero},
		{Index: 1, Reason: state.PendingTransferExpired},
	}
	if !slices.Equal(fails, want) {
		t.Fatalf("post at the deadline: got %v, want %v", fails, want)
	}
	if got := account(t, m, 2).DebitsPending; got != id128(15) {
		t.Fatalf("debits pending before sweep %v, want 15", got)
	}

	mustApply(t, m, 2e9+1)
	if got := account(t, m, 2).DebitsPending; got != id128(5) {
		t.Fatalf("debits pending after sweep %v, want 5", got)
	}
	fails, err = m.Apply(state.CreateTransfers, 2e9+2, encodeTransfers(
		state.Transfer{ID: id128(401), PendingID: id128(301), Flags: state.VoidPending},
		state.Transfer{ID: id128(402), PendingID: id128(302), Flags: state.VoidPending},
	), nil)
	if err != nil {
		t.Fatal(err)
	}
	if want := []state.Failure{{Index: 0, Reason: state.PendingTransferExpired}}; !slices.Equal(fails, want) {
		t.Fatalf("void after sweep: got %v, want %v", fails, want)
	}
	if err := m.CheckInvariants(); err != nil {
		t.Fatal(err)
	}
}

func TestExpiryIgnoresRecreatedTransfer(t *testing.T) {
	t.Parallel()
	m := newLedger(t)
	const at = 1e9 + 1e9
	fails, err := m.Apply(state.CreateTransfers, 1e9, encodeTransfers(
		state.Transfer{ID: id128(300), DebitAccountID: id128(2), CreditAccountID: id128(1), Amount: id128(5),
			Ledger: 1, Code: 1, Flags: state.Pending | state.Linked, Timeout: 1},
		state.Transfer{},
	), nil)
	if err != nil || len(fails) != 2 {
		t.Fatalf("chain should fail: %v %v", fails, err)
	}

	filler := make([]state.Transfer, 10)
	recreated := state.Transfer{ID: id128(300), DebitAccountID: id128(2), CreditAccountID: id128(1), Amount: id128(5),
		Ledger: 1, Code: 1, Flags: state.Pending}
	if _, err := m.Apply(state.CreateTransfers, at-10, encodeTransfers(append(filler, recreated)...), nil); err != nil {
		t.Fatal(err)
	}
	if got := m.LookupTransfers([]u128.Uint128{id128(300)}, nil); len(got) != 1 || got[0].Timestamp != at {
		t.Fatalf("recreated transfer %+v, want timestamp %d", got, uint64(at))
	}
	mustApply(t, m, at+5, state.Transfer{ID: id128(400), PendingID: id128(300), Flags: state.PostPending})
}

func TestApplyMalformed(t *testing.T) {
	t.Parallel()
	two := make([]byte, 2*state.TransferSize)
	tests := []struct {
		name string
		op   state.Op
		ts   uint64
		body []byte
	}{
		{"unknown op", 0, 1e12, nil},
		{"short account", state.CreateAccounts, 1e12, make([]byte, state.AccountSize-1)},
		{"transfer as account", state.CreateAccounts, 1e12, make([]byte, state.TransferSize)},
		{"short transfer", state.CreateTransfers, 1e12, make([]byte, state.TransferSize+1)},
		{"timestamp at limit", state.CreateTransfers, 1 << 63, nil},
		{"timestamp max", state.CreateTransfers, math.MaxUint64, two},
		{"last event past limit", state.CreateTransfers, 1<<63 - 1, two},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			m := newLedger(t)
			before := m.Digest()
			if _, err := m.Apply(tt.op, tt.ts, tt.body, nil); !errors.Is(err, state.ErrMalformedBatch) {
				t.Errorf("err = %v", err)
			}
			if m.Digest() != before {
				t.Error("state changed")
			}
		})
	}
}

func TestEmptyBatchMovesClock(t *testing.T) {
	t.Parallel()
	m := newLedger(t)
	mustApply(t, m, 1e9, state.Transfer{ID: id128(300), DebitAccountID: id128(2), CreditAccountID: id128(1), Amount: id128(5),
		Ledger: 1, Code: 1, Flags: state.Pending, Timeout: 100})
	mustApply(t, m, 200e9)
	if m.Last() != 200e9 {
		t.Fatalf("last %d after the empty batch, want %d", m.Last(), uint64(200e9))
	}

	fails, err := m.Apply(state.CreateTransfers, 2e9, encodeTransfers(
		state.Transfer{ID: id128(400), PendingID: id128(300), Flags: state.PostPending},
		state.Transfer{ID: id128(401), DebitAccountID: id128(2), CreditAccountID: id128(1), Amount: id128(1), Ledger: 1, Code: 1},
	), nil)
	if want := []state.Failure{{Index: 0, Reason: state.PendingTransferExpired}}; err != nil || !slices.Equal(fails, want) {
		t.Fatalf("post after the sweep: %v %v", fails, err)
	}
	if got := m.LookupTransfers([]u128.Uint128{id128(401)}, nil); len(got) != 1 || got[0].Timestamp != 200e9+2 {
		t.Fatalf("transfer after the sweep %+v, want timestamp %d", got, uint64(200e9+2))
	}
	if err := m.CheckInvariants(); err != nil {
		t.Fatal(err)
	}
}

func TestTimestampLimit(t *testing.T) {
	t.Parallel()
	m := newLedger(t)
	fails, err := m.Apply(state.CreateTransfers, 1<<63-3, encodeTransfers(
		state.Transfer{ID: id128(300), DebitAccountID: id128(2), CreditAccountID: id128(1), Amount: id128(5),
			Ledger: 1, Code: 1, Flags: state.Pending, Timeout: math.MaxUint32},
		state.Transfer{},
	), nil)
	if err != nil || len(fails) != 1 {
		t.Fatalf("batch up to the second to last timestamp: %v %v", fails, err)
	}
	mustApply(t, m, 5)
	for _, n := range []int{0, 1} {
		if _, err = m.Apply(state.CreateTransfers, 5, make([]byte, n*state.TransferSize), nil); !errors.Is(err, state.ErrMalformedBatch) {
			t.Fatalf("batch of %d after the last timestamp: err = %v", n, err)
		}
	}
	if got := account(t, m, 2).DebitsPending; got != id128(5) {
		t.Fatalf("pending with the longest timeout expired early: debits pending %v", got)
	}
	if err := m.CheckInvariants(); err != nil {
		t.Fatal(err)
	}
}

func TestChainAcrossBlockBoundary(t *testing.T) {
	t.Parallel()
	const before = 1<<14 - 4
	m, twin := newLedger(t), newLedger(t)
	filler := make([]state.Transfer, before-8)
	for i := range filler {
		filler[i] = state.Transfer{ID: id128(uint64(10_000 + i)), DebitAccountID: id128(1), CreditAccountID: id128(2), Amount: id128(1),
			Ledger: 1, Code: 1}
	}
	filler[0].Flags, filler[0].Timeout = state.Pending, 60
	for _, x := range []*state.Machine{m, twin} {
		mustApply(t, x, 1e9, filler...)
		if x.Transfers() != before {
			t.Fatalf("%d transfers stored, want %d", x.Transfers(), before)
		}
	}

	chain := []state.Transfer{{ID: id128(500), PendingID: id128(10_000), Flags: state.PostPending | state.Linked}}
	for i := range uint64(7) {
		chain = append(chain, state.Transfer{ID: id128(501 + i), DebitAccountID: id128(2), CreditAccountID: id128(1), Amount: id128(1),
			Ledger: 1, Code: 1, Flags: state.Linked})
	}
	chain = append(chain,
		state.Transfer{ID: id128(508), DebitAccountID: id128(1), CreditAccountID: id128(2), Amount: id128(3), Ledger: 1, Code: 1,
			Flags: state.Pending | state.Linked, Timeout: 1},
		state.Transfer{ID: id128(509), DebitAccountID: id128(1), CreditAccountID: id128(2), Ledger: 1, Code: 1},
	)
	fails, err := m.Apply(state.CreateTransfers, 2e9, encodeTransfers(chain...), nil)
	if err != nil || len(fails) != len(chain) || fails[len(chain)-1].Reason != state.AmountMustNotBeZero {
		t.Fatalf("chain over the boundary: %v %v", fails, err)
	}
	if _, err := twin.Apply(state.CreateTransfers, 2e9, make([]byte, len(chain)*state.TransferSize), nil); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(records(t, m), records(t, twin)) {
		t.Fatal("rolled back chain over the block boundary left traces")
	}

	chain[len(chain)-1].Amount = id128(1)
	mustApply(t, m, 3e9, chain...)
	if m.Transfers() != before+len(chain) {
		t.Fatalf("%d transfers stored, want %d", m.Transfers(), before+len(chain))
	}
	c, err := state.ReadSnapshot(bytes.NewReader(snapshot(t, m)))
	if err != nil {
		t.Fatal(err)
	}
	for _, x := range []*state.Machine{m, c} {
		if err := x.CheckInvariants(); err != nil {
			t.Fatal(err)
		}
		mustApply(t, x, 3e9+100, state.Transfer{ID: id128(600), PendingID: id128(508), Flags: state.VoidPending})
	}
	if c.Digest() != m.Digest() {
		t.Fatal("restored machine diverged")
	}
}

func TestApplyDoesNotAllocate(t *testing.T) {
	if bi, ok := debug.ReadBuildInfo(); ok && slices.Contains(bi.Settings, debug.BuildSetting{Key: "-race", Value: "true"}) {
		t.Skip("the race detector allocates on its own")
	}
	m := state.New(8, 8192)
	if fails, err := m.Apply(state.CreateAccounts, 1, encodeAccounts(
		state.Account{ID: id128(1), Ledger: 1, Code: 1},
		state.Account{ID: id128(2), Ledger: 1, Code: 1},
	), nil); err != nil || len(fails) > 0 {
		t.Fatal(fails, err)
	}

	id, ts := uint64(1000), uint64(1e9)
	var fails []state.Failure
	measure := func(trs ...state.Transfer) float64 {
		body := encodeTransfers(trs...)
		apply := func() {
			for i := range trs {
				id++
				id128(id).PutBytes(body[i*state.TransferSize:])
				if trs[i].Flags&state.PostPending != 0 {
					id128(id - 1).PutBytes(body[i*state.TransferSize+64:])
				}
			}
			var err error
			if fails, err = m.Apply(state.CreateTransfers, ts, body, fails[:0]); err != nil {
				t.Fatal(err)
			}
			ts += 1e6
		}
		return testing.AllocsPerRun(1000, apply)
	}
	transfer := state.Transfer{DebitAccountID: id128(1), CreditAccountID: id128(2), Amount: id128(1), Ledger: 1, Code: 1}
	pending := transfer
	pending.Flags, pending.Timeout = state.Pending, 60

	if allocs := measure(transfer); allocs != 0 || len(fails) > 0 {
		t.Errorf("transfer: %v allocs, failures %v", allocs, fails)
	}
	if allocs := measure(pending, state.Transfer{Flags: state.PostPending}); allocs != 0 || len(fails) > 0 {
		t.Errorf("pending and post: %v allocs, failures %v", allocs, fails)
	}
	broken := transfer
	broken.Flags = state.Linked
	if allocs := measure(broken, state.Transfer{Amount: id128(1)}); allocs != 0 || len(fails) != 2 {
		t.Errorf("failed chain: %v allocs, failures %v", allocs, fails)
	}
}
