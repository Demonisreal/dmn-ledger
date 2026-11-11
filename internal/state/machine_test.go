package state_test

import (
	"errors"
	"slices"
	"testing"

	"github.com/Demonisreal/dmn-ledger/internal/state"
	"github.com/Demonisreal/dmn-ledger/internal/u128"
)

var n = u128.From64

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
	fails, err := m.Apply(state.CreateTransfers, ts, encodeTransfers(trs...))
	if err != nil || fails != nil {
		t.Fatalf("Apply at %d: %v %v", ts, fails, err)
	}
}

func account(t *testing.T, m *state.Machine, id uint64) state.Account {
	t.Helper()
	found := m.LookupAccounts([]u128.Uint128{n(id)})
	if len(found) != 1 {
		t.Fatalf("account %d not found", id)
	}
	return found[0]
}

func newLedger(t *testing.T) *state.Machine {
	t.Helper()
	m := state.New()
	fails, err := m.Apply(state.CreateAccounts, 1, encodeAccounts(
		state.Account{ID: n(1), Ledger: 1, Code: 1},
		state.Account{ID: n(2), Ledger: 1, Code: 1},
		state.Account{ID: n(3), Ledger: 2, Code: 1},
		state.Account{ID: n(4), Ledger: 1, Code: 1, Flags: state.DebitsMustNotExceedCredits},
		state.Account{ID: n(5), Ledger: 1, Code: 1, Flags: state.CreditsMustNotExceedDebits},
		state.Account{ID: n(6), Ledger: 1, Code: 1},
		state.Account{ID: n(7), Ledger: 1, Code: 1},
	))
	if err != nil || fails != nil {
		t.Fatalf("accounts: %v %v", fails, err)
	}
	mustApply(t, m, 10,
		state.Transfer{ID: n(100), DebitAccountID: n(1), CreditAccountID: n(2), Amount: n(10), Ledger: 1, Code: 1},
		state.Transfer{ID: n(101), DebitAccountID: n(6), CreditAccountID: n(7), Amount: u128.Max.Sub(n(1)), Ledger: 1, Code: 1},
		state.Transfer{ID: n(200), DebitAccountID: n(1), CreditAccountID: n(2), Amount: n(10), Ledger: 1, Code: 1, Flags: state.Pending},
		state.Transfer{ID: n(201), DebitAccountID: n(1), CreditAccountID: n(2), Amount: n(10), Ledger: 1, Code: 1, Flags: state.Pending},
		state.Transfer{ID: n(202), PendingID: n(201), Flags: state.PostPending},
		state.Transfer{ID: n(203), DebitAccountID: n(1), CreditAccountID: n(2), Amount: n(10), Ledger: 1, Code: 1, Flags: state.Pending},
		state.Transfer{ID: n(204), PendingID: n(203), Flags: state.VoidPending},
	)
	return m
}

func TestCreateAccounts(t *testing.T) {
	t.Parallel()
	valid := state.Account{ID: n(50), Ledger: 1, Code: 1}
	tests := []struct {
		name string
		edit func(*state.Account)
		want state.Code
	}{
		{"ok", func(*state.Account) {}, state.OK},
		{"id zero", func(a *state.Account) { a.ID = u128.Uint128{} }, state.IDMustNotBeZero},
		{"id max", func(a *state.Account) { a.ID = u128.Max }, state.IDMustNotBeMax},
		{"unknown flag", func(a *state.Account) { a.Flags = 1 << 2 }, state.FlagsUnknown},
		{"both limits", func(a *state.Account) {
			a.Flags = state.DebitsMustNotExceedCredits | state.CreditsMustNotExceedDebits
		}, state.FlagsAreMutuallyExclusive},
		{"ledger zero", func(a *state.Account) { a.Ledger = 0 }, state.LedgerMustNotBeZero},
		{"code zero", func(a *state.Account) { a.Code = 0 }, state.CodeMustNotBeZero},
		{"balance set", func(a *state.Account) { a.CreditsPending = n(1) }, state.BalancesMustBeZero},
		{"timestamp set", func(a *state.Account) { a.Timestamp = 5 }, state.TimestampMustBeZero},
		{"exists", func(a *state.Account) { a.ID = n(1) }, state.Exists},
		{"exists with other code", func(a *state.Account) { a.ID, a.Code = n(1), 2 }, state.ExistsWithDifferentFields},
		{"ledger zero beats exists", func(a *state.Account) { a.ID, a.Ledger = n(1), 0 }, state.LedgerMustNotBeZero},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			m := newLedger(t)
			a := valid
			tt.edit(&a)
			fails, err := m.Apply(state.CreateAccounts, 20, encodeAccounts(a))
			if err != nil {
				t.Fatal(err)
			}
			var got state.Code
			if len(fails) > 0 {
				got = fails[0].Code
			}
			if got != tt.want {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCreateTransfers(t *testing.T) {
	t.Parallel()
	valid := state.Transfer{ID: n(1000), DebitAccountID: n(1), CreditAccountID: n(2), Amount: n(5), Ledger: 1, Code: 1}
	post := func(pending uint64) state.Transfer {
		return state.Transfer{ID: n(1000), PendingID: n(pending), Flags: state.PostPending}
	}
	tests := []struct {
		name string
		tr   state.Transfer
		edit func(*state.Transfer)
		want state.Code
	}{
		{"ok", valid, nil, state.OK},
		{"id zero", valid, func(tr *state.Transfer) { tr.ID = u128.Uint128{} }, state.IDMustNotBeZero},
		{"id max", valid, func(tr *state.Transfer) { tr.ID = u128.Max }, state.IDMustNotBeMax},
		{"unknown flag", valid, func(tr *state.Transfer) { tr.Flags = 1 << 4 }, state.FlagsUnknown},
		{"pending and post", valid, func(tr *state.Transfer) {
			tr.Flags = state.Pending | state.PostPending
		}, state.FlagsAreMutuallyExclusive},
		{"debit zero", valid, func(tr *state.Transfer) { tr.DebitAccountID = u128.Uint128{} }, state.DebitAccountIDMustNotBeZero},
		{"credit zero", valid, func(tr *state.Transfer) { tr.CreditAccountID = u128.Uint128{} }, state.CreditAccountIDMustNotBeZero},
		{"same accounts", valid, func(tr *state.Transfer) { tr.CreditAccountID = n(1) }, state.AccountsMustBeDifferent},
		{"pending id on normal", valid, func(tr *state.Transfer) { tr.PendingID = n(200) }, state.PendingIDMustBeZero},
		{"timeout on normal", valid, func(tr *state.Transfer) { tr.Timeout = 1 }, state.TimeoutRequiresPending},
		{"ledger zero", valid, func(tr *state.Transfer) { tr.Ledger = 0 }, state.LedgerMustNotBeZero},
		{"code zero", valid, func(tr *state.Transfer) { tr.Code = 0 }, state.CodeMustNotBeZero},
		{"amount zero", valid, func(tr *state.Transfer) { tr.Amount = u128.Uint128{} }, state.AmountMustNotBeZero},
		{"timestamp set", valid, func(tr *state.Transfer) { tr.Timestamp = 1 }, state.TimestampMustBeZero},
		{"debit missing", valid, func(tr *state.Transfer) { tr.DebitAccountID = n(99) }, state.DebitAccountNotFound},
		{"credit missing", valid, func(tr *state.Transfer) { tr.CreditAccountID = n(99) }, state.CreditAccountNotFound},
		{"accounts on two ledgers", valid, func(tr *state.Transfer) { tr.CreditAccountID = n(3) }, state.AccountsMustHaveSameLedger},
		{"transfer on other ledger", valid, func(tr *state.Transfer) { tr.Ledger = 2 }, state.TransferMustHaveSameLedgerAsAccounts},
		{"exists", valid, func(tr *state.Transfer) { tr.ID, tr.Amount = n(100), n(10) }, state.Exists},
		{"exists with other amount", valid, func(tr *state.Transfer) { tr.ID = n(100) }, state.ExistsWithDifferentFields},
		{"overflows debits", valid, func(tr *state.Transfer) { tr.DebitAccountID, tr.Amount = n(6), n(2) }, state.OverflowsDebits},
		{"overflows credits", valid, func(tr *state.Transfer) { tr.CreditAccountID, tr.Amount = n(7), n(2) }, state.OverflowsCredits},
		{"exceeds credits", valid, func(tr *state.Transfer) { tr.DebitAccountID = n(4) }, state.ExceedsCredits},
		{"exceeds debits", valid, func(tr *state.Transfer) { tr.CreditAccountID = n(5) }, state.ExceedsDebits},
		{"pending exceeds credits", valid, func(tr *state.Transfer) {
			tr.DebitAccountID, tr.Flags = n(4), state.Pending
		}, state.ExceedsCredits},

		{"post full", post(200), nil, state.OK},
		{"post partial", post(200), func(tr *state.Transfer) { tr.Amount = n(3) }, state.OK},
		{"void", post(200), func(tr *state.Transfer) { tr.Flags = state.VoidPending }, state.OK},
		{"void exact amount", post(200), func(tr *state.Transfer) {
			tr.Flags, tr.Amount = state.VoidPending, n(10)
		}, state.OK},
		{"post with matching fields", post(200), func(tr *state.Transfer) {
			tr.DebitAccountID, tr.CreditAccountID, tr.Ledger, tr.Code = n(1), n(2), 1, 1
		}, state.OK},
		{"pending id zero", post(0), nil, state.PendingIDMustNotBeZero},
		{"post with timeout", post(200), func(tr *state.Transfer) { tr.Timeout = 1 }, state.TimeoutRequiresPending},
		{"post with timestamp", post(200), func(tr *state.Transfer) { tr.Timestamp = 1 }, state.TimestampMustBeZero},
		{"post exists", post(201), func(tr *state.Transfer) { tr.ID = n(202) }, state.Exists},
		{"post exists resolved", post(201), func(tr *state.Transfer) {
			tr.ID, tr.Amount, tr.Ledger = n(202), n(10), 1
		}, state.Exists},
		{"post exists other pending", post(200), func(tr *state.Transfer) { tr.ID = n(202) }, state.ExistsWithDifferentFields},
		{"post exists other amount", post(201), func(tr *state.Transfer) {
			tr.ID, tr.Amount = n(202), n(4)
		}, state.ExistsWithDifferentFields},
		{"pending missing", post(999), nil, state.PendingTransferNotFound},
		{"pending is posted transfer", post(100), nil, state.PendingTransferNotPending},
		{"other debit", post(200), func(tr *state.Transfer) { tr.DebitAccountID = n(6) }, state.PendingTransferHasDifferentDebitAccount},
		{"other credit", post(200), func(tr *state.Transfer) { tr.CreditAccountID = n(6) }, state.PendingTransferHasDifferentCreditAccount},
		{"other ledger", post(200), func(tr *state.Transfer) { tr.Ledger = 2 }, state.PendingTransferHasDifferentLedger},
		{"other code", post(200), func(tr *state.Transfer) { tr.Code = 2 }, state.PendingTransferHasDifferentCode},
		{"already posted", post(201), nil, state.PendingTransferAlreadyPosted},
		{"already voided", post(203), nil, state.PendingTransferAlreadyVoided},
		{"post too much", post(200), func(tr *state.Transfer) { tr.Amount = n(11) }, state.ExceedsPendingTransferAmount},
		{"void partial", post(200), func(tr *state.Transfer) {
			tr.Flags, tr.Amount = state.VoidPending, n(9)
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
			fails, err := m.Apply(state.CreateTransfers, 20, encodeTransfers(tr))
			if err != nil {
				t.Fatal(err)
			}
			var got state.Code
			if len(fails) > 0 {
				got = fails[0].Code
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
	mustApply(t, m, 20, state.Transfer{ID: n(1000), PendingID: n(200), Amount: n(4), Flags: state.PostPending})

	dr := account(t, m, 1)
	if want := before.DebitsPending.Sub(n(10)); dr.DebitsPending != want {
		t.Errorf("debits pending %v, want %v", dr.DebitsPending, want)
	}
	if want := before.DebitsPosted.Add(n(4)); dr.DebitsPosted != want {
		t.Errorf("debits posted %v, want %v", dr.DebitsPosted, want)
	}
	stored := m.LookupTransfers([]u128.Uint128{n(1000)})
	want := state.Transfer{
		ID: n(1000), DebitAccountID: n(1), CreditAccountID: n(2), Amount: n(4), PendingID: n(200),
		Ledger: 1, Code: 1, Flags: state.PostPending, Timestamp: 20,
	}
	if len(stored) != 1 || stored[0] != want {
		t.Errorf("stored %+v, want %+v", stored, want)
	}
}

func TestLinked(t *testing.T) {
	t.Parallel()
	tr := func(id, dr, cr, amount uint64, flags state.TransferFlags) state.Transfer {
		return state.Transfer{ID: n(id), DebitAccountID: n(dr), CreditAccountID: n(cr), Amount: n(amount),
			Ledger: 1, Code: 1, Flags: flags}
	}
	tests := []struct {
		name string
		trs  []state.Transfer
		want []state.Result
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
		}, []state.Result{{Index: 1, Code: state.LinkedEventFailed}, {Index: 2, Code: state.ExceedsCredits}, {Index: 3, Code: state.LinkedEventFailed}}},
		{"chain sees its own effects", []state.Transfer{
			tr(1000, 2, 4, 3, state.Linked),
			tr(1001, 4, 2, 3, 0),
		}, nil},
		{"duplicate inside chain", []state.Transfer{
			tr(1000, 1, 2, 1, state.Linked),
			tr(1000, 1, 2, 1, state.Linked),
			tr(1001, 1, 2, 1, 0),
		}, []state.Result{
			{Index: 0, Code: state.LinkedEventFailed},
			{Index: 1, Code: state.Exists},
			{Index: 2, Code: state.LinkedEventFailed},
		}},
		{"open chain", []state.Transfer{
			tr(1000, 1, 2, 1, 0),
			tr(1001, 1, 2, 1, state.Linked),
			tr(1002, 99, 2, 1, state.Linked),
		}, []state.Result{{Index: 1, Code: state.LinkedEventChainOpen}, {Index: 2, Code: state.LinkedEventChainOpen}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			m := newLedger(t)
			before := m.Digest()
			fails, err := m.Apply(state.CreateTransfers, 20, encodeTransfers(tt.trs...))
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
				failed := slices.ContainsFunc(fails, func(f state.Result) bool { return f.Index == i })
				if found := len(m.LookupTransfers([]u128.Uint128{tr.ID})) == 1; found == failed {
					t.Errorf("transfer %v: found %v after failed=%v", tr.ID, found, failed)
				}
			}
			if len(tt.want) == len(tt.trs) && m.Digest() != before {
				t.Error("state changed although every event failed")
			}
		})
	}
}

func TestRollbackRestoresState(t *testing.T) {
	t.Parallel()
	m, ref := newLedger(t), newLedger(t)
	fails, err := m.Apply(state.CreateTransfers, 20, encodeTransfers(
		state.Transfer{ID: n(1000), PendingID: n(200), Amount: n(4), Flags: state.PostPending | state.Linked},
		state.Transfer{ID: n(1001), DebitAccountID: n(2), CreditAccountID: n(1), Amount: n(7), Ledger: 1, Code: 1,
			Flags: state.Pending | state.Linked, Timeout: 1},
		state.Transfer{ID: n(1002), PendingID: n(1001), Flags: state.VoidPending | state.Linked},
		state.Transfer{ID: n(1003), DebitAccountID: n(1), CreditAccountID: n(2), Amount: n(1), Ledger: 3, Code: 1},
	))
	if err != nil || len(fails) != 4 || fails[3].Code != state.TransferMustHaveSameLedgerAsAccounts {
		t.Fatalf("fails %v, err %v", fails, err)
	}
	if m.Digest() != ref.Digest() {
		t.Fatal("rolled back chain left traces")
	}
	mustApply(t, m, 3e9, state.Transfer{ID: n(1004), PendingID: n(200), Flags: state.PostPending})
}

func TestTimestamps(t *testing.T) {
	t.Parallel()
	m := state.New()
	steps := []struct {
		batch uint64
		count int
		first uint64
	}{
		{100, 3, 100},
		{50, 2, 103},
		{105, 1, 105},
		{1000, 0, 0},
		{900, 1, 900},
		{2000, 2, 2000},
	}
	var id uint64
	for _, s := range steps {
		var as []state.Account
		for range s.count {
			id++
			as = append(as, state.Account{ID: n(id), Ledger: 1, Code: 1})
		}
		if _, err := m.Apply(state.CreateAccounts, s.batch, encodeAccounts(as...)); err != nil {
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
		return state.Transfer{ID: n(id), DebitAccountID: n(2), CreditAccountID: n(1), Amount: n(5),
			Ledger: 1, Code: 1, Flags: state.Pending, Timeout: timeout}
	}
	mustApply(t, m, 1e9, pending(300, 1), pending(301, 1), pending(302, 0))

	fails, err := m.Apply(state.CreateTransfers, 2e9-1, encodeTransfers(
		state.Transfer{},
		state.Transfer{ID: n(400), PendingID: n(300), Flags: state.PostPending},
	))
	if err != nil {
		t.Fatal(err)
	}
	want := []state.Result{
		{Index: 0, Code: state.IDMustNotBeZero},
		{Index: 1, Code: state.PendingTransferExpired},
	}
	if !slices.Equal(fails, want) {
		t.Fatalf("post at the deadline: got %v, want %v", fails, want)
	}
	if got := account(t, m, 2).DebitsPending; got != n(15) {
		t.Fatalf("debits pending before sweep %v, want 15", got)
	}

	mustApply(t, m, 2e9+1)
	if got := account(t, m, 2).DebitsPending; got != n(5) {
		t.Fatalf("debits pending after sweep %v, want 5", got)
	}
	fails, err = m.Apply(state.CreateTransfers, 2e9+2, encodeTransfers(
		state.Transfer{ID: n(401), PendingID: n(301), Flags: state.VoidPending},
		state.Transfer{ID: n(402), PendingID: n(302), Flags: state.VoidPending},
	))
	if err != nil {
		t.Fatal(err)
	}
	if want := []state.Result{{Index: 0, Code: state.PendingTransferExpired}}; !slices.Equal(fails, want) {
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
		state.Transfer{ID: n(300), DebitAccountID: n(2), CreditAccountID: n(1), Amount: n(5),
			Ledger: 1, Code: 1, Flags: state.Pending | state.Linked, Timeout: 1},
		state.Transfer{},
	))
	if err != nil || len(fails) != 2 {
		t.Fatalf("chain should fail: %v %v", fails, err)
	}

	filler := make([]state.Transfer, 10)
	recreated := state.Transfer{ID: n(300), DebitAccountID: n(2), CreditAccountID: n(1), Amount: n(5),
		Ledger: 1, Code: 1, Flags: state.Pending}
	if _, err := m.Apply(state.CreateTransfers, at-10, encodeTransfers(append(filler, recreated)...)); err != nil {
		t.Fatal(err)
	}
	if got := m.LookupTransfers([]u128.Uint128{n(300)}); len(got) != 1 || got[0].Timestamp != at {
		t.Fatalf("recreated transfer %+v, want timestamp %d", got, uint64(at))
	}
	mustApply(t, m, at+5, state.Transfer{ID: n(400), PendingID: n(300), Flags: state.PostPending})
}

func TestApplyMalformed(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		op   state.Op
		body []byte
	}{
		{"unknown op", 0, nil},
		{"short account", state.CreateAccounts, make([]byte, state.AccountSize-1)},
		{"transfer as account", state.CreateAccounts, make([]byte, state.TransferSize)},
		{"short transfer", state.CreateTransfers, make([]byte, state.TransferSize+1)},
	}
	for _, tt := range tests {
		m := newLedger(t)
		before := m.Digest()
		if _, err := m.Apply(tt.op, 1e12, tt.body); !errors.Is(err, state.ErrMalformedBatch) {
			t.Errorf("%s: err = %v", tt.name, err)
		}
		if m.Digest() != before {
			t.Errorf("%s: state changed", tt.name)
		}
	}
}

func TestEncodingRoundTrip(t *testing.T) {
	t.Parallel()
	a := state.Account{ID: u128.Max, DebitsPending: n(1), DebitsPosted: n(2), CreditsPending: n(3),
		CreditsPosted: u128.Uint128{Hi: 4}, Ledger: 5, Code: 6, Flags: 7, Timestamp: 8}
	if b := state.AppendAccount(nil, a); len(b) != state.AccountSize || state.DecodeAccount(b) != a {
		t.Errorf("account round trip: %d bytes, %+v", len(b), state.DecodeAccount(b))
	}
	tr := state.Transfer{ID: n(1), DebitAccountID: n(2), CreditAccountID: n(3), Amount: u128.Max,
		PendingID: u128.Uint128{Hi: 5}, Timeout: 6, Ledger: 7, Code: 8, Flags: 9, Timestamp: 10}
	if b := state.AppendTransfer(nil, tr); len(b) != state.TransferSize || state.DecodeTransfer(b) != tr {
		t.Errorf("transfer round trip: %d bytes, %+v", len(b), state.DecodeTransfer(b))
	}
}
