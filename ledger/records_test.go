package ledger

import (
	"errors"
	"strconv"
	"strings"
	"testing"

	"github.com/Demonisreal/dmn-ledger/internal/raft"
	"github.com/Demonisreal/dmn-ledger/internal/state"
	"github.com/Demonisreal/dmn-ledger/internal/u128"
)

func TestRecordsMatchTheState(t *testing.T) {
	n := func(k uint64) Uint128 { return Uint128{Hi: k, Lo: k + 100} }
	a := Account{ID: n(1), DebitsPending: n(2), DebitsPosted: n(3), CreditsPending: n(4), CreditsPosted: n(5),
		Ledger: 6, Code: 7, Flags: CreditsMustNotExceedDebits, Timestamp: 8}
	sa := state.DecodeAccount(appendAccount(nil, a))
	wantA := state.Account{ID: u128.Uint128(n(1)), DebitsPending: u128.Uint128(n(2)), DebitsPosted: u128.Uint128(n(3)),
		CreditsPending: u128.Uint128(n(4)), CreditsPosted: u128.Uint128(n(5)),
		Ledger: 6, Code: 7, Flags: state.CreditsMustNotExceedDebits, Timestamp: 8}
	if sa != wantA {
		t.Fatalf("account on the wire %+v, want %+v", sa, wantA)
	}
	if back := decodeAccounts(state.AppendAccount(nil, wantA)); len(back) != 1 || back[0] != a {
		t.Fatalf("account from the wire %+v, want %+v", back, a)
	}

	tr := Transfer{ID: n(1), DebitAccountID: n(2), CreditAccountID: n(3), Amount: n(4), PendingID: n(5),
		Timeout: 6, Ledger: 7, Code: 8, Flags: Pending | Linked, Timestamp: 9}
	st := state.DecodeTransfer(appendTransfer(nil, tr))
	wantT := state.Transfer{ID: u128.Uint128(n(1)), DebitAccountID: u128.Uint128(n(2)), CreditAccountID: u128.Uint128(n(3)),
		Amount: u128.Uint128(n(4)), PendingID: u128.Uint128(n(5)),
		Timeout: 6, Ledger: 7, Code: 8, Flags: state.Pending | state.Linked, Timestamp: 9}
	if st != wantT {
		t.Fatalf("transfer on the wire %+v, want %+v", st, wantT)
	}
	if back := decodeTransfers(state.AppendTransfer(nil, wantT)); len(back) != 1 || back[0] != tr {
		t.Fatalf("transfer from the wire %+v, want %+v", back, tr)
	}
}

func TestConstantsMatchTheState(t *testing.T) {
	for _, pair := range [][2]Reason{
		{LinkedEventFailed, Reason(state.LinkedEventFailed)},
		{LinkedEventChainOpen, Reason(state.LinkedEventChainOpen)},
		{IDMustNotBeZero, Reason(state.IDMustNotBeZero)},
		{IDMustNotBeMax, Reason(state.IDMustNotBeMax)},
		{FlagsUnknown, Reason(state.FlagsUnknown)},
		{FlagsAreMutuallyExclusive, Reason(state.FlagsAreMutuallyExclusive)},
		{LedgerMustNotBeZero, Reason(state.LedgerMustNotBeZero)},
		{CodeMustNotBeZero, Reason(state.CodeMustNotBeZero)},
		{BalancesMustBeZero, Reason(state.BalancesMustBeZero)},
		{TimestampMustBeZero, Reason(state.TimestampMustBeZero)},
		{ExistsWithDifferentFields, Reason(state.ExistsWithDifferentFields)},
		{Exists, Reason(state.Exists)},
		{DebitAccountIDMustNotBeZero, Reason(state.DebitAccountIDMustNotBeZero)},
		{CreditAccountIDMustNotBeZero, Reason(state.CreditAccountIDMustNotBeZero)},
		{AccountsMustBeDifferent, Reason(state.AccountsMustBeDifferent)},
		{PendingIDMustBeZero, Reason(state.PendingIDMustBeZero)},
		{PendingIDMustNotBeZero, Reason(state.PendingIDMustNotBeZero)},
		{TimeoutRequiresPending, Reason(state.TimeoutRequiresPending)},
		{AmountMustNotBeZero, Reason(state.AmountMustNotBeZero)},
		{DebitAccountNotFound, Reason(state.DebitAccountNotFound)},
		{CreditAccountNotFound, Reason(state.CreditAccountNotFound)},
		{AccountsMustHaveSameLedger, Reason(state.AccountsMustHaveSameLedger)},
		{TransferMustHaveSameLedgerAsAccounts, Reason(state.TransferMustHaveSameLedgerAsAccounts)},
		{PendingTransferNotFound, Reason(state.PendingTransferNotFound)},
		{PendingTransferNotPending, Reason(state.PendingTransferNotPending)},
		{PendingTransferHasDifferentDebitAccount, Reason(state.PendingTransferHasDifferentDebitAccount)},
		{PendingTransferHasDifferentCreditAccount, Reason(state.PendingTransferHasDifferentCreditAccount)},
		{PendingTransferHasDifferentLedger, Reason(state.PendingTransferHasDifferentLedger)},
		{PendingTransferHasDifferentCode, Reason(state.PendingTransferHasDifferentCode)},
		{PendingTransferAlreadyPosted, Reason(state.PendingTransferAlreadyPosted)},
		{PendingTransferAlreadyVoided, Reason(state.PendingTransferAlreadyVoided)},
		{PendingTransferExpired, Reason(state.PendingTransferExpired)},
		{ExceedsPendingTransferAmount, Reason(state.ExceedsPendingTransferAmount)},
		{PendingTransferHasDifferentAmount, Reason(state.PendingTransferHasDifferentAmount)},
		{OverflowsDebits, Reason(state.OverflowsDebits)},
		{OverflowsCredits, Reason(state.OverflowsCredits)},
		{ExceedsCredits, Reason(state.ExceedsCredits)},
		{ExceedsDebits, Reason(state.ExceedsDebits)},
	} {
		if pair[0] != pair[1] || pair[0].String() != state.Reason(pair[1]).String() {
			t.Fatalf("reason %d %q, want %d %q", pair[0], pair[0], pair[1], state.Reason(pair[1]))
		}
	}
	if DebitsMustNotExceedCredits != AccountFlags(state.DebitsMustNotExceedCredits) ||
		CreditsMustNotExceedDebits != AccountFlags(state.CreditsMustNotExceedDebits) {
		t.Fatal("account flags differ from the state")
	}
	if Linked != TransferFlags(state.Linked) || Pending != TransferFlags(state.Pending) ||
		PostPending != TransferFlags(state.PostPending) || VoidPending != TransferFlags(state.VoidPending) {
		t.Fatal("transfer flags differ from the state")
	}
	for _, r := range []Role{Follower, PreCandidate, Candidate, Leader} {
		if r.String() != raft.Role(r).String() {
			t.Fatalf("role %d is %q", r, r)
		}
	}
}

func TestParse(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want Uint128
		err  error
	}{
		{"0", Uint128{}, nil},
		{"500", From64(500), nil},
		{"0xff", From64(255), nil},
		{"340282366920938463463374607431768211455", Uint128{Hi: ^uint64(0), Lo: ^uint64(0)}, nil},
		{"340282366920938463463374607431768211456", Uint128{}, strconv.ErrRange},
		{"", Uint128{}, strconv.ErrSyntax},
		{"-1", Uint128{}, strconv.ErrSyntax},
		{"0x", Uint128{}, strconv.ErrSyntax},
	} {
		got, err := Parse(tc.in)
		if got != tc.want || !errors.Is(err, tc.err) || (tc.err == nil) != (err == nil) {
			t.Fatalf("Parse(%q) = %v, %v, want %v, %v", tc.in, got, err, tc.want, tc.err)
		}
		if err == nil && !strings.HasPrefix(tc.in, "0x") && got.String() != tc.in {
			t.Fatalf("%q prints as %q", tc.in, got)
		}
	}
	if a, b := NewID(), NewID(); a == b {
		t.Fatalf("two random IDs are both %v", a)
	}
}
