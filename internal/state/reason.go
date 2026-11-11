package state

import "strconv"

type Reason uint32

const (
	OK                                       Reason = 0
	LinkedEventFailed                        Reason = 1
	LinkedEventChainOpen                     Reason = 2
	IDMustNotBeZero                          Reason = 3
	IDMustNotBeMax                           Reason = 4
	FlagsUnknown                             Reason = 5
	FlagsAreMutuallyExclusive                Reason = 6
	LedgerMustNotBeZero                      Reason = 7
	CodeMustNotBeZero                        Reason = 8
	BalancesMustBeZero                       Reason = 9
	TimestampMustBeZero                      Reason = 10
	ExistsWithDifferentFields                Reason = 11
	Exists                                   Reason = 12
	DebitAccountIDMustNotBeZero              Reason = 13
	CreditAccountIDMustNotBeZero             Reason = 14
	AccountsMustBeDifferent                  Reason = 15
	PendingIDMustBeZero                      Reason = 16
	PendingIDMustNotBeZero                   Reason = 17
	TimeoutRequiresPending                   Reason = 18
	AmountMustNotBeZero                      Reason = 19
	DebitAccountNotFound                     Reason = 20
	CreditAccountNotFound                    Reason = 21
	AccountsMustHaveSameLedger               Reason = 22
	TransferMustHaveSameLedgerAsAccounts     Reason = 23
	PendingTransferNotFound                  Reason = 24
	PendingTransferNotPending                Reason = 25
	PendingTransferHasDifferentDebitAccount  Reason = 26
	PendingTransferHasDifferentCreditAccount Reason = 27
	PendingTransferHasDifferentLedger        Reason = 28
	PendingTransferHasDifferentCode          Reason = 29
	PendingTransferAlreadyPosted             Reason = 30
	PendingTransferAlreadyVoided             Reason = 31
	PendingTransferExpired                   Reason = 32
	ExceedsPendingTransferAmount             Reason = 33
	PendingTransferHasDifferentAmount        Reason = 34
	OverflowsDebits                          Reason = 35
	OverflowsCredits                         Reason = 36
	ExceedsCredits                           Reason = 37
	ExceedsDebits                            Reason = 38
)

var reasonNames = [...]string{
	OK:                                       "ok",
	LinkedEventFailed:                        "linked_event_failed",
	LinkedEventChainOpen:                     "linked_event_chain_open",
	IDMustNotBeZero:                          "id_must_not_be_zero",
	IDMustNotBeMax:                           "id_must_not_be_max",
	FlagsUnknown:                             "flags_unknown",
	FlagsAreMutuallyExclusive:                "flags_are_mutually_exclusive",
	LedgerMustNotBeZero:                      "ledger_must_not_be_zero",
	CodeMustNotBeZero:                        "code_must_not_be_zero",
	BalancesMustBeZero:                       "balances_must_be_zero",
	TimestampMustBeZero:                      "timestamp_must_be_zero",
	ExistsWithDifferentFields:                "exists_with_different_fields",
	Exists:                                   "exists",
	DebitAccountIDMustNotBeZero:              "debit_account_id_must_not_be_zero",
	CreditAccountIDMustNotBeZero:             "credit_account_id_must_not_be_zero",
	AccountsMustBeDifferent:                  "accounts_must_be_different",
	PendingIDMustBeZero:                      "pending_id_must_be_zero",
	PendingIDMustNotBeZero:                   "pending_id_must_not_be_zero",
	TimeoutRequiresPending:                   "timeout_requires_pending",
	AmountMustNotBeZero:                      "amount_must_not_be_zero",
	DebitAccountNotFound:                     "debit_account_not_found",
	CreditAccountNotFound:                    "credit_account_not_found",
	AccountsMustHaveSameLedger:               "accounts_must_have_same_ledger",
	TransferMustHaveSameLedgerAsAccounts:     "transfer_must_have_same_ledger_as_accounts",
	PendingTransferNotFound:                  "pending_transfer_not_found",
	PendingTransferNotPending:                "pending_transfer_not_pending",
	PendingTransferHasDifferentDebitAccount:  "pending_transfer_has_different_debit_account",
	PendingTransferHasDifferentCreditAccount: "pending_transfer_has_different_credit_account",
	PendingTransferHasDifferentLedger:        "pending_transfer_has_different_ledger",
	PendingTransferHasDifferentCode:          "pending_transfer_has_different_code",
	PendingTransferAlreadyPosted:             "pending_transfer_already_posted",
	PendingTransferAlreadyVoided:             "pending_transfer_already_voided",
	PendingTransferExpired:                   "pending_transfer_expired",
	ExceedsPendingTransferAmount:             "exceeds_pending_transfer_amount",
	PendingTransferHasDifferentAmount:        "pending_transfer_has_different_amount",
	OverflowsDebits:                          "overflows_debits",
	OverflowsCredits:                         "overflows_credits",
	ExceedsCredits:                           "exceeds_credits",
	ExceedsDebits:                            "exceeds_debits",
}

func (r Reason) String() string {
	if r < Reason(len(reasonNames)) {
		return reasonNames[r]
	}
	return "reason(" + strconv.FormatUint(uint64(r), 10) + ")"
}
