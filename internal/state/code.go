package state

import "strconv"

type Code uint32

const (
	OK                                       Code = 0
	LinkedEventFailed                        Code = 1
	LinkedEventChainOpen                     Code = 2
	IDMustNotBeZero                          Code = 3
	IDMustNotBeMax                           Code = 4
	FlagsUnknown                             Code = 5
	FlagsAreMutuallyExclusive                Code = 6
	LedgerMustNotBeZero                      Code = 7
	CodeMustNotBeZero                        Code = 8
	BalancesMustBeZero                       Code = 9
	TimestampMustBeZero                      Code = 10
	ExistsWithDifferentFields                Code = 11
	Exists                                   Code = 12
	DebitAccountIDMustNotBeZero              Code = 13
	CreditAccountIDMustNotBeZero             Code = 14
	AccountsMustBeDifferent                  Code = 15
	PendingIDMustBeZero                      Code = 16
	PendingIDMustNotBeZero                   Code = 17
	TimeoutRequiresPending                   Code = 18
	AmountMustNotBeZero                      Code = 19
	DebitAccountNotFound                     Code = 20
	CreditAccountNotFound                    Code = 21
	AccountsMustHaveSameLedger               Code = 22
	TransferMustHaveSameLedgerAsAccounts     Code = 23
	PendingTransferNotFound                  Code = 24
	PendingTransferNotPending                Code = 25
	PendingTransferHasDifferentDebitAccount  Code = 26
	PendingTransferHasDifferentCreditAccount Code = 27
	PendingTransferHasDifferentLedger        Code = 28
	PendingTransferHasDifferentCode          Code = 29
	PendingTransferAlreadyPosted             Code = 30
	PendingTransferAlreadyVoided             Code = 31
	PendingTransferExpired                   Code = 32
	ExceedsPendingTransferAmount             Code = 33
	PendingTransferHasDifferentAmount        Code = 34
	OverflowsDebits                          Code = 35
	OverflowsCredits                         Code = 36
	ExceedsCredits                           Code = 37
	ExceedsDebits                            Code = 38
)

var codeNames = [...]string{
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

func (c Code) String() string {
	if c < Code(len(codeNames)) {
		return codeNames[c]
	}
	return "code(" + strconv.FormatUint(uint64(c), 10) + ")"
}
