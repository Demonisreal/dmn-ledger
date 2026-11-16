package ledger

import "github.com/Demonisreal/dmn-ledger/internal/state"

type Reason uint32

const (
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

func (r Reason) String() string {
	return state.Reason(r).String()
}
