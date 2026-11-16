package ledger

import (
	"crypto/rand"
	"errors"
	"fmt"
	"strconv"

	"github.com/Demonisreal/dmn-ledger/internal/client"
	"github.com/Demonisreal/dmn-ledger/internal/state"
	"github.com/Demonisreal/dmn-ledger/internal/u128"
)

type Uint128 struct{ Hi, Lo uint64 }

func From64(v uint64) Uint128 {
	return Uint128{Lo: v}
}

func Parse(s string) (Uint128, error) {
	u, err := u128.Parse(s)
	switch {
	case errors.Is(err, u128.ErrRange):
		return Uint128{}, fmt.Errorf("ledger: parse %q: %w", s, strconv.ErrRange)
	case err != nil:
		return Uint128{}, fmt.Errorf("ledger: parse %q: %w", s, strconv.ErrSyntax)
	}
	return Uint128(u), nil
}

func NewID() Uint128 {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return Uint128(u128.FromBytes(b[:]))
}

func (u Uint128) String() string {
	return u128.Uint128(u).String()
}

type AccountFlags uint16

const (
	DebitsMustNotExceedCredits AccountFlags = 1 << iota
	CreditsMustNotExceedDebits
)

type TransferFlags uint16

const (
	Linked TransferFlags = 1 << iota
	Pending
	PostPending
	VoidPending
)

type Account struct {
	ID             Uint128
	DebitsPending  Uint128
	DebitsPosted   Uint128
	CreditsPending Uint128
	CreditsPosted  Uint128
	Ledger         uint32
	Code           uint16
	Flags          AccountFlags
	Timestamp      uint64
}

type Transfer struct {
	ID              Uint128
	DebitAccountID  Uint128
	CreditAccountID Uint128
	Amount          Uint128
	PendingID       Uint128
	Timeout         uint32
	Ledger          uint32
	Code            uint16
	Flags           TransferFlags
	Timestamp       uint64
}

type Failure struct {
	Index  int
	Reason Reason
}

type Result struct {
	Timestamp uint64
	Batch     uint64
	Failures  []Failure
}

const (
	maxAccounts  = client.MaxBody / state.AccountSize
	maxTransfers = client.MaxBody / state.TransferSize
)

func appendAccount(b []byte, a Account) []byte {
	return state.AppendAccount(b, state.Account{
		ID:             u128.Uint128(a.ID),
		DebitsPending:  u128.Uint128(a.DebitsPending),
		DebitsPosted:   u128.Uint128(a.DebitsPosted),
		CreditsPending: u128.Uint128(a.CreditsPending),
		CreditsPosted:  u128.Uint128(a.CreditsPosted),
		Ledger:         a.Ledger,
		Code:           a.Code,
		Flags:          state.AccountFlags(a.Flags),
		Timestamp:      a.Timestamp,
	})
}

func decodeAccounts(body []byte) []Account {
	accounts := make([]Account, 0, len(body)/state.AccountSize)
	for ; len(body) >= state.AccountSize; body = body[state.AccountSize:] {
		a := state.DecodeAccount(body)
		accounts = append(accounts, Account{
			ID:             Uint128(a.ID),
			DebitsPending:  Uint128(a.DebitsPending),
			DebitsPosted:   Uint128(a.DebitsPosted),
			CreditsPending: Uint128(a.CreditsPending),
			CreditsPosted:  Uint128(a.CreditsPosted),
			Ledger:         a.Ledger,
			Code:           a.Code,
			Flags:          AccountFlags(a.Flags),
			Timestamp:      a.Timestamp,
		})
	}
	return accounts
}

func appendTransfer(b []byte, tr Transfer) []byte {
	return state.AppendTransfer(b, state.Transfer{
		ID:              u128.Uint128(tr.ID),
		DebitAccountID:  u128.Uint128(tr.DebitAccountID),
		CreditAccountID: u128.Uint128(tr.CreditAccountID),
		Amount:          u128.Uint128(tr.Amount),
		PendingID:       u128.Uint128(tr.PendingID),
		Timeout:         tr.Timeout,
		Ledger:          tr.Ledger,
		Code:            tr.Code,
		Flags:           state.TransferFlags(tr.Flags),
		Timestamp:       tr.Timestamp,
	})
}

func decodeTransfers(body []byte) []Transfer {
	transfers := make([]Transfer, 0, len(body)/state.TransferSize)
	for ; len(body) >= state.TransferSize; body = body[state.TransferSize:] {
		tr := state.DecodeTransfer(body)
		transfers = append(transfers, Transfer{
			ID:              Uint128(tr.ID),
			DebitAccountID:  Uint128(tr.DebitAccountID),
			CreditAccountID: Uint128(tr.CreditAccountID),
			Amount:          Uint128(tr.Amount),
			PendingID:       Uint128(tr.PendingID),
			Timeout:         tr.Timeout,
			Ledger:          tr.Ledger,
			Code:            tr.Code,
			Flags:           TransferFlags(tr.Flags),
			Timestamp:       tr.Timestamp,
		})
	}
	return transfers
}

func appendIDs(b []byte, ids []Uint128) []byte {
	for _, id := range ids {
		b = u128.Uint128(id).AppendBytes(b)
	}
	return b
}
