package state

import (
	"encoding/binary"

	"github.com/Demonisreal/dmn-ledger/internal/u128"
)

type Op uint8

const (
	CreateAccounts Op = iota + 1
	CreateTransfers
)

type AccountFlags uint16

const (
	DebitsMustNotExceedCredits AccountFlags = 1 << iota
	CreditsMustNotExceedDebits

	accountFlagsKnown = DebitsMustNotExceedCredits | CreditsMustNotExceedDebits
)

type TransferFlags uint16

const (
	Linked TransferFlags = 1 << iota
	Pending
	PostPending
	VoidPending

	transferFlagsKnown = Linked | Pending | PostPending | VoidPending
)

type Account struct {
	ID             u128.Uint128
	DebitsPending  u128.Uint128
	DebitsPosted   u128.Uint128
	CreditsPending u128.Uint128
	CreditsPosted  u128.Uint128
	Ledger         uint32
	Code           uint16
	Flags          AccountFlags
	Timestamp      uint64
}

type Transfer struct {
	ID              u128.Uint128
	DebitAccountID  u128.Uint128
	CreditAccountID u128.Uint128
	Amount          u128.Uint128
	PendingID       u128.Uint128
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

const (
	AccountSize  = 5*16 + 4 + 2 + 2 + 8
	TransferSize = 5*16 + 4 + 4 + 2 + 2 + 8
)

func AppendAccount(b []byte, a Account) []byte {
	b = a.ID.AppendBytes(b)
	b = a.DebitsPending.AppendBytes(b)
	b = a.DebitsPosted.AppendBytes(b)
	b = a.CreditsPending.AppendBytes(b)
	b = a.CreditsPosted.AppendBytes(b)
	b = binary.BigEndian.AppendUint32(b, a.Ledger)
	b = binary.BigEndian.AppendUint16(b, a.Code)
	b = binary.BigEndian.AppendUint16(b, uint16(a.Flags))
	return binary.BigEndian.AppendUint64(b, a.Timestamp)
}

func DecodeAccount(b []byte) Account {
	_ = b[AccountSize-1]
	return Account{
		ID:             u128.FromBytes(b[0:]),
		DebitsPending:  u128.FromBytes(b[16:]),
		DebitsPosted:   u128.FromBytes(b[32:]),
		CreditsPending: u128.FromBytes(b[48:]),
		CreditsPosted:  u128.FromBytes(b[64:]),
		Ledger:         binary.BigEndian.Uint32(b[80:]),
		Code:           binary.BigEndian.Uint16(b[84:]),
		Flags:          AccountFlags(binary.BigEndian.Uint16(b[86:])),
		Timestamp:      binary.BigEndian.Uint64(b[88:]),
	}
}

func AppendTransfer(b []byte, tr Transfer) []byte {
	b = tr.ID.AppendBytes(b)
	b = tr.DebitAccountID.AppendBytes(b)
	b = tr.CreditAccountID.AppendBytes(b)
	b = tr.Amount.AppendBytes(b)
	b = tr.PendingID.AppendBytes(b)
	b = binary.BigEndian.AppendUint32(b, tr.Timeout)
	b = binary.BigEndian.AppendUint32(b, tr.Ledger)
	b = binary.BigEndian.AppendUint16(b, tr.Code)
	b = binary.BigEndian.AppendUint16(b, uint16(tr.Flags))
	return binary.BigEndian.AppendUint64(b, tr.Timestamp)
}

func DecodeTransfer(b []byte) Transfer {
	_ = b[TransferSize-1]
	return Transfer{
		ID:              u128.FromBytes(b[0:]),
		DebitAccountID:  u128.FromBytes(b[16:]),
		CreditAccountID: u128.FromBytes(b[32:]),
		Amount:          u128.FromBytes(b[48:]),
		PendingID:       u128.FromBytes(b[64:]),
		Timeout:         binary.BigEndian.Uint32(b[80:]),
		Ledger:          binary.BigEndian.Uint32(b[84:]),
		Code:            binary.BigEndian.Uint16(b[88:]),
		Flags:           TransferFlags(binary.BigEndian.Uint16(b[90:])),
		Timestamp:       binary.BigEndian.Uint64(b[92:]),
	}
}
