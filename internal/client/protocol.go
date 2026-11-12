package client

import (
	"fmt"

	"github.com/Demonisreal/dmn-ledger/internal/u128"
)

const MaxBody = 1 << 20

type Op uint8

const (
	Register Op = iota + 1
	CreateAccounts
	CreateTransfers
	LookupAccounts
	LookupTransfers
)

var opNames = [...]string{
	Register:        "Register",
	CreateAccounts:  "CreateAccounts",
	CreateTransfers: "CreateTransfers",
	LookupAccounts:  "LookupAccounts",
	LookupTransfers: "LookupTransfers",
}

func (op Op) String() string {
	if op == 0 || int(op) >= len(opNames) {
		return fmt.Sprintf("Op(%d)", uint8(op))
	}
	return opNames[op]
}

func (op Op) Lookup() bool {
	return op == LookupAccounts || op == LookupTransfers
}

type Status uint8

const (
	OK Status = iota + 1
	NotLeader
	Busy
	Malformed
	SessionUnknown
	Stale
	BadClock
)

var statusNames = [...]string{
	OK:             "OK",
	NotLeader:      "NotLeader",
	Busy:           "Busy",
	Malformed:      "Malformed",
	SessionUnknown: "SessionUnknown",
	Stale:          "Stale",
	BadClock:       "BadClock",
}

func (s Status) String() string {
	if s == 0 || int(s) >= len(statusNames) {
		return fmt.Sprintf("Status(%d)", uint8(s))
	}
	return statusNames[s]
}

type Request struct {
	Client  u128.Uint128
	Session uint64
	Number  uint64
	Op      Op
	Body    []byte
}

type Reply struct {
	Client u128.Uint128
	Number uint64
	Op     Op
	Status Status
	Leader uint64
	Time   uint64
	Body   []byte
}
