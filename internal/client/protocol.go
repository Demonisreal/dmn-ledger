package client

import (
	"encoding/binary"
	"fmt"

	"github.com/Demonisreal/dmn-ledger/internal/state"
	"github.com/Demonisreal/dmn-ledger/internal/u128"
)

const (
	MaxBody     = 1 << 20
	IDSize      = 16
	FailureSize = 8
	MaxLinks    = 1024
)

type Op uint8

const (
	Register Op = iota + 1
	CreateAccounts
	CreateTransfers
	LookupAccounts
	LookupTransfers
	Prove
	Head
	Links
)

var opNames = [...]string{
	Register:        "Register",
	CreateAccounts:  "CreateAccounts",
	CreateTransfers: "CreateTransfers",
	LookupAccounts:  "LookupAccounts",
	LookupTransfers: "LookupTransfers",
	Prove:           "Prove",
	Head:            "Head",
	Links:           "Links",
}

func (op Op) String() string {
	if op == 0 || int(op) >= len(opNames) {
		return fmt.Sprintf("Op(%d)", uint8(op))
	}
	return opNames[op]
}

func (op Op) Lookup() bool {
	return op >= LookupAccounts && op <= Links
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

func AppendIDs(dst []byte, ids []u128.Uint128) []byte {
	for _, id := range ids {
		dst = id.AppendBytes(dst)
	}
	return dst
}

func DecodeIDs(body []byte) []u128.Uint128 {
	ids := make([]u128.Uint128, 0, len(body)/IDSize)
	for ; len(body) >= IDSize; body = body[IDSize:] {
		ids = append(ids, u128.FromBytes(body))
	}
	return ids
}

func AppendFailures(dst []byte, fails []state.Failure) []byte {
	for _, f := range fails {
		dst = binary.BigEndian.AppendUint32(dst, uint32(f.Index)) //nolint:gosec // G115: an index into a batch below MaxBody
		dst = binary.BigEndian.AppendUint32(dst, uint32(f.Reason))
	}
	return dst
}

func DecodeFailures(body []byte) []state.Failure {
	fails := make([]state.Failure, 0, len(body)/FailureSize)
	for ; len(body) >= FailureSize; body = body[FailureSize:] {
		fails = append(fails, state.Failure{
			Index:  int(binary.BigEndian.Uint32(body)),
			Reason: state.Reason(binary.BigEndian.Uint32(body[4:])),
		})
	}
	return fails
}
