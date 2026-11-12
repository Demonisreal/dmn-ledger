package raft

import (
	"errors"
	"fmt"
	"math/rand/v2"
)

var (
	ErrNotLeader = errors.New("raft: not leader")
	ErrBusy      = errors.New("raft: too many uncommitted entries")
	ErrNotReady  = errors.New("raft: no entry of the current term committed yet")
	ErrKind      = errors.New("raft: kind 0 is reserved")
	ErrSafety    = errors.New("raft: safety violation")
)

type Entry struct {
	Index uint64
	Term  uint64
	Kind  uint8
	Body  []byte
}

type HardState struct {
	Term   uint64
	Vote   uint64
	Repair bool
}

type Snapshot struct {
	Index uint64
	Term  uint64
}

type Log interface {
	Snapshot() Snapshot
	FirstIndex() uint64
	LastIndex() uint64
	Term(i uint64) (uint64, error)
	Entries(lo, hi uint64, maxBytes int64) ([]Entry, error)
}

type Config struct {
	ID             uint64
	Peers          []uint64
	ElectionTicks  int
	HeartbeatTicks int
	SnapshotTicks  int
	MaxAppendBytes int64
	MaxUncommitted uint64
	Rand           *rand.Rand
	Log            Log
	HardState      HardState
}

type MsgType uint8

const (
	MsgPreVote MsgType = iota + 1
	MsgPreVoteResp
	MsgVote
	MsgVoteResp
	MsgApp
	MsgAppResp
	MsgSnap
	MsgRepairProbe
	MsgRepairInfo
)

func (t MsgType) String() string {
	switch t {
	case MsgPreVote:
		return "PreVote"
	case MsgPreVoteResp:
		return "PreVoteResp"
	case MsgVote:
		return "Vote"
	case MsgVoteResp:
		return "VoteResp"
	case MsgApp:
		return "App"
	case MsgAppResp:
		return "AppResp"
	case MsgSnap:
		return "Snap"
	case MsgRepairProbe:
		return "RepairProbe"
	case MsgRepairInfo:
		return "RepairInfo"
	}
	return fmt.Sprintf("MsgType(%d)", uint8(t))
}

type Message struct {
	Type    MsgType
	From    uint64
	To      uint64
	Term    uint64
	Index   uint64
	LogTerm uint64
	Commit  uint64
	Seq     uint64
	Reject  bool
	Entries []Entry
}

type Ready struct {
	HardState *HardState
	Snapshot  *Snapshot
	Entries   []Entry
	Messages  []Message
	Commit    uint64
	Reads     []Read
}

type Read struct {
	Tag   uint64
	Index uint64
}

type Role uint8

const (
	Follower Role = iota
	PreCandidate
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case PreCandidate:
		return "precandidate"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	}
	return fmt.Sprintf("Role(%d)", uint8(r))
}

type Status struct {
	ID     uint64
	Role   Role
	Term   uint64
	Vote   uint64
	Lead   uint64
	Commit uint64
	Last   uint64
	Repair bool
}
