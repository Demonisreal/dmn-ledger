package demo

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"time"
)

type Kind uint8

const (
	Kill Kind = iota + 1
	Freeze
	Cut
	Flip
)

var kindNames = [...]string{Kill: "kill", Freeze: "freeze", Cut: "cut", Flip: "flip"}

func (k Kind) String() string {
	return kindNames[k]
}

func (k Kind) MarshalText() ([]byte, error) {
	return []byte(k.String()), nil
}

func (k *Kind) UnmarshalText(b []byte) error {
	for kind := range Flip + 1 {
		if kindNames[kind] == string(b) {
			*k = kind
			return nil
		}
	}
	return fmt.Errorf("demo: unknown kind %q", b)
}

type Show interface {
	Act(kind Kind, target int, from netip.Prefix) (until time.Time, err error)
	State() State
	Lines(after uint64) []Line
	Proof(ctx context.Context) (Proof, error)
	Watching(streams int)
	Alive() bool
}

var (
	ErrRecovering = errors.New("demo: the cluster is not ready")
	ErrBusy       = errors.New("demo: an action is running")
	ErrCooling    = errors.New("demo: too soon after the last action")
	ErrLimited    = errors.New("demo: too soon for this address")
	ErrYoung      = errors.New("demo: the log of the node is too short for a flip")
	ErrNoLeader   = errors.New("demo: no leader")
	ErrEmpty      = errors.New("demo: nothing confirmed yet")
	ErrFinding    = errors.New("demo: a finding stands")
)

type State struct {
	Rev     uint64    `json:"-"`
	Now     int64     `json:"now"`
	Started int64     `json:"started"`
	Phase   string    `json:"phase"`
	Load    string    `json:"load"`
	Rest    Rest      `json:"rest"`
	Viewers int       `json:"viewers"`
	Cluster Cluster   `json:"cluster"`
	Nodes   [3]Node   `json:"nodes"`
	Links   [6]Link   `json:"links"`
	Books   Books     `json:"books"`
	Action  *Action   `json:"action"`
	Next    int64     `json:"next"`
	Finding *Finding  `json:"finding"`
	Totals  Totals    `json:"totals"`
	Recent  []Booking `json:"recent"`
	Lines   []Line    `json:"lines,omitempty"`
}

type Rest struct {
	Since int64 `json:"since"`
	Until int64 `json:"until"`
}

type Cluster struct {
	N      int    `json:"n"`
	ID     string `json:"id"`
	Since  int64  `json:"since"`
	Why    string `json:"why"`
	Limit  int    `json:"limit"`
	Failed string `json:"failed"`
}

type Node struct {
	ID       int    `json:"id"`
	Pid      int    `json:"pid"`
	Since    int64  `json:"since"`
	Proc     string `json:"proc"`
	Answers  bool   `json:"answers"`
	Role     string `json:"role"`
	Term     uint64 `json:"term"`
	Leader   uint64 `json:"leader"`
	Commit   uint64 `json:"commit"`
	Applied  uint64 `json:"applied"`
	Last     uint64 `json:"last"`
	Apply    string `json:"apply"`
	HeadHash string `json:"headhash"`
	Head     uint64 `json:"head"`
	Repair   bool   `json:"repair"`
	Key      string `json:"key"`
	Note     Note   `json:"note"`
}

type Note struct {
	At   int64  `json:"at"`
	Text string `json:"text"`
}

type Link struct {
	From int  `json:"from"`
	To   int  `json:"to"`
	Cut  bool `json:"cut"`
}

type Books struct {
	Confirmed int     `json:"confirmed"`
	Refused   int     `json:"refused"`
	Open      int     `json:"open"`
	Sum       Sum     `json:"sum"`
	Recount   Recount `json:"recount"`
	Judged    Judged  `json:"judged"`
}

type Sum struct {
	At       int64      `json:"at"`
	Value    string     `json:"value"`
	Want     string     `json:"want"`
	Accounts [6]Account `json:"accounts"`
}

type Account struct {
	ID      int    `json:"id"`
	Balance string `json:"balance"`
}

type Recount struct {
	At        int64  `json:"at"`
	After     Kind   `json:"after"`
	Confirmed int    `json:"confirmed"`
	Found     int    `json:"found"`
	Refused   int    `json:"refused"`
	Strays    int    `json:"strays"`
	Index     uint64 `json:"index"`
	Hash      string `json:"hash"`
}

type Judged struct {
	Windows int    `json:"windows"`
	Calls   int    `json:"calls"`
	Upto    string `json:"upto"`
	At      int64  `json:"at"`
}

type Action struct {
	Kind     Kind   `json:"kind"`
	Target   int    `json:"target"`
	Node     int    `json:"node"`
	Role     string `json:"role"`
	Term     uint64 `json:"term"`
	Phase    string `json:"phase"`
	Began    int64  `json:"began"`
	LiftAt   int64  `json:"liftat"`
	Ended    int64  `json:"ended"`
	File     string `json:"file"`
	Size     int64  `json:"size"`
	Offset   int64  `json:"offset"`
	Bit      int    `json:"bit"`
	Before   uint8  `json:"before"`
	After    uint8  `json:"after"`
	Repair   string `json:"repair"`
	NonVoter int64  `json:"nonvoter"`
}

type Finding struct {
	At       int64  `json:"at"`
	Check    string `json:"check"`
	Reason   string `json:"reason"`
	ResetAt  int64  `json:"resetat"`
	Evidence string `json:"evidence"`
}

type Totals struct {
	Clusters  int       `json:"clusters"`
	Confirmed int       `json:"confirmed"`
	Windows   int       `json:"windows"`
	Actions   Actions   `json:"actions"`
	Healed    int       `json:"healed"`
	Found     int       `json:"found"`
	Findings  []Finding `json:"findings"`
}

type Actions struct {
	Kill   int `json:"kill"`
	Freeze int `json:"freeze"`
	Cut    int `json:"cut"`
	Flip   int `json:"flip"`
}

type Booking struct {
	ID      string `json:"id"`
	Debit   int    `json:"debit"`
	Credit  int    `json:"credit"`
	Amount  string `json:"amount"`
	At      int64  `json:"at"`
	Refused string `json:"refused"`
}

type Line struct {
	N    uint64 `json:"n"`
	At   int64  `json:"at"`
	Src  string `json:"src"`
	Text string `json:"text"`
}

type Proof struct {
	At       int64     `json:"at"`
	Cluster  string    `json:"cluster"`
	Keys     [3]string `json:"keys"`
	Transfer Booking   `json:"transfer"`
	Receipt  string    `json:"receipt"`
	Base64   string    `json:"base64"`
	Head     Head      `json:"head"`
}

type Head struct {
	Seq    uint64 `json:"seq"`
	Hash   string `json:"hash"`
	Signer int    `json:"signer"`
}
