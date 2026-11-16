package load

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/Demonisreal/dmn-ledger/ledger"
)

var ErrWorkload = errors.New("load: unknown workload")

const (
	Ledger         = 1
	Code           = 1
	Endowment      = 1_000_000_000_000
	ReserveTimeout = 300
	maxAmount      = 100
	hotShare       = 80
	postShare      = 90
)

type Workload struct {
	Name     string
	Accounts int
	Hot      int
	TwoPhase bool
}

var workloads = []Workload{
	{Name: "uniform-10k", Accounts: 10_000},
	{Name: "uniform-1m", Accounts: 1_000_000},
	{Name: "hot", Accounts: 10_000, Hot: 8},
	{Name: "twophase", Accounts: 10_000, TwoPhase: true},
}

func Workloads() []Workload {
	return append([]Workload(nil), workloads...)
}

func Named(name string) (Workload, error) {
	for _, w := range workloads {
		if w.Name == name {
			return w, nil
		}
	}
	return Workload{}, fmt.Errorf("%w %q", ErrWorkload, name)
}

func (w Workload) Bank() uint64 {
	return uint64(w.Accounts) + 1 //nolint:gosec // G115: a count of accounts, never negative
}

func (w Workload) Endowment() uint64 {
	return uint64(w.Accounts) * Endowment //nolint:gosec // G115: a count of accounts, never negative
}

type Kind uint8

const (
	Single Kind = iota + 1
	Reserve
	Post
	Void
)

func (k Kind) String() string {
	switch k {
	case Single:
		return "single"
	case Reserve:
		return "reserve"
	case Post:
		return "post"
	case Void:
		return "void"
	}
	return "none"
}

type Op struct {
	Kind    Kind
	ID      ledger.Uint128
	Debit   uint64
	Credit  uint64
	Amount  uint64
	Pending ledger.Uint128
}

type Arrival struct {
	First Op
	Then  Op
}

type Stream struct {
	w     Workload
	rng   *rand.Rand
	clock uint64
}

func NewStream(w Workload, seed uint64) *Stream {
	return &Stream{w: w, rng: rand.New(rand.NewPCG(seed, 0))} //nolint:gosec // G404: seeded on purpose, both systems get the same stream
}

func (s *Stream) next(due time.Duration) Arrival {
	ms := s.clock + uint64(due/time.Millisecond) //nolint:gosec // G115: due is never negative
	debit, credit := s.pair()
	amount := 1 + s.rng.Uint64N(maxAmount)
	if !s.w.TwoPhase {
		return Arrival{First: Op{Kind: Single, ID: s.id(ms), Debit: debit, Credit: credit, Amount: amount}}
	}
	reserve := Op{Kind: Reserve, ID: s.id(ms), Debit: debit, Credit: credit, Amount: amount}
	settle := Op{Kind: Post, ID: s.id(ms), Debit: debit, Credit: credit, Amount: amount, Pending: reserve.ID}
	if s.rng.IntN(100) >= postShare {
		settle.Kind = Void
	}
	return Arrival{First: reserve, Then: settle}
}

func (s *Stream) pair() (debit, credit uint64) {
	n := uint64(s.w.Accounts) //nolint:gosec // G115: a count of accounts, never negative
	if s.w.Hot > 0 && s.rng.IntN(100) < hotShare {
		credit = 1 + s.rng.Uint64N(uint64(s.w.Hot)) //nolint:gosec // G115: a count of accounts, never negative
		debit = 1 + s.rng.Uint64N(n-1)
		if debit >= credit {
			debit++
		}
		return debit, credit
	}
	debit = 1 + s.rng.Uint64N(n)
	credit = 1 + s.rng.Uint64N(n-1)
	if credit >= debit {
		credit++
	}
	return debit, credit
}

func (s *Stream) id(ms uint64) ledger.Uint128 {
	return ledger.Uint128{Hi: ms<<16 | s.rng.Uint64()>>48, Lo: s.rng.Uint64()}
}
