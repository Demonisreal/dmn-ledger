package load

import (
	"context"
	"time"

	"github.com/Demonisreal/dmn-ledger/ledger"
)

type Target interface {
	Setup(ctx context.Context, w Workload) error
	Do(ctx context.Context, lane int, op Op) (Outcome, error)
	Sample(ctx context.Context) (Inside, error)
	Verify(ctx context.Context, want Books) (Tally, error)
	Close() error
}

type Outcome struct {
	Existed bool
	Retries int
}

type Inside []Part

type Part struct {
	Name     string
	Role     string
	Term     uint64
	Syncs    uint64
	SyncTime time.Duration
	Requests uint64
	Counters map[string]uint64
	Stale    bool
}

type Books struct {
	Confirmed uint64
	Existed   uint64
	Posted    uint64
	Pending   uint64
	Unsure    Unsure
	Sample    []ledger.Uint128
}

type Doubt struct {
	Calls  uint64 `json:"calls"`
	Amount uint64 `json:"amount"`
}

type Unsure struct {
	Single  Doubt `json:"single"`
	Reserve Doubt `json:"reserve"`
	Post    Doubt `json:"post"`
	Void    Doubt `json:"void"`
}

func (u Unsure) Calls() uint64 {
	return u.Single.Calls + u.Reserve.Calls + u.Post.Calls + u.Void.Calls
}

func (u *Unsure) add(op Op) {
	d := &u.Single
	switch op.Kind {
	case Reserve:
		d = &u.Reserve
	case Post:
		d = &u.Post
	case Void:
		d = &u.Void
	}
	d.Calls++
	d.Amount += op.Amount
}

func (b *Books) Count(op Op, out Outcome, err error) {
	if err != nil {
		b.Unsure.add(op)
		return
	}
	b.Confirmed++
	if out.Existed {
		b.Existed++
		return
	}
	switch op.Kind {
	case Single:
		b.Posted += op.Amount
	case Reserve:
		b.Pending += op.Amount
	case Post:
		b.Pending -= op.Amount
		b.Posted += op.Amount
	case Void:
		b.Pending -= op.Amount
	}
}

type Tally struct {
	Found          int
	DebitsPosted   uint64
	CreditsPosted  uint64
	DebitsPending  uint64
	CreditsPending uint64
}

type Null struct{}

func (Null) Setup(context.Context, Workload) error { return nil }

func (Null) Do(context.Context, int, Op) (Outcome, error) { return Outcome{}, nil }

func (Null) Sample(context.Context) (Inside, error) { return nil, nil }

func (Null) Verify(_ context.Context, want Books) (Tally, error) {
	return Tally{Found: len(want.Sample)}, nil
}

func (Null) Close() error { return nil }
