package pg

import (
	"context"

	"github.com/Demonisreal/dmn-ledger/bench/load"
)

const maxBatch = 10_485

type answer struct {
	out load.Outcome
	err error
}

type call struct {
	op   load.Op
	done chan struct{}
	answer
}

type bundler struct {
	calls chan *call
	send  func(ctx context.Context, ops []load.Op) []answer
	limit int
}

func newBundler(send func(ctx context.Context, ops []load.Op) []answer) *bundler {
	return &bundler{calls: make(chan *call), send: send, limit: maxBatch}
}

func (b *bundler) do(ctx context.Context, op load.Op) (load.Outcome, error) {
	c := &call{op: op, done: make(chan struct{})}
	select {
	case b.calls <- c:
	case <-ctx.Done():
		return load.Outcome{}, ctx.Err()
	}
	select {
	case <-c.done:
		return c.out, c.err
	case <-ctx.Done():
		return load.Outcome{}, ctx.Err()
	}
}

func (b *bundler) run(ctx context.Context) {
	var next *call
	for {
		if next == nil {
			select {
			case next = <-b.calls:
			case <-ctx.Done():
				return
			}
		}
		batch := []*call{next}
		next = nil
	fill:
		for len(batch) < b.limit {
			select {
			case c := <-b.calls:
				if twoPhase(c.op) != twoPhase(batch[0].op) {
					next = c
					break fill
				}
				batch = append(batch, c)
			default:
				break fill
			}
		}
		ops := make([]load.Op, len(batch))
		for i, c := range batch {
			ops[i] = c.op
		}
		for i, a := range b.send(ctx, ops) {
			batch[i].answer = a
			close(batch[i].done)
		}
	}
}

func twoPhase(op load.Op) bool {
	return op.Kind != load.Single
}
