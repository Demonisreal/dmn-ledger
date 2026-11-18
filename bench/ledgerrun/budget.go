package ledgerrun

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/Demonisreal/dmn-ledger/bench/load"
)

func (c *Cluster) Fit(ctx context.Context, p load.Plan) (load.Plan, error) {
	p, err := fit(p, c.w, c.budget-(c.w.Bank()-1))
	if err != nil || c.written.Load()+planned(p, c.w) <= c.budget {
		return p, err
	}
	return p, c.reset(ctx)
}

const MinWindow = 6 * time.Second

func Budget(nodes int) uint64 {
	if nodes == 3 {
		return 2_000_000
	}
	return 6_000_000
}

func Fits(p load.Plan, w load.Workload, budget uint64) error {
	_, err := fit(p, w, budget-(w.Bank()-1))
	return err
}

func fit(p load.Plan, w load.Workload, room uint64) (load.Plan, error) {
	if planned(p, w) <= room {
		return p, nil
	}
	each := perSecond(p, w)
	secs := room / each
	warm := max(secs/4, 1)
	if secs < warm+uint64(MinWindow/time.Second) {
		return p, fmt.Errorf("%w: %d transfers a second, room for %d", ErrBudget, each, room)
	}
	p.Warmup = time.Duration(warm) * time.Second      //nolint:gosec // G115: seconds of a stage
	p.Window = time.Duration(secs-warm) * time.Second //nolint:gosec // G115: seconds of a stage
	p.MaxWindow = p.Window
	return p, nil
}

func planned(p load.Plan, w load.Workload) uint64 {
	secs := (p.Warmup + max(p.Window, p.MaxWindow)) / time.Second
	return perSecond(p, w) * uint64(secs) //nolint:gosec // G115: whole seconds of a valid plan
}

func perSecond(p load.Plan, w load.Workload) uint64 {
	each := uint64(p.Rate) //nolint:gosec // G115: a rate is positive
	if w.TwoPhase {
		each *= 2
	}
	return each
}

func (c *Cluster) reset(ctx context.Context) error {
	c.hangUp()
	if err := c.halt(); err != nil {
		return err
	}
	if err := os.RemoveAll(c.run); err != nil {
		return err
	}
	c.written.Store(0)
	if err := c.boot(ctx); err != nil {
		return err
	}
	return c.Setup(ctx, c.w)
}
