package pg

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Demonisreal/dmn-ledger/bench/load"
	"github.com/jackc/pgx/v5/pgconn"
)

var (
	ErrRefused = errors.New("pg: refused")
	ErrGaveUp  = fmt.Errorf("pg: gave up (%w)", load.ErrShed)
)

const (
	maxTries = 20
	maxPause = 50 * time.Millisecond

	serializationFailure = "40001"
	deadlockDetected     = "40P01"
)

type retried struct {
	deadlocks, serializations atomic.Uint64
}

func (r *retried) retry(ctx context.Context, try func() error) (int, error) {
	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	for n := 0; ; n++ {
		err := try()
		var pe *pgconn.PgError
		if !errors.As(err, &pe) || pe.Code != serializationFailure && pe.Code != deadlockDetected {
			return n, err
		}
		if n+1 == maxTries {
			return n, fmt.Errorf("%w after %d tries: %w", ErrGaveUp, maxTries, err)
		}
		if pe.Code == deadlockDetected {
			r.deadlocks.Add(1)
		} else {
			r.serializations.Add(1)
		}
		timer.Reset(backoff(n))
		select {
		case <-ctx.Done():
			return n, ctx.Err()
		case <-timer.C:
		}
	}
}

func backoff(n int) time.Duration {
	bound := min(time.Millisecond<<min(n, 6), maxPause)
	return time.Duration(rand.Int64N(int64(bound) + 1)) //nolint:gosec // G404: jitter for a retry, not a secret
}

func refused(err error) bool {
	var pe *pgconn.PgError
	return errors.As(err, &pe) && (strings.HasPrefix(pe.Code, "23") || strings.HasPrefix(pe.Code, "P0"))
}
