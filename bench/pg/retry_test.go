package pg

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Demonisreal/dmn-ledger/bench/load"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestRetryEnds(t *testing.T) {
	serialization := &pgconn.PgError{Code: "40001"}
	deadlock := &pgconn.PgError{Code: "40P01"}
	overdraft := &pgconn.PgError{Code: "23514"}
	for _, tc := range []struct {
		name                      string
		fail                      error
		failing                   int
		tries                     int
		want                      error
		deadlocks, serializations uint64
	}{
		{"first try", nil, 0, 1, nil, 0, 0},
		{"serialization twice, then through", serialization, 2, 3, nil, 0, 2},
		{"deadlock, then through", deadlock, 1, 2, nil, 1, 0},
		{"refused is not retried", overdraft, 1, 1, overdraft, 0, 0},
		{"serialization every time", serialization, 100, maxTries, ErrGaveUp, 0, maxTries - 1},
		{"deadlock every time", deadlock, 100, maxTries, ErrGaveUp, maxTries - 1, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var seen retried
				tries := 0
				start := time.Now()
				retries, err := seen.retry(context.Background(), func() error {
					tries++
					if tries <= tc.failing {
						return tc.fail
					}
					return nil
				})
				if tries != tc.tries || retries != tc.tries-1 || !errors.Is(err, tc.want) || tc.want == nil && err != nil {
					t.Fatalf("%d tries, %d retries, %v; want %d tries, %v", tries, retries, err, tc.tries, tc.want)
				}
				if shed := errors.Is(err, load.ErrShed); shed != (tc.want == ErrGaveUp) {
					t.Fatalf("%v: given up for the load generator %v, want %v", err, shed, tc.want == ErrGaveUp)
				}
				if d, s := seen.deadlocks.Load(), seen.serializations.Load(); d != tc.deadlocks || s != tc.serializations {
					t.Fatalf("counted %d deadlock and %d serialization retries, want %d and %d", d, s, tc.deadlocks, tc.serializations)
				}
				if waited, most := time.Since(start), time.Duration(retries)*maxPause; waited > most {
					t.Fatalf("waited %v over %d retries, at most %v", waited, retries, most)
				}
			})
		})
	}
}

func TestRetryStopsWithItsContext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		var seen retried
		tries := 0
		_, err := seen.retry(ctx, func() error {
			tries++
			if tries == 3 {
				cancel()
			}
			return &pgconn.PgError{Code: "40001"}
		})
		if !errors.Is(err, context.Canceled) || tries != 3 {
			t.Fatalf("%d tries, %v; want 3 tries and context.Canceled", tries, err)
		}
	})
}

func TestBackoffIsBounded(t *testing.T) {
	ms := time.Millisecond
	bounds := []time.Duration{1 * ms, 2 * ms, 4 * ms, 8 * ms, 16 * ms, 32 * ms, 50 * ms, 50 * ms}
	for n := range maxTries {
		bound := bounds[min(n, len(bounds)-1)]
		lowest, highest := bound, time.Duration(0)
		for range 2000 {
			d := backoff(n)
			if d < 0 || d > bound {
				t.Fatalf("pause %v after try %d, want 0 to %v", d, n, bound)
			}
			lowest, highest = min(lowest, d), max(highest, d)
		}
		if lowest > bound/10 || highest < bound*9/10 {
			t.Fatalf("pauses after try %d spread from %v to %v, want most of 0 to %v", n, lowest, highest, bound)
		}
	}
}
