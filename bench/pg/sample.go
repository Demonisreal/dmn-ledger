package pg

import (
	"context"
	"fmt"
	"time"

	"github.com/Demonisreal/dmn-ledger/bench/load"
)

const sampleSQL = `SELECT
    (SELECT coalesce(sum(fsyncs), 0)::bigint FROM pg_stat_io WHERE object = 'wal'),
    (SELECT coalesce(sum(fsync_time), 0) FROM pg_stat_io WHERE object = 'wal'),
    (SELECT coalesce(sum(writes), 0)::bigint FROM pg_stat_io WHERE object = 'wal'),
    w.wal_records, w.wal_fpi, w.wal_bytes::bigint, c.num_done,
    (SELECT coalesce(sum(autovacuum_count), 0)::bigint FROM pg_stat_user_tables),
    (SELECT coalesce(sum(autoanalyze_count), 0)::bigint FROM pg_stat_user_tables)
FROM pg_stat_wal w, pg_stat_checkpointer c`

func (c *Client) Sample(ctx context.Context) (load.Inside, error) {
	var syncs, writes, records, fpi, bytes, checkpoints, vacuums, analyzes uint64
	var syncMs float64
	err := c.probe.QueryRow(ctx, sampleSQL).Scan(&syncs, &syncMs, &writes, &records, &fpi, &bytes, &checkpoints, &vacuums, &analyzes)
	if err != nil {
		return nil, err
	}
	return load.Inside{{
		Name:     "postgres",
		Syncs:    syncs,
		SyncTime: time.Duration(syncMs * float64(time.Millisecond)),
		Requests: c.requests.Load(),
		Counters: map[string]uint64{
			"wal_writes":  writes,
			"wal_records": records,
			"wal_fpi":     fpi,
			"wal_bytes":   bytes,
			"checkpoints": checkpoints,
			"autovacuum":  vacuums,
			"autoanalyze": analyzes,
		},
	}}, nil
}

func (c *Client) Verify(ctx context.Context, want load.Books) (load.Tally, error) {
	var t load.Tally
	var accounts uint64
	err := c.pool.QueryRow(ctx, `SELECT count(*), coalesce(sum(debits_posted), 0)::bigint, coalesce(sum(credits_posted), 0)::bigint,
        coalesce(sum(debits_pending), 0)::bigint, coalesce(sum(credits_pending), 0)::bigint FROM accounts`).
		Scan(&accounts, &t.DebitsPosted, &t.CreditsPosted, &t.DebitsPending, &t.CreditsPending)
	if err != nil {
		return t, fmt.Errorf("sum accounts: %w", err)
	}
	if len(want.Sample) > 0 {
		ids := make([][16]byte, len(want.Sample))
		for i, id := range want.Sample {
			ids[i] = key(id)
		}
		if err := c.pool.QueryRow(ctx, "SELECT count(*) FROM transfers WHERE id = ANY ($1)", ids).Scan(&t.Found); err != nil {
			return t, fmt.Errorf("look up transfers: %w", err)
		}
	}
	c.mu.Lock()
	books := c.books
	c.mu.Unlock()
	books.Sample = want.Sample
	return t, compare(t, accounts, c.w, books)
}

func compare(t load.Tally, accounts uint64, w load.Workload, want load.Books) error {
	u := want.Unsure
	posted, maybePosted := want.Posted+w.Endowment(), u.Single.Amount+u.Post.Amount
	pending, released := want.Pending+u.Reserve.Amount, u.Post.Amount+u.Void.Amount
	unsure := ""
	if u.Calls() > 0 {
		unsure = fmt.Sprintf(", %d calls unanswered", u.Calls())
	}
	switch {
	case accounts != w.Bank():
		return fmt.Errorf("%w: found %d of %d accounts", ErrBooks, accounts, w.Bank())
	case t.DebitsPosted != t.CreditsPosted:
		return fmt.Errorf("%w: %d debits posted, %d credits posted", ErrBooks, t.DebitsPosted, t.CreditsPosted)
	case t.DebitsPosted < posted || t.DebitsPosted > posted+maybePosted:
		return fmt.Errorf("%w: %d posted, want %d confirmed and %d endowment%s", ErrBooks, t.DebitsPosted, want.Posted, w.Endowment(), unsure)
	case t.DebitsPending != t.CreditsPending || t.DebitsPending > pending || t.DebitsPending < want.Pending-released:
		return fmt.Errorf("%w: %d debits pending, %d credits pending, want %d%s", ErrBooks, t.DebitsPending, t.CreditsPending, want.Pending, unsure)
	case t.Found != len(want.Sample):
		return fmt.Errorf("%w: found %d of %d sampled transfers", ErrBooks, t.Found, len(want.Sample))
	}
	return nil
}
