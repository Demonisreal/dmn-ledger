package pg

import (
	"context"
	"fmt"
	"time"

	"github.com/Demonisreal/dmn-ledger/bench/load"
)

const sampleSQL = `WITH io AS (
    SELECT coalesce(sum(fsyncs) FILTER (WHERE context = 'normal'), 0)::bigint AS syncs,
           coalesce(sum(fsync_time) FILTER (WHERE context = 'normal'), 0) AS sync_ms,
           coalesce(sum(writes) FILTER (WHERE context = 'normal'), 0)::bigint AS writes,
           coalesce(sum(fsyncs) FILTER (WHERE context = 'init'), 0)::bigint AS init_syncs
    FROM pg_stat_io WHERE object = 'wal'
), tables AS (
    SELECT coalesce(sum(autovacuum_count), 0)::bigint AS vacuums,
           coalesce(sum(autoanalyze_count), 0)::bigint AS analyzes,
           coalesce(sum(n_tup_ins) FILTER (WHERE relname = 'transfers'), 0)::bigint AS inserted,
           coalesce(sum(n_tup_upd) FILTER (WHERE relname = 'accounts'), 0)::bigint AS account_updates,
           coalesce(sum(n_tup_hot_upd) FILTER (WHERE relname = 'accounts'), 0)::bigint AS account_hot_updates,
           coalesce(sum(n_tup_upd) FILTER (WHERE relname = 'transfers'), 0)::bigint AS transfer_updates,
           coalesce(sum(n_tup_hot_upd) FILTER (WHERE relname = 'transfers'), 0)::bigint AS transfer_hot_updates
    FROM pg_stat_user_tables
)
SELECT io.syncs, io.sync_ms, io.writes, io.init_syncs, w.wal_records, w.wal_fpi, w.wal_bytes::bigint, c.num_done,
       t.vacuums, t.analyzes, t.inserted, t.account_updates, t.account_hot_updates, t.transfer_updates, t.transfer_hot_updates,
       d.deadlocks
FROM io, tables t, pg_stat_wal w, pg_stat_checkpointer c, pg_stat_database d
WHERE d.datname = current_database()`

var sampled = []string{
	"wal_writes", "wal_init_syncs", "wal_records", "wal_fpi", "wal_bytes", "checkpoints", "autovacuum", "autoanalyze",
	"inserted", "accounts_updates", "accounts_hot_updates", "transfers_updates", "transfers_hot_updates", "deadlocks",
}

func (c *Client) Sample(ctx context.Context) (load.Inside, error) {
	var syncs uint64
	var syncMs float64
	counts := make([]uint64, len(sampled))
	dest := []any{&syncs, &syncMs}
	for i := range counts {
		dest = append(dest, &counts[i])
	}
	if err := c.probe.QueryRow(ctx, sampleSQL).Scan(dest...); err != nil {
		return nil, err
	}
	counters := make(map[string]uint64, len(sampled)+2)
	for i, name := range sampled {
		counters[name] = counts[i]
	}
	counters["deadlock_retries"] = c.retried.deadlocks.Load()
	counters["serialization_retries"] = c.retried.serializations.Load()
	return load.Inside{{
		Name:     "postgres",
		Syncs:    syncs,
		SyncTime: time.Duration(syncMs * float64(time.Millisecond)),
		Requests: c.requests.Load(),
		Counters: counters,
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
