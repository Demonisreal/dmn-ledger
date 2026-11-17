//go:build pg

package pg

import (
	"cmp"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Demonisreal/dmn-ledger/bench/load"
	"github.com/Demonisreal/dmn-ledger/ledger"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

var srv *Server

var (
	rules    = load.Workload{Name: "rules", Accounts: 16}
	twoRules = load.Workload{Name: "rules", Accounts: 16, TwoPhase: true}
)

func TestMain(m *testing.M) {
	dir := filepath.Join(cmp.Or(os.Getenv("BENCH_DIR"), "/var/tmp/dmn-ledger-bench"), "pg")
	var err error
	if srv, err = Start(context.Background(), dir); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	code := m.Run()
	if err := srv.Stop(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		code = 1
	}
	os.Exit(code)
}

func dial(t *testing.T, v Variant, lanes int, w load.Workload) *Client {
	t.Helper()
	c, err := Dial(t.Context(), srv, Config{Variant: v, Lanes: lanes})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	if err := c.Setup(t.Context(), w); err != nil {
		t.Fatal(err)
	}
	return c
}

func bounded(t *testing.T) context.Context {
	t.Helper()
	end, ok := t.Deadline()
	if !ok {
		return t.Context()
	}
	ctx, cancel := context.WithDeadline(t.Context(), end.Add(-15*time.Second))
	t.Cleanup(cancel)
	return ctx
}

type account struct {
	debitsPending, debitsPosted, creditsPending, creditsPosted uint64
}

func balances(t *testing.T, c *Client, id uint64) account {
	t.Helper()
	var a account
	err := c.pool.QueryRow(t.Context(), "SELECT debits_pending, debits_posted, credits_pending, credits_posted FROM accounts WHERE id = $1", id).
		Scan(&a.debitsPending, &a.debitsPosted, &a.creditsPending, &a.creditsPosted)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func booksBalance(t *testing.T, c *Client) {
	t.Helper()
	var debits, credits, pendingDebits, pendingCredits, broken uint64
	err := c.pool.QueryRow(t.Context(), `SELECT sum(debits_posted)::bigint, sum(credits_posted)::bigint,
        sum(debits_pending)::bigint, sum(credits_pending)::bigint,
        count(*) FILTER (WHERE least(debits_pending, debits_posted, credits_pending, credits_posted) < 0
            OR flags & 1 <> 0 AND debits_posted + debits_pending > credits_posted)
        FROM accounts`).Scan(&debits, &credits, &pendingDebits, &pendingCredits, &broken)
	if err != nil {
		t.Fatal(err)
	}
	if debits != credits || pendingDebits != pendingCredits || broken != 0 {
		t.Fatalf("debits %d, credits %d, pending %d and %d, %d accounts over their limit", debits, credits, pendingDebits, pendingCredits, broken)
	}
}

func sample(t *testing.T, c *Client) load.Part {
	t.Helper()
	in, err := c.Sample(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	return in[0]
}

func drained(t *testing.T, c *Client) load.Part {
	t.Helper()
	c.pool.Close()
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(50 * time.Millisecond) {
		var n int
		if err := c.probe.QueryRow(t.Context(), "SELECT numbackends FROM pg_stat_database WHERE datname = current_database()").Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d sessions still on the database after the pool closed", n)
		}
	}
	return sample(t, c)
}

func transfers(t *testing.T, c *Client) uint64 {
	t.Helper()
	var n uint64
	if err := c.pool.QueryRow(t.Context(), "SELECT count(*) FROM transfers").Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestPostgresRules(t *testing.T) {
	for _, v := range []Variant{V1, V2, V3, V4} {
		t.Run(v.String(), func(t *testing.T) {
			t.Run("many lanes", func(t *testing.T) { manyLanes(t, v) })
			t.Run("same id", func(t *testing.T) { sameID(t, v) })
			t.Run("overdraft", func(t *testing.T) { overdraft(t, v) })
		})
	}
}

func manyLanes(t *testing.T, v Variant) {
	const lanes, callers, n = 32, 256, 20_000
	c := dial(t, v, lanes, rules)
	ctx := bounded(t)
	rng := rand.New(rand.NewPCG(uint64(v), 9))
	ops := make([]load.Op, n)
	for i := range ops {
		dr := 1 + rng.Uint64N(16)
		cr := 1 + rng.Uint64N(15)
		if cr >= dr {
			cr++
		}
		ops[i] = load.Op{Kind: load.Single, ID: ledger.Uint128{Hi: uint64(i) + 1, Lo: rng.Uint64()}, Debit: dr, Credit: cr, Amount: 1 + rng.Uint64N(100)}
	}
	began := time.Now()
	outs := make([]load.Outcome, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	for k := range callers {
		wg.Go(func() {
			for i := k; i < n; i += callers {
				outs[i], errs[i] = c.Do(ctx, i%lanes, ops[i])
			}
		})
	}
	wg.Wait()
	if r := sample(t, c).Counters["deadlock_retries"]; r != 0 {
		t.Fatalf("%v: %d retries after a deadlock; accounts are locked in id order", v, r)
	}

	var want load.Books
	retries, gaveUp := 0, 0
	for i, op := range ops {
		retries += outs[i].Retries
		switch {
		case errs[i] == nil && !outs[i].Existed:
			want.Confirmed++
			want.Posted += op.Amount
			if i%20 == 0 {
				want.Sample = append(want.Sample, op.ID)
			}
		case errors.Is(errs[i], ErrGaveUp) && v.serializable():
			gaveUp++
		default:
			t.Fatalf("transfer %d: %+v, %v", i, outs[i], errs[i])
		}
	}
	t.Logf("%v: %d booked, %d retries, %d gave up, %v", v, want.Confirmed, retries, gaveUp, time.Since(began).Round(time.Millisecond))
	if !v.serializable() && retries != 0 {
		t.Fatalf("%v retried %d times; with locks in a fixed order nothing deadlocks or fails to serialize", v, retries)
	}
	if want.Confirmed == 0 || want.Confirmed+uint64(gaveUp) != uint64(n) {
		t.Fatalf("%d booked and %d given up of %d", want.Confirmed, gaveUp, n)
	}
	if got := transfers(t, c); got != want.Confirmed {
		t.Fatalf("%d transfers stored, %d confirmed", got, want.Confirmed)
	}
	booksBalance(t, c)
	if _, err := c.Verify(t.Context(), want); err != nil {
		t.Fatal(err)
	}
	if d := drained(t, c).Counters["deadlocks"]; d != 0 {
		t.Fatalf("%v: Postgres counted %d deadlocks; accounts are locked in id order", v, d)
	}
}

func sameID(t *testing.T, v Variant) {
	c := dial(t, v, 32, rules)
	ctx := bounded(t)
	once := load.Op{Kind: load.Single, ID: ledger.Uint128{Hi: 7, Lo: 7}, Debit: 1, Credit: 2, Amount: 5}
	for k, existed := range []bool{false, true} {
		out, err := c.Do(ctx, 0, once)
		if err != nil || out.Existed != existed {
			t.Fatalf("call %d: %+v, %v; want existed %v", k, out, err, existed)
		}
	}
	twice := once
	twice.ID = ledger.Uint128{Hi: 8, Lo: 8}
	lanes := []int{0, 1, 2, 3, 4, 5, 6, 7, 3, 3, 3, 3, 3, 3, 3, 3}
	outs := make([]load.Outcome, len(lanes))
	errs := make([]error, len(lanes))
	var wg sync.WaitGroup
	for k, lane := range lanes {
		wg.Go(func() { outs[k], errs[k] = c.Do(ctx, lane, twice) })
	}
	wg.Wait()
	fresh := 0
	for k := range lanes {
		if errs[k] != nil {
			t.Fatalf("call %d on lane %d: %v", k, lanes[k], errs[k])
		}
		if !outs[k].Existed {
			fresh++
		}
	}
	if fresh != 1 {
		t.Fatalf("%d of %d calls with the same id booked it", fresh, len(lanes))
	}
	if a := balances(t, c, 1); a.debitsPosted != 10 {
		t.Fatalf("account 1 has %d debits posted, want 10 from two transfers", a.debitsPosted)
	}
	if got := transfers(t, c); got != 2 {
		t.Fatalf("%d transfers stored, want 2", got)
	}
	if _, err := c.Verify(ctx, load.Books{Confirmed: 2, Posted: 10, Sample: []ledger.Uint128{once.ID, twice.ID}}); err != nil {
		t.Fatal(err)
	}
}

func overdraft(t *testing.T, v Variant) {
	const amount, covered, tries = 7, 10, 100
	c := dial(t, v, 32, rules)
	ctx := bounded(t)
	for _, sql := range []string{
		fmt.Sprintf("UPDATE accounts SET credits_posted = %d WHERE id = 1", amount*covered),
		fmt.Sprintf("UPDATE accounts SET debits_posted = debits_posted - %d WHERE id = 17", load.Endowment-amount*covered),
	} {
		if _, err := c.pool.Exec(ctx, sql); err != nil {
			t.Fatal(err)
		}
	}
	errs := make([]error, tries)
	var wg sync.WaitGroup
	for k := range tries {
		op := load.Op{Kind: load.Single, ID: ledger.Uint128{Hi: 9, Lo: uint64(k)}, Debit: 1, Credit: 2, Amount: amount}
		wg.Go(func() { _, errs[k] = c.Do(ctx, k%32, op) })
	}
	wg.Wait()
	ok, gaveUp := 0, 0
	for k, err := range errs {
		switch {
		case err == nil:
			ok++
		case errors.Is(err, ErrGaveUp) && v.serializable():
			gaveUp++
		case !errors.Is(err, ErrRefused) || !strings.Contains(err.Error(), "debits_within_credits"):
			t.Fatalf("transfer %d: %v", k, err)
		}
	}
	t.Logf("%v: %d booked, %d gave up", v, ok, gaveUp)
	if ok != covered {
		t.Fatalf("%d of %d transfers went through, the account covers %d", ok, tries, covered)
	}
	if a := balances(t, c, 1); a.debitsPosted != amount*covered || a.creditsPosted != amount*covered {
		t.Fatalf("account 1 holds %+v", a)
	}
	booksBalance(t, c)
}

func TestTwoPhase(t *testing.T) {
	for _, v := range []Variant{V1, V3} {
		t.Run(v.String(), func(t *testing.T) {
			c := dial(t, v, 32, twoRules)
			ctx := t.Context()
			n := uint64(0)
			op := func(kind load.Kind, amount uint64, pending ledger.Uint128) load.Op {
				n++
				return load.Op{Kind: kind, ID: ledger.Uint128{Hi: 3, Lo: n}, Debit: 1, Credit: 2, Amount: amount, Pending: pending}
			}
			do := func(lane int, o load.Op, existed bool, fails error) {
				t.Helper()
				out, err := c.Do(ctx, lane, o)
				if !errors.Is(err, fails) || fails == nil && (err != nil || out.Existed != existed) {
					t.Fatalf("%v %v: %+v, %v; want existed %v, %v", o.Kind, o.ID, out, err, existed, fails)
				}
			}
			expect := func(a account) {
				t.Helper()
				got := balances(t, c, 1)
				got.creditsPosted -= load.Endowment
				if got != a {
					t.Fatalf("account 1 holds %+v, want %+v", got, a)
				}
			}

			reserve := op(load.Reserve, 30, ledger.Uint128{})
			do(0, reserve, false, nil)
			expect(account{debitsPending: 30})
			post := op(load.Post, 30, reserve.ID)
			do(1, post, false, nil)
			do(2, post, true, nil)
			expect(account{debitsPosted: 30})
			do(3, op(load.Post, 30, reserve.ID), false, ErrRefused)
			do(4, op(load.Void, 30, reserve.ID), false, ErrRefused)
			do(5, op(load.Post, 30, ledger.Uint128{Hi: 99}), false, ErrRefused)

			held := op(load.Reserve, 40, ledger.Uint128{})
			do(6, held, false, nil)
			expect(account{debitsPending: 40, debitsPosted: 30})
			release := op(load.Void, 40, held.ID)
			do(7, release, false, nil)
			expect(account{debitsPosted: 30})

			want := load.Books{Posted: 30, Sample: []ledger.Uint128{reserve.ID, post.ID, held.ID, release.ID}}
			for round := range 20 {
				r := op(load.Reserve, uint64(round+1), ledger.Uint128{})
				do(round%32, r, false, nil)
				settles := []load.Op{op(load.Post, r.Amount, r.ID), op(load.Void, r.Amount, r.ID)}
				lanes := []int{8, 9}
				if round%2 == 0 {
					lanes = []int{10, 10}
				}
				errs := make([]error, 2)
				var wg sync.WaitGroup
				for k := range settles {
					wg.Go(func() { _, errs[k] = c.Do(ctx, lanes[k], settles[k]) })
				}
				wg.Wait()
				switch {
				case errs[0] == nil && errors.Is(errs[1], ErrRefused):
					want.Posted += r.Amount
					want.Sample = append(want.Sample, settles[0].ID)
				case errs[1] == nil && errors.Is(errs[0], ErrRefused):
					want.Sample = append(want.Sample, settles[1].ID)
				default:
					t.Fatalf("round %d: post %v, void %v; want exactly one through", round, errs[0], errs[1])
				}
				want.Sample = append(want.Sample, r.ID)
			}
			booksBalance(t, c)
			if a := balances(t, c, 1); a.debitsPending != 0 || a.debitsPosted != want.Posted {
				t.Fatalf("account 1 holds %+v, want %d posted and nothing pending", a, want.Posted)
			}
			if _, err := c.Verify(ctx, want); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestTwoPhaseOnlyUnderReadCommitted(t *testing.T) {
	for _, v := range []Variant{V2, V4} {
		c, err := Dial(t.Context(), srv, Config{Variant: v, Lanes: 1})
		if err != nil {
			t.Fatal(err)
		}
		if err := c.Setup(t.Context(), twoRules); !errors.Is(err, ErrTwoPhase) {
			t.Fatalf("%v: %v, want ErrTwoPhase", v, err)
		}
		if err := c.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestDurabilityIsChecked(t *testing.T) {
	for _, tc := range []struct {
		commit string
		want   error
	}{
		{"on", nil},
		{"off", ErrNotDurable},
	} {
		cfg, err := pgx.ParseConfig(srv.dsn("postgres"))
		if err != nil {
			t.Fatal(err)
		}
		cfg.RuntimeParams["synchronous_commit"] = tc.commit
		conn, err := pgx.ConnectConfig(t.Context(), cfg)
		if err != nil {
			t.Fatal(err)
		}
		if err := durable(t.Context(), conn); !errors.Is(err, tc.want) || tc.want == nil && err != nil {
			t.Fatalf("synchronous_commit %s: %v, want %v", tc.commit, err, tc.want)
		}
		if err := conn.Close(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
}

func TestVerifyFindsAMissingTransfer(t *testing.T) {
	for _, tc := range []struct {
		name  string
		spoil string
	}{
		{"a sampled transfer is gone", "DELETE FROM transfers WHERE id = (SELECT id FROM transfers ORDER BY id LIMIT 1)"},
		{"a balance moved on its own", "UPDATE accounts SET credits_posted = credits_posted + 1 WHERE id = 3"},
		{"an amount landed twice", "UPDATE accounts SET debits_posted = debits_posted + 5, credits_posted = credits_posted + 5 WHERE id = 4"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := dial(t, V1, 4, rules)
			var want load.Books
			for i := range uint64(50) {
				op := load.Op{Kind: load.Single, ID: ledger.Uint128{Hi: 1, Lo: i}, Debit: 1 + i%16, Credit: 1 + (i+1)%16, Amount: 5}
				if _, err := c.Do(t.Context(), 0, op); err != nil {
					t.Fatal(err)
				}
				want.Confirmed++
				want.Posted += op.Amount
				want.Sample = append(want.Sample, op.ID)
			}
			if _, err := c.Verify(t.Context(), want); err != nil {
				t.Fatal(err)
			}
			if _, err := c.pool.Exec(t.Context(), tc.spoil); err != nil {
				t.Fatal(err)
			}
			if _, err := c.Verify(t.Context(), want); !errors.Is(err, ErrBooks) {
				t.Fatalf("got %v, want ErrBooks", err)
			}
		})
	}
}

func TestSampleCountsWALSyncs(t *testing.T) {
	const n = 200
	c := dial(t, V1, 1, rules)
	before := sample(t, c)
	for i := range uint64(n) {
		op := load.Op{Kind: load.Single, ID: ledger.Uint128{Hi: 2, Lo: i}, Debit: 1 + i%16, Credit: 1 + (i+1)%16, Amount: 1}
		if _, err := c.Do(t.Context(), 0, op); err != nil {
			t.Fatal(err)
		}
	}
	after := drained(t, c)
	syncs, took := after.Syncs-before.Syncs, after.SyncTime-before.SyncTime
	t.Logf("%d transfers one after another: %d WAL syncs in %v, %d requests", n, syncs, took, after.Requests-before.Requests)
	if syncs < n || syncs > 2*n || took <= 0 {
		t.Fatalf("%d transfers one after another made %d WAL syncs taking %v, want one each", n, syncs, took)
	}
	if got := after.Requests - before.Requests; got != n {
		t.Fatalf("%d requests counted, want %d", got, n)
	}
	for _, name := range sampled {
		if _, ok := after.Counters[name]; !ok {
			t.Fatalf("no counter %s in %v", name, after.Counters)
		}
	}
	grew := func(name string) uint64 { return after.Counters[name] - before.Counters[name] }
	t.Logf("inserted %d, accounts updated %d (%d HOT), transfers updated %d", grew("inserted"), grew("accounts_updates"), grew("accounts_hot_updates"), grew("transfers_updates"))
	if grew("inserted") != n || grew("accounts_updates") != 2*n || grew("transfers_updates") != 0 || grew("accounts_hot_updates") > grew("accounts_updates") {
		t.Fatalf("%d transfers counted as %d inserted, %d account updates (%d HOT), %d transfer updates; want %d, %d, at most all, 0",
			n, grew("inserted"), grew("accounts_updates"), grew("accounts_hot_updates"), grew("transfers_updates"), n, 2*n)
	}
}

func TestSampleKeepsSegmentInitApart(t *testing.T) {
	const switches = 16
	c := dial(t, V1, 1, rules)
	before := sample(t, c)
	for range switches {
		for _, sql := range []string{"SELECT pg_current_xact_id()", "SELECT pg_switch_wal()"} {
			if _, err := c.pool.Exec(t.Context(), sql); err != nil {
				t.Fatal(err)
			}
		}
	}
	after := drained(t, c)
	created := after.Counters["wal_init_syncs"] - before.Counters["wal_init_syncs"]
	t.Logf("%d segment switches: %d syncs of new segments, %d on the commit path", switches, created, after.Syncs-before.Syncs)
	if created == 0 {
		t.Fatalf("%d segment switches synced no new segment", switches)
	}
	const io = "SELECT sum(fsyncs) FILTER (WHERE context = 'normal')::bigint, sum(fsyncs) FILTER (WHERE context = 'init')::bigint FROM pg_stat_io WHERE object = 'wal'"
	for range 20 {
		var normal, segs, normalAgain, segsAgain uint64
		if err := c.probe.QueryRow(t.Context(), io).Scan(&normal, &segs); err != nil {
			t.Fatal(err)
		}
		got := sample(t, c)
		if err := c.probe.QueryRow(t.Context(), io).Scan(&normalAgain, &segsAgain); err != nil {
			t.Fatal(err)
		}
		if normal != normalAgain || segs != segsAgain {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		if got.Syncs != normal || got.Counters["wal_init_syncs"] != segs {
			t.Fatalf("sampled %d syncs and %d syncs of new segments, pg_stat_io holds %d in context normal and %d in init", got.Syncs, got.Counters["wal_init_syncs"], normal, segs)
		}
		return
	}
	t.Fatal("pg_stat_io never held still")
}

func TestSampleCountsDeadlocks(t *testing.T) {
	c := dial(t, V1, 1, rules)
	ctx := bounded(t)
	before := sample(t, c)
	txs := make([]pgx.Tx, 2)
	for k := range txs {
		tx, err := c.pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
		if _, err := tx.Exec(ctx, "UPDATE accounts SET code = code WHERE id = $1", k+1); err != nil {
			t.Fatal(err)
		}
		txs[k] = tx
	}
	errs := make([]error, 2)
	var wg sync.WaitGroup
	for k, tx := range txs {
		wg.Go(func() { _, errs[k] = tx.Exec(ctx, "UPDATE accounts SET code = code WHERE id = $1", 2-k) })
	}
	wg.Wait()
	failed := 0
	for _, err := range errs {
		var pe *pgconn.PgError
		if errors.As(err, &pe) && pe.Code == deadlockDetected {
			failed++
		}
	}
	if failed != 1 {
		t.Fatalf("crossed updates gave %v, want one deadlock", errs)
	}
	for _, tx := range txs {
		_ = tx.Rollback(ctx)
	}
	after := drained(t, c)
	if got := after.Counters["deadlocks"] - before.Counters["deadlocks"]; got != 1 {
		t.Fatalf("one deadlock sampled as %d", got)
	}
}

func TestSingleTransfersShareAPoolOf32(t *testing.T) {
	for _, tc := range []struct {
		v            Variant
		lanes, conns int
	}{{V1, 4, 32}, {V2, 4, 32}, {V1, 64, 32}, {V3, 4, 4}, {V4, 4, 4}} {
		t.Run(fmt.Sprintf("%v %d lanes", tc.v, tc.lanes), func(t *testing.T) {
			c := dial(t, tc.v, tc.lanes, rules)
			if got := c.pool.Stat().TotalConns(); int(got) != tc.conns {
				t.Fatalf("%d connections open, want %d", got, tc.conns)
			}
			if got := c.Describe().Settings["connections"]; got != strconv.Itoa(tc.conns) {
				t.Fatalf("described with %q connections, want %d", got, tc.conns)
			}
		})
	}
}

func TestSettleLeavesKeyShareFree(t *testing.T) {
	for _, v := range []Variant{V1, V3} {
		t.Run(v.String(), func(t *testing.T) {
			c := dial(t, v, 4, twoRules)
			ctx := bounded(t)
			reserve := load.Op{Kind: load.Reserve, ID: ledger.Uint128{Hi: 4, Lo: 1}, Debit: 1, Credit: 2, Amount: 3}
			if _, err := c.Do(ctx, 0, reserve); err != nil {
				t.Fatal(err)
			}
			post := load.Op{Kind: load.Post, ID: ledger.Uint128{Hi: 4, Lo: 2}, Debit: 1, Credit: 2, Amount: 3, Pending: reserve.ID}
			sql, args := "SELECT settle_locked($1, $2, $3)", []any{key(post.ID), key(post.Pending), true}
			if v.batched() {
				sql, args = c.batchCall([]load.Op{post})
			}
			tx, err := c.pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()
			if _, err := tx.Exec(ctx, sql, args...); err != nil {
				t.Fatal(err)
			}
			var one int
			if err := c.pool.QueryRow(ctx, "SELECT 1 FROM transfers WHERE id = $1 FOR NO KEY UPDATE NOWAIT", key(reserve.ID)).Scan(&one); err == nil {
				t.Fatal("a second settle could lock the reservation while the first is open")
			}
			if err := c.pool.QueryRow(ctx, "SELECT 1 FROM transfers WHERE id = $1 FOR KEY SHARE NOWAIT", key(reserve.ID)).Scan(&one); err != nil {
				t.Fatalf("a key share lock on the reservation has to wait for the settle: %v", err)
			}
		})
	}
}

func TestBatchSettlesWithTheFirstOfTwins(t *testing.T) {
	const n = 40
	c := dial(t, V3, 1, twoRules)
	ctx := bounded(t)
	reserves := make([]load.Op, n)
	ids := make([][16]byte, n)
	for i := range reserves {
		k := uint64(i)
		reserves[i] = load.Op{Kind: load.Reserve, ID: ledger.Uint128{Hi: 5, Lo: k}, Debit: 1 + k%8, Credit: 9 + k%8, Amount: k + 1}
		ids[i] = key(reserves[i].ID)
	}
	for i, a := range c.batch(ctx, reserves) {
		if a.err != nil || a.out.Existed {
			t.Fatalf("reserve %d: %+v, %v", i, a.out, a.err)
		}
	}
	rng := rand.New(rand.NewPCG(4, 4))
	var settles []load.Op
	for i, r := range reserves {
		for _, kind := range []load.Kind{load.Post, load.Void} {
			settles = append(settles, load.Op{Kind: kind, ID: ledger.Uint128{Hi: 6, Lo: uint64(i)}, Debit: r.Debit, Credit: r.Credit, Amount: r.Amount, Pending: r.ID})
		}
	}
	rng.Shuffle(len(settles), func(i, j int) { settles[i], settles[j] = settles[j], settles[i] })
	first := map[ledger.Uint128]load.Kind{}
	var posted uint64
	for _, op := range settles {
		if _, seen := first[op.Pending]; !seen {
			first[op.Pending] = op.Kind
			if op.Kind == load.Post {
				posted += op.Amount
			}
		}
	}
	answered := map[ledger.Uint128]bool{}
	for i, a := range c.batch(ctx, settles) {
		if a.err != nil || a.out.Existed != answered[settles[i].ID] {
			t.Fatalf("settle %d (%v of %v): %+v, %v", i, settles[i].Kind, settles[i].Pending, a.out, a.err)
		}
		answered[settles[i].ID] = true
	}
	rows, err := c.pool.Query(ctx, "SELECT id, state FROM transfers WHERE id = ANY ($1)", ids)
	if err != nil {
		t.Fatal(err)
	}
	var id [16]byte
	var state int16
	wrong := 0
	_, err = pgx.ForEachRow(rows, []any{&id, &state}, func() error {
		r := ledger.Uint128{Hi: binary.BigEndian.Uint64(id[:8]), Lo: binary.BigEndian.Uint64(id[8:])}
		if want := map[load.Kind]int16{load.Post: 2, load.Void: 3}[first[r]]; state != want {
			wrong++
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	var debits, pending uint64
	if err := c.pool.QueryRow(ctx, "SELECT sum(debits_posted)::bigint, sum(debits_pending + credits_pending)::bigint FROM accounts WHERE id <= 16").Scan(&debits, &pending); err != nil {
		t.Fatal(err)
	}
	if wrong != 0 || debits != posted || pending != 0 {
		t.Fatalf("%d of %d reservations closed against the first settle in the batch, %d posted, want %d, %d still pending", n-wrong, n, debits, posted, pending)
	}
	booksBalance(t, c)
}

func TestFsyncIsMeasured(t *testing.T) {
	out, err := srv.TestFsync(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	for line := range strings.Lines(out) {
		if strings.Contains(line, "fdatasync") && strings.Contains(line, "ops/sec") {
			t.Logf("%s", strings.TrimSpace(line))
			return
		}
	}
	t.Fatalf("no line for fdatasync in\n%s", out)
}

func TestConfigurationIsApplied(t *testing.T) {
	c, err := Dial(t.Context(), srv, Config{Variant: V3, Lanes: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	d := c.Describe()
	t.Logf("%s", d.Version)
	if d.Kind != "postgres" || d.Variant != "V3" || d.Lanes != 4 || d.Cores != Cores || !strings.HasPrefix(d.Version, "PostgreSQL 18.") {
		t.Fatalf("described as %+v", d)
	}
	for name, want := range map[string]string{
		"listen_addresses":    "127.0.0.1",
		"port":                "55439",
		"shared_buffers":      "131072 8kB",
		"max_wal_size":        "8192 MB",
		"checkpoint_timeout":  "900 s",
		"wal_sync_method":     "fdatasync",
		"track_wal_io_timing": "on",
	} {
		if got := d.Settings[name]; got != want {
			t.Errorf("%s is %q, want %q", name, got, want)
		}
	}
	for _, name := range []string{"fsync", "synchronous_commit", "full_page_writes"} {
		if got, ok := d.Settings[name]; ok {
			t.Errorf("%s is set to %q, want the default", name, got)
		}
	}
	if _, err := os.Stat(filepath.Join("/sys/fs/cgroup", srv.Cgroup(), "cpu.stat")); err != nil {
		t.Errorf("no CPU time for the container: %v", err)
	}
}
