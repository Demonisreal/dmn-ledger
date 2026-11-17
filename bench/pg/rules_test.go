//go:build pg

package pg

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Demonisreal/dmn-ledger/bench/load"
	"github.com/Demonisreal/dmn-ledger/ledger"
	"github.com/jackc/pgx/v5"
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
	const lanes, callers = 32, 256
	n := 20_000
	if v == V2 {
		n = 2_000
	}
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
	before, err := c.Sample(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	for i := range uint64(n) {
		op := load.Op{Kind: load.Single, ID: ledger.Uint128{Hi: 2, Lo: i}, Debit: 1 + i%16, Credit: 1 + (i+1)%16, Amount: 1}
		if _, err := c.Do(t.Context(), 0, op); err != nil {
			t.Fatal(err)
		}
	}
	var after load.Inside
	for deadline := time.Now().Add(15 * time.Second); time.Now().Before(deadline); time.Sleep(500 * time.Millisecond) {
		if after, err = c.Sample(t.Context()); err != nil {
			t.Fatal(err)
		}
		if after[0].Syncs-before[0].Syncs >= n {
			break
		}
	}
	syncs, took := after[0].Syncs-before[0].Syncs, after[0].SyncTime-before[0].SyncTime
	t.Logf("%d transfers one after another: %d WAL syncs in %v, %d requests", n, syncs, took, after[0].Requests-before[0].Requests)
	if syncs < n || syncs > 2*n || took <= 0 {
		t.Fatalf("%d transfers one after another made %d WAL syncs taking %v, want one each", n, syncs, took)
	}
	if got := after[0].Requests - before[0].Requests; got != n {
		t.Fatalf("%d requests counted, want %d", got, n)
	}
	for _, name := range []string{"checkpoints", "wal_bytes", "wal_fpi", "autovacuum"} {
		if _, ok := after[0].Counters[name]; !ok {
			t.Fatalf("no counter %s in %v", name, after[0].Counters)
		}
	}
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
