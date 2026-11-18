package pg

import (
	"context"
	_ "embed"
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/Demonisreal/dmn-ledger/bench/load"
	"github.com/Demonisreal/dmn-ledger/bench/raw"
	"github.com/Demonisreal/dmn-ledger/ledger"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrNotDurable = errors.New("pg: not durable")
	ErrTwoPhase   = errors.New("pg: two-phase transfers only under read committed")
	ErrBooks      = errors.New("pg: books do not add up")
)

const (
	runDB       = "bench"
	singleConns = 32
)

//go:embed schema.sql
var schema string

type Variant uint8

const (
	V1 Variant = iota + 1
	V2
	V3
	V4
)

func (v Variant) String() string {
	return fmt.Sprintf("V%d", uint8(v))
}

func (v Variant) batched() bool {
	return v == V3 || v == V4
}

func (v Variant) serializable() bool {
	return v == V2 || v == V4
}

type Config struct {
	Variant Variant
	Lanes   int
}

func (cfg Config) conns() int {
	if cfg.Variant.batched() {
		return cfg.Lanes
	}
	return singleConns
}

type Client struct {
	srv      *Server
	cfg      Config
	admin    *pgx.Conn
	version  string
	settings map[string]string

	w        load.Workload
	pool     *pgxpool.Pool
	probe    *pgx.Conn
	bundlers []*bundler
	stop     context.CancelFunc
	loops    sync.WaitGroup
	requests atomic.Uint64
	retried  retried
	mu       sync.Mutex
	books    load.Books
}

func Dial(ctx context.Context, srv *Server, cfg Config) (*Client, error) {
	if cfg.Variant < V1 || cfg.Variant > V4 || cfg.Lanes < 1 {
		return nil, fmt.Errorf("pg: variant %v and %d lanes, want V1 to V4 and at least one lane", cfg.Variant, cfg.Lanes)
	}
	admin, err := pgx.Connect(ctx, srv.dsn("postgres"))
	if err != nil {
		return nil, err
	}
	c := &Client{srv: srv, cfg: cfg, admin: admin, settings: map[string]string{}}
	if err := c.describe(ctx); err != nil {
		_ = admin.Close(ctx)
		return nil, err
	}
	c.settings["connections"] = strconv.Itoa(cfg.conns())
	return c, nil
}

func (c *Client) describe(ctx context.Context) error {
	if err := durable(ctx, c.admin); err != nil {
		return err
	}
	if err := c.admin.QueryRow(ctx, "SELECT version()").Scan(&c.version); err != nil {
		return err
	}
	rows, err := c.admin.Query(ctx, "SELECT name, setting || coalesce(' ' || unit, '') FROM pg_settings WHERE source <> 'default'")
	if err != nil {
		return err
	}
	var name, setting string
	_, err = pgx.ForEachRow(rows, []any{&name, &setting}, func() error {
		c.settings[name] = setting
		return nil
	})
	return err
}

func durable(ctx context.Context, conn *pgx.Conn) error {
	var fsync, commit, pages string
	err := conn.QueryRow(ctx, "SELECT current_setting('fsync'), current_setting('synchronous_commit'), current_setting('full_page_writes')").
		Scan(&fsync, &commit, &pages)
	if err != nil {
		return err
	}
	if fsync != "on" || commit != "on" || pages != "on" {
		return fmt.Errorf("%w: fsync %s, synchronous_commit %s, full_page_writes %s", ErrNotDurable, fsync, commit, pages)
	}
	return nil
}

func (c *Client) Setup(ctx context.Context, w load.Workload) error {
	if w.TwoPhase && c.cfg.Variant.serializable() {
		return fmt.Errorf("%w: %v", ErrTwoPhase, c.cfg.Variant)
	}
	c.w = w
	c.mu.Lock()
	c.books = load.Books{}
	c.mu.Unlock()
	tpl := "tpl_" + strings.ReplaceAll(w.Name, "-", "_")
	var exists bool
	if err := c.admin.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)", tpl).Scan(&exists); err != nil {
		return err
	}
	if !exists {
		if err := c.template(ctx, tpl); err != nil {
			_, dropped := c.admin.Exec(ctx, "DROP DATABASE IF EXISTS "+pgx.Identifier{tpl}.Sanitize()+" WITH (FORCE)")
			return errors.Join(fmt.Errorf("pg: template %s: %w", tpl, err), dropped)
		}
	}
	for _, sql := range []string{
		"DROP DATABASE IF EXISTS " + runDB + " WITH (FORCE)",
		"CREATE DATABASE " + runDB + " TEMPLATE " + pgx.Identifier{tpl}.Sanitize() + " STRATEGY = FILE_COPY",
		"CHECKPOINT",
	} {
		if _, err := c.admin.Exec(ctx, sql); err != nil {
			return err
		}
	}
	return c.open(ctx)
}

func (c *Client) template(ctx context.Context, tpl string) error {
	if _, err := c.admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{tpl}.Sanitize()); err != nil {
		return err
	}
	if err := c.fill(ctx, tpl); err != nil {
		return err
	}
	conn, err := pgx.Connect(ctx, c.srv.dsn(tpl))
	if err != nil {
		return err
	}
	defer conn.Close(ctx)
	_, err = conn.Exec(ctx, "VACUUM (FREEZE, ANALYZE)")
	return err
}

func (c *Client) fill(ctx context.Context, tpl string) error {
	conn, err := pgx.Connect(ctx, c.srv.dsn(tpl))
	if err != nil {
		return err
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, schema); err != nil {
		return err
	}
	bank := c.w.Bank()
	id := uint64(0)
	_, err = conn.CopyFrom(ctx, pgx.Identifier{"accounts"},
		[]string{"id", "ledger", "code", "flags", "debits_posted", "credits_posted"},
		pgx.CopyFromFunc(func() ([]any, error) {
			id++
			switch {
			case id < bank:
				return []any{id, int32(load.Ledger), int16(load.Code), int16(ledger.DebitsMustNotExceedCredits), uint64(0), uint64(load.Endowment)}, nil
			case id == bank:
				return []any{id, int32(load.Ledger), int16(load.Code), int16(0), c.w.Endowment(), uint64(0)}, nil
			}
			return nil, nil
		}))
	if err != nil {
		return err
	}
	_, err = conn.Exec(ctx, `INSERT INTO transfers (id, debit_account_id, credit_account_id, amount, ledger, code)
        SELECT ('00000000-0000-0000-0000-' || lpad(to_hex(id), 12, '0'))::uuid, $1::bigint, id, $2::bigint, $3::integer, $4::smallint
        FROM accounts WHERE id < $1::bigint ORDER BY id`, int64(bank), int64(load.Endowment), int32(load.Ledger), int16(load.Code)) //nolint:gosec // G115: a count of accounts
	if err != nil {
		return err
	}
	_, err = conn.Exec(ctx, "CREATE EXTENSION pg_prewarm")
	return err
}

func (c *Client) open(ctx context.Context) error {
	cfg, err := pgxpool.ParseConfig(c.srv.dsn(runDB))
	if err != nil {
		return err
	}
	isolation := "read committed"
	if c.cfg.Variant.serializable() {
		isolation = "serializable"
	}
	cfg.ConnConfig.RuntimeParams["default_transaction_isolation"] = isolation
	cfg.MaxConns = int32(c.cfg.conns()) //nolint:gosec // G115: a handful of lanes or 32
	cfg.MinConns = cfg.MaxConns
	cfg.AfterConnect = durable
	if c.pool, err = pgxpool.NewWithConfig(ctx, cfg); err != nil {
		return err
	}
	conns := make([]*pgxpool.Conn, 0, cfg.MaxConns)
	defer func() {
		for _, conn := range conns {
			conn.Release()
		}
	}()
	for range cfg.MaxConns {
		conn, err := c.pool.Acquire(ctx)
		if err != nil {
			return err
		}
		conns = append(conns, conn)
	}
	if _, err := conns[0].Exec(ctx, "SELECT pg_prewarm('accounts'), pg_prewarm('accounts_pkey')"); err != nil {
		return err
	}
	if c.probe, err = pgx.Connect(ctx, c.srv.dsn(runDB)); err != nil {
		return err
	}
	if c.cfg.Variant.batched() {
		loops, stop := context.WithCancel(context.WithoutCancel(ctx))
		c.stop = stop
		for range c.cfg.Lanes {
			b := newBundler(c.batch)
			c.bundlers = append(c.bundlers, b)
			c.loops.Go(func() { b.run(loops) })
		}
	}
	return nil
}

func (c *Client) Do(ctx context.Context, lane int, op load.Op) (load.Outcome, error) {
	out, err := c.book(ctx, lane, op)
	if rolledBack(err) {
		return out, err
	}
	c.mu.Lock()
	c.books.Count(op, out, err)
	c.mu.Unlock()
	return out, err
}

func (c *Client) book(ctx context.Context, lane int, op load.Op) (load.Outcome, error) {
	if c.bundlers != nil {
		return c.bundlers[lane].do(ctx, op)
	}
	return c.one(ctx, op)
}

func rolledBack(err error) bool {
	return errors.Is(err, ErrRefused) || errors.Is(err, ErrGaveUp)
}

func (c *Client) one(ctx context.Context, op load.Op) (load.Outcome, error) {
	sql, args := "SELECT post_locked($1, $2, $3, $4, $5, $6)", []any{key(op.ID), op.Debit, op.Credit, op.Amount, int32(load.Ledger), int16(load.Code)}
	switch op.Kind {
	case load.Reserve:
		sql = "SELECT reserve_locked($1, $2, $3, $4, $5, $6)"
	case load.Post, load.Void:
		sql, args = "SELECT settle_locked($1, $2, $3)", []any{key(op.ID), key(op.Pending), op.Kind == load.Post}
	}
	var fresh bool
	retries, err := c.retried.retry(ctx, func() error {
		return c.pool.QueryRow(ctx, sql, args...).Scan(&fresh)
	})
	switch {
	case refused(err):
		return load.Outcome{Retries: retries}, fmt.Errorf("%w: %s %v: %w", ErrRefused, op.Kind, op.ID, err)
	case err != nil:
		return load.Outcome{Retries: retries}, err
	}
	c.requests.Add(1)
	return load.Outcome{Existed: !fresh, Retries: retries}, nil
}

func (c *Client) batch(ctx context.Context, ops []load.Op) []answer {
	sql, args := c.batchCall(ops)
	var fresh map[[16]byte]bool
	retries, err := c.retried.retry(ctx, func() error {
		rows, err := c.pool.Query(ctx, sql, args...)
		if err != nil {
			return err
		}
		ids, err := pgx.CollectRows(rows, pgx.RowTo[[16]byte])
		if err != nil {
			return err
		}
		fresh = make(map[[16]byte]bool, len(ids))
		for _, id := range ids {
			fresh[id] = true
		}
		return nil
	})
	answers := make([]answer, len(ops))
	switch {
	case refused(err):
		for i, op := range ops {
			answers[i].out, answers[i].err = c.one(ctx, op)
			answers[i].out.Retries += retries
		}
	case err != nil:
		for i := range answers {
			answers[i] = answer{out: load.Outcome{Retries: retries}, err: fmt.Errorf("pg: batch of %d: %w", len(ops), err)}
		}
	default:
		c.requests.Add(1)
		for i, op := range ops {
			id := key(op.ID)
			answers[i].out = load.Outcome{Existed: !fresh[id], Retries: retries}
			delete(fresh, id)
		}
	}
	return answers
}

func (c *Client) batchCall(ops []load.Op) (string, []any) {
	tids := make([][16]byte, len(ops))
	drs := make([]uint64, len(ops))
	crs := make([]uint64, len(ops))
	amts := make([]uint64, len(ops))
	for i, op := range ops {
		tids[i], drs[i], crs[i], amts[i] = key(op.ID), op.Debit, op.Credit, op.Amount
	}
	if !twoPhase(ops[0]) {
		return "SELECT post_batch_locked($1, $2, $3, $4, $5, $6)", []any{tids, drs, crs, amts, int32(load.Ledger), int16(load.Code)}
	}
	pids := make([]pgtype.UUID, len(ops))
	posts := make([]bool, len(ops))
	for i, op := range ops {
		if op.Kind != load.Reserve {
			pids[i] = pgtype.UUID{Bytes: key(op.Pending), Valid: true}
		}
		posts[i] = op.Kind == load.Post
	}
	return "SELECT twophase_batch_locked($1, $2, $3, $4, $5, $6, $7, $8)",
		[]any{tids, pids, drs, crs, amts, posts, int32(load.Ledger), int16(load.Code)}
}

func key(id ledger.Uint128) [16]byte {
	var b [16]byte
	binary.BigEndian.PutUint64(b[:8], id.Hi)
	binary.BigEndian.PutUint64(b[8:], id.Lo)
	return b
}

func (c *Client) Describe() raw.Target {
	return raw.Target{Kind: "postgres", Variant: c.cfg.Variant.String(), Lanes: c.cfg.Lanes, Cores: Cores, Version: c.version, Settings: c.settings}
}

func (c *Client) Close() error {
	if c.stop != nil {
		c.stop()
		c.loops.Wait()
	}
	var errs []error
	if c.probe != nil {
		errs = append(errs, c.probe.Close(context.Background()))
	}
	if c.pool != nil {
		c.pool.Close()
	}
	ctx := context.Background()
	for _, sql := range []string{"DROP DATABASE IF EXISTS " + runDB + " WITH (FORCE)", "CHECKPOINT"} {
		if _, err := c.admin.Exec(ctx, sql); err != nil {
			errs = append(errs, err)
			break
		}
	}
	return errors.Join(append(errs, c.admin.Close(ctx))...)
}
