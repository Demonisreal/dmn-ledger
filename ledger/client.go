package ledger

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/Demonisreal/dmn-ledger/internal/client"
	"github.com/Demonisreal/dmn-ledger/internal/u128"
)

var (
	ErrUncertain = errors.New("ledger: outcome unknown")
	ErrEvicted   = errors.New("ledger: session evicted")
	ErrClosed    = errors.New("ledger: client closed")
	ErrTooLarge  = errors.New("ledger: batch too large")
	ErrRejected  = errors.New("ledger: batch rejected")
)

const (
	tickEvery   = 20 * time.Millisecond
	dialTimeout = time.Second
	redialAfter = 250 * time.Millisecond
)

type Config struct {
	Cluster uint64
	Nodes   []string
	Timeout time.Duration
}

type dialFunc func(ctx context.Context, addr string) (net.Conn, error)

type Client struct {
	cfg   Config
	id    u128.Uint128
	dial  dialFunc
	stop  context.CancelFunc
	wg    sync.WaitGroup
	ended <-chan struct{}

	calls    chan *call
	withdraw chan *call
	replies  chan arrival
	losses   chan loss

	sess    *client.Session
	tick    *time.Ticker
	conns   []*conn
	down    []time.Time
	queue   []*call
	batch   []*call
	last    uint64
	heard   bool
	evicted bool
}

type call struct {
	ctx     context.Context
	op      client.Op
	body    []byte
	records int
	linked  bool
	off     int
	done    chan outcome
	gone    bool
}

type outcome struct {
	res  Result
	body []byte
	err  error
}

func Dial(ctx context.Context, cfg Config) (*Client, error) {
	return open(ctx, cfg, NewID(), dialTCP)
}

func dialTCP(ctx context.Context, addr string) (net.Conn, error) {
	var d net.Dialer
	return d.DialContext(ctx, "tcp", addr)
}

func open(ctx context.Context, cfg Config, id Uint128, dial dialFunc) (*Client, error) {
	if cfg.Cluster == 0 || len(cfg.Nodes) == 0 {
		return nil, errors.New("ledger: config needs a cluster ID and at least one node")
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = time.Second
	}
	nodes := make([]uint64, len(cfg.Nodes))
	for i := range nodes {
		nodes[i] = uint64(i) + 1
	}
	loop, stop := context.WithCancel(context.Background())
	c := &Client{
		cfg:      cfg,
		id:       u128.Uint128(id),
		dial:     dial,
		stop:     stop,
		ended:    loop.Done(),
		calls:    make(chan *call),
		withdraw: make(chan *call),
		replies:  make(chan arrival),
		losses:   make(chan loss),
		sess:     client.New(u128.Uint128(id), nodes, max(1, int(cfg.Timeout/tickEvery))),
		conns:    make([]*conn, len(cfg.Nodes)),
		down:     make([]time.Time, len(cfg.Nodes)),
	}
	register := &call{ctx: ctx, op: client.Register, done: make(chan outcome, 1)}
	c.batch = []*call{register}
	c.wg.Go(func() { c.run(loop) })
	if o := c.wait(ctx, register); o.err != nil {
		_ = c.Close()
		return nil, o.err
	}
	return c, nil
}

func (c *Client) CreateAccounts(ctx context.Context, accounts []Account) (Result, error) {
	if len(accounts) > maxAccounts {
		return Result{}, fmt.Errorf("%w: %d accounts, at most %d", ErrTooLarge, len(accounts), maxAccounts)
	}
	cl := &call{op: client.CreateAccounts, records: len(accounts)}
	for _, a := range accounts {
		cl.body = appendAccount(cl.body, a)
	}
	o := c.do(ctx, cl)
	return o.res, o.err
}

func (c *Client) CreateTransfers(ctx context.Context, transfers []Transfer) (Result, error) {
	if len(transfers) > maxTransfers {
		return Result{}, fmt.Errorf("%w: %d transfers, at most %d", ErrTooLarge, len(transfers), maxTransfers)
	}
	cl := &call{op: client.CreateTransfers, records: len(transfers)}
	for _, tr := range transfers {
		cl.body = appendTransfer(cl.body, tr)
	}
	cl.linked = len(transfers) > 0 && transfers[len(transfers)-1].Flags&Linked != 0
	o := c.do(ctx, cl)
	return o.res, o.err
}

func (c *Client) LookupAccounts(ctx context.Context, ids []Uint128) ([]Account, error) {
	if len(ids) > maxAccounts {
		return nil, fmt.Errorf("%w: %d IDs, at most %d accounts", ErrTooLarge, len(ids), maxAccounts)
	}
	o := c.do(ctx, &call{op: client.LookupAccounts, body: appendIDs(nil, ids)})
	if o.err != nil {
		return nil, o.err
	}
	return decodeAccounts(o.body), nil
}

func (c *Client) LookupTransfers(ctx context.Context, ids []Uint128) ([]Transfer, error) {
	if len(ids) > maxTransfers {
		return nil, fmt.Errorf("%w: %d IDs, at most %d transfers", ErrTooLarge, len(ids), maxTransfers)
	}
	o := c.do(ctx, &call{op: client.LookupTransfers, body: appendIDs(nil, ids)})
	if o.err != nil {
		return nil, o.err
	}
	return decodeTransfers(o.body), nil
}

func (c *Client) Close() error {
	c.stop()
	c.wg.Wait()
	return nil
}

func (c *Client) do(ctx context.Context, cl *call) outcome {
	cl.ctx, cl.done = ctx, make(chan outcome, 1)
	select {
	case c.calls <- cl:
	case <-ctx.Done():
		return outcome{err: ctx.Err()}
	case <-c.ended:
		return outcome{err: ErrClosed}
	}
	return c.wait(ctx, cl)
}

func (c *Client) wait(ctx context.Context, cl *call) outcome {
	select {
	case o := <-cl.done:
		return o
	case <-ctx.Done():
	}
	select {
	case c.withdraw <- cl:
	case <-c.ended:
	}
	return <-cl.done
}

func (cl *call) answer(o outcome) {
	if cl.gone {
		return
	}
	cl.gone = true
	cl.done <- o
}

func (cl *call) unsure(cause error) error {
	if cl.op == client.CreateAccounts || cl.op == client.CreateTransfers {
		return fmt.Errorf("%w: %w", ErrUncertain, cause)
	}
	return cause
}
