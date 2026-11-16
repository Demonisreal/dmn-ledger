package ledger

import (
	"context"
	"encoding/binary"
	"errors"
	"net"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Demonisreal/dmn-ledger/internal/client"
	"github.com/Demonisreal/dmn-ledger/internal/state"
	"github.com/Demonisreal/dmn-ledger/internal/u128"
	"github.com/Demonisreal/dmn-ledger/internal/wire"
)

const (
	cluster  = 7
	session  = 42
	patience = 20 * time.Second
)

var (
	alice      = Uint128{Hi: 7, Lo: 3}
	errRefused = errors.New("connection refused")
)

type fake struct {
	t      *testing.T
	addrs  []string
	reqs   chan request
	dials  chan dialing
	refuse func(node uint64) bool
}

type request struct {
	node uint64
	req  client.Request
	nc   net.Conn
	at   time.Time
}

type dialing struct {
	node uint64
	at   time.Time
}

type created struct {
	res Result
	err error
}

func newFake(t *testing.T) *fake {
	return &fake{
		t:     t,
		addrs: []string{"n1:7101", "n2:7101", "n3:7101"},
		reqs:  make(chan request, 64),
		dials: make(chan dialing, 64),
	}
}

func (f *fake) config() Config {
	return Config{Cluster: cluster, Nodes: f.addrs, Timeout: time.Second}
}

func (f *fake) dial(_ context.Context, addr string) (net.Conn, error) {
	node := uint64(slices.Index(f.addrs, addr)) + 1
	select {
	case f.dials <- dialing{node: node, at: time.Now()}:
	default:
		return nil, errors.New("too many dials")
	}
	if f.refuse != nil && f.refuse(node) {
		return nil, errRefused
	}
	mine, theirs := net.Pipe()
	go f.serve(node, theirs)
	return mine, nil
}

func (f *fake) serve(node uint64, nc net.Conn) {
	defer nc.Close()
	for {
		frame, _, err := wire.ReadFrame(nc, nil, cluster, wire.MaxClientBody)
		if err != nil {
			return
		}
		req, err := frame.Request()
		if err != nil {
			f.t.Errorf("node %d got a frame that is not a request: %v", node, err)
			return
		}
		select {
		case f.reqs <- request{node: node, req: req, nc: nc, at: time.Now()}:
		default:
			f.t.Errorf("node %d: more than %d requests nobody took", node, cap(f.reqs))
			return
		}
	}
}

func (f *fake) next(t *testing.T) request {
	t.Helper()
	select {
	case g := <-f.reqs:
		return g
	case <-time.After(patience):
		t.Fatalf("no request within %v", patience)
	}
	return request{}
}

func await[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case got := <-ch:
		return got
	case <-time.After(patience):
		t.Fatalf("no answer within %v", patience)
	}
	var zero T
	return zero
}

func (f *fake) quiet(t *testing.T, d time.Duration) {
	t.Helper()
	time.Sleep(d)
	if len(f.reqs) > 0 {
		g := <-f.reqs
		t.Fatalf("node %d got %v #%d, want nothing", g.node, g.req.Op, g.req.Number)
	}
}

func (g request) reply(t *testing.T, r client.Reply) {
	t.Helper()
	r.Client, r.Number, r.Op = g.req.Client, g.req.Number, g.req.Op
	if _, err := g.nc.Write(wire.AppendReply(nil, cluster, r)); err != nil {
		t.Fatalf("reply from node %d: %v", g.node, err)
	}
}

func (g request) ok(t *testing.T) {
	t.Helper()
	g.reply(t, client.Reply{Status: client.OK, Leader: g.node})
}

func (g request) transfers() []uint64 {
	var ids []uint64
	for b := g.req.Body; len(b) >= state.TransferSize; b = b[state.TransferSize:] {
		ids = append(ids, state.DecodeTransfer(b).ID.Lo)
	}
	return ids
}

func dialed(t *testing.T, f *fake) *Client {
	t.Helper()
	opened := make(chan *Client, 1)
	go func() {
		c, err := open(context.Background(), f.config(), alice, f.dial)
		if err != nil {
			t.Errorf("dial: %v", err)
		}
		opened <- c
	}()
	g := f.next(t)
	if g.node != 1 || g.req.Op != client.Register || g.req.Number != 0 {
		t.Fatalf("first request %v #%d to node %d, want Register #0 to node 1", g.req.Op, g.req.Number, g.node)
	}
	g.reply(t, client.Reply{Status: client.OK, Leader: 1, Body: binary.BigEndian.AppendUint64(nil, session)})
	c := await(t, opened)
	if c == nil {
		t.FailNow()
	}
	return c
}

func transfer(id uint64, flags TransferFlags) Transfer {
	return Transfer{
		ID:              From64(id),
		DebitAccountID:  From64(1),
		CreditAccountID: From64(2),
		Amount:          From64(id),
		Ledger:          1,
		Code:            1,
		Flags:           flags,
	}
}

func transfers(ids ...uint64) []Transfer {
	var trs []Transfer
	for _, id := range ids {
		trs = append(trs, transfer(id, 0))
	}
	return trs
}

func goCreate(ctx context.Context, c *Client, trs []Transfer) <-chan created {
	done := make(chan created, 1)
	go func() {
		res, err := c.CreateTransfers(ctx, trs)
		done <- created{res: res, err: err}
	}()
	synctest.Wait()
	return done
}

func goCreateAccounts(ctx context.Context, c *Client, accounts []Account) <-chan created {
	done := make(chan created, 1)
	go func() {
		res, err := c.CreateAccounts(ctx, accounts)
		done <- created{res: res, err: err}
	}()
	synctest.Wait()
	return done
}

func goLookup(ctx context.Context, c *Client, ids ...uint64) <-chan error {
	done := make(chan error, 1)
	go func() {
		var keys []Uint128
		for _, id := range ids {
			keys = append(keys, From64(id))
		}
		_, err := c.LookupAccounts(ctx, keys)
		done <- err
	}()
	synctest.Wait()
	return done
}

func ids(keys []u128.Uint128) []uint64 {
	var lo []uint64
	for _, k := range keys {
		lo = append(lo, k.Lo)
	}
	return lo
}
