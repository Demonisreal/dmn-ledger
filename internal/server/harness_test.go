package server

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Demonisreal/dmn-ledger/internal/client"
	"github.com/Demonisreal/dmn-ledger/internal/disk"
	"github.com/Demonisreal/dmn-ledger/internal/raft"
	"github.com/Demonisreal/dmn-ledger/internal/state"
	"github.com/Demonisreal/dmn-ledger/internal/u128"
	"github.com/Demonisreal/dmn-ledger/internal/wal"
	"github.com/Demonisreal/dmn-ledger/internal/wire"
)

const (
	testCluster  = 7
	testTick     = 10 * time.Millisecond
	patience     = 5 * time.Second
	testSessions = 64
)

var errRefused = errors.New("pipe: connection refused")

type pipeNet struct {
	mu    sync.Mutex
	ears  map[string]*pipeListener
	tried map[string][]time.Time
	sent  func(frame []byte)
}

func newPipeNet() *pipeNet {
	return &pipeNet{ears: make(map[string]*pipeListener), tried: make(map[string][]time.Time)}
}

func (n *pipeNet) dials(addr string) []time.Time {
	n.mu.Lock()
	defer n.mu.Unlock()
	return slices.Clone(n.tried[addr])
}

type pipeListener struct {
	addr  string
	conns chan net.Conn
	done  chan struct{}
	once  sync.Once
	tap   func(frame []byte)
}

type pipeAddr string

func (a pipeAddr) Network() string { return "pipe" }
func (a pipeAddr) String() string  { return string(a) }

func (l *pipeListener) Accept() (net.Conn, error) {
	select {
	case conn := <-l.conns:
		return conn, nil
	case <-l.done:
		return nil, net.ErrClosed
	}
}

func (l *pipeListener) Close() error {
	l.once.Do(func() { close(l.done) })
	return nil
}

func (l *pipeListener) Addr() net.Addr {
	return pipeAddr(l.addr)
}

func (n *pipeNet) listen(addr string) *pipeListener {
	l := &pipeListener{addr: addr, conns: make(chan net.Conn), done: make(chan struct{})}
	n.mu.Lock()
	n.ears[addr] = l
	n.mu.Unlock()
	return l
}

func (n *pipeNet) dial(ctx context.Context, addr string) (net.Conn, error) {
	n.mu.Lock()
	l := n.ears[addr]
	n.tried[addr] = append(n.tried[addr], time.Now())
	n.mu.Unlock()
	if l == nil {
		return nil, errRefused
	}
	near, far := net.Pipe()
	dialed := near
	if n.sent != nil {
		dialed = &tappedConn{Conn: near, tap: n.sent}
	}
	served := far
	if l.tap != nil {
		served = &tappedConn{Conn: far, tap: l.tap}
	}
	select {
	case l.conns <- served:
		return dialed, nil
	case <-l.done:
	case <-ctx.Done():
	}
	_ = near.Close()
	_ = far.Close()
	return nil, errRefused
}

type tappedConn struct {
	net.Conn
	tap func(frame []byte)
}

func (c *tappedConn) Write(b []byte) (int, error) {
	c.tap(b)
	return c.Conn.Write(b)
}

type hookFS struct {
	disk.FS
	mu     sync.Mutex
	dirty  map[string]bool
	syncs  int
	onSync func(name string) error
	onRead func(name string, off int64)
	keep   bool
	writes []diskWrite
	turn   chan struct{}
}

type diskWrite struct {
	name   string
	bytes  []byte
	synced bool
	sync   bool
}

func newHookFS() *hookFS {
	return &hookFS{FS: disk.OS{}, dirty: make(map[string]bool)}
}

func (h *hookFS) Create(name string) (disk.File, error) {
	f, err := h.FS.Create(name)
	if err != nil {
		return nil, err
	}
	return &hookFile{File: f, fs: h, name: name}, nil
}

func (h *hookFS) Open(name string) (disk.File, error) {
	f, err := h.FS.Open(name)
	if err != nil {
		return nil, err
	}
	return &hookFile{File: f, fs: h, name: name}, nil
}

func (h *hookFS) reading(fn func(name string, off int64)) {
	h.mu.Lock()
	h.onRead = fn
	h.mu.Unlock()
}

func (h *hookFS) hook(fn func(name string) error) {
	h.mu.Lock()
	h.onSync = fn
	h.mu.Unlock()
}

func (h *hookFS) unsynced() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.dirty)
}

func (h *hookFS) unsyncedNames() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	var names []string
	for name := range h.dirty {
		names = append(names, filepath.Base(name))
	}
	return names
}

func (h *hookFS) walSyncs() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.syncs
}

func (h *hookFS) touch(name string, b []byte) {
	if strings.Contains(name, string(filepath.Separator)+recvDir+string(filepath.Separator)) {
		return
	}
	h.mu.Lock()
	h.dirty[name] = true
	if h.keep {
		h.writes = append(h.writes, diskWrite{name: name, bytes: slices.Clone(b)})
	}
	h.mu.Unlock()
}

func (h *hookFS) sealed(needle []byte) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	at := slices.IndexFunc(h.writes, func(w diskWrite) bool { return isWAL(w.name) && bytes.Contains(w.bytes, needle) })
	switch {
	case at < 0:
		return errors.New("the entry is not written")
	case !h.writes[at].synced:
		return errors.New("the entry is written but not synced")
	}
	name := h.writes[at].name
	at += 1 + slices.IndexFunc(h.writes[at+1:], func(w diskWrite) bool { return w.sync && w.name == name })
	for _, w := range h.writes[at+1:] {
		if !w.sync && strings.TrimSuffix(filepath.Base(w.name), ".tmp") == wal.MetaName {
			if !w.synced {
				return errors.New("the entry is synced, its seal is not")
			}
			return nil
		}
	}
	return errors.New("the entry is synced but not sealed after its sync")
}

type hookFile struct {
	disk.File
	fs   *hookFS
	name string
}

func (f *hookFile) ReadAt(b []byte, off int64) (int, error) {
	f.fs.mu.Lock()
	fn := f.fs.onRead
	f.fs.mu.Unlock()
	if fn != nil {
		fn(f.name, off)
	}
	return f.File.ReadAt(b, off)
}

func (h *hookFS) settle() {
	if h.turn == nil {
		return
	}
	h.turn <- struct{}{}
	synctest.Wait()
	<-h.turn
}

func (f *hookFile) WriteAt(b []byte, off int64) (int, error) {
	f.fs.settle()
	f.fs.touch(f.name, b)
	return f.File.WriteAt(b, off)
}

func (f *hookFile) Truncate(size int64) error {
	f.fs.settle()
	f.fs.touch(f.name, nil)
	return f.File.Truncate(size)
}

func (f *hookFile) Sync() error {
	f.fs.mu.Lock()
	fn := f.fs.onSync
	f.fs.mu.Unlock()
	if fn != nil {
		if err := fn(f.name); err != nil {
			return err
		}
	}
	if err := f.File.Sync(); err != nil {
		return err
	}
	f.fs.mu.Lock()
	delete(f.fs.dirty, f.name)
	for i := range f.fs.writes {
		if f.fs.writes[i].name == f.name {
			f.fs.writes[i].synced = true
		}
	}
	if f.fs.keep {
		f.fs.writes = append(f.fs.writes, diskWrite{name: f.name, synced: true, sync: true})
	}
	if isWAL(f.name) {
		f.fs.syncs++
	}
	f.fs.mu.Unlock()
	return nil
}

func isWAL(name string) bool {
	return filepath.Base(filepath.Dir(name)) == "wal"
}

func peerAddr(id uint64) string {
	return fmt.Sprintf("peer%d", id)
}

func clientAddr(id uint64) string {
	return fmt.Sprintf("client%d", id)
}

func (n *pipeNet) config(id uint64, nodes int, dir string) Config {
	peers := make([]string, nodes)
	for i := range peers {
		peers[i] = peerAddr(uint64(i) + 1)
	}
	return Config{
		ID:            id,
		Cluster:       testCluster,
		Peers:         peers,
		Dir:           dir,
		FS:            disk.OS{},
		Tick:          testTick,
		SnapshotEvery: 100_000,
		MaxSessions:   testSessions,
		MaxClients:    16,
		Clients:       n.listen(clientAddr(id)),
		Peering:       n.listen(peerAddr(id)),
		Dial:          n.dial,
	}
}

type testNode struct {
	t    *testing.T
	srv  *Server
	stop context.CancelFunc
	done chan struct{}
	err  error
}

func start(t *testing.T, cfg Config) *testNode {
	t.Helper()
	srv, err := Open(cfg)
	if err != nil {
		t.Fatalf("open node %d: %v", cfg.ID, err)
	}
	return run(t, srv)
}

func run(t *testing.T, srv *Server) *testNode {
	ctx, stop := context.WithCancel(context.Background())
	n := &testNode{t: t, srv: srv, stop: stop, done: make(chan struct{})}
	go func() {
		n.err = srv.Run(ctx)
		close(n.done)
	}()
	t.Cleanup(func() { _ = n.halt() })
	return n
}

func (n *testNode) halt() error {
	n.stop()
	<-n.done
	return n.err
}

func (n *testNode) wait(within time.Duration) error {
	n.t.Helper()
	select {
	case <-n.done:
		return n.err
	case <-time.After(within):
		n.t.Fatalf("Run did not return within %s", within)
		return nil
	}
}

type testClient struct {
	t       *testing.T
	conn    net.Conn
	id      u128.Uint128
	session uint64
	number  uint64
	buf     []byte
}

func (n *pipeNet) connect(t *testing.T, addr string, id uint64) *testClient {
	t.Helper()
	conn, err := n.dial(context.Background(), addr)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return &testClient{t: t, conn: conn, id: u128.From64(id)}
}

func (c *testClient) write(frame []byte) {
	c.t.Helper()
	_ = c.conn.SetWriteDeadline(time.Now().Add(patience))
	if _, err := c.conn.Write(frame); err != nil {
		c.t.Fatalf("client %v: write: %v", c.id, err)
	}
}

func (c *testClient) read() (wire.Frame, error) {
	_ = c.conn.SetReadDeadline(time.Now().Add(patience))
	var frame wire.Frame
	var err error
	frame, c.buf, err = wire.ReadFrame(c.conn, c.buf, testCluster, wire.MaxClientBody)
	return frame, err
}

func (c *testClient) send(op client.Op, body []byte) client.Request {
	c.t.Helper()
	c.number++
	req := client.Request{Client: c.id, Session: c.session, Number: c.number, Op: op, Body: body}
	c.write(wire.AppendRequest(nil, testCluster, req))
	return req
}

func (c *testClient) recv() client.Reply {
	c.t.Helper()
	frame, err := c.read()
	if err != nil {
		c.t.Fatalf("client %v: read: %v", c.id, err)
	}
	r, err := frame.Reply()
	if err != nil {
		c.t.Fatalf("client %v: reply: %v", c.id, err)
	}
	return r
}

func (c *testClient) call(op client.Op, body []byte) client.Reply {
	c.t.Helper()
	req := c.send(op, body)
	r := c.recv()
	if r.Number != req.Number || r.Op != op {
		c.t.Fatalf("client %v: reply %s #%d to %s #%d", c.id, r.Op, r.Number, op, req.Number)
	}
	return r
}

func (c *testClient) ok(op client.Op, body []byte) client.Reply {
	c.t.Helper()
	r := c.call(op, body)
	if r.Status != client.OK || !r.Op.Lookup() && len(r.Body) != 0 {
		c.t.Fatalf("client %v: %s #%d: %s with body %x", c.id, op, r.Number, r.Status, r.Body)
	}
	return r
}

func (c *testClient) register() {
	c.t.Helper()
	c.write(wire.AppendRequest(nil, testCluster, client.Request{Client: c.id, Op: client.Register}))
	r := c.recv()
	if r.Status != client.OK || r.Op != client.Register || len(r.Body) != 8 {
		c.t.Fatalf("client %v: register: %s with %d bytes", c.id, r.Status, len(r.Body))
	}
	c.session = binary.BigEndian.Uint64(r.Body)
}

func (c *testClient) status() wire.Status {
	c.t.Helper()
	c.write(wire.AppendStatusRequest(nil, testCluster))
	frame, err := c.read()
	if err != nil {
		c.t.Fatalf("client %v: status: %v", c.id, err)
	}
	st, err := frame.Status()
	if err != nil {
		c.t.Fatalf("client %v: status: %v", c.id, err)
	}
	return st
}

func (c *testClient) leading() {
	c.t.Helper()
	for range 500 {
		if c.status().Role == raft.Leader {
			return
		}
		time.Sleep(testTick)
	}
	c.t.Fatalf("client %v: node did not lead within 5s", c.id)
}

func (c *testClient) shut(within time.Duration) bool {
	c.t.Helper()
	_ = c.conn.SetReadDeadline(time.Now().Add(within))
	_, err := io.Copy(io.Discard, c.conn)
	return err == nil
}

func accounts(ids ...uint64) []byte {
	var b []byte
	for _, id := range ids {
		b = state.AppendAccount(b, state.Account{ID: u128.From64(id), Ledger: 1, Code: 1})
	}
	return b
}

func transfer(id, dr, cr, amount uint64) []byte {
	return state.AppendTransfer(nil, state.Transfer{
		ID:              u128.From64(id),
		DebitAccountID:  u128.From64(dr),
		CreditAccountID: u128.From64(cr),
		Amount:          u128.From64(amount),
		Ledger:          1,
		Code:            1,
	})
}

func hold(id uint64, timeout uint32) []byte {
	return state.AppendTransfer(nil, state.Transfer{
		ID:              u128.From64(id),
		DebitAccountID:  u128.From64(1),
		CreditAccountID: u128.From64(2),
		Amount:          u128.From64(5),
		Ledger:          1,
		Code:            1,
		Flags:           state.Pending,
		Timeout:         timeout,
	})
}

func pending(t *testing.T, c *testClient, id uint64) u128.Uint128 {
	t.Helper()
	r := c.ok(client.LookupAccounts, u128.From64(id).AppendBytes(nil))
	if len(r.Body) != state.AccountSize {
		t.Fatalf("lookup of account %d: %d bytes", id, len(r.Body))
	}
	return state.DecodeAccount(r.Body).DebitsPending
}

func leaderOf(t *testing.T, pn *pipeNet, nodes int) uint64 {
	t.Helper()
	probes := make([]*testClient, nodes)
	for i := range probes {
		probes[i] = pn.connect(t, clientAddr(uint64(i)+1), 1<<40+uint64(i))
	}
	for range 1000 {
		for i, p := range probes {
			if p.status().Role == raft.Leader {
				return uint64(i) + 1
			}
		}
		time.Sleep(testTick)
	}
	t.Fatalf("no leader among %d nodes within 10s", nodes)
	return 0
}

type testPeer struct {
	t    *testing.T
	id   uint64
	conn net.Conn
	buf  []byte
}

func (pn *pipeNet) greet(t *testing.T, to, from uint64, kind uint8) *testPeer {
	t.Helper()
	conn, err := pn.dial(context.Background(), peerAddr(to))
	if err != nil {
		t.Fatalf("dial %s: %v", peerAddr(to), err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	p := &testPeer{t: t, id: from, conn: conn}
	p.write(wire.AppendHello(nil, testCluster, wire.Hello{From: from, To: to, MaxSessions: testSessions, Kind: kind}))
	frame, err := p.read()
	if err != nil {
		t.Fatalf("hello from node %d: %v", to, err)
	}
	if h, err := frame.Hello(); err != nil || h != (wire.Hello{From: to, To: from, MaxSessions: testSessions, Kind: kind}) {
		t.Fatalf("hello from node %d: %+v, %v", to, h, err)
	}
	return p
}

func (p *testPeer) write(frame []byte) {
	p.t.Helper()
	_ = p.conn.SetWriteDeadline(time.Now().Add(patience))
	if _, err := p.conn.Write(frame); err != nil {
		p.t.Fatalf("peer %d: write: %v", p.id, err)
	}
}

func (p *testPeer) read() (wire.Frame, error) {
	_ = p.conn.SetReadDeadline(time.Now().Add(patience))
	var frame wire.Frame
	var err error
	frame, p.buf, err = wire.ReadFrame(p.conn, p.buf, testCluster, wire.MaxPeerBody)
	return frame, err
}

func (p *testPeer) send(m raft.Message) {
	p.t.Helper()
	frame, err := wire.AppendMessage(nil, testCluster, m)
	if err != nil {
		p.t.Fatal(err)
	}
	p.write(frame)
}

func (p *testPeer) shut(within time.Duration) bool {
	p.t.Helper()
	_ = p.conn.SetReadDeadline(time.Now().Add(within))
	_, err := io.Copy(io.Discard, p.conn)
	return err == nil
}

func header(frame []byte, size uint32) []byte {
	head := slices.Clone(frame[:wire.HeaderSize])
	binary.BigEndian.PutUint32(head[16:], size)
	return head
}
