package server

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Demonisreal/dmn-ledger/internal/client"
	"github.com/Demonisreal/dmn-ledger/internal/disk"
	"github.com/Demonisreal/dmn-ledger/internal/raft"
	"github.com/Demonisreal/dmn-ledger/internal/u128"
	"github.com/Demonisreal/dmn-ledger/internal/wire"
)

var errInjected = errors.New("injected fsync failure")

func openTicks(t *testing.T) ticks {
	t.Helper()
	srv, err := Open(newPipeNet().config(1, 1, t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.node.Close(); err != nil {
		t.Fatal(err)
	}
	return srv.ticks
}

func TestTicksFollowTheMonotonicClock(t *testing.T) {
	tk := openTicks(t)
	ms := time.Millisecond
	for _, step := range []struct {
		at   time.Duration
		owed int64
		wake time.Duration
	}{
		{0, 0, 10 * ms},
		{10*ms - 1, 0, 1},
		{10 * ms, 1, 10 * ms},
		{10 * ms, 0, 10 * ms},
		{35 * ms, 2, 5 * ms},
		{39 * ms, 0, ms},
		{170 * ms, 14, 10 * ms},
		{171 * ms, 0, 9 * ms},
		{180 * ms, 1, 10 * ms},
		{675 * ms, 40, 5 * ms},
	} {
		if owed := tk.take(step.at); owed != step.owed {
			t.Fatalf("at %s: %d ticks owed, want %d", step.at, owed, step.owed)
		}
		if wake := tk.wake(step.at); wake != step.wake {
			t.Fatalf("at %s: next tick in %s, want %s", step.at, wake, step.wake)
		}
	}
}

func TestStallOwesLessThanAnElection(t *testing.T) {
	tk := openTicks(t)
	if owed := tk.take(5 * time.Second); owed != electionTicks-2*heartbeatTicks {
		t.Fatalf("after a stall of 5s: %d ticks, want %d", owed, electionTicks-2*heartbeatTicks)
	}
	if owed := tk.take(5 * time.Second); owed != 0 {
		t.Fatalf("right after the stall: %d more ticks, want 0", owed)
	}
	if wake := tk.wake(5 * time.Second); wake != testTick {
		t.Fatalf("after the stall the next tick is %s away, want %s", wake, testTick)
	}
	if owed := tk.take(5*time.Second + 3*testTick); owed != 3 {
		t.Fatalf("30ms after the stall: %d ticks, want 3", owed)
	}
}

func single(t *testing.T, fsys disk.FS) (*pipeNet, Config) {
	t.Helper()
	pn := newPipeNet()
	cfg := pn.config(1, 1, t.TempDir())
	if fsys != nil {
		cfg.FS = fsys
	}
	return pn, cfg
}

func TestNothingLeavesBeforeItIsSynced(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fsys := newHookFS()
		fsys.hook(func(string) error {
			time.Sleep(time.Millisecond)
			return nil
		})
		pn, cfg := single(t, fsys)
		frames := 0
		cfg.Clients.(*pipeListener).tap = func(frame []byte) {
			if f, _, err := wire.Decode(frame, testCluster, wire.MaxClientBody); err == nil && f.Type == wire.TypeStatus {
				return
			}
			frames++
			if n := fsys.unsynced(); n != 0 {
				t.Errorf("a reply reaches the connection while %d files hold unsynced writes", n)
			}
		}
		n := start(t, cfg)
		c := pn.connect(t, clientAddr(1), 1)
		c.leading()
		c.register()
		c.ok(client.CreateAccounts, accounts(1, 2))
		for i := range 30 {
			c.ok(client.CreateTransfers, transfer(100+uint64(i), 1, 2, 1))
		}
		if frames != 32 {
			t.Errorf("%d replies checked, want 32", frames)
		}
		if err := n.halt(); err != nil {
			t.Fatal(err)
		}
	})
}

type entryBook struct {
	mu      sync.Mutex
	needles map[uint64][]byte
}

func needle(e raft.Entry) []byte {
	switch {
	case e.Kind == 1 && len(e.Body) == client.IDSize:
		return e.Body
	case e.Kind == 2 && len(e.Body) >= client.IDSize+16:
		return e.Body[:client.IDSize+16]
	}
	return nil
}

func guard(t *testing.T, id uint64, fsys *hookFS, book *entryBook, session func() uint64) func(frame []byte) {
	return func(frame []byte) {
		f, _, err := wire.Decode(frame, testCluster, wire.MaxPeerBody)
		if err != nil {
			t.Errorf("node %d writes a frame that does not decode: %v", id, err)
			return
		}
		if f.Type == wire.TypeHello || f.Type == wire.TypeStatus {
			return
		}
		if names := fsys.unsyncedNames(); len(names) != 0 {
			t.Errorf("node %d: a frame of type %#x reaches a connection while %v hold unsynced writes", id, uint8(f.Type), names)
		}
		if f.Type == wire.TypeReply {
			r, err := f.Reply()
			if err != nil || r.Status != client.OK || r.Op.Lookup() {
				return
			}
			promise := r.Client.AppendBytes(nil)
			if r.Op != client.Register {
				promise = binary.BigEndian.AppendUint64(binary.BigEndian.AppendUint64(promise, session()), r.Number)
			}
			if err := fsys.sealed(promise); err != nil {
				t.Errorf("node %d answers %s #%d: %v", id, r.Op, r.Number, err)
			}
			return
		}
		m, err := f.Message()
		if err != nil {
			return
		}
		book.mu.Lock()
		for _, e := range m.Entries {
			if n := needle(e); n != nil {
				book.needles[e.Index] = slices.Clone(n)
			}
		}
		var promise []byte
		switch {
		case m.Type == raft.MsgApp && len(m.Entries) > 0:
			promise = book.needles[m.Entries[len(m.Entries)-1].Index]
		case m.Type == raft.MsgAppResp && !m.Reject:
			promise = book.needles[m.Index]
		}
		book.mu.Unlock()
		if promise != nil {
			if err := fsys.sealed(promise); err != nil {
				t.Errorf("node %d sends %s up to %d to node %d: %v", id, m.Type, m.Index+uint64(len(m.Entries)), m.To, err)
			}
		}
	}
}

func TestNothingLeavesAnyNodeBeforeItIsSynced(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pn := newPipeNet()
		book := &entryBook{needles: make(map[uint64][]byte)}
		var c *testClient
		session := func() uint64 { return c.session }
		var nodes []*testNode
		turn := make(chan struct{}, 1)
		for i := range 3 {
			id := uint64(i) + 1
			fsys := newHookFS()
			fsys.keep, fsys.turn = true, turn
			fsys.hook(func(string) error {
				time.Sleep(time.Millisecond)
				return nil
			})
			cfg := pn.config(id, 3, t.TempDir())
			cfg.FS = fsys
			check := guard(t, id, fsys, book, session)
			cfg.Clients.(*pipeListener).tap = check
			cfg.Peering.(*pipeListener).tap = check
			cfg.Dial = func(ctx context.Context, addr string) (net.Conn, error) {
				conn, err := pn.dial(ctx, addr)
				if err != nil {
					return nil, err
				}
				return &tappedConn{Conn: conn, tap: check}, nil
			}
			nodes = append(nodes, start(t, cfg))
		}
		lead := leaderOf(t, pn, 3)
		c = pn.connect(t, clientAddr(lead), 1)
		c.register()
		c.ok(client.CreateAccounts, accounts(1, 2))
		for i := range 30 {
			c.ok(client.CreateTransfers, transfer(100+uint64(i), 1, 2, 1))
		}
		book.mu.Lock()
		entries := len(book.needles)
		book.mu.Unlock()
		if entries < 32 {
			t.Fatalf("%d entries seen on the wire, want at least 32", entries)
		}
		for _, n := range nodes {
			if err := n.halt(); err != nil {
				t.Fatal(err)
			}
		}
	})
}

func TestFailedProcessSendsNothing(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fsys := newHookFS()
		pn, cfg := single(t, fsys)
		n := start(t, cfg)
		c := pn.connect(t, clientAddr(1), 1)
		c.leading()
		c.register()
		syncs := 0
		fsys.hook(func(string) error {
			syncs++
			if syncs == 2 {
				return errInjected
			}
			return nil
		})
		c.send(client.CreateAccounts, accounts(1, 2))
		if err := n.wait(patience); !errors.Is(err, errInjected) {
			t.Fatalf("Run returned %v, want the failed fsync", err)
		}
		if frame, err := c.read(); !errors.Is(err, io.EOF) {
			t.Fatalf("after the failed fsync the client reads %x, %v; want the connection closed", frame.Body, err)
		}
	})
}

func TestRequestsDuringASyncShareTheNext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fsys := newHookFS()
		pn, cfg := single(t, fsys)
		cfg.MaxClients = 64
		n := start(t, cfg)
		var cs []*testClient
		for i := range 51 {
			c := pn.connect(t, clientAddr(1), uint64(i)+1)
			if i == 0 {
				c.leading()
			}
			c.register()
			cs = append(cs, c)
		}
		gate := make(chan struct{})
		fsys.hook(func(name string) error {
			if isWAL(name) {
				<-gate
			}
			return nil
		})
		cs[0].send(client.CreateAccounts, accounts(1000))
		synctest.Wait()
		for i, c := range cs[1:] {
			c.send(client.CreateAccounts, accounts(uint64(i)+1))
		}
		synctest.Wait()
		before := fsys.walSyncs()
		close(gate)
		for _, c := range cs {
			if r := c.recv(); r.Status != client.OK {
				t.Fatalf("client %v: %s", c.id, r.Status)
			}
		}
		if syncs := fsys.walSyncs() - before; syncs > 2 {
			t.Errorf("51 requests took %d log syncs, want the held one and one for the 50 that came during it", syncs)
		}
		if err := n.halt(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestSlowRoundsLoseNoTicks(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fsys := newHookFS()
		pn, cfg := single(t, fsys)
		n := start(t, cfg)
		c := pn.connect(t, clientAddr(1), 1)
		c.leading()
		c.register()
		c.ok(client.CreateAccounts, accounts(1, 2))
		fsys.hook(func(name string) error {
			if isWAL(name) {
				time.Sleep(17 * time.Millisecond)
			}
			return nil
		})
		end := time.Now().Add(10 * time.Second)
		calls := 0
		for id := uint64(100); time.Now().Before(end); id++ {
			sent := wallClock()
			r := c.ok(client.CreateTransfers, transfer(id, 1, 2, 1))
			if got := wallClock(); r.Time < sent || r.Time > got {
				t.Fatalf("transfer %d after %s: stamped %d, sent at %d, answered at %d", id, time.Until(end), r.Time, sent, got)
			}
			calls++
		}
		if calls < 200 {
			t.Fatalf("%d transfers in 10s", calls)
		}
		if err := n.halt(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestBusyRoundsLoseNoTicks(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fsys := newHookFS()
		pn, cfg := single(t, fsys)
		n := start(t, cfg)
		a := pn.connect(t, clientAddr(1), 1)
		b := pn.connect(t, clientAddr(1), 2)
		a.leading()
		a.register()
		b.register()
		a.ok(client.CreateAccounts, accounts(1, 2))
		fsys.hook(func(name string) error {
			if isWAL(name) {
				time.Sleep(17 * time.Millisecond)
			}
			return nil
		})
		end := time.Now().Add(5 * time.Second)
		var wg sync.WaitGroup
		for k, c := range []*testClient{a, b} {
			wg.Go(func() {
				for id := uint64(1000 * (k + 1)); time.Now().Before(end); id++ {
					sent := wallClock()
					req := c.send(client.CreateTransfers, transfer(id, 1, 2, 1))
					frame, err := c.read()
					if err != nil {
						t.Errorf("client %d: %v", k, err)
						return
					}
					r, err := frame.Reply()
					if err != nil || r.Number != req.Number || r.Status != client.OK {
						t.Errorf("client %d: reply %+v, %v", k, r, err)
						return
					}
					if r.Time < sent {
						t.Errorf("client %d, transfer %d, %s before the end: stamped %d, sent at %d, %s behind", k, id, time.Until(end), r.Time, sent, time.Duration(sent-r.Time))
						return
					}
				}
			})
		}
		wg.Wait()
		if err := n.halt(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestStatusShowsOnlyWhatIsSynced(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pn := newPipeNet()
		pn.fake(t, 2, 1, listening)
		pn.fake(t, 3, 1, listening)
		fsys := newHookFS()
		cfg := pn.config(1, 3, t.TempDir())
		cfg.FS = fsys
		n := start(t, cfg)
		lead := pn.greet(t, 1, 2, wire.KindMessages)
		probe := pn.connect(t, clientAddr(1), 1)
		grow := func(index uint64) {
			before := uint64(0)
			if index > 1 {
				before = 2
			}
			lead.send(raft.Message{Type: raft.MsgApp, From: 2, To: 1, Term: 2, Index: index - 1, LogTerm: before,
				Entries: []raft.Entry{{Index: index, Term: 2, Kind: 1, Body: u128.From64(index).AppendBytes(nil)}}})
		}
		grow(1)
		for range 100 {
			if probe.status().Last == 1 {
				break
			}
			time.Sleep(testTick)
		}
		gates := make(chan chan struct{}, 1)
		fsys.hook(func(name string) error {
			if isWAL(name) {
				select {
				case gate := <-gates:
					<-gate
				default:
				}
			}
			return nil
		})
		first, second := make(chan struct{}), make(chan struct{})
		gates <- first
		grow(2)
		synctest.Wait()
		grow(3)
		probe.write(wire.AppendStatusRequest(nil, testCluster))
		synctest.Wait()
		gates <- second
		close(first)
		synctest.Wait()
		_ = probe.conn.SetReadDeadline(time.Now().Add(time.Second))
		if frame, _, err := wire.ReadFrame(probe.conn, nil, testCluster, wire.MaxClientBody); err == nil {
			st, _ := frame.Status()
			t.Fatalf("a status with the log at %d arrived while the sync of entry 3 was held", st.Last)
		}
		close(second)
		frame, err := probe.read()
		if err != nil {
			t.Fatalf("no status after the sync: %v", err)
		}
		if st, err := frame.Status(); err != nil || st.Last != 3 || st.Term != 2 {
			t.Fatalf("status after the sync: %+v, %v; want the log at 3 in term 2", st, err)
		}
		if err := n.halt(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestStopLeavesNothingRunning(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pn := newPipeNet()
		dirs := []string{t.TempDir(), t.TempDir(), t.TempDir()}
		var nodes []*testNode
		for i, dir := range dirs {
			nodes = append(nodes, start(t, pn.config(uint64(i)+1, 3, dir)))
		}
		lead := leaderOf(t, pn, 3)
		c := pn.connect(t, clientAddr(lead), 1)
		c.register()
		c.ok(client.CreateAccounts, accounts(1, 2))
		idle := pn.connect(t, clientAddr(lead), 2)
		idle.status()
		for _, n := range nodes {
			if err := n.halt(); err != nil {
				t.Fatalf("Run after the context ended: %v", err)
			}
		}
		if left := running(); left != "" {
			t.Fatalf("server goroutines left after Run returned:\n%s", left)
		}
		if !c.shut(time.Second) || !idle.shut(time.Second) {
			t.Fatal("client connections stay open after Run returned")
		}
		for i, dir := range dirs {
			cfg := pn.config(uint64(i)+1, 3, dir)
			srv, err := Open(cfg)
			if err != nil {
				t.Fatalf("reopen node %d: %v", i+1, err)
			}
			if err := srv.node.Close(); err != nil {
				t.Fatal(err)
			}
		}
	})
	synctest.Test(t, func(t *testing.T) {
		pn := newPipeNet()
		behind := pn.fake(t, 2, 1, deaf)
		pn.fake(t, 3, 1, following)
		fsys := newHookFS()
		cfg := pn.config(1, 3, t.TempDir())
		cfg.FS, cfg.SnapshotEvery, cfg.SegmentSize = fsys, 10, smallSegment
		n := start(t, cfg)
		c := pn.connect(t, clientAddr(1), 1)
		c.leading()
		c.register()
		fill(c, 40)
		reading := make(chan struct{})
		var once sync.Once
		fsys.reading(func(name string, _ int64) {
			if filepath.Base(filepath.Dir(name)) == snapDir {
				once.Do(func() {
					close(reading)
					time.Sleep(time.Second)
				})
			}
		})
		behind.set(lagging)
		select {
		case <-reading:
		case <-time.After(20 * time.Second):
			t.Fatal("no snapshot read within 20s")
		}
		if err := n.halt(); err != nil {
			t.Fatal(err)
		}
		if left := running(); left != "" {
			t.Fatalf("server goroutines left after Run returned during a snapshot transfer:\n%s", left)
		}
	})
}

func running() string {
	buf := make([]byte, 1<<20)
	buf = buf[:runtime.Stack(buf, true)]
	var left []string
	for _, stack := range strings.Split(string(buf), "\n\n") {
		if strings.Contains(stack, "internal/server.(*Server).") || strings.Contains(stack, "internal/server.(*clientConn).") {
			left = append(left, stack)
		}
	}
	return strings.Join(left, "\n\n")
}

func TestSecondServerOnTheSameDirFails(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pn, cfg := single(t, nil)
		n := start(t, cfg)
		spool := filepath.Join(cfg.Dir, recvDir, "3.tmp")
		if err := os.WriteFile(spool, []byte("in flight"), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := Open(pn.config(1, 1, cfg.Dir)); !errors.Is(err, disk.ErrLocked) {
			t.Fatalf("second Open on the same dir: %v, want %v", err, disk.ErrLocked)
		}
		if _, err := os.Stat(spool); err != nil {
			t.Fatalf("the refused Open touched the running node's spool: %v", err)
		}
		if err := n.halt(); err != nil {
			t.Fatal(err)
		}
		srv, err := Open(pn.config(1, 1, cfg.Dir))
		if err != nil {
			t.Fatalf("Open after the first server stopped: %v", err)
		}
		if err := srv.node.Close(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestLeftoverSpoolIsRemovedOnStart(t *testing.T) {
	_, cfg := single(t, nil)
	recv := filepath.Join(cfg.Dir, recvDir)
	if err := os.MkdirAll(recv, 0o750); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"2.tmp", "3.tmp"} {
		if err := os.WriteFile(filepath.Join(recv, name), make([]byte, 4096), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	srv, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = srv.node.Close() }()
	left, err := os.ReadDir(recv)
	if err != nil {
		t.Fatal(err)
	}
	if len(left) != 0 {
		t.Fatalf("after Open %d files are left in %s", len(left), recv)
	}
}
