package server

import (
	"errors"
	"net"
	"os"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Demonisreal/dmn-ledger/internal/client"
	"github.com/Demonisreal/dmn-ledger/internal/raft"
	"github.com/Demonisreal/dmn-ledger/internal/u128"
	"github.com/Demonisreal/dmn-ledger/internal/wire"
)

func (c *testClient) quiet(within time.Duration) {
	c.t.Helper()
	_ = c.conn.SetReadDeadline(time.Now().Add(within))
	if _, _, err := wire.ReadFrame(c.conn, nil, testCluster, wire.MaxClientBody); !errors.Is(err, os.ErrDeadlineExceeded) {
		c.t.Fatalf("client %v: got a frame or a closed connection (%v), want nothing", c.id, err)
	}
}

func TestFirstFrameMustArriveInTime(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pn, cfg := single(t, nil)
		n := start(t, cfg)
		silent := pn.connect(t, clientAddr(1), 1)
		if silent.shut(firstFrame - time.Millisecond) {
			t.Fatal("a silent connection was closed before 5s")
		}
		if !silent.shut(2 * time.Millisecond) {
			t.Fatal("a silent connection is still open after 5s")
		}
		late := pn.connect(t, clientAddr(1), 2)
		time.Sleep(firstFrame - time.Millisecond)
		late.status()
		time.Sleep(time.Minute)
		if st := late.status(); st.ID != 1 {
			t.Fatalf("status after a minute of silence: %+v", st)
		}
		if err := n.halt(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestBodyMustFollowTheHeader(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pn, cfg := single(t, nil)
		n := start(t, cfg)
		c := pn.connect(t, clientAddr(1), 1)
		c.status()
		frame := wire.AppendRequest(nil, testCluster, client.Request{Client: c.id, Op: client.CreateAccounts, Body: accounts(1)})
		c.write(frame[:wire.HeaderSize])
		if c.shut(bodyTimeout - time.Millisecond) {
			t.Fatal("the connection was closed before the body was 10s late")
		}
		if !c.shut(2 * time.Millisecond) {
			t.Fatal("the connection is still open 10s after a header without body")
		}
		if err := n.halt(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestOversizedFrameClosesTheConnection(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pn := newPipeNet()
		n := start(t, pn.config(1, 3, t.TempDir()))
		request := wire.AppendRequest(nil, testCluster, client.Request{Op: client.Register})
		for _, tc := range []struct {
			size   uint32
			closed bool
		}{
			{wire.MaxClientBody, false},
			{wire.MaxClientBody + 1, true},
		} {
			c := pn.connect(t, clientAddr(1), 1)
			c.write(header(request, tc.size))
			if got := c.shut(100 * time.Millisecond); got != tc.closed {
				t.Errorf("client port, header with %d bytes: closed %v, want %v", tc.size, got, tc.closed)
			}
		}
		ping := mustMessage(t, raft.Message{Type: raft.MsgAppResp, From: 2, To: 1, Term: 1})
		for _, tc := range []struct {
			size   uint32
			closed bool
		}{
			{wire.MaxPeerBody, false},
			{wire.MaxPeerBody + 1, true},
		} {
			p := pn.greet(t, 1, 2, wire.KindMessages)
			p.write(header(ping, tc.size))
			if got := p.shut(100 * time.Millisecond); got != tc.closed {
				t.Errorf("peer port, header with %d bytes: closed %v, want %v", tc.size, got, tc.closed)
			}
		}
		if err := n.halt(); err != nil {
			t.Fatal(err)
		}
	})
}

func toss(conn net.Conn, frame []byte) {
	_ = conn.SetWriteDeadline(time.Now().Add(time.Second))
	_, _ = conn.Write(frame)
}

func mustMessage(t *testing.T, m raft.Message) []byte {
	t.Helper()
	frame, err := wire.AppendMessage(nil, testCluster, m)
	if err != nil {
		t.Fatal(err)
	}
	return frame
}

func TestOtherClusterIsClosed(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pn := newPipeNet()
		n := start(t, pn.config(1, 3, t.TempDir()))
		for name, frame := range map[string][]byte{
			"request":        wire.AppendRequest(nil, testCluster+1, client.Request{Client: u128.From64(1), Op: client.Register}),
			"status request": wire.AppendStatusRequest(nil, testCluster+1),
		} {
			c := pn.connect(t, clientAddr(1), 1)
			toss(c.conn, frame)
			if !c.shut(100 * time.Millisecond) {
				t.Errorf("client port: %s of another cluster leaves the connection open", name)
			}
		}
		conn, err := pn.dial(t.Context(), peerAddr(1))
		if err != nil {
			t.Fatal(err)
		}
		stranger := &testPeer{t: t, id: 2, conn: conn}
		toss(stranger.conn, wire.AppendHello(nil, testCluster+1, wire.Hello{From: 2, To: 1, MaxSessions: testSessions, Kind: wire.KindMessages}))
		if !stranger.shut(100 * time.Millisecond) {
			t.Error("peer port: hello of another cluster leaves the connection open")
		}
		p := pn.greet(t, 1, 2, wire.KindMessages)
		frame, err := wire.AppendMessage(nil, testCluster+1, raft.Message{Type: raft.MsgAppResp, From: 2, To: 1, Term: 1})
		if err != nil {
			t.Fatal(err)
		}
		toss(p.conn, frame)
		if !p.shut(100 * time.Millisecond) {
			t.Error("peer port: a message of another cluster leaves the connection open")
		}
		if err := n.halt(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestPeerFrameOnTheClientPort(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pn := newPipeNet()
		n := start(t, pn.config(1, 3, t.TempDir()))
		id := u128.From64(1)
		for name, frame := range map[string][]byte{
			"hello":          wire.AppendHello(nil, testCluster, wire.Hello{From: 2, To: 1, MaxSessions: testSessions, Kind: wire.KindMessages}),
			"raft message":   mustMessage(t, raft.Message{Type: raft.MsgAppResp, From: 2, To: 1, Term: 1}),
			"snapshot chunk": wire.AppendChunk(nil, testCluster, []byte{1}),
			"reply":          wire.AppendReply(nil, testCluster, client.Reply{Client: id, Op: client.Register, Status: client.OK}),
			"status":         wire.AppendStatus(nil, testCluster, wire.Status{ID: 1}),
		} {
			c := pn.connect(t, clientAddr(1), 1)
			toss(c.conn, frame)
			if !c.shut(100 * time.Millisecond) {
				t.Errorf("client port: a %s leaves the connection open", name)
			}
		}
		for name, frame := range map[string][]byte{
			"request":        wire.AppendRequest(nil, testCluster, client.Request{Client: id, Op: client.Register}),
			"status request": wire.AppendStatusRequest(nil, testCluster),
			"second hello":   wire.AppendHello(nil, testCluster, wire.Hello{From: 2, To: 1, MaxSessions: testSessions, Kind: wire.KindMessages}),
		} {
			p := pn.greet(t, 1, 2, wire.KindMessages)
			toss(p.conn, frame)
			if !p.shut(100 * time.Millisecond) {
				t.Errorf("peer port: a %s leaves the connection open", name)
			}
		}
		if err := n.halt(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestClientLimit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pn, cfg := single(t, nil)
		cfg.MaxClients = 2
		n := start(t, cfg)
		open := []*testClient{pn.connect(t, clientAddr(1), 1), pn.connect(t, clientAddr(1), 2)}
		for _, c := range open {
			c.status()
		}
		for round := range 3 {
			extra := pn.connect(t, clientAddr(1), 3)
			if !extra.shut(10 * time.Millisecond) {
				t.Fatalf("round %d: a third connection stays open with MaxClients 2", round)
			}
			_ = open[0].conn.Close()
			synctest.Wait()
			open[0] = pn.connect(t, clientAddr(1), 4)
			open[0].status()
			open[1].status()
		}
		if err := n.halt(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestOneClientIDPerConnection(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pn, cfg := single(t, nil)
		n := start(t, cfg)
		c := pn.connect(t, clientAddr(1), 1)
		c.leading()
		c.register()
		c.write(wire.AppendRequest(nil, testCluster, client.Request{Client: u128.From64(2), Op: client.Register}))
		if !c.shut(100 * time.Millisecond) {
			t.Fatal("a second client id on the same connection leaves it open")
		}
		other := pn.connect(t, clientAddr(1), 2)
		other.register()
		if err := n.halt(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestReplyGoesToTheNewestConnection(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fsys := newHookFS()
		pn, cfg := single(t, fsys)
		n := start(t, cfg)
		old := pn.connect(t, clientAddr(1), 1)
		old.leading()
		old.register()
		blocker := pn.connect(t, clientAddr(1), 2)
		blocker.register()

		fresh := pn.connect(t, clientAddr(1), 1)
		fresh.session = old.session
		fresh.ok(client.CreateAccounts, accounts(1))
		old.quiet(100 * time.Millisecond)

		old.number = fresh.number
		old.ok(client.CreateAccounts, accounts(2))
		fresh.quiet(100 * time.Millisecond)
		fresh.number = old.number

		gate := make(chan struct{})
		fsys.hook(func(name string) error {
			if isWAL(name) {
				<-gate
			}
			return nil
		})
		blocker.send(client.CreateAccounts, accounts(3))
		synctest.Wait()
		req := fresh.send(client.CreateAccounts, accounts(4))
		synctest.Wait()
		_ = old.conn.Close()
		synctest.Wait()
		close(gate)
		blocker.recv()
		if r := fresh.recv(); r.Number != req.Number || r.Status != client.OK {
			t.Fatalf("reply %s #%d, want OK #%d", r.Status, r.Number, req.Number)
		}
		if err := n.halt(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestClosedConnectionIsForgotten(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pn, cfg := single(t, nil)
		n := start(t, cfg)
		gone := pn.connect(t, clientAddr(1), 1)
		gone.leading()
		gone.register()
		moved := pn.connect(t, clientAddr(1), 2)
		moved.register()
		again := pn.connect(t, clientAddr(1), 2)
		again.session = moved.session
		again.number = moved.number
		again.ok(client.CreateAccounts, accounts(1))
		_ = gone.conn.Close()
		_ = moved.conn.Close()
		synctest.Wait()
		if err := n.halt(); err != nil {
			t.Fatal(err)
		}
		if _, ok := n.srv.conns[again.id]; len(n.srv.conns) != 1 || !ok {
			t.Fatalf("the table holds %d connections, want only the open one for client %v", len(n.srv.conns), again.id)
		}
	})
}

func TestSlowClientIsDisconnected(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pn, cfg := single(t, nil)
		n := start(t, cfg)
		slow := pn.connect(t, clientAddr(1), 1)
		ask := wire.AppendStatusRequest(nil, testCluster)
		sent := time.Now()
		for range 10 {
			_ = slow.conn.SetWriteDeadline(time.Now().Add(time.Second))
			if _, err := slow.conn.Write(ask); err != nil {
				break
			}
		}
		synctest.Wait()
		other := pn.connect(t, clientAddr(1), 2)
		other.status()
		if waited := time.Since(sent); waited > 100*time.Millisecond {
			t.Fatalf("another client waited %s while one did not read", waited)
		}
		if !slow.shut(time.Millisecond) {
			t.Fatal("a client that does not read stays connected")
		}
		if err := n.halt(); err != nil {
			t.Fatal(err)
		}
	})
}
