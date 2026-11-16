package server

import (
	"context"
	"io"
	"net"
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
	"github.com/Demonisreal/dmn-ledger/internal/wal"
	"github.com/Demonisreal/dmn-ledger/internal/wire"
)

type fakeMode uint8

const (
	listening fakeMode = iota
	following
	deaf
	lagging
)

type fakePeer struct {
	pn   *pipeNet
	id   uint64
	node uint64
	mode fakeMode
	l    *pipeListener

	mu     sync.Mutex
	got    []raft.Message
	back   net.Conn
	conns  []net.Conn
	pace   time.Duration
	open   int
	most   int
	chunks []time.Time
	ends   int
	beats  []time.Time
	votes  []time.Time
	spell  time.Duration
	logged []raft.Entry
	stopAt func(raft.Entry) bool
}

func (f *fakePeer) set(mode fakeMode) {
	f.mu.Lock()
	f.mode = mode
	f.mu.Unlock()
}

func (f *fakePeer) current() fakeMode {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.mode
}

func (f *fakePeer) sink(conn net.Conn) {
	f.mu.Lock()
	f.open++
	f.most = max(f.most, f.open)
	pace := f.pace
	f.mu.Unlock()
	began := time.Now()
	defer func() {
		f.mu.Lock()
		f.open--
		f.spell = max(f.spell, time.Since(began))
		f.mu.Unlock()
	}()
	var buf []byte
	for {
		select {
		case <-time.After(pace):
		case <-f.l.done:
			return
		}
		frame, next, err := wire.ReadFrame(conn, buf, testCluster, wire.MaxPeerBody)
		if err != nil {
			return
		}
		buf = next
		f.mu.Lock()
		switch frame.Type {
		case wire.TypeSnapshotChunk:
			f.chunks = append(f.chunks, time.Now())
		case wire.TypeSnapshotEnd:
			f.ends++
		}
		f.mu.Unlock()
	}
}

func (f *fakePeer) cut() {
	_ = f.l.Close()
	f.pn.mu.Lock()
	delete(f.pn.ears, f.l.addr)
	f.pn.mu.Unlock()
	f.mu.Lock()
	for _, conn := range f.conns {
		_ = conn.Close()
	}
	f.mu.Unlock()
}

func (pn *pipeNet) fake(t *testing.T, id, node uint64, mode fakeMode) *fakePeer {
	f := &fakePeer{pn: pn, id: id, node: node, mode: mode, l: pn.listen(peerAddr(id))}
	go f.accept()
	t.Cleanup(func() {
		f.cut()
		f.mu.Lock()
		if f.back != nil {
			_ = f.back.Close()
		}
		f.mu.Unlock()
	})
	return f
}

func (f *fakePeer) accept() {
	for {
		conn, err := f.l.Accept()
		if err != nil {
			return
		}
		f.mu.Lock()
		f.conns = append(f.conns, conn)
		f.mu.Unlock()
		go f.serve(conn)
	}
}

func (f *fakePeer) serve(conn net.Conn) {
	defer func() { _ = conn.Close() }()
	frame, _, err := wire.ReadFrame(conn, nil, testCluster, wire.MaxPeerBody)
	if err != nil {
		return
	}
	h, err := frame.Hello()
	if err != nil {
		return
	}
	if _, err := conn.Write(wire.AppendHello(nil, testCluster, wire.Hello{From: f.id, To: h.From, MaxSessions: testSessions, Kind: h.Kind})); err != nil {
		return
	}
	switch {
	case f.current() == deaf:
		<-f.l.done
		return
	case h.Kind == wire.KindSnapshot:
		f.sink(conn)
		return
	}
	var buf []byte
	for {
		frame, buf, err = wire.ReadFrame(conn, buf, testCluster, wire.MaxPeerBody)
		if err != nil {
			return
		}
		m, err := frame.Message()
		if err != nil {
			return
		}
		f.mu.Lock()
		f.got = append(f.got, raft.Message{Type: m.Type, From: m.From, To: m.To, Term: m.Term, Index: m.Index, Seq: m.Seq, Commit: m.Commit})
		switch m.Type {
		case raft.MsgApp:
			f.beats = append(f.beats, time.Now())
		case raft.MsgPreVote:
			f.votes = append(f.votes, time.Now())
		}
		for _, e := range m.Entries {
			e.Body = slices.Clone(e.Body)
			f.logged = append(f.logged, e)
			if f.stopAt != nil && f.stopAt(e) {
				f.mode = listening
			}
		}
		f.mu.Unlock()
		f.respond(m)
	}
}

func (f *fakePeer) respond(m raft.Message) {
	var r raft.Message
	mode := f.current()
	switch {
	case mode == lagging && m.Type == raft.MsgApp:
		r = raft.Message{Type: raft.MsgAppResp, Term: m.Term, Index: 1, Reject: true, Seq: m.Seq}
	case mode != following:
		return
	case m.Type == raft.MsgPreVote:
		r = raft.Message{Type: raft.MsgPreVoteResp, Term: m.Term}
	case m.Type == raft.MsgVote:
		r = raft.Message{Type: raft.MsgVoteResp, Term: m.Term}
	case m.Type == raft.MsgApp:
		r = raft.Message{Type: raft.MsgAppResp, Term: m.Term, Index: m.Index + uint64(len(m.Entries)), Seq: m.Seq}
	default:
		return
	}
	r.From, r.To = f.id, f.node
	frame, err := wire.AppendMessage(nil, testCluster, r)
	if err != nil {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.back == nil {
		f.back, err = dialHello(f.pn, f.node, f.id, wire.KindMessages)
		if err != nil {
			return
		}
	}
	if _, err := f.back.Write(frame); err != nil {
		_ = f.back.Close()
		f.back = nil
	}
}

func dialHello(pn *pipeNet, to, from uint64, kind uint8) (net.Conn, error) {
	conn, err := pn.dial(context.Background(), peerAddr(to))
	if err != nil {
		return nil, err
	}
	_ = conn.SetDeadline(time.Now().Add(patience))
	if _, err := conn.Write(wire.AppendHello(nil, testCluster, wire.Hello{From: from, To: to, MaxSessions: testSessions, Kind: kind})); err != nil {
		_ = conn.Close()
		return nil, err
	}
	if _, _, err := wire.ReadFrame(conn, nil, testCluster, wire.MaxPeerBody); err != nil {
		_ = conn.Close()
		return nil, err
	}
	_ = conn.SetDeadline(time.Time{})
	return conn, nil
}

func (f *fakePeer) messages(t raft.MsgType) []raft.Message {
	f.mu.Lock()
	defer f.mu.Unlock()
	var picked []raft.Message
	for _, m := range f.got {
		if m.Type == t {
			picked = append(picked, m)
		}
	}
	return picked
}

func TestHelloFromAStrangerIsRefused(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pn := newPipeNet()
		n := start(t, pn.config(1, 3, t.TempDir()))
		for name, first := range map[string][]byte{
			"unknown sender":  wire.AppendHello(nil, testCluster, wire.Hello{From: 9, To: 1, MaxSessions: testSessions, Kind: wire.KindMessages}),
			"own id":          wire.AppendHello(nil, testCluster, wire.Hello{From: 1, To: 1, MaxSessions: testSessions, Kind: wire.KindMessages}),
			"other target":    wire.AppendHello(nil, testCluster, wire.Hello{From: 2, To: 3, MaxSessions: testSessions, Kind: wire.KindMessages}),
			"unknown kind":    wire.AppendHello(nil, testCluster, wire.Hello{From: 2, To: 1, MaxSessions: testSessions, Kind: 3}),
			"kind 0":          wire.AppendHello(nil, testCluster, wire.Hello{From: 2, To: 1, MaxSessions: testSessions}),
			"message first":   mustMessage(t, raft.Message{Type: raft.MsgAppResp, From: 2, To: 1, Term: 1}),
			"request first":   wire.AppendRequest(nil, testCluster, client.Request{Client: u128.From64(1), Op: client.Register}),
			"truncated hello": wire.AppendHello(nil, testCluster, wire.Hello{From: 2, To: 1, MaxSessions: testSessions, Kind: wire.KindMessages})[:30],
		} {
			conn, err := pn.dial(t.Context(), peerAddr(1))
			if err != nil {
				t.Fatal(err)
			}
			toss(conn, first)
			_ = conn.SetReadDeadline(time.Now().Add(helloTimeout + time.Millisecond))
			if b, err := io.ReadAll(conn); err != nil || len(b) != 0 {
				t.Errorf("%s: read %d bytes and %v, want the connection closed without a hello", name, len(b), err)
			}
		}
		silent, err := pn.dial(t.Context(), peerAddr(1))
		if err != nil {
			t.Fatal(err)
		}
		stranger := &testPeer{t: t, conn: silent}
		if stranger.shut(helloTimeout - time.Millisecond) {
			t.Fatal("a silent peer connection was closed before 2s")
		}
		if !stranger.shut(2 * time.Millisecond) {
			t.Fatal("a peer connection without hello is still open after 2s")
		}
		if err := n.halt(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestHelloIsReadWithItsOwnLimit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pn := newPipeNet()
		pn.fake(t, 3, 1, listening)
		ear := pn.listen(peerAddr(2))
		n := start(t, pn.config(1, 3, t.TempDir()))
		hello := wire.AppendHello(nil, testCluster, wire.Hello{From: 2, To: 1, MaxSessions: testSessions, Kind: wire.KindMessages})
		for _, size := range []uint32{wire.HelloSize + 1, wire.MaxPeerBody} {
			conn, err := pn.dial(t.Context(), peerAddr(1))
			if err != nil {
				t.Fatal(err)
			}
			toss(conn, header(hello, size))
			if !(&testPeer{t: t, conn: conn}).shut(time.Millisecond) {
				t.Errorf("a hello header with %d bytes leaves the connection open", size)
			}
		}

		conn, err := ear.Accept()
		if err != nil {
			t.Fatal(err)
		}
		dialed := &testPeer{t: t, id: 2, conn: conn}
		if _, err := dialed.read(); err != nil {
			t.Fatalf("no hello from node 1: %v", err)
		}
		toss(conn, header(wire.AppendHello(nil, testCluster, wire.Hello{From: 2, To: 1, MaxSessions: testSessions, Kind: wire.KindMessages}), wire.MaxPeerBody))
		if !dialed.shut(time.Millisecond) {
			t.Error("node 1 waits for the body of an answer that claims 4 MiB for a hello")
		}
		_ = ear.Close()
		if err := n.halt(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestPeerConnectionsBeforeTheHelloAreLimited(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pn := newPipeNet()
		pn.fake(t, 2, 1, listening)
		pn.fake(t, 3, 1, listening)
		n := start(t, pn.config(1, 3, t.TempDir()))
		knock := func() *testPeer {
			conn, err := pn.dial(t.Context(), peerAddr(1))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = conn.Close() })
			return &testPeer{t: t, id: 2, conn: conn}
		}
		var silent []*testPeer
		for range 2 * hellosOfPeer {
			silent = append(silent, knock())
		}
		if !knock().shut(time.Millisecond) {
			t.Fatalf("a peer connection beyond %d waiting for their hello stays open", 2*hellosOfPeer)
		}
		silent[0].write(wire.AppendHello(nil, testCluster, wire.Hello{From: 2, To: 1, MaxSessions: testSessions, Kind: wire.KindMessages}))
		if _, err := silent[0].read(); err != nil {
			t.Fatalf("no hello back: %v", err)
		}
		if knock().shut(time.Millisecond) {
			t.Fatal("a hello did not free its place")
		}
		_ = silent[1].conn.Close()
		synctest.Wait()
		if knock().shut(time.Millisecond) {
			t.Fatal("a closed connection did not free its place")
		}
		if !knock().shut(time.Millisecond) {
			t.Fatal("all places are taken again, and still a connection stays open")
		}
		if err := n.halt(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestMessageFromAnotherPeerIsRefused(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pn := newPipeNet()
		pn.fake(t, 2, 1, listening)
		pn.fake(t, 3, 1, listening)
		n := start(t, pn.config(1, 3, t.TempDir()))
		good := raft.Message{Type: raft.MsgAppResp, From: 2, To: 1, Term: 1}
		p := pn.greet(t, 1, 2, wire.KindMessages)
		p.send(good)
		if p.shut(100 * time.Millisecond) {
			t.Fatal("a well-formed message closed the connection")
		}
		for name, m := range map[string]raft.Message{
			"from another peer": {Type: raft.MsgAppResp, From: 3, To: 1, Term: 1},
			"to another node":   {Type: raft.MsgAppResp, From: 2, To: 3, Term: 1},
			"snapshot":          {Type: raft.MsgSnap, From: 2, To: 1, Term: 1, Index: 5, LogTerm: 1},
		} {
			p := pn.greet(t, 1, 2, wire.KindMessages)
			toss(p.conn, mustMessage(t, m))
			if !p.shut(100 * time.Millisecond) {
				t.Errorf("%s: the connection stays open", name)
			}
		}
		if err := n.halt(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestNewPeerConnectionReplacesTheOld(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pn := newPipeNet()
		pn.fake(t, 2, 1, listening)
		pn.fake(t, 3, 1, listening)
		n := start(t, pn.config(1, 3, t.TempDir()))
		old := pn.greet(t, 1, 2, wire.KindMessages)
		other := pn.greet(t, 1, 3, wire.KindMessages)
		fresh := pn.greet(t, 1, 2, wire.KindMessages)
		if !old.shut(100 * time.Millisecond) {
			t.Fatal("the older connection of peer 2 stays open")
		}
		fresh.send(raft.Message{Type: raft.MsgAppResp, From: 2, To: 1, Term: 1})
		other.send(raft.Message{Type: raft.MsgAppResp, From: 3, To: 1, Term: 1})
		if fresh.shut(100*time.Millisecond) || other.shut(time.Millisecond) {
			t.Fatal("the newest connection of peer 2 or the one of peer 3 was closed")
		}
		if err := n.halt(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestPeerReconnectsWithBackoff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pn := newPipeNet()
		pn.fake(t, 3, 1, following)
		n := start(t, pn.config(1, 3, t.TempDir()))
		c := pn.connect(t, clientAddr(1), 1)
		c.leading()
		time.Sleep(4 * time.Second)
		tried := pn.dials(peerAddr(2))
		if len(tried) < 6 || len(tried) > 12 {
			t.Fatalf("%d dials to a missing peer in about 5s, want 6 to 12", len(tried))
		}
		want := minBackoff
		for i := 1; i < len(tried); i++ {
			gap := tried[i].Sub(tried[i-1])
			if gap < want || gap > want+heartbeatTicks*testTick {
				t.Fatalf("dial %d came %s after the one before, want %s", i, gap, want)
			}
			want = min(2*want, maxBackoff)
		}

		up := pn.fake(t, 2, 1, listening)
		time.Sleep(2 * time.Second)
		if len(up.messages(raft.MsgApp)) == 0 {
			t.Fatal("no heartbeat reached peer 2 after it came up")
		}
		up.cut()
		cut := time.Now()
		time.Sleep(time.Second)
		after := slices.DeleteFunc(pn.dials(peerAddr(2)), func(at time.Time) bool { return at.Before(cut) })
		if len(after) == 0 || after[0].Sub(cut) > minBackoff+heartbeatTicks*testTick {
			t.Fatalf("after a working connection broke the next dial came %v later, want the backoff back at %s", after, minBackoff)
		}
		if err := n.halt(); err != nil {
			t.Fatal(err)
		}
	})
}

func deafPeer(t *testing.T) (*testNode, *testClient) {
	t.Helper()
	pn := newPipeNet()
	pn.fake(t, 2, 1, deaf)
	pn.fake(t, 3, 1, following)
	n := start(t, pn.config(1, 3, t.TempDir()))
	c := pn.connect(t, clientAddr(1), 1)
	c.leading()
	c.register()
	return n, c
}

func TestFullPeerQueueDrops(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		n, c := deafPeer(t)
		slow := n.srv.peers[2]
		time.Sleep(4 * time.Second)
		if k := len(slow.out); k != peerQueue {
			t.Fatalf("%d heartbeats queued for a peer that has not read for 4s, want the queue full at %d", k, peerQueue)
		}
		began := time.Now()
		c.ok(client.CreateAccounts, accounts(1))
		if waited := time.Since(began); waited > 100*time.Millisecond {
			t.Fatalf("a request took %s while the queue of peer 2 was full", waited)
		}
		if err := n.halt(); err != nil {
			t.Fatal(err)
		}

		n, c = deafPeer(t)
		slow = n.srv.peers[2]
		var most int64
		ids := uint64(1)
		for range 12 {
			batch := make([]uint64, 10_000)
			for i := range batch {
				batch[i] = ids
				ids++
			}
			began := time.Now()
			c.ok(client.CreateAccounts, accounts(batch...))
			if waited := time.Since(began); waited > 100*time.Millisecond {
				t.Fatalf("a request took %s while peer 2 did not read", waited)
			}
			for range 20 {
				time.Sleep(testTick)
				most = max(most, slow.queued.Load())
			}
		}
		if most > peerBytes || most < peerBytes-2*maxAppendBytes {
			t.Fatalf("at most %d bytes queued for a peer that does not read, want the limit of %d reached and kept", most, peerBytes)
		}
		if err := n.halt(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestEveryStartDrawsItsOwnNonce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pn := newPipeNet()
		dir := t.TempDir()
		meta, _, err := wal.OpenMeta(disk.OS{}, dir)
		if err != nil {
			t.Fatal(err)
		}
		if err := meta.Save(1, 0, true); err != nil {
			t.Fatal(err)
		}
		if err := meta.Close(); err != nil {
			t.Fatal(err)
		}
		pn.fake(t, 3, 1, listening)
		lead := pn.fake(t, 2, 1, listening)
		var nonces []uint64
		for start := range 2 {
			n := run(t, open(t, pn.config(1, 3, dir)))
			p := pn.greet(t, 1, 2, wire.KindMessages)
			p.send(raft.Message{Type: raft.MsgApp, From: 2, To: 1, Term: 2})
			for range 100 {
				if probes := lead.messages(raft.MsgRepairProbe); len(probes) > start {
					nonces = append(nonces, probes[start].Seq)
					break
				}
				time.Sleep(testTick)
			}
			if len(nonces) != start+1 {
				t.Fatalf("start %d: no repair probe within 1s", start+1)
			}
			if err := n.halt(); err != nil {
				t.Fatal(err)
			}
		}
		if nonces[0] == nonces[1] {
			t.Fatalf("both starts probe with nonce %d", nonces[0])
		}
	})
}

func open(t *testing.T, cfg Config) *Server {
	t.Helper()
	srv, err := Open(cfg)
	if err != nil {
		t.Fatalf("open node %d: %v", cfg.ID, err)
	}
	return srv
}

func TestFrozenFollowerDoesNotCampaign(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pn := newPipeNet()
		fsys := newHookFS()
		holds := make(chan time.Duration, 4)
		fsys.hook(func(name string) error {
			if isWAL(name) {
				select {
				case d := <-holds:
					time.Sleep(d)
				default:
				}
			}
			return nil
		})
		peers := []*fakePeer{pn.fake(t, 2, 1, listening), pn.fake(t, 3, 1, listening)}
		cfg := pn.config(1, 3, t.TempDir())
		cfg.FS = fsys
		n := start(t, cfg)
		lead := pn.greet(t, 1, 2, wire.KindMessages)
		index := uint64(0)
		termAt := func(i uint64) uint64 {
			if i == 0 {
				return 0
			}
			return 2
		}
		beat := func() {
			lead.send(raft.Message{Type: raft.MsgApp, From: 2, To: 1, Term: 2, Index: index, LogTerm: termAt(index)})
		}
		grow := func() {
			index++
			lead.send(raft.Message{Type: raft.MsgApp, From: 2, To: 1, Term: 2, Index: index - 1, LogTerm: termAt(index - 1),
				Entries: []raft.Entry{{Index: index, Term: 2, Kind: 1, Body: u128.From64(index).AppendBytes(nil)}}})
		}
		beat()
		time.Sleep(10 * testTick)
		for round := range 2 {
			holds <- 600 * time.Millisecond
			holds <- 5 * time.Second
			grow()
			time.Sleep(testTick)
			grow()
			for range 140 {
				time.Sleep(heartbeatTicks * testTick)
				beat()
			}
			if len(holds) != 0 {
				t.Fatalf("round %d: %d stalls did not happen", round, len(holds))
			}
			for _, p := range peers {
				p.mu.Lock()
				asked := len(p.votes)
				p.mu.Unlock()
				if asked != 0 {
					t.Fatalf("round %d: the follower asked peer %d for %d pre-votes around two stalls of 600ms and 5s", round, p.id, asked)
				}
			}
		}
		if st := pn.connect(t, clientAddr(1), 99).status(); st.Role != raft.Follower || st.Lead != 2 || st.Term != 2 {
			t.Fatalf("after the stalls: %s of term %d following %d", st.Role, st.Term, st.Lead)
		}
		if err := n.halt(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestStalledLeaderReadsAcksBeforeItChecksQuorum(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pn := newPipeNet()
		fsys := newHookFS()
		holds := make(chan time.Duration, 1)
		fsys.hook(func(name string) error {
			if isWAL(name) {
				select {
				case d := <-holds:
					time.Sleep(d)
				default:
				}
			}
			return nil
		})
		peers := []*fakePeer{pn.fake(t, 2, 1, following), pn.fake(t, 3, 1, following)}
		cfg := pn.config(1, 3, t.TempDir())
		cfg.FS = fsys
		began := time.Now()
		n := start(t, cfg)
		most := time.Duration(n.srv.ticks.most)
		writer := pn.connect(t, clientAddr(1), 7)
		probe := pn.connect(t, clientAddr(1), 8)

		var first time.Time
		for first.IsZero() {
			time.Sleep(testTick)
			peers[0].mu.Lock()
			if len(peers[0].beats) > 0 {
				first = peers[0].beats[0]
			}
			peers[0].mu.Unlock()
		}
		elected := first.Sub(began) / testTick
		check := began.Add((elected + electionTicks) * testTick)
		term := peers[0].messages(raft.MsgApp)[0].Term

		time.Sleep(time.Until(check.Add(-testTick / 2)))
		for _, p := range peers {
			p.set(listening)
		}
		time.Sleep(time.Until(check.Add((electionTicks-most)*testTick + testTick/2)))
		if role := probe.status().Role; role != raft.Leader {
			t.Fatalf("before the stall: %s", role)
		}
		stall := most*testTick + testTick/2
		holds <- stall
		writer.write(wire.AppendRequest(nil, testCluster, client.Request{Client: writer.id, Op: client.Register}))
		time.Sleep(200 * time.Millisecond)
		if len(holds) != 0 {
			t.Fatal("the stall did not happen")
		}
		pn.connect(t, clientAddr(1), 9).write(wire.AppendStatusRequest(nil, testCluster))
		synctest.Wait()
		ack := pn.greet(t, 1, 2, wire.KindMessages)
		ack.send(raft.Message{Type: raft.MsgAppResp, From: 2, To: 1, Term: term, Index: 1})
		time.Sleep(stall - 200*time.Millisecond + 2*testTick)
		if role := probe.status().Role; role != raft.Leader {
			t.Fatalf("after a stall of %s with an ack waiting: %s, the leader checked its quorum before it read the ack", stall, role)
		}
	})
}

func TestPeerWithAnotherMaxSessionsIsRefused(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pn := newPipeNet()
		dialed := make(chan net.Conn, 16)
		for _, id := range []uint64{2, 3} {
			l := pn.listen(peerAddr(id))
			t.Cleanup(func() { _ = l.Close() })
			go func() {
				for {
					conn, err := l.Accept()
					if err != nil {
						return
					}
					dialed <- conn
				}
			}()
		}
		cfg := pn.config(1, 3, t.TempDir())
		out := logged(&cfg)
		n := start(t, cfg)

		conn := <-dialed
		t.Cleanup(func() { _ = conn.Close() })
		_ = conn.SetDeadline(time.Now().Add(patience))
		frame, _, err := wire.ReadFrame(conn, nil, testCluster, wire.HelloSize)
		if err != nil {
			t.Fatal(err)
		}
		h, err := frame.Hello()
		if err != nil || h.From != 1 || h.MaxSessions != testSessions {
			t.Fatalf("node 1 greets with %+v, %v", h, err)
		}
		toss(conn, wire.AppendHello(nil, testCluster, wire.Hello{From: h.To, To: 1, MaxSessions: testSessions + 1, Kind: h.Kind}))
		if b, err := io.ReadAll(conn); err != nil || len(b) != 0 {
			t.Fatalf("node 1 sent %d bytes after a hello with another MaxSessions, %v", len(b), err)
		}

		for _, kind := range []uint8{wire.KindMessages, wire.KindSnapshot} {
			time.Sleep(refusalGap)
			conn, err := pn.dial(t.Context(), peerAddr(1))
			if err != nil {
				t.Fatal(err)
			}
			toss(conn, wire.AppendHello(nil, testCluster, wire.Hello{From: 2, To: 1, MaxSessions: testSessions + 1, Kind: kind}))
			_ = conn.SetReadDeadline(time.Now().Add(patience))
			if b, err := io.ReadAll(conn); err != nil || len(b) != 0 {
				t.Errorf("hello of kind %d with MaxSessions %d: read %d bytes and %v, want the connection closed without a hello", kind, testSessions+1, len(b), err)
			}
		}
		synctest.Wait()
		if err := n.halt(); err != nil {
			t.Fatal(err)
		}
		refusals := out.count("refused peer hello from pipe: server: peer runs with another MaxSessions: node ")
		if exact := out.count(" has 65, node 1 has 64"); refusals != 3 || exact != 3 {
			t.Fatalf("%d refusals, %d naming both values; want one for the hello node 1 got back and two for the hellos it was sent:\n%s", refusals, exact, strings.Join(out.lines(), "\n"))
		}
	})
}
