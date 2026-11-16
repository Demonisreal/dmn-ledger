package faultnet

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"math/bits"
	"math/rand/v2"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/Demonisreal/dmn-ledger/internal/raft"
	"github.com/Demonisreal/dmn-ledger/internal/wire"
)

const cluster = 1

type echo struct {
	addr string

	mu    sync.Mutex
	got   []byte
	first time.Time
}

func startEcho(t *testing.T) *echo {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	e := &echo{addr: ln.Addr().String()}
	var (
		wg     sync.WaitGroup
		mu     sync.Mutex
		conns  []net.Conn
		closed bool
	)
	wg.Go(func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			if closed {
				mu.Unlock()
				_ = c.Close()
				continue
			}
			conns = append(conns, c)
			mu.Unlock()
			wg.Go(func() { e.serve(c) })
		}
	})
	t.Cleanup(func() {
		_ = ln.Close()
		mu.Lock()
		closed = true
		for _, c := range conns {
			_ = c.Close()
		}
		mu.Unlock()
		wg.Wait()
	})
	return e
}

func (e *echo) serve(c net.Conn) {
	buf := make([]byte, 64<<10)
	for {
		n, err := c.Read(buf)
		e.mu.Lock()
		if n > 0 && e.first.IsZero() {
			e.first = time.Now()
		}
		e.got = append(e.got, buf[:n]...)
		e.mu.Unlock()
		if _, werr := c.Write(buf[:n]); werr != nil || err != nil {
			return
		}
	}
}

func (e *echo) seen() []byte {
	e.mu.Lock()
	defer e.mu.Unlock()
	return bytes.Clone(e.got)
}

func listen(t *testing.T, target string) *Link {
	t.Helper()
	l, err := Listen(target, rand.New(rand.NewPCG(7, 1)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := l.Close(); err != nil {
			t.Error(err)
		}
	})
	return l
}

func dial(t *testing.T, l *Link) net.Conn {
	t.Helper()
	c, err := net.DialTimeout("tcp", l.Addr(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func chunk(i, size int) []byte {
	body := bytes.Repeat([]byte{byte(i)}, size)
	binary.BigEndian.PutUint32(body, uint32(i))
	return wire.AppendChunk(nil, cluster, body)
}

func message(t *testing.T, m raft.Message) []byte {
	t.Helper()
	frame, err := wire.AppendMessage(nil, cluster, m)
	if err != nil {
		t.Fatal(err)
	}
	return frame
}

func send(t *testing.T, c net.Conn, frames ...[]byte) []byte {
	t.Helper()
	stream := bytes.Join(frames, nil)
	if err := c.SetWriteDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Write(stream); err != nil {
		t.Fatalf("write: %v", err)
	}
	return stream
}

func expect(t *testing.T, c net.Conn, want []byte) {
	t.Helper()
	if err := c.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	back := make([]byte, len(want))
	if _, err := io.ReadFull(c, back); err != nil {
		t.Fatalf("read %d bytes back: %v", len(want), err)
	}
	if !bytes.Equal(back, want) {
		t.Fatal("bytes came back changed")
	}
}

func expectClosed(t *testing.T, c net.Conn) {
	t.Helper()
	if err := c.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	n, err := c.Read(make([]byte, 64))
	switch {
	case errors.Is(err, os.ErrDeadlineExceeded):
		t.Fatal("connection still open")
	case err == nil:
		t.Fatalf("read %d bytes from a closed connection", n)
	}
}

func expectSilence(t *testing.T, c net.Conn, e *echo) {
	t.Helper()
	if err := c.SetReadDeadline(time.Now().Add(200 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if n, err := c.Read(make([]byte, 64)); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("read %d bytes, %v; want nothing", n, err)
	}
	if got := e.seen(); len(got) > 0 {
		t.Fatalf("target got %d bytes, want none", len(got))
	}
}

func readFrames(t *testing.T, r io.Reader, until wire.Type) []wire.Frame {
	t.Helper()
	var frames []wire.Frame
	for {
		f, _, err := wire.ReadFrame(r, nil, cluster, wire.MaxPeerBody)
		if err != nil {
			t.Fatalf("frame %d: %v", len(frames), err)
		}
		frames = append(frames, f)
		if f.Type == until {
			return frames
		}
	}
}

func held(l *Link) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for c := range l.conns {
		n += c.held
	}
	return n
}

func TestCut(t *testing.T) {
	e := startEcho(t)
	l := listen(t, e.addr)
	c := dial(t, l)
	expect(t, c, send(t, c, chunk(1, 100)))

	l.Set(Fault{Cut: true})
	expectClosed(t, c)
	expectClosed(t, dial(t, l))
}

func TestBlackhole(t *testing.T) {
	e := startEcho(t)
	l := listen(t, e.addr)
	c := dial(t, l)
	l.Set(Fault{Blackhole: true})

	for i := range 256 {
		send(t, c, chunk(i, 64<<10))
	}
	expectSilence(t, c, e)

	l.Set(Fault{})
	expectClosed(t, c)
	fresh := dial(t, l)
	expect(t, fresh, send(t, fresh, chunk(1, 100)))
}

func TestStall(t *testing.T) {
	e := startEcho(t)
	l := listen(t, e.addr)
	c := dial(t, l)
	if err := c.(*net.TCPConn).SetWriteBuffer(64 << 10); err != nil {
		t.Fatal(err)
	}
	l.Set(Fault{Stall: true})

	var frames [][]byte
	for i := range 256 {
		frames = append(frames, chunk(i, 64<<10))
	}
	stream := bytes.Join(frames, nil)
	sent := 0
	for sent < len(stream) {
		if err := c.SetWriteDeadline(time.Now().Add(300 * time.Millisecond)); err != nil {
			t.Fatal(err)
		}
		n, err := c.Write(stream[sent:min(sent+len(frames[0]), len(stream))])
		sent += n
		if errors.Is(err, os.ErrDeadlineExceeded) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if sent == len(stream) {
		t.Fatalf("all %d bytes went out while stalled", sent)
	}
	if n := held(l); n > len(frames[0]) {
		t.Fatalf("proxy holds %d bytes while stalled, want at most one frame of %d", n, len(frames[0]))
	}
	expectSilence(t, c, e)

	l.Set(Fault{})
	upto := (sent + len(frames[0]) - 1) / len(frames[0]) * len(frames[0])
	if err := c.SetDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	back := make(chan error, 1)
	go func() {
		got := make([]byte, upto)
		_, err := io.ReadFull(c, got)
		if err == nil && !bytes.Equal(got, stream[:upto]) {
			err = errors.New("stream came back changed")
		}
		back <- err
	}()
	if _, err := c.Write(stream[sent:upto]); err != nil {
		t.Fatal(err)
	}
	if err := <-back; err != nil {
		t.Fatalf("after heal: %v", err)
	}
	if !bytes.Equal(e.seen(), stream[:upto]) {
		t.Fatal("target got another stream")
	}
}

func TestDelayKeepsTheOrder(t *testing.T) {
	const delay = 50 * time.Millisecond
	e := startEcho(t)
	l := listen(t, e.addr)
	l.Set(Fault{Delay: delay, Jitter: delay})
	c := dial(t, l)

	var stream []byte
	sent := make([]time.Time, 100)
	for i := range sent {
		sent[i] = time.Now()
		stream = append(stream, send(t, c, chunk(i, 16))...)
	}
	if err := c.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	var back bytes.Buffer
	r := io.TeeReader(c, &back)
	for i := range sent {
		f, _, err := wire.ReadFrame(r, nil, cluster, wire.MaxPeerBody)
		if err != nil {
			t.Fatalf("frame %d: %v", i, err)
		}
		if early := time.Since(sent[i]); early < delay {
			t.Fatalf("frame %d back after %v, want at least %v", i, early, delay)
		}
		if got := binary.BigEndian.Uint32(f.Body); got != uint32(i) {
			t.Fatalf("frame %d came back as frame %d", i, got)
		}
	}
	if !bytes.Equal(back.Bytes(), stream) || !bytes.Equal(e.seen(), stream) {
		t.Fatal("stream changed on the way")
	}
	e.mu.Lock()
	arrived := e.first.Sub(sent[0])
	e.mu.Unlock()
	if arrived < delay {
		t.Fatalf("target got the first frame after %v, want at least %v", arrived, delay)
	}
}

func TestDelayHoldsAtMostFourMiB(t *testing.T) {
	e := startEcho(t)
	l := listen(t, e.addr)
	l.Set(Fault{Delay: time.Hour})
	c := dial(t, l)

	frame := chunk(1, 64<<10)
	sent := 0
	for sent < 16<<20 {
		if err := c.SetWriteDeadline(time.Now().Add(300 * time.Millisecond)); err != nil {
			t.Fatal(err)
		}
		n, err := c.Write(frame)
		sent += n
		if errors.Is(err, os.ErrDeadlineExceeded) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if n := held(l); n > maxHeld || n <= maxHeld-len(frame) {
		t.Fatalf("proxy holds %d bytes after %d were sent, want just under %d", n, sent, maxHeld)
	}
	expectSilence(t, c, e)
}

func TestLossDropsWholeFrames(t *testing.T) {
	for _, tc := range []struct {
		name     string
		loss     float64
		min, max int
	}{
		{name: "all", loss: 1, min: 0, max: 0},
		{name: "half", loss: 0.5, min: 60, max: 140},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := startEcho(t)
			l := listen(t, e.addr)
			l.Set(Fault{Loss: tc.loss})
			c := dial(t, l)

			frames := [][]byte{wire.AppendHello(nil, cluster, wire.Hello{From: 1, To: 2, Kind: wire.KindMessages})}
			for i := range 200 {
				m := raft.Message{Type: raft.MsgAppResp, From: 1, To: 2, Term: 3, Index: uint64(i + 1)}
				frames = append(frames, message(t, m))
				if i%10 == 0 {
					frames = append(frames, wire.AppendStatusRequest(nil, cluster))
				}
			}
			frames = append(frames, chunk(0, 8))
			send(t, c, frames...)

			if err := c.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
				t.Fatal(err)
			}
			var back bytes.Buffer
			got := readFrames(t, io.TeeReader(c, &back), wire.TypeSnapshotChunk)
			if got[0].Type != wire.TypeHello {
				t.Fatalf("first frame %#x, want the hello", got[0].Type)
			}
			kept, status := 0, 0
			last := uint64(0)
			for _, f := range got[1:] {
				switch f.Type {
				case wire.TypeStatusRequest:
					status++
				case wire.TypeMessage + wire.Type(raft.MsgAppResp):
					m, err := f.Message()
					if err != nil {
						t.Fatal(err)
					}
					if m.Index <= last {
						t.Fatalf("index %d after %d", m.Index, last)
					}
					last = m.Index
					kept++
				}
			}
			if status != 20 {
				t.Fatalf("%d of 20 status requests came through", status)
			}
			if kept < tc.min || kept > tc.max {
				t.Fatalf("%d of 200 raft frames kept, want %d to %d", kept, tc.min, tc.max)
			}
			if !bytes.Equal(e.seen(), back.Bytes()) {
				t.Fatal("target got another stream than came back")
			}
		})
	}
}

func TestLossSparesSnapshots(t *testing.T) {
	e := startEcho(t)
	l := listen(t, e.addr)
	l.Set(Fault{Loss: 1})
	snap := message(t, raft.Message{Type: raft.MsgSnap, From: 1, To: 2, Term: 3, Index: 40, LogTerm: 2, Commit: 40, Seq: 5})

	c := dial(t, l)
	frames := [][]byte{wire.AppendHello(nil, cluster, wire.Hello{From: 1, To: 2, Kind: wire.KindSnapshot}), snap}
	castagnoli := crc32.MakeTable(crc32.Castagnoli)
	crc := uint32(0)
	size := 0
	for i := range 40 {
		frame := chunk(i, 4<<10)
		crc = crc32.Update(crc, castagnoli, frame[wire.HeaderSize:])
		size += len(frame) - wire.HeaderSize
		frames = append(frames, frame)
	}
	frames = append(frames, wire.AppendSnapshotEnd(nil, cluster, uint64(size), crc))
	stream := send(t, c, frames...)
	expect(t, c, stream)
	if !bytes.Equal(e.seen(), stream) {
		t.Fatal("target got another stream")
	}

	other := dial(t, l)
	hello := wire.AppendHello(nil, cluster, wire.Hello{From: 1, To: 2, Kind: wire.KindMessages})
	end := chunk(0, 8)
	send(t, other, hello, snap, end)
	if err := other.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if got := readFrames(t, other, wire.TypeSnapshotChunk); len(got) != 2 {
		t.Fatalf("%d frames on a message connection, want the snapshot message lost", len(got))
	}
}

func TestFlip(t *testing.T) {
	e := startEcho(t)
	l := listen(t, e.addr)
	l.Set(Fault{Flip: 1})
	c := dial(t, l)

	var frames [][]byte
	for i := range 50 {
		frames = append(frames, chunk(i, 100))
	}
	stream := send(t, c, frames...)
	if err := c.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	back := make([]byte, len(stream))
	if _, err := io.ReadFull(c, back); err != nil {
		t.Fatal(err)
	}
	at := 0
	for i, frame := range frames {
		flipped := 0
		for j := range frame {
			flipped += bits.OnesCount8(frame[j] ^ back[at+j])
		}
		if flipped != 1 {
			t.Fatalf("frame %d has %d flipped bits, want 1", i, flipped)
		}
		at += len(frame)
	}
	if !bytes.Equal(e.seen(), back) {
		t.Fatal("target got another stream than came back")
	}
}

func TestHeal(t *testing.T) {
	for name, f := range map[string]Fault{
		"cut":       {Cut: true},
		"blackhole": {Blackhole: true},
		"stall":     {Stall: true},
		"delay":     {Delay: time.Hour},
		"loss":      {Loss: 1},
		"flip":      {Flip: 1},
	} {
		t.Run(name, func(t *testing.T) {
			e := startEcho(t)
			l := listen(t, e.addr)
			old := dial(t, l)
			expect(t, old, send(t, old, chunk(0, 100)))
			l.Set(f)
			l.Set(Fault{})

			vote := message(t, raft.Message{Type: raft.MsgVote, From: 1, To: 2, Term: 4, Index: 9, LogTerm: 3})
			if f.Cut {
				expectClosed(t, old)
			} else {
				expect(t, old, send(t, old, vote))
			}
			fresh := dial(t, l)
			expect(t, fresh, send(t, fresh, vote))
		})
	}
}
