package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Demonisreal/dmn-ledger/internal/raft"
	"github.com/Demonisreal/dmn-ledger/internal/wire"
)

const (
	peerQueue    = 64
	peerBytes    = 8 << 20
	dialTimeout  = time.Second
	helloTimeout = 2 * time.Second
	minBackoff   = 50 * time.Millisecond
	maxBackoff   = time.Second
	hellosOfPeer = 4
)

var (
	errStranger      = errors.New("server: frame from an unexpected peer")
	errSnapshotBusy  = errors.New("server: a snapshot from this peer is still in flight")
	errTooManyHellos = errors.New("server: too many peer connections waiting for their hello")
)

type peer struct {
	id        uint64
	addr      string
	out       chan []byte
	queued    atomic.Int64
	sending   atomic.Bool
	receiving atomic.Bool

	mu sync.Mutex
	in net.Conn
}

func (p *peer) push(frame []byte) {
	size := int64(len(frame))
	if p.queued.Add(size) > peerBytes {
		p.queued.Add(-size)
		return
	}
	select {
	case p.out <- frame:
	default:
		p.queued.Add(-size)
	}
}

func (p *peer) replace(conn net.Conn) {
	p.mu.Lock()
	old := p.in
	p.in = conn
	p.mu.Unlock()
	if old != nil {
		_ = old.Close()
	}
}

func (p *peer) forget(conn net.Conn) {
	p.mu.Lock()
	if p.in == conn {
		p.in = nil
	}
	p.mu.Unlock()
}

func (s *Server) writePeer(ctx context.Context, p *peer) {
	pause := minBackoff
	var conn net.Conn
	var done func()
	defer func() {
		if conn != nil {
			done()
		}
	}()
	for {
		var frame []byte
		select {
		case frame = <-p.out:
		case <-ctx.Done():
			return
		}
		p.queued.Add(-int64(len(frame)))
		var err error
		if conn == nil {
			conn, done, err = s.dial(ctx, p, wire.KindMessages)
		}
		if err == nil {
			_ = conn.SetWriteDeadline(time.Now().Add(writeTimeout))
			_, err = conn.Write(frame)
		}
		if err == nil {
			pause = minBackoff
			continue
		}
		if conn != nil {
			done()
			conn = nil
		}
		if !sleep(ctx, pause) {
			return
		}
		pause = min(2*pause, maxBackoff)
	}
}

func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func (s *Server) dial(ctx context.Context, p *peer, kind uint8) (net.Conn, func(), error) {
	dctx, cancel := context.WithTimeout(ctx, dialTimeout)
	conn, err := s.cfg.Dial(dctx, p.addr)
	cancel()
	if err != nil {
		return nil, nil, err
	}
	done := closeOnStop(ctx, conn)
	if err := s.greet(conn, p.id, kind); err != nil {
		done()
		return nil, nil, err
	}
	return conn, done, nil
}

func (s *Server) greet(conn net.Conn, to uint64, kind uint8) error {
	_ = conn.SetDeadline(time.Now().Add(helloTimeout))
	if _, err := conn.Write(wire.AppendHello(nil, s.cfg.Cluster, wire.Hello{From: s.cfg.ID, To: to, Kind: kind})); err != nil {
		return err
	}
	frame, _, err := wire.ReadFrame(conn, nil, s.cfg.Cluster, wire.HelloSize)
	if err != nil {
		return err
	}
	h, err := frame.Hello()
	if err != nil {
		return err
	}
	if h != (wire.Hello{From: to, To: s.cfg.ID, Kind: kind}) {
		return errStranger
	}
	return conn.SetDeadline(time.Time{})
}

func (s *Server) acceptPeers(ctx context.Context) {
	for {
		conn, err := s.cfg.Peering.Accept()
		if err != nil {
			if !keepAccepting(ctx, err) {
				return
			}
			continue
		}
		select {
		case s.hellos <- struct{}{}:
			s.wg.Go(func() { s.servePeer(ctx, conn) })
		default:
			s.refused("peer hello", conn, errTooManyHellos)
			_ = conn.Close()
		}
	}
}

func (s *Server) servePeer(ctx context.Context, conn net.Conn) {
	done := closeOnStop(ctx, conn)
	defer done()
	h, err := s.hello(conn)
	<-s.hellos
	if err != nil {
		s.refused("peer hello", conn, err)
		return
	}
	p := s.peers[h.From]
	if h.Kind == wire.KindSnapshot {
		if !p.receiving.CompareAndSwap(false, true) {
			s.refused("snapshot", conn, errSnapshotBusy)
			return
		}
		s.receiveSnapshot(ctx, conn, p, h)
		return
	}
	p.replace(conn)
	defer p.forget(conn)
	err = s.welcome(conn, h)
	if err == nil {
		err = s.readPeer(ctx, conn, p)
	}
	if ctx.Err() == nil {
		s.refused("peer message", conn, err)
	}
}

func (s *Server) hello(conn net.Conn) (wire.Hello, error) {
	_ = conn.SetDeadline(time.Now().Add(helloTimeout))
	frame, _, err := wire.ReadFrame(conn, nil, s.cfg.Cluster, wire.HelloSize)
	if err != nil {
		return wire.Hello{}, err
	}
	h, err := frame.Hello()
	switch {
	case err != nil:
		return wire.Hello{}, err
	case h.To != s.cfg.ID, s.peers[h.From] == nil, h.Kind != wire.KindMessages && h.Kind != wire.KindSnapshot:
		return wire.Hello{}, errStranger
	}
	return h, nil
}

func (s *Server) welcome(conn net.Conn, h wire.Hello) error {
	if _, err := conn.Write(wire.AppendHello(nil, s.cfg.Cluster, wire.Hello{From: s.cfg.ID, To: h.From, Kind: h.Kind})); err != nil {
		return err
	}
	return conn.SetDeadline(time.Time{})
}

func (s *Server) readPeer(ctx context.Context, conn net.Conn, p *peer) error {
	r := &frameReader{conn: conn}
	var buf []byte
	for {
		r.idle()
		frame, next, err := wire.ReadFrame(r, buf, s.cfg.Cluster, wire.MaxPeerBody)
		if err != nil {
			return err
		}
		m, err := frame.Message()
		switch {
		case err != nil:
			return err
		case m.From != p.id || m.To != s.cfg.ID || m.Type == raft.MsgSnap:
			return fmt.Errorf("%w: %s from %d to %d on the connection of node %d", errStranger, m.Type, m.From, m.To, p.id)
		}
		buf = next
		if len(m.Entries) > 0 {
			buf = nil
		}
		if !s.post(ctx, event{kind: gotMessage, msg: m}) {
			return nil
		}
	}
}
