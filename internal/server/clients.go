package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/Demonisreal/dmn-ledger/internal/client"
	"github.com/Demonisreal/dmn-ledger/internal/u128"
	"github.com/Demonisreal/dmn-ledger/internal/wire"
)

const (
	clientQueue  = 4
	firstFrame   = 5 * time.Second
	bodyTimeout  = 10 * time.Second
	writeTimeout = 5 * time.Second
	acceptPause  = 50 * time.Millisecond
)

var (
	errTooManyClients = errors.New("server: too many client connections")
	errSecondClient   = errors.New("server: a second client id on one connection")
	errFrameType      = errors.New("server: frame of the wrong type for this port")
)

type clientConn struct {
	conn net.Conn
	out  chan []byte
	gone chan struct{}
}

type frameReader struct {
	conn net.Conn
	head int
}

func (r *frameReader) Read(b []byte) (int, error) {
	if r.head == 0 {
		_ = r.conn.SetReadDeadline(time.Now().Add(bodyTimeout))
		r.head = -1
	}
	n, err := r.conn.Read(b)
	if r.head > 0 {
		r.head -= n
	}
	return n, err
}

func (r *frameReader) idle() {
	_ = r.conn.SetReadDeadline(time.Time{})
	r.head = wire.HeaderSize
}

func (s *Server) acceptClients(ctx context.Context) {
	for {
		conn, err := s.cfg.Clients.Accept()
		if err != nil {
			if !keepAccepting(ctx, err) {
				return
			}
			continue
		}
		select {
		case s.slots <- struct{}{}:
			s.wg.Go(func() { s.serveClient(ctx, conn) })
		default:
			s.refused("client", conn, errTooManyClients)
			_ = conn.Close()
		}
	}
}

func (s *Server) serveClient(ctx context.Context, conn net.Conn) {
	c := &clientConn{conn: conn, out: make(chan []byte, clientQueue), gone: make(chan struct{})}
	done := closeOnStop(ctx, conn)
	s.wg.Go(c.write)
	id, bound, err := s.readClient(ctx, c)
	if ctx.Err() == nil {
		s.refused("client frame", conn, err)
	}
	done()
	close(c.gone)
	if bound {
		s.post(ctx, event{kind: connClosed, conn: c, req: client.Request{Client: id}})
	}
	<-s.slots
}

func (s *Server) readClient(ctx context.Context, c *clientConn) (u128.Uint128, bool, error) {
	var id u128.Uint128
	bound := false
	r := &frameReader{conn: c.conn, head: -1}
	_ = c.conn.SetReadDeadline(time.Now().Add(firstFrame))
	var buf []byte
	for {
		var frame wire.Frame
		var err error
		frame, buf, err = wire.ReadFrame(r, buf, s.cfg.Cluster, wire.MaxClientBody)
		if err != nil {
			return id, bound, err
		}
		ev := event{kind: gotStatus, conn: c}
		switch frame.Type {
		case wire.TypeRequest:
			req, err := frame.Request()
			switch {
			case err != nil:
				return id, bound, err
			case bound && req.Client != id:
				return id, bound, errSecondClient
			}
			id, bound = req.Client, true
			req.Body = bytes.Clone(req.Body)
			ev.kind, ev.req = gotRequest, req
		case wire.TypeStatusRequest:
			if err := frame.StatusRequest(); err != nil {
				return id, bound, err
			}
		default:
			return id, bound, fmt.Errorf("%w: %#x", errFrameType, uint8(frame.Type))
		}
		if !s.post(ctx, ev) {
			return id, bound, nil
		}
		r.idle()
	}
}

func (s *Server) post(ctx context.Context, ev event) bool {
	select {
	case s.events <- ev:
		return true
	case <-ctx.Done():
		return false
	}
}

func (c *clientConn) write() {
	for {
		select {
		case frame := <-c.out:
			_ = c.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
			if _, err := c.conn.Write(frame); err != nil {
				_ = c.conn.Close()
				return
			}
		case <-c.gone:
			return
		}
	}
}

func (s *Server) reply(c *clientConn, frame []byte) {
	select {
	case c.out <- frame:
	default:
		_ = c.conn.Close()
	}
}
