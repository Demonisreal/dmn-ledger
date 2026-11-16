package client

import (
	"encoding/binary"
	"errors"
	"slices"

	"github.com/Demonisreal/dmn-ledger/internal/u128"
)

const (
	busyTicks = 2
	busyHops  = 3
)

var (
	ErrEvicted  = errors.New("client: session evicted")
	ErrInFlight = errors.New("client: a request is still in flight")
	ErrOp       = errors.New("client: op cannot be submitted")
)

type Message struct {
	To      uint64
	Request Request
}

type Session struct {
	id      u128.Uint128
	nodes   []uint64
	retry   int
	session uint64
	number  uint64

	req      Request
	inflight bool
	evicted  bool
	target   int
	ticks    int
	due      int
	busy     int
	hop      bool
	sent     bool

	done   Reply
	ready  bool
	outbox []Message
}

func New(id u128.Uint128, nodes []uint64, retryTicks int) *Session {
	if len(nodes) == 0 || retryTicks < 1 {
		panic("client: New needs at least one node and retryTicks of at least 1")
	}
	s := &Session{id: id, nodes: slices.Clone(nodes), retry: retryTicks}
	s.target = int(id.Lo % uint64(len(nodes))) //nolint:gosec // G115: below len(nodes)
	s.start(Request{Client: id, Op: Register})
	return s
}

func (s *Session) Submit(op Op, body []byte) error {
	switch {
	case s.evicted:
		return ErrEvicted
	case s.inflight:
		return ErrInFlight
	case op <= Register || op > Links:
		return ErrOp
	}
	s.number++
	s.start(Request{Client: s.id, Session: s.session, Number: s.number, Op: op, Body: slices.Clone(body)})
	return nil
}

func (s *Session) start(req Request) {
	s.req, s.inflight, s.busy = req, true, 0
	s.send()
}

func (s *Session) send() {
	s.outbox = append(s.outbox, Message{To: s.nodes[s.target], Request: s.req})
	s.sent = true
	s.ticks, s.due, s.hop = 0, s.retry, true
}

func (s *Session) Tick() {
	s.sent = false
	if !s.inflight {
		return
	}
	s.ticks++
	if s.ticks < s.due {
		return
	}
	if s.hop {
		s.target, s.busy = (s.target+1)%len(s.nodes), 0
	}
	s.send()
}

func (s *Session) Step(r Reply) {
	if !s.inflight || r.Client != s.id || r.Number != s.req.Number {
		return
	}
	switch r.Status {
	case NotLeader:
		if k := slices.Index(s.nodes, r.Leader); r.Leader != 0 && k >= 0 {
			s.target = k
		} else {
			s.target = (s.target + 1) % len(s.nodes)
		}
		s.busy = 0
		if !s.sent {
			s.send()
		} else {
			s.ticks, s.due, s.hop = 0, 1, false
		}
		return
	case Busy, BadClock:
		s.busy++
		s.ticks, s.due, s.hop = 0, busyTicks, s.busy >= busyHops
		return
	case SessionUnknown:
		s.evicted = true
	case OK:
		if s.req.Op == Register {
			if len(r.Body) != 8 {
				return
			}
			s.session = binary.BigEndian.Uint64(r.Body)
		}
	}
	s.inflight = false
	s.done, s.ready = r, true
}

func (s *Session) Unreachable(node uint64) {
	if !s.inflight || s.nodes[s.target] != node {
		return
	}
	s.target, s.busy = (s.target+1)%len(s.nodes), 0
	if !s.sent {
		s.send()
	} else {
		s.ticks, s.due, s.hop = 0, 1, false
	}
}

func (s *Session) Since() uint64 {
	return s.session
}

func (s *Session) Outbox() []Message {
	out := s.outbox
	s.outbox = nil
	return out
}

func (s *Session) Result() (Reply, bool) {
	if !s.ready {
		return Reply{}, false
	}
	s.ready = false
	return s.done, true
}
