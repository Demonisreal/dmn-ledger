package client

import "testing"

func TestUnreachableMovesOn(t *testing.T) {
	s := registered(t)
	m := submit(t, s, CreateTransfers)
	s.Tick()
	s.Unreachable(m.To)
	out := s.Outbox()
	if len(out) != 1 || out[0].To != 2 {
		t.Fatalf("after node %d went away: %+v, want one request to node 2", m.To, out)
	}
	for range 9 {
		s.Tick()
	}
	if out := s.Outbox(); len(out) != 0 {
		t.Fatalf("resent before the timeout of node 2: %+v", out)
	}
	s.Tick()
	if out := s.Outbox(); len(out) != 1 || out[0].To != 3 {
		t.Fatalf("after the timeout of node 2: %+v, want node 3", out)
	}
}

func TestUnreachableOncePerTick(t *testing.T) {
	s := registered(t)
	m := submit(t, s, CreateTransfers)
	for tick := range 7 {
		next := m.To%3 + 1
		s.Tick()
		s.Unreachable(m.To)
		attempts := 0
		for range 5 {
			for _, again := range s.Outbox() {
				attempts++
				m = again
				s.Unreachable(m.To)
			}
		}
		if attempts != 1 || m.To != next {
			t.Fatalf("tick %d: %d attempts, the last to node %d, want one to node %d", tick, attempts, m.To, next)
		}
	}
}

func TestUnreachableOfAnotherNodeChangesNothing(t *testing.T) {
	s := registered(t)
	s.Unreachable(1)
	if out := s.Outbox(); len(out) != 0 {
		t.Fatalf("session without a request sent %+v", out)
	}
	m := submit(t, s, CreateTransfers)
	if m.To != 1 {
		t.Fatalf("request went to node %d, want node 1", m.To)
	}
	for range 5 {
		s.Tick()
	}
	for _, node := range []uint64{2, 3, 0, 9} {
		s.Unreachable(node)
	}
	for range 4 {
		s.Tick()
	}
	if out := s.Outbox(); len(out) != 0 {
		t.Fatalf("resent before the timeout of node 1: %+v", out)
	}
	s.Tick()
	if out := s.Outbox(); len(out) != 1 || out[0].To != 2 {
		t.Fatalf("after the timeout of node 1: %+v, want node 2", out)
	}
}

func TestUnreachableKeepsTheNumber(t *testing.T) {
	s := registered(t)
	m := submit(t, s, CreateTransfers)
	for hop := range 4 {
		s.Tick()
		s.Unreachable(m.To)
		out := s.Outbox()
		if len(out) != 1 {
			t.Fatalf("hop %d: outbox %+v, want one request", hop, out)
		}
		if got := out[0].Request; got.Number != m.Request.Number || got.Session != m.Request.Session || string(got.Body) != string(m.Request.Body) {
			t.Fatalf("hop %d: retry %+v differs from %+v", hop, got, m.Request)
		}
		m = out[0]
	}
	s.Step(Reply{Client: alice, Number: m.Request.Number, Op: CreateTransfers, Status: OK})
	if r, ok := s.Result(); !ok || r.Status != OK {
		t.Fatalf("result %+v %v, want OK", r, ok)
	}
	if next := submit(t, s, CreateTransfers); next.Request.Number != m.Request.Number+1 || next.To != m.To {
		t.Fatalf("next request %+v, want number %d to node %d", next, m.Request.Number+1, m.To)
	}
}
