package client

import (
	"encoding/binary"
	"errors"
	"testing"

	"github.com/Demonisreal/dmn-ledger/internal/u128"
)

var (
	alice = u128.Uint128{Hi: 7, Lo: 3}
	nodes = []uint64{1, 2, 3}
)

func registered(t *testing.T) *Session {
	t.Helper()
	s := New(alice, nodes, 10)
	out := s.Outbox()
	if len(out) != 1 || out[0].Request.Op != Register || out[0].Request.Number != 0 || out[0].To != 1 {
		t.Fatalf("first message %+v, want Register #0 to node 1", out)
	}
	s.Step(Reply{Client: alice, Op: Register, Status: OK, Body: binary.BigEndian.AppendUint64(nil, 42)})
	if r, ok := s.Result(); !ok || r.Op != Register {
		t.Fatalf("register result %+v %v", r, ok)
	}
	return s
}

func submit(t *testing.T, s *Session, op Op) Message {
	t.Helper()
	if err := s.Submit(op, []byte{1}); err != nil {
		t.Fatal(err)
	}
	out := s.Outbox()
	if len(out) != 1 {
		t.Fatalf("outbox %+v, want one request", out)
	}
	return out[0]
}

func TestRegisterComesFirst(t *testing.T) {
	s := New(alice, nodes, 10)
	if err := s.Submit(CreateAccounts, nil); !errors.Is(err, ErrInFlight) {
		t.Fatalf("submit during register: %v", err)
	}
	s.Outbox()
	s.Step(Reply{Client: alice, Op: Register, Status: OK, Body: []byte{1, 2}})
	if _, ok := s.Result(); ok {
		t.Fatal("register reply without a session number was taken")
	}
	s.Step(Reply{Client: alice, Op: Register, Status: OK, Body: binary.BigEndian.AppendUint64(nil, 42)})
	if _, ok := s.Result(); !ok {
		t.Fatal("no register result")
	}
	m := submit(t, s, CreateAccounts)
	if m.Request.Session != 42 || m.Request.Number != 1 || m.Request.Client != alice {
		t.Fatalf("request %+v, want session 42 number 1", m.Request)
	}
}

func TestNumbersRiseForEveryRequest(t *testing.T) {
	s := registered(t)
	for want, op := range []Op{CreateAccounts, LookupAccounts, CreateTransfers, LookupTransfers} {
		m := submit(t, s, op)
		if m.Request.Number != uint64(want+1) {
			t.Fatalf("%s got number %d, want %d", op, m.Request.Number, want+1)
		}
		if err := s.Submit(op, nil); !errors.Is(err, ErrInFlight) {
			t.Fatalf("second submit: %v", err)
		}
		s.Step(Reply{Client: alice, Number: m.Request.Number, Op: op, Status: OK})
		if _, ok := s.Result(); !ok {
			t.Fatal("no result")
		}
		if _, ok := s.Result(); ok {
			t.Fatal("result reported twice")
		}
	}
}

func TestForeignRepliesAreDropped(t *testing.T) {
	s := registered(t)
	m := submit(t, s, CreateTransfers)
	for _, r := range []Reply{
		{Client: alice, Number: 0, Op: Register, Status: OK},
		{Client: alice, Number: 2, Op: CreateTransfers, Status: OK},
		{Client: u128.From64(9), Number: m.Request.Number, Op: CreateTransfers, Status: OK},
	} {
		s.Step(r)
		if _, ok := s.Result(); ok {
			t.Fatalf("reply %+v was taken", r)
		}
	}
}

func TestNotLeaderFollowsTheHint(t *testing.T) {
	s := registered(t)
	m := submit(t, s, CreateTransfers)
	s.Step(Reply{Client: alice, Number: m.Request.Number, Status: NotLeader, Leader: 3})
	if out := s.Outbox(); len(out) != 0 {
		t.Fatalf("resent in the tick it was first sent: %+v", out)
	}
	s.Tick()
	out := s.Outbox()
	if len(out) != 1 || out[0].To != 3 || out[0].Request.Number != m.Request.Number {
		t.Fatalf("after hint: %+v, want the same request to node 3", out)
	}
	s.Tick()
	s.Step(Reply{Client: alice, Number: m.Request.Number, Status: NotLeader})
	out = s.Outbox()
	if len(out) != 1 || out[0].To != 1 {
		t.Fatalf("without hint: %+v, want node 1", out)
	}
	s.Step(Reply{Client: alice, Number: m.Request.Number, Status: NotLeader, Leader: 2})
	if out := s.Outbox(); len(out) != 0 {
		t.Fatalf("second resend in one tick: %+v", out)
	}
}

func TestBusyWaits(t *testing.T) {
	s := registered(t)
	m := submit(t, s, CreateTransfers)
	s.Tick()
	s.Step(Reply{Client: alice, Number: m.Request.Number, Status: Busy})
	for i := 1; i < busyTicks; i++ {
		s.Tick()
		if out := s.Outbox(); len(out) != 0 {
			t.Fatalf("resent after %d ticks", i)
		}
	}
	s.Tick()
	if out := s.Outbox(); len(out) != 1 || out[0].To != m.To {
		t.Fatalf("after the pause: %+v, want the same node", out)
	}
}

func TestTimeoutMovesToTheNextNode(t *testing.T) {
	s := registered(t)
	m := submit(t, s, CreateTransfers)
	for range 9 {
		s.Tick()
	}
	if out := s.Outbox(); len(out) != 0 {
		t.Fatalf("resent early: %+v", out)
	}
	s.Tick()
	out := s.Outbox()
	if len(out) != 1 || out[0].To != 2 {
		t.Fatalf("after the timeout: %+v, want node 2", out)
	}
	if got := out[0].Request; got.Number != m.Request.Number || string(got.Body) != string(m.Request.Body) {
		t.Fatalf("retry %+v differs from %+v", got, m.Request)
	}
}

func TestEvictionEndsTheSession(t *testing.T) {
	s := registered(t)
	m := submit(t, s, CreateTransfers)
	s.Step(Reply{Client: alice, Number: m.Request.Number, Status: SessionUnknown})
	if r, ok := s.Result(); !ok || r.Status != SessionUnknown {
		t.Fatalf("result %+v %v, want SessionUnknown", r, ok)
	}
	if err := s.Submit(CreateTransfers, nil); !errors.Is(err, ErrEvicted) {
		t.Fatalf("submit after eviction: %v", err)
	}
	s.Tick()
	if out := s.Outbox(); len(out) != 0 {
		t.Fatalf("evicted session still sends: %+v", out)
	}
}

func TestSubmitRejectsRegister(t *testing.T) {
	s := registered(t)
	for _, op := range []Op{0, Register, LookupTransfers + 1} {
		if err := s.Submit(op, nil); !errors.Is(err, ErrOp) {
			t.Fatalf("submit %s: %v", op, err)
		}
	}
}
