package check

import (
	"bytes"
	"encoding/binary"
	"maps"
	"math"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/Demonisreal/dmn-ledger/internal/audit"
	"github.com/Demonisreal/dmn-ledger/internal/client"
	"github.com/Demonisreal/dmn-ledger/internal/disk"
	"github.com/Demonisreal/dmn-ledger/internal/oracle"
	"github.com/Demonisreal/dmn-ledger/internal/raft"
	"github.com/Demonisreal/dmn-ledger/internal/replica"
	"github.com/Demonisreal/dmn-ledger/internal/sim"
	"github.com/Demonisreal/dmn-ledger/internal/state"
	"github.com/Demonisreal/dmn-ledger/internal/u128"
	"github.com/Demonisreal/dmn-ledger/internal/wal"
)

const soloTick = 10_000_000

type answer struct {
	req request
	r   client.Reply
}

type solo struct {
	t       *testing.T
	fsys    *disk.Sim
	n       *replica.Node
	clock   uint64
	numbers map[uint64]uint64
	granted map[uint64]u128.Uint128
	oks     []answer
}

func openSolo(t *testing.T, limit int) *solo {
	t.Helper()
	s := &solo{
		t:       t,
		fsys:    disk.NewSim(rand.New(rand.NewPCG(1, 9))),
		clock:   1_000_000_000_000,
		numbers: make(map[uint64]uint64),
		granted: make(map[uint64]u128.Uint128),
	}
	n, err := replica.Open(replica.Config{
		ID:             1,
		Peers:          []uint64{1},
		FS:             s.fsys,
		Dir:            "/n",
		Clock:          func() uint64 { return s.clock },
		Rand:           rand.New(rand.NewPCG(1, 1)),
		Nonces:         rand.NewChaCha8([32]byte{1}),
		MaxSessions:    limit,
		SnapshotEvery:  1000,
		ClockStep:      soloTick + soloTick/4,
		ElectionTicks:  10,
		HeartbeatTicks: 2,
		SnapshotTicks:  50,
		MaxAppendBytes: 1 << 20,
		MaxUncommitted: 4096,
	})
	if err != nil {
		t.Fatal(err)
	}
	s.n = n
	for range 200 {
		if st := n.Status(); st.Role == raft.Leader && st.Applied == st.Last {
			return s
		}
		s.clock += soloTick
		n.Tick()
		s.process()
	}
	t.Fatalf("no leader: %+v", n.Status())
	return nil
}

func (s *solo) process() []client.Reply {
	s.t.Helper()
	out, err := s.n.Process()
	if err != nil {
		s.t.Fatal(err)
	}
	return out.Replies
}

func (s *solo) call(req client.Request) client.Reply {
	s.t.Helper()
	s.n.Request(req)
	for range 20 {
		for _, r := range s.process() {
			if r.Client == req.Client && r.Number == req.Number && r.Op == req.Op {
				return r
			}
		}
		s.clock += soloTick
		s.n.Tick()
	}
	s.t.Fatalf("no reply to %+v", req)
	return client.Reply{}
}

func (s *solo) register(id uint64) uint64 {
	s.t.Helper()
	r := s.call(client.Request{Client: u128.From64(id), Op: client.Register})
	if r.Status != client.OK || len(r.Body) != 8 {
		s.t.Fatalf("register: %+v", r)
	}
	since := binary.BigEndian.Uint64(r.Body)
	s.granted[since] = r.Client
	return since
}

func (s *solo) write(id, session, number uint64, op client.Op, body []byte, want client.Status) {
	s.t.Helper()
	req := client.Request{Client: u128.From64(id), Session: session, Number: number, Op: op, Body: body}
	r := s.call(req)
	if r.Status != want {
		s.t.Fatalf("%s #%d of client %d in session %d: %s, want %s", op, number, id, session, r.Status, want)
	}
	if want == client.OK {
		s.oks = append(s.oks, answer{request{client: req.Client, session: session, number: number}, r})
	}
}

func (s *solo) next(id, session uint64, op client.Op, body []byte) {
	s.t.Helper()
	s.numbers[session]++
	s.write(id, session, s.numbers[session], op, body, client.OK)
}

func (s *solo) log() []raft.Entry {
	s.t.Helper()
	if err := s.n.Close(); err != nil {
		s.t.Fatal(err)
	}
	l, err := wal.Open(s.fsys, "/n/wal", wal.Options{Nonces: rand.NewChaCha8([32]byte{1})})
	if err != nil {
		s.t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	last, err := l.LastIndex()
	if err != nil {
		s.t.Fatal(err)
	}
	stored, err := l.Entries(1, last+1, math.MaxInt64)
	if err != nil {
		s.t.Fatal(err)
	}
	ents := make([]raft.Entry, len(stored))
	for i, e := range stored {
		ents[i] = raft.Entry{Index: e.Index, Term: e.Term, Kind: uint8(e.Kind), Body: e.Body}
	}
	return ents
}

func accountsBody(ids ...uint64) []byte {
	var b []byte
	for _, id := range ids {
		b = state.AppendAccount(b, state.Account{ID: u128.From64(id), Ledger: 1, Code: 1})
	}
	return b
}

func transferBody(id uint64) []byte {
	return state.AppendTransfer(nil, state.Transfer{
		ID:              u128.From64(id),
		DebitAccountID:  u128.From64(1),
		CreditAccountID: u128.From64(2),
		Amount:          u128.From64(5),
		Ledger:          1,
		Code:            1,
	})
}

func TestShadowAgreesWithTheNode(t *testing.T) {
	cases := []struct {
		name  string
		limit int
		runs  int
		play  func(s *solo)
	}{
		{"retry gets the stored reply", 4, 2, func(s *solo) {
			a := s.register(1)
			s.next(1, a, client.CreateAccounts, accountsBody(1, 2))
			s.next(1, a, client.CreateTransfers, transferBody(10))
			s.write(1, a, 2, client.CreateTransfers, transferBody(10), client.OK)
		}},
		{"smaller number is stale", 4, 3, func(s *solo) {
			a := s.register(1)
			s.next(1, a, client.CreateAccounts, accountsBody(1, 2))
			s.next(1, a, client.CreateTransfers, transferBody(10))
			s.write(1, a, 1, client.CreateAccounts, accountsBody(1, 2), client.Stale)
			s.next(1, a, client.CreateTransfers, transferBody(11))
		}},
		{"second register keeps the session", 4, 1, func(s *solo) {
			a := s.register(1)
			s.next(1, a, client.CreateAccounts, accountsBody(1, 2))
			if again := s.register(1); again != a {
				s.t.Fatalf("second register gave session %d, the first %d", again, a)
			}
			s.write(1, a, 1, client.CreateAccounts, accountsBody(1, 2), client.OK)
		}},
		{"unknown client", 4, 0, func(s *solo) {
			s.write(1, 2, 1, client.CreateAccounts, accountsBody(1, 2), client.SessionUnknown)
		}},
		{"full table evicts the oldest access", 2, 3, func(s *solo) {
			a, b := s.register(1), s.register(2)
			s.next(1, a, client.CreateAccounts, accountsBody(1, 2))
			c := s.register(3)
			s.write(2, b, 1, client.CreateTransfers, transferBody(10), client.SessionUnknown)
			s.next(1, a, client.CreateTransfers, transferBody(11))
			s.next(3, c, client.CreateTransfers, transferBody(12))
		}},
		{"late register opens a new session", 2, 3, func(s *solo) {
			a := s.register(1)
			s.next(1, a, client.CreateAccounts, accountsBody(1, 2))
			s.register(2)
			c := s.register(3)
			late := s.register(1)
			if late == a {
				s.t.Fatalf("late register revived session %d", a)
			}
			s.write(1, a, 2, client.CreateTransfers, transferBody(10), client.SessionUnknown)
			s.next(1, late, client.CreateTransfers, transferBody(11))
			s.next(3, c, client.CreateTransfers, transferBody(12))
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := openSolo(t, tc.limit)
			tc.play(s)
			digest, held := s.n.LedgerDigest(), s.n.Status().Sessions
			sh := newShadow(tc.limit)
			for _, e := range s.log() {
				sh.apply(e)
			}
			if got := sh.ledger.Digest(); got != digest {
				t.Errorf("shadow digest %x, the node has %x", got[:4], digest[:4])
			}
			if len(sh.sessions) != held {
				t.Errorf("shadow holds %d sessions, the node %d", len(sh.sessions), held)
			}
			for _, ses := range sh.sessions {
				if since, ok := s.n.Session(ses.client); !ok || since != ses.since {
					t.Errorf("shadow holds session %d of %v, the node %d (%v)", ses.since, ses.client, since, ok)
				}
			}
			if !maps.Equal(sh.opened, s.granted) {
				t.Errorf("shadow opened sessions %v, the node granted %v", sh.opened, s.granted)
			}
			if len(sh.ran) != tc.runs {
				t.Errorf("shadow ran %d requests, want %d", len(sh.ran), tc.runs)
			}
			for _, ok := range s.oks {
				exec, ran := sh.ran[ok.req]
				if !ran || exec.status != client.OK || exec.time != ok.r.Time || !bytes.Equal(exec.fails, ok.r.Body) {
					t.Errorf("request %+v got time %d and failures %x, the shadow has %+v (%v)", ok.req, ok.r.Time, ok.r.Body, exec, ran)
				}
			}
		})
	}
}

func requestEntry(index uint64, req request, op client.Op, ts uint64, recs []byte) raft.Entry {
	body := req.client.AppendBytes(nil)
	body = binary.BigEndian.AppendUint64(body, req.session)
	body = binary.BigEndian.AppendUint64(body, req.number)
	body = append(body, byte(op))
	body = binary.BigEndian.AppendUint64(body, ts)
	return raft.Entry{Index: index, Term: 1, Kind: entryRequest, Body: append(body, recs...)}
}

func TestShadowSkipsBrokenEntries(t *testing.T) {
	sh := newShadow(4)
	id, other := u128.From64(1), u128.From64(2)
	whole := requestEntry(3, request{client: id, session: 1, number: 1}, client.CreateAccounts, 100, accountsBody(1))
	for _, e := range []raft.Entry{
		{Index: 1, Term: 1, Kind: entryRegister, Body: id.AppendBytes(nil)},
		{Index: 2, Term: 1, Kind: entryRegister, Body: other.AppendBytes(nil)[:client.IDSize-1]},
		{Index: 3, Term: 1, Kind: entryRegister, Body: append(other.AppendBytes(nil), 0)},
		{Index: 4, Term: 1, Kind: entryRequest, Body: whole.Body[:requestHead-1]},
		{Index: 5, Term: 1, Kind: 0, Body: whole.Body},
		{Index: 6, Term: 1, Kind: 3, Body: whole.Body},
	} {
		sh.apply(e)
	}
	if len(sh.sessions) != 1 || sh.sessions[0].last != 0 || len(sh.ran) != 0 || sh.ledger.Digest() != oracle.New().Digest() {
		t.Fatalf("broken entries left sessions %+v and %d executions", sh.sessions, len(sh.ran))
	}
	sh.apply(whole)
	if exec := sh.ran[request{client: id, session: 1, number: 1}]; exec.status != client.OK {
		t.Fatalf("the whole request after the broken ones: %+v", exec)
	}
}

func TestShadowKeepsARejectedBatch(t *testing.T) {
	id := u128.From64(1)
	first, second := request{client: id, session: 1, number: 1}, request{client: id, session: 1, number: 2}
	cases := []struct {
		name string
		op   client.Op
		ts   uint64
		recs []byte
	}{
		{"no write", client.LookupTransfers, 100, transferBody(10)},
		{"half an account", client.CreateAccounts, 100, accountsBody(1)[:state.AccountSize/2]},
		{"time out of range", client.CreateAccounts, 1 << 63, accountsBody(1)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sh := newShadow(4)
			sh.apply(raft.Entry{Index: 1, Term: 1, Kind: entryRegister, Body: id.AppendBytes(nil)})
			sh.apply(requestEntry(2, first, tc.op, tc.ts, tc.recs))
			if exec, ran := sh.ran[first]; !ran || exec.status != client.Malformed || sh.ledger.Digest() != oracle.New().Digest() {
				t.Fatalf("rejected batch: %+v (%v)", exec, ran)
			}
			sh.apply(requestEntry(3, first, client.CreateAccounts, 100, accountsBody(1)))
			if exec := sh.ran[first]; exec.index != 2 || sh.ledger.Last() != 0 {
				t.Fatalf("the retry of a rejected batch ran at %d", exec.index)
			}
			sh.apply(requestEntry(4, second, client.CreateAccounts, 100, accountsBody(1)))
			if exec := sh.ran[second]; exec.status != client.OK || exec.time != 100 || len(exec.fails) != 0 {
				t.Fatalf("the request after a rejected batch: %+v", exec)
			}
		})
	}
}

func TestSaidFindsTwoBodiesForOneEntry(t *testing.T) {
	first := raft.Entry{Index: 7, Term: 3, Kind: entryRequest, Body: []byte("debit")}
	cases := []struct {
		name  string
		again raft.Entry
		check string
	}{
		{"same entry", raft.Entry{Index: 7, Term: 3, Kind: entryRequest, Body: []byte("debit")}, ""},
		{"other body", raft.Entry{Index: 7, Term: 3, Kind: entryRequest, Body: []byte("credit")}, "S3"},
		{"other kind", raft.Entry{Index: 7, Term: 3, Kind: entryRegister, Body: []byte("debit")}, "S3"},
		{"other term", raft.Entry{Index: 7, Term: 4, Kind: entryRequest, Body: []byte("credit")}, ""},
		{"other index", raft.Entry{Index: 8, Term: 3, Kind: entryRequest, Body: []byte("credit")}, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w, _, _ := calmWatch(t)
			sent := first
			sent.Body = slices.Clone(first.Body)
			if err := w.said(1, []raft.Message{{Type: raft.MsgApp, To: 2, Term: 4, Entries: []raft.Entry{sent}}}); err != nil {
				t.Fatal(err)
			}
			sent.Body[0]++
			err := w.said(1, []raft.Message{{Type: raft.MsgApp, To: 3, Term: 4, Entries: []raft.Entry{tc.again}}})
			reason := ""
			if tc.check != "" {
				reason = "another body"
			}
			wantViolation(t, err, tc.check, reason)
		})
	}
}

func replayed(t *testing.T) (*watch, []*replica.Node) {
	t.Helper()
	w := newWatch(Config{Seed: 2, Profile: sim.ProfileCalm, Nodes: 3})
	if _, err := w.run(w.disk()); err != nil {
		t.Fatalf("a clean run: %v", err)
	}
	return w, []*replica.Node{w.c.Node(1), w.c.Node(2), w.c.Node(3)}
}

func lastWrite(t *testing.T, calls []sim.Call) *sim.Call {
	t.Helper()
	for i := len(calls) - 1; i >= 0; i-- {
		if !calls[i].Op.Lookup() && len(calls[i].Body) > 0 {
			return &calls[i]
		}
	}
	t.Fatal("no write")
	return nil
}

func asked(call *sim.Call) request {
	return request{client: call.ID, session: call.Session, number: call.Reply.Number}
}

func looked(c sim.Call) bool {
	return c.Op == client.LookupAccounts || c.Op == client.LookupTransfers
}

func TestReplayLogFindsWhatDiffers(t *testing.T) {
	cases := []struct {
		name   string
		tamper func(call *sim.Call, sh *shadow)
		reason string
	}{
		{"clean run", func(*sim.Call, *shadow) {}, ""},
		{"reply with another time", func(call *sim.Call, _ *shadow) {
			call.Reply.Time++
		}, "got time"},
		{"reply with another failure", func(call *sim.Call, _ *shadow) {
			call.Reply.Body = append(slices.Clone(call.Reply.Body), 0, 0, 0, 0, 0, 0, 0, 1)
		}, "got time"},
		{"confirmed batch the log rejects", func(call *sim.Call, sh *shadow) {
			exec := sh.ran[asked(call)]
			exec.status = client.Malformed
			sh.ran[asked(call)] = exec
		}, "got time"},
		{"request with another body", func(call *sim.Call, _ *shadow) {
			call.Body = slices.Clone(call.Body)
			call.Body[len(call.Body)-1]++
		}, "is not the request"},
		{"request with another op", func(call *sim.Call, _ *shadow) {
			call.Op = client.CreateAccounts + client.CreateTransfers - call.Op
		}, "is not the request"},
		{"session the log never opens", func(call *sim.Call, _ *shadow) {
			call.Session++
		}, "never opens"},
		{"confirmed and never executed", func(call *sim.Call, sh *shadow) {
			delete(sh.ran, asked(call))
		}, "never executes"},
		{"execution no client asked for", func(call *sim.Call, sh *shadow) {
			spare := asked(call)
			spare.number += 1000
			sh.ran[spare] = sh.ran[asked(call)]
		}, "the clients sent"},
		{"session the log does not leave", func(_ *sim.Call, sh *shadow) {
			sh.sessions = sh.sessions[1:]
		}, "sessions, the log leaves"},
		{"session with another number", func(_ *sim.Call, sh *shadow) {
			sh.sessions[0].since++
		}, "holds session"},
		{"session of another client", func(_ *sim.Call, sh *shadow) {
			sh.sessions[0].client.Lo++
		}, "no session of"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w, nodes := replayed(t)
			sh, err := w.shadow(nodes[0].Status().Applied)
			if err != nil {
				t.Fatal(err)
			}
			tc.tamper(lastWrite(t, w.c.Calls()), sh)
			err = w.executed(sh)
			if err == nil {
				err = w.agrees(sh, nodes)
			}
			wantViolation(t, err, "S11", tc.reason)
		})
	}
}

func TestReplayLogComparesTheHead(t *testing.T) {
	w, nodes := replayed(t)
	sh, err := w.shadow(nodes[0].Status().Applied)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.agrees(sh, nodes); err != nil {
		t.Fatalf("a clean run: %v", err)
	}
	if _, err := sh.ledger.Apply(state.CreateAccounts, 0, nil); err != nil {
		t.Fatal(err)
	}
	wantViolation(t, w.agrees(sh, nodes), "S12", "the log gives")
}

func TestReplayLogNeedsEveryCommittedEntry(t *testing.T) {
	w, nodes := replayed(t)
	applied := nodes[0].Status().Applied
	sent := w.entries[[2]uint64{applied, w.committed[applied]}]
	delete(w.entries, [2]uint64{applied, w.committed[applied]})
	wantViolation(t, w.replayLog(nodes), "S11", "no MsgApp carried it")

	w.entries[[2]uint64{applied, w.committed[applied]}] = sent
	wantViolation(t, w.replayLog(nodes), "", "")
	delete(w.committed, applied-1)
	wantViolation(t, w.replayLog(nodes), "S11", "no node showed the term")
}

func TestReplayLogChecksReads(t *testing.T) {
	cases := []struct {
		name   string
		tamper func(read *sim.Call)
		reason string
	}{
		{"answer with another byte", func(read *sim.Call) {
			read.Reply.Body = slices.Clone(read.Reply.Body)
			read.Reply.Body[len(read.Reply.Body)-1]++
		}, "does not show"},
		{"answer cut short", func(read *sim.Call) {
			read.Reply.Body = read.Reply.Body[:len(read.Reply.Body)-1]
		}, "does not show"},
		{"answer of the empty ledger", func(read *sim.Call) {
			read.Reply.Time = 0
		}, "does not show"},
		{"refused read with any answer", func(read *sim.Call) {
			read.Reply.Status = client.SessionUnknown
			read.Reply.Body = read.Reply.Body[:len(read.Reply.Body)-1]
		}, ""},
		{"empty ledger after a write had ended", func(read *sim.Call) {
			read.Reply.Time, read.Reply.Body = 0, nil
		}, "had ended"},
		{"time between two batches", func(read *sim.Call) {
			read.Reply.Time = 1
		}, "no prefix of the log"},
		{"time after the last batch", func(read *sim.Call) {
			read.Reply.Time = math.MaxUint64
		}, "no prefix of the log"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w, nodes := replayed(t)
			calls := w.c.Calls()
			k := slices.IndexFunc(calls, func(c sim.Call) bool { return looked(c) && len(c.Reply.Body) > 0 })
			if k < 0 {
				t.Fatal("no read that found something")
			}
			tc.tamper(&calls[k])
			wantViolation(t, w.replayLog(nodes), "S11", tc.reason)
		})
	}
}

func TestReplayLogChecksProofs(t *testing.T) {
	found := func(c sim.Call) bool { return c.Op == client.Prove && len(c.Reply.Body) > 0 }
	cases := []struct {
		name   string
		pick   func(c sim.Call) bool
		tamper func(read *sim.Call, calls []sim.Call)
		reason string
	}{
		{"receipt with another byte", found, func(read *sim.Call, _ []sim.Call) {
			read.Reply.Body[len(read.Reply.Body)-1]++
		}, "does not show"},
		{"receipt dropped", found, func(read *sim.Call, _ []sim.Call) {
			read.Reply.Body = nil
		}, "does not show"},
		{"receipt for nothing", func(c sim.Call) bool { return c.Op == client.Prove && len(c.Reply.Body) == 0 }, func(read *sim.Call, calls []sim.Call) {
			read.Reply.Body = calls[slices.IndexFunc(calls, found)].Reply.Body
		}, "does not show"},
		{"head one ahead", func(c sim.Call) bool { return c.Op == client.Head }, func(read *sim.Call, _ []sim.Call) {
			read.Reply.Body[9]++
		}, "does not show"},
		{"one link short", func(c sim.Call) bool { return c.Op == client.Links && len(c.Reply.Body) > 0 }, func(read *sim.Call, _ []sim.Call) {
			read.Reply.Body = read.Reply.Body[:len(read.Reply.Body)-audit.LinkSize]
		}, "does not show"},
		{"proof at a time between two batches", found, func(read *sim.Call, _ []sim.Call) {
			read.Reply.Time = 1
		}, "no prefix of the log"},
		{"head of the empty ledger after a write had ended", func(c sim.Call) bool { return c.Op == client.Head && c.Reply.Time > 0 }, func(read *sim.Call, _ []sim.Call) {
			read.Reply.Time, read.Reply.Body = 0, audit.AppendHead(nil, audit.Head{})
		}, "had ended"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w, nodes := replayed(t)
			calls := w.c.Calls()
			k := slices.IndexFunc(calls, tc.pick)
			if k < 0 {
				t.Fatal("no such read in the run")
			}
			calls[k].Reply.Body = slices.Clone(calls[k].Reply.Body)
			tc.tamper(&calls[k], calls)
			wantViolation(t, w.replayLog(nodes), "S13", tc.reason)
		})
	}
}

func TestReplayLogTakesReadsInTimeOrder(t *testing.T) {
	w, nodes := replayed(t)
	calls := w.c.Calls()
	early := slices.IndexFunc(calls, func(c sim.Call) bool { return c.Op.Lookup() })
	late := slices.IndexFunc(calls, func(c sim.Call) bool { return c.Op.Lookup() && c.Reply.Time > calls[early].Reply.Time })
	if early < 0 || late < 0 {
		t.Fatal("no two reads at different times")
	}
	calls[early], calls[late] = calls[late], calls[early]
	if _, err := w.shadow(nodes[0].Status().Applied); err != nil {
		t.Fatalf("a read answered before an earlier call: %v", err)
	}
}

func TestReplayLogFindsAStaleRead(t *testing.T) {
	type pick struct {
		calls              []sim.Call
		read, write, spare int
	}
	cases := []struct {
		name   string
		tamper func(p pick)
		reason string
	}{
		{"clean run", func(pick) {}, ""},
		{"read older than a write that had ended", func(p pick) {
			p.calls[p.read].Reply.Time = p.calls[p.write].Reply.Time - 1
		}, "had ended"},
		{"read as old as that write", func(p pick) {
			p.calls[p.read].Reply.Time = p.calls[p.write].Reply.Time
		}, ""},
		{"write that ended as the read began", func(p pick) {
			p.calls[p.read].Reply.Time = p.calls[p.write].Reply.Time - 1
			p.calls[p.write].End = p.calls[p.read].Start
		}, ""},
		{"write that was refused", func(p pick) {
			p.calls[p.read].Reply.Time = p.calls[p.write].Reply.Time - 1
			p.calls[p.write].Reply.Status = client.SessionUnknown
		}, ""},
		{"read that was refused", func(p pick) {
			p.calls[p.read].Reply.Time = p.calls[p.write].Reply.Time - 1
			p.calls[p.read].Reply.Status = client.SessionUnknown
		}, ""},
		{"write older than a write that had ended", func(p pick) {
			p.calls[p.read].Reply.Time = p.calls[p.write].Reply.Time - 1
			p.calls[p.read].Op = client.CreateTransfers
		}, ""},
		{"read older than a read that had ended", func(p pick) {
			p.calls[p.read].Reply.Time = p.calls[p.write].Reply.Time - 1
			p.calls[p.write].Op = client.LookupAccounts
		}, "had ended"},
		{"newest write ended first", func(p pick) {
			p.calls[p.spare].Reply.Status = client.OK
			p.calls[p.write].Reply.Time = p.calls[p.spare].Reply.Time + 2
			p.calls[p.read].Reply.Time = p.calls[p.spare].Reply.Time + 1
		}, "had ended"},
		{"earlier call still open", func(p pick) {
			p.calls[p.spare].Reply.Status = client.OK
			p.calls[p.write].End = p.calls[p.read].Start + 1
			p.calls[p.read].Reply.Time = p.calls[p.spare].Reply.Time - 1
		}, "had ended"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w, _ := replayed(t)
			calls := w.c.Calls()
			write := slices.IndexFunc(calls, func(c sim.Call) bool { return !c.Op.Lookup() })
			spare := write + 1 + slices.IndexFunc(calls[write+1:], func(c sim.Call) bool { return !c.Op.Lookup() && c.End > calls[write].End })
			read := slices.IndexFunc(calls, func(c sim.Call) bool { return looked(c) && c.Start > calls[spare].End })
			if write < 0 || spare == write || read < spare {
				t.Fatal("no read that began after two writes had ended")
			}
			for i := range calls[:read] {
				if i != write {
					calls[i].Reply.Status = client.SessionUnknown
				}
			}
			tc.tamper(pick{calls, read, write, spare})
			wantViolation(t, w.stale(), "S11", tc.reason)
		})
	}
}

func TestReplayLogExecutesARetryOnce(t *testing.T) {
	w, nodes := replayed(t)
	applied := nodes[0].Status().Applied
	sh, err := w.shadow(applied)
	if err != nil {
		t.Fatal(err)
	}
	call := lastWrite(t, w.c.Calls())
	exec, ran := sh.ran[asked(call)]
	if !ran {
		t.Fatalf("the log does not execute %s #%d", call.Op, call.Reply.Number)
	}
	term := w.committed[applied]
	retry := w.entries[[2]uint64{exec.index, w.committed[exec.index]}]
	retry.Index, retry.Term = applied+1, term
	w.entries[[2]uint64{applied + 1, term}], w.committed[applied+1] = retry, term

	sh, err = w.shadow(applied + 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.executed(sh); err != nil {
		t.Fatalf("a retry in the log: %v", err)
	}
	if err := w.agrees(sh, nodes); err != nil {
		t.Fatalf("a retry in the log: %v", err)
	}
}

func TestRunEndsWithTheLogReplay(t *testing.T) {
	w, _ := replayed(t)
	if err := w.end(); err != nil {
		t.Fatalf("a clean run: %v", err)
	}
	first := [2]uint64{1, w.committed[1]}
	if e := w.entries[first]; e.Kind != 0 {
		t.Fatalf("entry 1 is %+v, want the empty entry of the first leader", e)
	}
	w.entries[first] = raft.Entry{Index: 1, Term: first[1], Kind: entryRegister, Body: u128.From64(77).AppendBytes(nil)}
	wantViolation(t, w.end(), "S11", "sessions, the log leaves")
	clear(w.entries)
	wantViolation(t, w.end(), "S11", "no MsgApp carried it")
}

func TestBeyondChecksTheAnswers(t *testing.T) {
	cases := []struct {
		name   string
		tamper func(w *watch, write, read *sim.Call)
		reason string
	}{
		{"clean run", func(*watch, *sim.Call, *sim.Call) {}, ""},
		{"reply with another time", func(_ *watch, write, _ *sim.Call) {
			write.Reply.Time++
		}, "got time"},
		{"reply with another failure", func(_ *watch, write, _ *sim.Call) {
			write.Reply.Body = append(slices.Clone(write.Reply.Body), 0, 0, 0, 0, 0, 0, 0, 1)
		}, "got time"},
		{"request with another body", func(_ *watch, write, _ *sim.Call) {
			write.Body = slices.Clone(write.Body)
			write.Body[len(write.Body)-1]++
		}, "is not the request"},
		{"write without an answer", func(_ *watch, write, _ *sim.Call) {
			write.End, write.Reply = 0, client.Reply{}
		}, ""},
		{"request in the log that no client sent", func(w *watch, write, _ *sim.Call) {
			top := w.reach()
			stray := requestEntry(top+1, request{client: write.ID, session: write.Session, number: 1 << 40}, write.Op, write.Reply.Time, write.Body)
			stray.Term = w.committed[top]
			w.entries[[2]uint64{top + 1, stray.Term}], w.committed[top+1] = stray, stray.Term
		}, "the clients sent"},
		{"read with another byte", func(_ *watch, _, read *sim.Call) {
			read.Reply.Body = slices.Clone(read.Reply.Body)
			read.Reply.Body[len(read.Reply.Body)-1]++
		}, "does not show"},
		{"read at a time between two batches", func(_ *watch, _, read *sim.Call) {
			read.Reply.Time = 1
		}, "no prefix of the log"},
		{"read of the empty ledger after a write had ended", func(_ *watch, _, read *sim.Call) {
			read.Reply.Time, read.Reply.Body = 0, nil
		}, "had ended"},
		{"committed entry no message carried", func(w *watch, _, _ *sim.Call) {
			top := w.reach()
			delete(w.entries, [2]uint64{top, w.committed[top]})
		}, "no MsgApp carried it"},
		{"gap below the highest commit", func(w *watch, _, _ *sim.Call) {
			delete(w.committed, w.reach()-1)
		}, "no node showed the term"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := outside(t)
			calls := w.c.Calls()
			write, read, open := -1, -1, 0
			for i, call := range calls {
				switch {
				case call.End == 0:
					open++
				case call.Reply.Status != client.OK:
				case !call.Op.Lookup() && write < 0:
					write = i
				case looked(call) && len(call.Reply.Body) > 0:
					read = i
				}
			}
			if write < 0 || read < 0 || calls[read].Start <= calls[write].End || open == 0 {
				t.Fatalf("write %d, read %d, %d calls without an answer: want a read after a write and an open call", write, read, open)
			}
			tc.tamper(w, &calls[write], &calls[read])
			wantViolation(t, w.end(), "S11", tc.reason)
		})
	}
}
