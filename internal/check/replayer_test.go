package check

import (
	"encoding/binary"
	"math"
	"slices"
	"testing"

	"github.com/Demonisreal/dmn-ledger/internal/audit"
	"github.com/Demonisreal/dmn-ledger/internal/client"
	"github.com/Demonisreal/dmn-ledger/internal/sim"
	"github.com/Demonisreal/dmn-ledger/internal/state"
	"github.com/Demonisreal/dmn-ledger/internal/u128"
)

func replied(at uint64, body []byte) client.Reply {
	return client.Reply{Status: client.OK, Time: at, Body: body}
}

func debtor(posted uint64) []byte {
	return state.AppendAccount(nil, state.Account{ID: u128.From64(1), DebitsPosted: u128.From64(posted), Ledger: 1, Code: 1, Timestamp: 100})
}

func booking() []sim.Call {
	return []sim.Call{
		{Client: 1, Op: client.CreateAccounts, Body: accountsBody(1, 2), Start: 10, End: 20, Reply: replied(100, nil)},
		{Client: 2, Op: client.CreateTransfers, Body: transferBody(10), Start: 30, End: 40, Reply: replied(200, nil)},
		{Client: 1, Op: client.LookupAccounts, Body: u128.From64(1).AppendBytes(nil), Start: 50, End: 60, Reply: replied(200, debtor(5))},
	}
}

func TestReplayerChecksAWindow(t *testing.T) {
	cases := []struct {
		name   string
		tamper func(calls []sim.Call)
		check  string
		reason string
	}{
		{"clean window", func([]sim.Call) {}, "", ""},
		{"transfer refused, the oracle books it", func(calls []sim.Call) {
			calls[1].Reply.Body = client.AppendFailures(nil, []state.Failure{{Reason: state.Exists}})
		}, "S8", "c2 call 1 (CreateTransfers at 200) got failures"},
		{"transfer booked, the oracle refuses it", func(calls []sim.Call) {
			calls[0].Body = accountsBody(1)
		}, "S8", "c2 call 1 (CreateTransfers at 200) got failures"},
		{"write inside the times of an earlier batch", func(calls []sim.Call) {
			calls[1].Reply.Time = 101
		}, "S8", "c2 call 1 has time 101, the ledger is already at 101"},
		{"batch cut short", func(calls []sim.Call) {
			calls[0].Body = calls[0].Body[1:]
		}, "S8", "c1 call 0: "},
		{"read ended without OK", func(calls []sim.Call) {
			calls[2].Reply.Status = client.Busy
		}, "S8", "c1 call 2 (LookupAccounts) ended with Busy"},
		{"write ended without OK", func(calls []sim.Call) {
			calls[1].Reply.Status = client.SessionUnknown
		}, "S8", "c2 call 1 (CreateTransfers) ended with SessionUnknown"},
		{"read misses a transfer that had ended", func(calls []sim.Call) {
			calls[2].Reply = replied(101, debtor(0))
		}, "S9", "window 0 with calls 0 to 2 is not linearizable"},
		{"read sees a transfer that had not begun", func(calls []sim.Call) {
			calls[2].Start, calls[2].End = 22, 28
		}, "S9", "window 0 with calls 0 to 2 is not linearizable"},
		{"read overlaps the transfer and misses it", func(calls []sim.Call) {
			calls[2].Start = 35
			calls[2].Reply = replied(101, debtor(0))
		}, "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			calls := booking()
			tc.tamper(calls)
			wantViolation(t, NewReplayer().Window(calls), tc.check, tc.reason)
		})
	}
}

func TestReplayerAcceptsReadsWithoutTime(t *testing.T) {
	cases := []struct {
		name   string
		reply  client.Reply
		reason string
	}{
		{"no time, what the ledger holds", replied(0, debtor(5)), ""},
		{"no time, what the ledger held before the transfer", replied(0, debtor(0)), "not linearizable"},
		{"what the ledger holds at a time no write gave", replied(150, debtor(5)), "not linearizable"},
		{"what the ledger holds at the time of an earlier write", replied(101, debtor(5)), "not linearizable"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			calls := booking()
			calls[2].Reply = tc.reply
			wantViolation(t, NewReplayer().Window(calls), "S9", tc.reason)
		})
	}
}

func TestSimulatorReadsNeedTheirTime(t *testing.T) {
	calls := booking()
	calls[2].Reply = replied(0, debtor(5))
	r := NewReplayer()
	r.timed = true
	wantViolation(t, r.Window(calls), "S9", "not linearizable")
}

func proofs(t *testing.T) []sim.Call {
	t.Helper()
	m := state.New(0, 0)
	for _, call := range booking()[:2] {
		if _, err := m.Apply(stateOp(call.Op), call.Reply.Time, call.Body, nil); err != nil {
			t.Fatal(err)
		}
	}
	receipt := func(r audit.Receipt, found bool) []byte {
		if !found {
			t.Fatal("no receipt in the booked ledger")
		}
		return audit.AppendReceipt(nil, r)
	}
	var links []byte
	for _, l := range m.Links(0, math.MaxUint64, nil) {
		links = audit.AppendLink(links, l)
	}
	read := func(op client.Op, body, reply []byte) sim.Call {
		return sim.Call{Client: 3, Op: op, Body: body, Start: 50, End: 60, Reply: replied(200, reply)}
	}
	return []sim.Call{
		read(client.Head, nil, audit.AppendHead(nil, m.Head())),
		read(client.Prove, append([]byte{audit.KindAccount}, u128.From64(1).AppendBytes(nil)...), receipt(m.ProveAccount(u128.From64(1)))),
		read(client.Prove, append([]byte{audit.KindTransfer}, u128.From64(10).AppendBytes(nil)...), receipt(m.ProveTransfer(u128.From64(10)))),
		read(client.Prove, binary.BigEndian.AppendUint64([]byte{audit.KindBatch}, 200), receipt(m.ProveBatch(200))),
		read(client.Prove, append([]byte{audit.KindExpiry}, u128.From64(10).AppendBytes(nil)...), nil),
		read(client.Links, binary.BigEndian.AppendUint64(make([]byte, 8), math.MaxUint64), links),
	}
}

func TestReplayerChecksProofs(t *testing.T) {
	const (
		head = iota
		account
		transfer
		batch
		expiry
		links
	)
	cases := []struct {
		name   string
		tamper func(reads []sim.Call)
		check  string
	}{
		{"every read as the ledger holds it", func([]sim.Call) {}, ""},
		{"head with another hash", func(reads []sim.Call) {
			reads[head].Reply.Body[len(reads[head].Reply.Body)-1]++
		}, "S13"},
		{"head of the batch before", func(reads []sim.Call) {
			_, before, _, _ := audit.Decode(reads[account].Reply.Body)
			reads[head].Reply.Body = audit.AppendHead(nil, before)
		}, "S13"},
		{"receipt with another byte", func(reads []sim.Call) {
			reads[account].Reply.Body[len(reads[account].Reply.Body)-1]++
		}, "S13"},
		{"receipt one byte short", func(reads []sim.Call) {
			reads[transfer].Reply.Body = reads[transfer].Reply.Body[:len(reads[transfer].Reply.Body)-1]
		}, "S13"},
		{"receipt of another account", func(reads []sim.Call) {
			reads[account].Body = append([]byte{audit.KindAccount}, u128.From64(2).AppendBytes(nil)...)
		}, "S13"},
		{"receipt for a transfer nobody booked", func(reads []sim.Call) {
			reads[transfer].Body = append([]byte{audit.KindTransfer}, u128.From64(11).AppendBytes(nil)...)
		}, "S13"},
		{"no receipt for a booked transfer", func(reads []sim.Call) {
			reads[transfer].Reply.Body = nil
		}, "S13"},
		{"receipt of a transfer as its expiry", func(reads []sim.Call) {
			reads[expiry].Reply.Body = reads[transfer].Reply.Body
		}, "S13"},
		{"receipt of a batch at another time", func(reads []sim.Call) {
			reads[batch].Body = binary.BigEndian.AppendUint64([]byte{audit.KindBatch}, 100)
		}, "S13"},
		{"one link short", func(reads []sim.Call) {
			reads[links].Reply.Body = reads[links].Reply.Body[:audit.LinkSize]
		}, "S13"},
		{"one link more", func(reads []sim.Call) {
			reads[links].Reply.Body = append(reads[links].Reply.Body, reads[links].Reply.Body[:audit.LinkSize]...)
		}, "S13"},
		{"proof at a time no write gave", func(reads []sim.Call) {
			reads[head].Reply.Time = 150
		}, "S13"},
		{"head of the ledger before the transfer had begun", func(reads []sim.Call) {
			reads[head].Start, reads[head].End = 22, 28
		}, "S13"},
		{"a wrong head and a wrong lookup", func(reads []sim.Call) {
			reads[head].Reply.Body[len(reads[head].Reply.Body)-1]++
			reads[head+1].Op, reads[head+1].Reply.Body = client.LookupAccounts, debtor(0)
			reads[head+1].Body = u128.From64(1).AppendBytes(nil)
		}, "S9"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reads := proofs(t)
			for i := range reads {
				reads[i].Reply.Body = slices.Clone(reads[i].Reply.Body)
			}
			tc.tamper(reads)
			reason := ""
			if tc.check != "" {
				reason = "not linearizable"
			}
			wantViolation(t, NewReplayer().Window(append(booking()[:2], reads...)), tc.check, reason)
		})
	}
}

func TestReplayerAcceptsProofsWithoutTime(t *testing.T) {
	reads := proofs(t)
	for i := range reads {
		reads[i].Reply.Time = 0
	}
	if err := NewReplayer().Window(append(booking()[:2], reads...)); err != nil {
		t.Fatal(err)
	}
	r := NewReplayer()
	r.timed = true
	wantViolation(t, r.Window(append(booking()[:2], reads...)), "S13", "with its proofs")
}

func TestReplayerCarriesTheLedgerIntoTheNextWindow(t *testing.T) {
	calls := booking()
	r := NewReplayer()
	for _, window := range [][]sim.Call{calls[:1], calls[1:]} {
		if err := r.Window(window); err != nil {
			t.Fatal(err)
		}
	}
	if last := r.Ledger().Last(); last != 200 {
		t.Fatalf("ledger at %d, want 200", last)
	}
	held := r.Ledger().LookupAccounts([]u128.Uint128{u128.From64(1)})
	if len(held) != 1 || held[0].DebitsPosted != u128.From64(5) {
		t.Fatalf("ledger holds %+v, want account 1 with 5 posted", held)
	}
	wantViolation(t, NewReplayer().Window(calls[1:]), "S8", "c2 call 0 (CreateTransfers at 200) got failures")

	read := sim.Call{Client: 3, Op: client.LookupAccounts, Body: u128.From64(1).AppendBytes(nil), Start: 70, End: 80, Reply: replied(101, debtor(0))}
	wantViolation(t, r.Window([]sim.Call{read}), "S9", "window 2 with calls 3 to 3 is not linearizable")
	read.Reply.Status = client.Stale
	wantViolation(t, r.Window([]sim.Call{read}), "S8", "c3 call 4 (LookupAccounts) ended with Stale")
}

func TestReplayerFindsATimeBeforeAnEarlierWindow(t *testing.T) {
	cases := []struct {
		name   string
		at     uint64
		check  string
		reason string
	}{
		{"later than the earlier window", 201, "", ""},
		{"time of the last write of the earlier window", 200, "S8", "c1 call 2 has time 200, the ledger is already at 200"},
		{"earlier than a write of the earlier window", 150, "S9", "c1 call 2 has time 150, a call of an earlier window has 200"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := NewReplayer()
			if err := r.Window(booking()[:2]); err != nil {
				t.Fatal(err)
			}
			next := sim.Call{Client: 1, Op: client.CreateTransfers, Body: transferBody(11), Start: 70, End: 80, Reply: replied(tc.at, nil)}
			wantViolation(t, r.Window([]sim.Call{next}), tc.check, tc.reason)
		})
	}
}

func TestReplayComparesTheHead(t *testing.T) {
	w, nodes := finished(t)
	calls := w.c.Calls()
	for i := len(calls) - 1; i >= 0; i-- {
		if !calls[i].Op.Lookup() && len(calls[i].Body) > 0 && len(calls[i].Reply.Body) == 0 {
			calls[i].Body = nil
			wantViolation(t, w.replay(nodes[0]), "S12", "the oracle has")
			return
		}
	}
	t.Fatal("no write booked without failures")
}

func lastWindow(t *testing.T, w *watch) int {
	t.Helper()
	bounds := w.c.Windows()
	if len(bounds) == 0 {
		t.Fatal("a single window")
	}
	return bounds[len(bounds)-1]
}

func TestReplayNamesAWrongReplyBeforeAStaleRead(t *testing.T) {
	w, nodes := finished(t)
	calls, last := w.c.Calls(), lastWindow(t, w)
	read := slices.IndexFunc(calls[:last], func(c sim.Call) bool { return looked(c) && len(c.Reply.Body) > 0 })
	write := slices.IndexFunc(calls[last:], func(c sim.Call) bool { return !c.Op.Lookup() })
	if read < 0 || write < 0 {
		t.Fatal("no read that found something before the last window, or no write in it")
	}
	calls[read].Reply.Body = slices.Clone(calls[read].Reply.Body)
	calls[read].Reply.Body[0] ^= 1
	wantViolation(t, w.replay(nodes[0]), "S9", "is not linearizable")

	calls[last+write].Reply.Body = append(slices.Clone(calls[last+write].Reply.Body), make([]byte, client.FailureSize)...)
	wantViolation(t, w.replay(nodes[0]), "S8", "got failures")
}

func TestReplayStopsAtATimeBeforeAnEarlierWindow(t *testing.T) {
	w, nodes := finished(t)
	calls, last := w.c.Calls(), lastWindow(t, w)
	write := slices.IndexFunc(calls[last:], func(c sim.Call) bool { return !c.Op.Lookup() })
	if write < 0 {
		t.Fatal("no write in the last window")
	}
	calls[last+write].Reply.Time = 1
	wantViolation(t, w.replay(nodes[0]), "S9", "a call of an earlier window has")
}
