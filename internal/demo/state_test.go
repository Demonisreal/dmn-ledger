//go:build linux

package demo

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Demonisreal/dmn-ledger/internal/client"
	"github.com/Demonisreal/dmn-ledger/internal/history"
	"github.com/Demonisreal/dmn-ledger/internal/sim"
	"github.com/Demonisreal/dmn-ledger/internal/state"
	"github.com/Demonisreal/dmn-ledger/internal/u128"
)

func TestStateIsBounded(t *testing.T) {
	s := steady()
	r := &run{ctx: context.Background(), began: time.Now(), fresh: make(chan struct{}), calls: map[int]sim.Call{}, limit: make(chan struct{}, 1)}
	for i := range 10_000 {
		s.say("demo", fmt.Sprint("line ", i))
		s.hear(r, 1+i%3, fmt.Sprintf("2026/11/19 10:00:00.000000 line %d %s", i, strings.Repeat("x", 1000)))
	}
	lines := s.Lines(0)
	if len(lines) != journalSize || lines[len(lines)-1].N != s.journal.next {
		t.Fatalf("journal keeps %d lines, the last numbered %d of %d", len(lines), lines[len(lines)-1].N, s.journal.next)
	}
	for id, e := range r.ears {
		if len(e.lines) != earSize || e.total != 3334-min(id, 1) {
			t.Fatalf("n%d keeps %d lines of %d", id+1, len(e.lines), e.total)
		}
		for _, h := range e.lines {
			if len(h.text) > lineMax {
				t.Fatalf("n%d keeps a line of %d bytes", id+1, len(h.text))
			}
		}
	}
	for i := range 100 {
		s.mu.Lock()
		r.found = nil
		s.record(r, time.Now(), "S8", fmt.Sprint(i, strings.Repeat(" deadbeef", 100)))
		s.mu.Unlock()
	}
	st := s.State()
	if len(st.Totals.Findings) != findingsMax || st.Totals.Found != 100 || !strings.HasPrefix(st.Totals.Findings[0].Reason, "80 ") {
		t.Fatalf("%d findings kept of %d, the first %.10q", len(st.Totals.Findings), st.Totals.Found, st.Totals.Findings[0].Reason)
	}
	for _, f := range st.Totals.Findings {
		if len(f.Reason) > reasonMax {
			t.Fatalf("a reason of %d bytes", len(f.Reason))
		}
	}
	for i := range 1000 {
		var body []byte
		for k := range 4 {
			r.sent = append(r.sent, sent{1, 2, 100})
			r.marks = append(r.marks, unanswered)
			body = state.AppendTransfer(body, state.Transfer{Ledger: 1, Code: 1, Amount: u128.From64(100), ID: u128.From64(uint64(4*i + k + 1))})
		}
		tr := history.Turn{N: i, Call: sim.Call{Op: client.CreateTransfers, Body: body}}
		s.opened(r, tr)
		s.settle(r, tr, uint64(4*i+1), 4, client.Reply{Status: client.OK})
		s.sink(r, []sim.Call{tr.Call})
	}
	if st := s.State(); len(st.Recent) != recentMax || len(r.calls) != 0 || st.Recent[0].ID != "4000" {
		t.Fatalf("%d recent transfers, the newest %s; %d calls tracked", len(st.Recent), st.Recent[0].ID, len(r.calls))
	}

	synctest.Test(t, func(t *testing.T) {
		s := steady()
		accepted := 0
		for i := range 1000 {
			if _, err := act(s, kinds[i%4], 1+i%3, addr(byte(i))); err == nil {
				accepted++
				done(t, s)
			}
			if len(s.visits) > 3 {
				t.Fatalf("%d addresses kept after %d actions", len(s.visits), accepted)
			}
			time.Sleep(time.Second)
		}
		if accepted < 90 {
			t.Fatalf("%d of 1000 actions accepted, one every 10s is 100", accepted)
		}
	})
}

func TestStateTravelsAsJSON(t *testing.T) {
	s := steady()
	s.st.Action = &Action{Kind: Flip, Target: 2, Node: 2, Role: "follower", Phase: "done", File: "n2/wal/wal-00000000000000000001.log", Before: 0x40, Repair: "yes", NonVoter: 77}
	s.st.Finding = &Finding{Check: "S8", Reason: "c1 call 3"}
	s.st.Books.Recount = Recount{After: Cut, Confirmed: 12, Found: 12, Hash: "8223cbcf9076f709"}
	s.st.Totals.Findings = append(s.st.Totals.Findings, *s.st.Finding)
	s.st.Recent = append(s.st.Recent, Booking{ID: "12", Debit: 1, Credit: 2, Amount: "100", Refused: "exceeds_credits"})
	st := s.State()
	b, err := json.Marshal(st)
	if err != nil {
		t.Fatal(err)
	}
	var back State
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(back, st) {
		t.Fatalf("back from JSON:\n%+v\nwant\n%+v", back, st)
	}
	if err := json.Unmarshal([]byte(`{"action":{"kind":"reboot"}}`), &back); err == nil {
		t.Fatal("an unknown kind decoded")
	}
}

func TestPickStaysInTheSealedHalf(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	bits := map[int]bool{}
	for range 10_000 {
		size := int64(8<<10) + rng.Int64N(64<<20-8<<10+1)
		off, bit := pick(rng, size)
		if off < size/4 || off >= size/2 || bit < 0 || bit > 7 {
			t.Fatalf("size %d: byte %d bit %d, want a byte in [%d, %d) and a bit below 8", size, off, bit, size/4, size/2)
		}
		bits[bit] = true
	}
	if len(bits) != 8 {
		t.Fatalf("bits %v, want all eight", bits)
	}
}

func TestTipChangesTheNamedBit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal-00000000000000000001.log")
	before := make([]byte, 64<<10)
	_, _ = rand.NewChaCha8([32]byte{3}).Read(before)
	if err := os.WriteFile(path, before, 0o600); err != nil {
		t.Fatal(err)
	}
	tp, err := flipBit(path, rand.New(rand.NewPCG(5, 6)))
	if err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for i := range after {
		if i != int(tp.offset) && after[i] != before[i] {
			t.Fatalf("byte %d changed, the tip names byte %d", i, tp.offset)
		}
	}
	if tp.size != int64(len(before)) || tp.before != before[tp.offset] || tp.after != after[tp.offset] || tp.before^tp.after != 1<<tp.bit {
		t.Fatalf("tip %+v, the file had %#x and has %#x at byte %d", tp, before[tp.offset], after[tp.offset], tp.offset)
	}
}
