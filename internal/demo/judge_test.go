//go:build linux

package demo

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/Demonisreal/dmn-ledger/internal/client"
	"github.com/Demonisreal/dmn-ledger/internal/state"
	"github.com/Demonisreal/dmn-ledger/internal/u128"
	"github.com/Demonisreal/dmn-ledger/ledger"
)

func TestLoadRestsWhileTheHubReportsNoViewers(t *testing.T) {
	s := stage(t, func(s *Stage) { s.pace.rest = 2 * time.Second })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go newHub(s).run(ctx)
	await(t, 10*time.Second, "a resting load behind a hub without viewers", func() bool { return s.State().Load == "resting" })
}

func settled(t *testing.T, s *Stage) (*run, []*ledger.NodeStatus) {
	t.Helper()
	s.mu.Lock()
	r := s.cur
	s.mu.Unlock()
	var sts []*ledger.NodeStatus
	await(t, 10*time.Second, "three statuses at one index", func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), sightWithin)
		defer cancel()
		sts = r.c.Statuses(ctx)
		return sts[0] != nil && sts[1] != nil && sts[2] != nil && sts[0].Applied == sts[1].Applied && sts[1].Applied == sts[2].Applied
	})
	return r, sts
}

func TestAgreementFindings(t *testing.T) {
	for _, tc := range []struct {
		check string
		bend  func(sts []*ledger.NodeStatus)
	}{
		{"L4", func(sts []*ledger.NodeStatus) { sts[1].Repair = true }},
		{"S7", func(sts []*ledger.NodeStatus) { sts[2].ApplyHash[0] ^= 1 }},
	} {
		t.Run(tc.check, func(t *testing.T) {
			s := stage(t, func(s *Stage) { s.pace.light, s.pace.full = time.Hour, time.Hour })
			r, sts := settled(t, s)
			tc.bend(sts)
			s.judge(r, sts)
			st := s.State()
			if st.Finding == nil || st.Finding.Check != tc.check {
				t.Fatalf("finding %+v, want %s", st.Finding, tc.check)
			}
		})
	}
}

func TestHeadsMustAgree(t *testing.T) {
	s := stage(t, func(s *Stage) { s.pace.light, s.pace.full = time.Hour, time.Hour })
	r, sts := settled(t, s)
	if why := s.judge(r, sts); why != "" {
		t.Skipf("the nodes moved on: %s", why)
	}
	sts[2].Head[0] ^= 1
	if why := s.judge(r, sts); why == "" {
		t.Fatal("three nodes with two different heads count as agreed")
	}
}

func TestKeyMismatchIsAFinding(t *testing.T) {
	s := stage(t, func(s *Stage) { s.pace.full = time.Second })
	s.mu.Lock()
	keys := s.cur.c.Keys()
	s.mu.Unlock()
	forged := bytes.Clone(keys[1])
	forged[0] ^= 1
	keys[1] = forged
	await(t, 10*time.Second, "a finding", func() bool { return s.State().Finding != nil })
	if f := s.State().Finding; f.Check != "key" {
		t.Fatalf("finding %s: %s", f.Check, f.Reason)
	}
}

func TestRepairLineOutsideAFlipIsAFinding(t *testing.T) {
	s := stage(t, nil)
	s.mu.Lock()
	r := s.cur
	s.mu.Unlock()
	s.hear(r, 3, "2026/11/19 16:00:00.000000 repair flag set in term 4")
	st := s.State()
	if st.Finding == nil || st.Finding.Check != "L4" || !strings.Contains(st.Finding.Reason, "n3") {
		t.Fatalf("finding %+v, want L4 for n3", st.Finding)
	}
}

func TestBalanceRules(t *testing.T) {
	books := func() []state.Account {
		var accts []state.Account
		for _, id := range owned {
			a := state.Account{ID: u128.From64(id), Ledger: 1, Code: 1}
			if id == spring {
				a.DebitsPosted = u128.From64(want)
			} else {
				a.CreditsPosted = u128.From64(funding)
			}
			accts = append(accts, a)
		}
		return accts
	}
	if sum, why := balance(books()); why != "" || sum.Value != fmt.Sprint(want) {
		t.Fatalf("sound books: sum %s, %s", sum.Value, why)
	}
	for _, tc := range []struct {
		name string
		bend func(accts []state.Account)
	}{
		{"money from nowhere on account 2", func(a []state.Account) { a[1].CreditsPosted = u128.From64(funding + 1) }},
		{"account 9 issued five more and took five back", func(a []state.Account) {
			a[6].DebitsPosted, a[6].CreditsPosted = u128.From64(want+5), u128.From64(5)
			a[0].CreditsPosted, a[0].DebitsPosted = u128.From64(funding+5), u128.From64(5)
		}},
		{"account 1 paid five to account 9", func(a []state.Account) {
			a[6].CreditsPosted = u128.From64(5)
			a[0].DebitsPosted = u128.From64(5)
		}},
		{"a pending amount", func(a []state.Account) { a[3].DebitsPending = u128.From64(1) }},
		{"six accounts", func(a []state.Account) { copy(a, a[1:]) }},
		{"a foreign account", func(a []state.Account) { a[2].ID = u128.From64(77) }},
	} {
		accts := books()
		tc.bend(accts)
		if tc.name == "six accounts" {
			accts = accts[:6]
		}
		if _, why := balance(accts); why == "" {
			t.Errorf("%s: the books count as sound", tc.name)
		}
	}
}

func TestBurstOfNodeLinesIsCounted(t *testing.T) {
	s := New(Config{Log: log.New(io.Discard, "", 0)})
	r := &run{fresh: make(chan struct{})}
	r.ctx, r.cancel = context.WithCancel(context.Background())
	defer r.cancel()
	for i := range 50 {
		s.hear(r, 2, fmt.Sprintf("2026/11/19 16:00:00.%06d follower in term %d, leader 1", i, i))
	}
	shown := 0
	for _, l := range s.Lines(0) {
		if l.Src == "n2" {
			shown++
		}
	}
	if shown != burstMax {
		t.Fatalf("%d of 50 lines of one second shown, want %d", shown, burstMax)
	}
	s.mu.Lock()
	s.flushEar(r, 2, time.Now().Add(2*time.Second))
	s.mu.Unlock()
	all := s.Lines(0)
	if last := all[len(all)-1]; last.Src != "demo" || last.Text != fmt.Sprintf(saySkipped, 50-burstMax, 2) {
		t.Fatalf("after the burst: %s %q", last.Src, last.Text)
	}
}

func TestAliveNeedsTheSight(t *testing.T) {
	s := New(Config{Log: log.New(io.Discard, "", 0)})
	if !s.Alive() {
		t.Fatal("a new stage is not alive")
	}
	s.mu.Lock()
	s.beats.sight = time.Now().Add(-sightAlive - time.Second)
	s.mu.Unlock()
	if s.Alive() {
		t.Fatalf("alive with a sight that is silent for %v", sightAlive+time.Second)
	}
	s.mu.Lock()
	s.beats.sight, s.beats.loop = time.Now(), time.Now().Add(-loopAlive-time.Second)
	s.mu.Unlock()
	if s.Alive() {
		t.Fatalf("alive with a loop that is silent for %v", loopAlive+time.Second)
	}
}

func killNode(s *Stage, i int) error {
	return syscall.Kill(s.State().Nodes[i].Pid, syscall.SIGKILL)
}

func TestCountGoesToTheChecker(t *testing.T) {
	var armed atomic.Bool
	s := stage(t, func(s *Stage) {
		s.pace.full = time.Hour
		issue := s.issue
		s.issue = func(ctx context.Context, cl *ledger.Client, op client.Op, body []byte) (client.Reply, error) {
			reply, err := issue(ctx, cl, op, body)
			if err != nil || !armed.Load() || op != client.LookupAccounts || len(reply.Body) < state.AccountSize {
				return reply, err
			}
			first := state.DecodeAccount(reply.Body)
			first.Code = 7
			reply.Body = append(state.AppendAccount(nil, first), reply.Body[state.AccountSize:]...)
			return reply, err
		}
	})
	armed.Store(true)
	await(t, 20*time.Second, "a finding", func() bool { return s.State().Finding != nil })
	if f := s.State().Finding; f.Check != "S9" {
		t.Fatalf("finding %s: %s", f.Check, f.Reason)
	}
}

func TestAcceptedActionDoesNotOutliveItsCluster(t *testing.T) {
	s := stage(t, func(s *Stage) { s.pace.stand = 2 * time.Second })
	if err := killNode(s, 0); err != nil {
		t.Fatal(err)
	}
	await(t, 10*time.Second, "a finding", func() bool { return s.State().Finding != nil })
	s.mu.Lock()
	s.acting, s.began = true, time.Now()
	s.wishes <- wish{Kill, 2, 1}
	s.mu.Unlock()
	await(t, 30*time.Second, "a second cluster", func() bool {
		st := s.State()
		return st.Cluster.N == 2 && st.Phase != "setup"
	})
	time.Sleep(500 * time.Millisecond)
	s.mu.Lock()
	acting, waiting := s.acting, len(s.wishes)
	s.mu.Unlock()
	if st := s.State(); acting || waiting != 0 || st.Action != nil || st.Totals.Actions.Kill != 0 {
		t.Fatalf("after the reset: acting %v, %d wishes wait, action %+v, %d kills", acting, waiting, st.Action, st.Totals.Actions.Kill)
	}
}

func TestHubReportsTheLastViewerGone(t *testing.T) {
	show := newFake()
	h := newHub(show)
	v := h.join(addr(1))
	h.beat()
	if _, _, watching := show.counted(); watching != 1 {
		t.Fatalf("%d viewers reported with one stream", watching)
	}
	h.leave(v)
	h.beat()
	if _, _, watching := show.counted(); watching != 0 {
		t.Fatalf("%d viewers reported after the last stream closed", watching)
	}
}

func TestFullCountAtTheRealLimit(t *testing.T) {
	if testing.Short() {
		t.Skip("books 20 000 transfers")
	}
	var slowest, widest atomic.Int64
	s := stage(t, func(s *Stage) {
		s.pace.limit = standard.limit
		s.pace.pause = 2 * time.Millisecond
		s.pace.full = 3 * time.Second
		issue := s.issue
		s.issue = func(ctx context.Context, cl *ledger.Client, op client.Op, body []byte) (client.Reply, error) {
			began := time.Now()
			reply, err := issue(ctx, cl, op, body)
			if op == client.LookupTransfers {
				slowest.Store(max(slowest.Load(), int64(time.Since(began))))
				widest.Store(max(widest.Load(), int64(len(reply.Body))))
			}
			return reply, err
		}
	})
	began := time.Now()
	var last State
	await(t, 8*time.Minute, "a second cluster after 20 000 transfers", func() bool {
		st := s.State()
		if st.Cluster.N == 1 {
			last = st
		}
		return st.Totals.Found != 0 || st.Cluster.N == 2 && st.Phase == "steady"
	})
	st := s.State()
	t.Logf("first cluster: %d confirmed, %d refused, last full count found %d, judged up to %s in %d windows; %v to the second cluster; slowest LookupTransfers %v, largest reply %d bytes",
		last.Books.Confirmed, last.Books.Refused, last.Books.Recount.Found, last.Books.Judged.Upto, last.Books.Judged.Windows,
		time.Since(began).Round(time.Second), time.Duration(slowest.Load()).Round(time.Millisecond), widest.Load())
	if st.Totals.Found != 0 {
		t.Fatalf("finding %s: %s", st.Totals.Findings[0].Check, st.Totals.Findings[0].Reason)
	}
	if st.Cluster.Why != "limit" || last.Books.Recount.Found < standard.limit {
		t.Fatalf("cluster 2 after %q, the last full count of cluster 1 found %d", st.Cluster.Why, last.Books.Recount.Found)
	}
}

func TestProofPrefersACallWithAPath(t *testing.T) {
	s := stage(t, func(s *Stage) { s.pace.light, s.pace.full = time.Hour, time.Hour })
	s.mu.Lock()
	r := s.cur
	s.mu.Unlock()
	await(t, 20*time.Second, "two confirmed transfers, one from a call with several", func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return r.provable != 0 && s.st.Books.Confirmed > 4
	})
	if err := r.rec.Hold(context.Background(), 5*time.Second); err != nil {
		t.Fatal(err)
	}
	defer r.rec.Resume()
	s.mu.Lock()
	want, other := r.provable, uint64(0)
	for i, m := range r.marks {
		if id := uint64(i + 1); m == confirmed && id != want {
			other = id
		}
	}
	r.latest = other
	s.mu.Unlock()
	p, err := s.Proof(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if p.Transfer.ID != fmt.Sprint(want) {
		t.Fatalf("proof for transfer %s, the newest from a call with several transfers is %d (the newest of all was set to %d)", p.Transfer.ID, want, other)
	}
}
