//go:build linux

package demo

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/Demonisreal/dmn-ledger/internal/local/localtest"
)

func findingDuring(t *testing.T, kind Kind) (*Stage, State, int, uint64) {
	t.Helper()
	s := stage(t, func(s *Stage) {
		s.pace.holds[kind] = 2 * time.Second
		s.pace.stand = 3 * time.Second
	})
	localtest.Await(t, 20*time.Second, "a log for a flip", func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.sight.sizes[0] >= youngLog && s.sight.sizes[1] >= youngLog && s.sight.sizes[2] >= youngLog
	})
	before := s.State()
	target := follower(before)
	victim := 0
	for _, n := range before.Nodes {
		if n.ID != target && n.Role == "follower" {
			victim = n.Pid
		}
	}
	if target == 0 || victim == 0 {
		t.Fatalf("no two followers in %+v", before.Nodes)
	}
	s.during = func(Kind) {
		if err := syscall.Kill(victim, syscall.SIGKILL); err != nil {
			t.Error(err)
		}
		deadline := time.Now().Add(10 * time.Second)
		for s.State().Finding == nil && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
	}
	mark := s.journal.next
	if _, err := s.Act(kind, target, addr(1)); err != nil {
		t.Fatal(err)
	}
	localtest.Await(t, 30*time.Second, "the action over", func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return !s.acting
	})
	s.during = nil
	st := s.State()
	if st.Finding == nil || st.Finding.Check != "F1" {
		t.Fatalf("finding %+v, want F1 while it stands", st.Finding)
	}
	return s, st, target, mark
}

func TestKillSaysNoStartAfterAFinding(t *testing.T) {
	s, st, target, mark := findingDuring(t, Kill)
	for _, l := range s.Lines(mark) {
		if l.Src == "demo" && strings.HasPrefix(l.Text, fmt.Sprintf("n%d startet nach ", target)) {
			t.Errorf("the journal says %q, n%d runs as PID %d (%s)", l.Text, target, st.Nodes[target-1].Pid, st.Nodes[target-1].Proc)
		}
	}
	if n := st.Nodes[target-1]; n.Pid != 0 || n.Proc != "down" || strings.Contains(n.Note.Text, "gestartet") {
		t.Errorf("n%d: pid %d, proc %s, note %q; it was killed and never started", target, n.Pid, n.Proc, n.Note.Text)
	}
}

func TestFreezeSaysNoSigcontAfterAFinding(t *testing.T) {
	s, st, target, mark := findingDuring(t, Freeze)
	for _, l := range s.Lines(mark) {
		if l.Src == "demo" && strings.HasPrefix(l.Text, fmt.Sprintf("SIGCONT an n%d", target)) {
			t.Errorf("the journal says %q, the process is in state %s", l.Text, localtest.ProcState(st.Nodes[target-1].Pid))
		}
	}
	time.Sleep(300 * time.Millisecond)
	st = s.State()
	if n := st.Nodes[target-1]; localtest.ProcState(n.Pid) != "T" || n.Proc != "frozen" {
		t.Errorf("n%d: process %d in state %s, shown as %s", target, n.Pid, localtest.ProcState(n.Pid), n.Proc)
	}
}

func TestFlipSaysNoStartAfterAFinding(t *testing.T) {
	s, st, target, mark := findingDuring(t, Flip)
	for _, l := range s.Lines(mark) {
		if l.Src == "demo" && strings.HasPrefix(l.Text, fmt.Sprintf("n%d gestartet", target)) {
			t.Errorf("the journal says %q, n%d runs as PID %d (%s)", l.Text, target, st.Nodes[target-1].Pid, st.Nodes[target-1].Proc)
		}
	}
}

func TestLoadIsNotRunningWhileAFindingStands(t *testing.T) {
	s := stage(t, func(s *Stage) { s.pace.stand = 3 * time.Second })
	if err := syscall.Kill(s.State().Nodes[1].Pid, syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	localtest.Await(t, 10*time.Second, "a finding", func() bool { return s.State().Finding != nil })
	for range 20 {
		st := s.State()
		if st.Finding == nil {
			break
		}
		if st.Load != "held" {
			t.Fatalf("load %q while the finding %s stands and every node is halted", st.Load, st.Finding.Check)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func TestFlipSparesAHaltedNode(t *testing.T) {
	s := New(Config{Dir: t.TempDir(), Ledgerd: bin, Log: log.New(testLog{t}, "", 0)})
	s.pace = quick
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.watch(ctx)
	r, err := s.setup(ctx, "start")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		s.quiet(r)
		s.lower(r)
	}()
	n := r.c.Node(2)
	localtest.Await(t, 20*time.Second, "a log for a flip", func() bool {
		_, size := newest(n.Dir())
		return size >= youngLog
	})
	n.Halt()
	pid := n.Pid()
	time.Sleep(100 * time.Millisecond)
	path, _ := newest(n.Dir())
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	mark := s.journal.next
	s.act(r, wish{kind: Flip, target: 2})
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if state := localtest.ProcState(pid); state != "gone" && !bytes.Equal(before, after) {
		for _, l := range s.Lines(mark) {
			t.Log(l.Src, l.Text)
		}
		t.Fatalf("a bit flipped in %s while its process %d is in state %s", path, pid, state)
	}
}

func TestNoCallAfterTheLastCount(t *testing.T) {
	s := stage(t, func(s *Stage) { s.pace.limit = 150 })
	answered, begun := 0, 0
	for k := 1; k <= 6; k++ {
		localtest.Await(t, 60*time.Second, fmt.Sprintf("cluster %d", k), func() bool {
			st := s.State()
			return st.Cluster.N == k && st.Phase == "steady"
		})
		s.mu.Lock()
		r := s.cur
		s.mu.Unlock()
		localtest.Await(t, 60*time.Second, fmt.Sprintf("the end of cluster %d", k), func() bool { return s.State().Cluster.N > k })
		s.mu.Lock()
		for _, call := range r.calls {
			begun++
			if call.End != 0 {
				answered++
			}
		}
		s.mu.Unlock()
	}
	t.Logf("over 6 clusters: %d calls began after the last count, %d of them were answered and never judged", begun, answered)
	if begun != 0 {
		t.Fatalf("%d calls began after the last count of their cluster, %d were answered; none of them reached the checker", begun, answered)
	}
}
