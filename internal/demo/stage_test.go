//go:build linux

package demo

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
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

var bin string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "demo-bin-")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	build := exec.Command("go", "build", "-o", dir+string(filepath.Separator), "../../cmd/ledgerd") //nolint:gosec // G204: the directory comes from MkdirTemp
	if out, err := build.CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "build ledgerd: %v\n%s", err, out)
		_ = os.RemoveAll(dir)
		os.Exit(1)
	}
	bin = filepath.Join(dir, "ledgerd")
	code := m.Run()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

var quick = pace{
	holds: [Flip + 1]time.Duration{Kill: 300 * time.Millisecond, Freeze: 300 * time.Millisecond, Cut: 300 * time.Millisecond},
	light: time.Second,
	full:  time.Minute,
	rest:  time.Hour,
	stand: time.Second,
	retry: 10 * time.Millisecond,
	pause: 100 * time.Millisecond,
	limit: 1_000_000,
}

type testLog struct{ t *testing.T }

func (l testLog) Write(b []byte) (int, error) {
	l.t.Log(strings.TrimSuffix(string(b), "\n"))
	return len(b), nil
}

type bench struct {
	s      *Stage
	cancel context.CancelFunc
	ran    chan error
}

func stage(t *testing.T, tune func(s *Stage)) *Stage {
	t.Helper()
	return started(t, tune).s
}

func started(t *testing.T, tune func(s *Stage)) *bench {
	t.Helper()
	s := New(Config{Dir: t.TempDir(), Ledgerd: bin, Log: log.New(testLog{t}, "", 0)})
	s.pace = quick
	if tune != nil {
		tune(s)
	}
	ctx, cancel := context.WithCancel(context.Background())
	b := &bench{s: s, cancel: cancel, ran: make(chan error, 1)}
	go func() { b.ran <- s.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-b.ran; err != nil {
			t.Errorf("Run: %v", err)
		}
		if kids := children(t); len(kids) > 0 {
			t.Errorf("ledgerd still running after Run: %v", kids)
		}
	})
	s.Watching(1)
	await(t, 30*time.Second, "a steady cluster", func() bool { return s.State().Phase == "steady" })
	return b
}

func await(t *testing.T, within time.Duration, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(within)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatalf("%s: not within %v", what, within)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func children(t *testing.T) []int {
	t.Helper()
	entries, err := os.ReadDir("/proc")
	if err != nil {
		t.Fatal(err)
	}
	var pids []int
	for _, e := range entries {
		pid, err := strconv.Atoi(e.Name())
		if err != nil {
			continue
		}
		stat, err := os.ReadFile(filepath.Join("/proc", e.Name(), "stat"))
		if err != nil {
			continue
		}
		comm, rest, _ := strings.Cut(string(stat), ") ")
		f := strings.Fields(rest)
		if strings.HasSuffix(comm, "(ledgerd") && len(f) > 1 && f[1] == strconv.Itoa(os.Getpid()) {
			pids = append(pids, pid)
		}
	}
	return pids
}

func procState(pid int) string {
	stat, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return "gone"
	}
	_, rest, _ := strings.Cut(string(stat), ") ")
	return strings.Fields(rest)[0]
}

func queued(t *testing.T, addr string) int {
	t.Helper()
	_, port, _ := strings.Cut(addr, ":")
	p, _ := strconv.Atoi(port)
	local := fmt.Sprintf("0100007F:%04X", p)
	b, err := os.ReadFile("/proc/net/tcp")
	if err != nil {
		t.Fatal(err)
	}
	for line := range strings.Lines(string(b)) {
		f := strings.Fields(line)
		if len(f) > 4 && f[1] == local && f[3] == "0A" {
			_, rx, _ := strings.Cut(f[4], ":")
			n, _ := strconv.ParseInt(rx, 16, 64)
			return int(n)
		}
	}
	return 0
}

func follower(st State) int {
	for _, n := range st.Nodes {
		if n.Role == "follower" {
			return n.ID
		}
	}
	return 0
}

func perform(t *testing.T, s *Stage, kind Kind, target int, from byte) State {
	t.Helper()
	await(t, 30*time.Second, fmt.Sprintf("%s on %d accepted", kind, target), func() bool {
		_, err := s.Act(kind, target, addr(from))
		return err == nil
	})
	await(t, 60*time.Second, fmt.Sprintf("%s on %d done", kind, target), func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return !s.acting
	})
	return s.State()
}

func healed(t *testing.T, st State, before State, kind Kind) {
	t.Helper()
	a := st.Action
	switch {
	case st.Totals.Found != 0:
		t.Fatalf("finding %s: %s", st.Totals.Findings[0].Check, st.Totals.Findings[0].Reason)
	case a.Kind != kind || a.Phase != "done" || st.Phase != "steady":
		t.Fatalf("action %s in phase %s, the stage in %s", a.Kind, a.Phase, st.Phase)
	case st.Books.Recount.After != kind || st.Books.Recount.At < a.LiftAt:
		t.Fatalf("last full count after %q at %d, the action lifted at %d", st.Books.Recount.After, st.Books.Recount.At, a.LiftAt)
	case st.Books.Sum.Value != fmt.Sprint(want) || st.Books.Recount.Found != st.Books.Recount.Confirmed:
		t.Fatalf("sum %s, %d confirmed, %d found", st.Books.Sum.Value, st.Books.Recount.Confirmed, st.Books.Recount.Found)
	case st.Totals.Healed != before.Totals.Healed+1:
		t.Fatalf("healed %d, before %d", st.Totals.Healed, before.Totals.Healed)
	}
}

func lines(s *Stage, after uint64, src ...string) []Line {
	return slices.DeleteFunc(s.Lines(after), func(l Line) bool { return !slices.Contains(src, l.Src) })
}

func inOrder(t *testing.T, ls []Line, want ...string) {
	t.Helper()
	k := 0
	for _, l := range ls {
		if k < len(want) && strings.HasPrefix(l.Text, want[k]) {
			k++
		}
	}
	if k < len(want) {
		var texts []string
		for _, l := range ls {
			texts = append(texts, l.Src+" "+l.Text)
		}
		t.Fatalf("missing %q after the lines before it; the lines:\n%s", want[k], strings.Join(texts, "\n"))
	}
}

func TestKillHealsItself(t *testing.T) {
	s := stage(t, nil)
	for _, target := range []string{"leader", "follower"} {
		t.Run(target, func(t *testing.T) {
			before := s.State()
			id := 0
			if target == "follower" {
				id = follower(before)
			}
			mark := s.journal.next
			st := perform(t, s, Kill, id, 1)
			healed(t, st, before, Kill)
			n := st.Action.Node
			if id == 0 && st.Action.Role != "leader" || id != 0 && n != id {
				t.Fatalf("hit n%d as %s, aimed at %s", n, st.Action.Role, target)
			}
			if st.Nodes[n-1].Pid == before.Nodes[n-1].Pid || st.Nodes[n-1].Note.Text == "" {
				t.Fatalf("n%d runs as PID %d, before %d, note %q", n, st.Nodes[n-1].Pid, before.Nodes[n-1].Pid, st.Nodes[n-1].Note.Text)
			}
			inOrder(t, lines(s, mark, "demo", "check"), fmt.Sprintf("SIGKILL an n%d (PID %d)", n, before.Nodes[n-1].Pid), fmt.Sprintf("n%d startet nach ", n), "Zählung nach der Störung: alle drei Knoten")
			inOrder(t, lines(s, mark, fmt.Sprintf("n%d", n)), time.Now().UTC().Format("2006/"))
			if id == 0 {
				elected := slices.ContainsFunc(lines(s, mark, "n1", "n2", "n3"), func(l Line) bool { return strings.Contains(l.Text, " leader in term ") })
				if !elected {
					t.Fatal("no node line about a new leader after the leader was killed")
				}
			}
		})
	}
}

func TestFreezeHealsItself(t *testing.T) {
	s := stage(t, nil)
	for _, target := range []string{"leader", "follower"} {
		t.Run(target, func(t *testing.T) {
			before := s.State()
			id := 0
			if target == "follower" {
				id = follower(before)
			}
			mark := s.journal.next
			st := perform(t, s, Freeze, id, 1)
			healed(t, st, before, Freeze)
			n := st.Action.Node
			if st.Nodes[n-1].Pid != before.Nodes[n-1].Pid || st.Nodes[n-1].Proc != "up" {
				t.Fatalf("n%d runs as PID %d (%s), before %d", n, st.Nodes[n-1].Pid, st.Nodes[n-1].Proc, before.Nodes[n-1].Pid)
			}
			if got := procState(st.Nodes[n-1].Pid); got == "T" {
				t.Fatalf("n%d is still stopped", n)
			}
			inOrder(t, lines(s, mark, "demo", "check"), fmt.Sprintf("SIGSTOP an n%d (PID %d)", n, before.Nodes[n-1].Pid), fmt.Sprintf("SIGCONT an n%d nach ", n), "Zählung nach der Störung: alle drei Knoten")
		})
	}
}

func TestFrozenNodeIsAskedOncePerSecond(t *testing.T) {
	s := stage(t, func(s *Stage) { s.pace.holds[Freeze] = standard.holds[Freeze] })
	before := s.State()
	mark := s.journal.next
	if _, err := s.Act(Freeze, 0, addr(1)); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	addrs := s.cur.c.Addrs()
	s.mu.Unlock()
	samples, answered, waiting := 0, 0, 0
	await(t, 30*time.Second, "the freeze lifted", func() bool {
		st := s.State()
		a := st.Action
		if a != nil && a.Phase == "struck" && time.Since(time.UnixMilli(a.LiftAt)) > -4500*time.Millisecond {
			samples++
			if st.Nodes[a.Node-1].Answers {
				answered++
			}
			waiting = max(waiting, queued(t, addrs[a.Node-1]))
		}
		return a != nil && a.Phase != "struck"
	})
	await(t, 30*time.Second, "the freeze done", func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return !s.acting
	})
	healed(t, s.State(), before, Freeze)
	if samples < 50 || answered != 0 {
		t.Fatalf("the frozen node answers in %d of %d samples", answered, samples)
	}
	if waiting > 10 {
		t.Fatalf("%d connections wait at the client port of the frozen node", waiting)
	}
	t.Logf("at most %d connections waited at the frozen node", waiting)
	for _, l := range s.Lines(mark) {
		if strings.Contains(l.Text, "too many client connections") {
			t.Fatalf("%s %s", l.Src, l.Text)
		}
	}
}

func TestCutHealsItself(t *testing.T) {
	s := stage(t, nil)
	for _, target := range []string{"leader", "follower"} {
		t.Run(target, func(t *testing.T) {
			before := s.State()
			id := 0
			if target == "follower" {
				id = follower(before)
			}
			mark := s.journal.next
			st := perform(t, s, Cut, id, 1)
			healed(t, st, before, Cut)
			n := st.Action.Node
			for _, l := range st.Links {
				if l.Cut {
					t.Fatalf("link n%d->n%d still shown as cut", l.From, l.To)
				}
			}
			inOrder(t, lines(s, mark, "demo", "check"), fmt.Sprintf("n%d ist von ", n), fmt.Sprintf("Links von und zu n%d nach ", n), "Zählung nach der Störung: alle drei Knoten")
		})
	}
}

func flipped(t *testing.T, s *Stage, target int, from byte) (State, State, []byte, uint64) {
	t.Helper()
	var seg []byte
	s.during = func(Kind) {
		a := s.State().Action
		path, _ := newest(filepath.Join(s.cfg.Dir, "run", fmt.Sprintf("n%d", a.Node)))
		seg, _ = os.ReadFile(path)
	}
	before := s.State()
	mark := s.journal.next
	st := perform(t, s, Flip, target, from)
	s.during = nil
	healed(t, st, before, Flip)
	return st, before, seg, mark
}

func repaired(t *testing.T, s *Stage, st State, seg []byte, mark uint64) {
	t.Helper()
	a := st.Action
	n := a.Node
	if want := fmt.Sprintf("n%d/wal/", n); !strings.HasPrefix(a.File, want) || a.Size != int64(len(seg)) {
		t.Fatalf("flipped %s of %d bytes, the segment before had %d bytes, want it under %s", a.File, a.Size, len(seg), want)
	}
	if seg[a.Offset] != a.Before || a.Before^a.After != 1<<a.Bit {
		t.Fatalf("byte %d was %#x, the action says %#x became %#x at bit %d", a.Offset, seg[a.Offset], a.Before, a.After, a.Bit)
	}
	if a.Repair != "yes" {
		t.Fatalf("repair %q, want yes", a.Repair)
	}
	var began, cleared time.Time
	for _, l := range lines(s, mark, fmt.Sprintf("n%d", n)) {
		switch {
		case strings.Contains(l.Text, " started: ") && strings.Contains(l.Text, " repair yes,"):
			began, _ = stamp(l.Text)
		case !began.IsZero() && strings.Contains(l.Text, " repair flag cleared "):
			cleared, _ = stamp(l.Text)
		}
	}
	if began.IsZero() || cleared.IsZero() {
		t.Fatalf("n%d logged no start with repair yes and a cleared flag after it", n)
	}
	if ms := cleared.Sub(began).Round(time.Millisecond).Milliseconds(); a.NonVoter != ms {
		t.Fatalf("nonvoter %d ms, the node lines are %d ms apart", a.NonVoter, ms)
	}
	inOrder(t, lines(s, mark, "demo", "check"), fmt.Sprintf("SIGKILL an n%d", n), "Bit gekippt in "+a.File, fmt.Sprintf("n%d gestartet (PID %d)", n, st.Nodes[n-1].Pid),
		fmt.Sprintf("n%d hat den Schaden beim Start erkannt und %d ms", n, a.NonVoter), "Zählung nach der Störung: alle drei Knoten")
	t.Logf("n%d: byte %d of %d, bit %d, %d ms without a vote", n, a.Offset, a.Size, a.Bit, a.NonVoter)
}

func TestFlipIsRepaired(t *testing.T) {
	s := stage(t, nil)
	st, _, seg, mark := flipped(t, s, follower(s.State()), 1)
	repaired(t, s, st, seg, mark)
}

func TestFlipOnTheLeader(t *testing.T) {
	s := stage(t, nil)
	st, before, seg, mark := flipped(t, s, 0, 1)
	if st.Action.Role != "leader" || before.Nodes[st.Action.Node-1].Role != "leader" {
		t.Fatalf("flipped n%d as %s", st.Action.Node, st.Action.Role)
	}
	repaired(t, s, st, seg, mark)
}

func TestFlipStaysInTheNodeDir(t *testing.T) {
	s := stage(t, nil)
	target := follower(s.State())
	run := filepath.Join(s.cfg.Dir, "run")
	files := map[string][]byte{}
	var paused bool
	s.during = func(Kind) {
		keys, _ := filepath.Glob(filepath.Join(run, "n*.key"))
		for _, path := range keys {
			files[path], _ = os.ReadFile(path)
		}
		for id := 1; id <= 3; id++ {
			if id == target {
				continue
			}
			dir := filepath.Join(run, fmt.Sprintf("n%d", id))
			root := os.DirFS(dir)
			_ = fs.WalkDir(root, ".", func(path string, d fs.DirEntry, err error) error {
				if err == nil && !d.IsDir() {
					files[filepath.Join(dir, path)], _ = fs.ReadFile(root, path)
				}
				return nil
			})
		}
		paused = true
	}
	perform(t, s, Flip, target, 1)
	if !paused || len(files) == 0 {
		t.Fatal("no files read during the flip")
	}
	for path, old := range files {
		now, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		if filepath.Base(path) == "meta" {
			continue
		}
		if !bytes.HasPrefix(now, old) {
			t.Fatalf("%s changed: %d bytes before, %d now", path, len(old), len(now))
		}
	}
	var names []string
	for path := range files {
		rel, _ := filepath.Rel(run, path)
		names = append(names, rel)
	}
	slices.Sort(names)
	t.Logf("files of the two other nodes, meta rewritten, the rest only grew: %v", names)
}

func TestEveryFaultIsLifted(t *testing.T) {
	for _, kind := range kinds {
		t.Run(kind.String(), func(t *testing.T) {
			s := New(Config{Dir: t.TempDir(), Ledgerd: bin, Log: log.New(testLog{t}, "", 0)})
			s.pace = quick
			s.pace.holds[kind] = time.Hour
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
			target := 2
			lifted := func(how string) {
				t.Helper()
				n := r.c.Node(target)
				if !n.Running() || procState(n.Pid()) == "T" {
					t.Fatalf("%s: n%d runs %v, state %s", how, target, n.Running(), procState(n.Pid()))
				}
				s.mu.Lock()
				cut := len(r.cut)
				s.mu.Unlock()
				if cut != 0 {
					t.Fatalf("%s: %d links shown as cut", how, cut)
				}
			}
			await(t, 10*time.Second, "a log for a flip", func() bool {
				_, size := newest(r.c.Node(target).Dir())
				return size >= youngLog
			})
			s.during = func(Kind) { panic("hook") }
			func() {
				defer func() {
					if recover() == nil {
						t.Fatal("the hook did not panic")
					}
				}()
				s.act(r, wish{kind: kind, target: target})
			}()
			s.during = nil
			lifted("after a panic")
			if !s.count(r, true, 0) {
				t.Fatalf("the nodes do not agree after the panic: %+v", s.finding(r))
			}
			s.during = func(Kind) { r.cancel() }
			s.act(r, wish{kind: kind, target: target})
			s.during = nil
			lifted("after the context ended")
			await(t, 10*time.Second, "agreement after the context ended", func() bool {
				ctx, cancel := context.WithTimeout(context.Background(), sightWithin)
				defer cancel()
				sts := r.c.Statuses(ctx)
				return agreed(sts) && sts[0].Commit == sts[1].Commit && sts[1].Commit == sts[2].Commit && !sts[target-1].Repair
			})
		})
	}
}

func TestFaultIsLiftedOnShutdown(t *testing.T) {
	for _, kind := range []Kind{Freeze, Cut} {
		t.Run(kind.String(), func(t *testing.T) {
			var pids []int
			b := started(t, func(s *Stage) { s.pace.holds[kind] = time.Hour })
			b.s.during = func(Kind) {
				for _, n := range b.s.State().Nodes {
					pids = append(pids, n.Pid)
				}
				go func() {
					time.Sleep(200 * time.Millisecond)
					b.cancel()
				}()
			}
			if _, err := b.s.Act(kind, 0, addr(1)); err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-b.ran:
				b.ran <- err
			case <-time.After(30 * time.Second):
				t.Fatal("Run did not end within 30s")
			}
			if len(pids) != 3 {
				t.Fatalf("pids %v", pids)
			}
			for _, pid := range pids {
				if st := procState(pid); st != "gone" {
					t.Fatalf("process %d is in state %s after Run ended", pid, st)
				}
			}
		})
	}
}

func TestNeverMoreThanThreeNodes(t *testing.T) {
	s := stage(t, func(s *Stage) { s.pace.limit = 300 })
	most, stop := 0, make(chan struct{})
	sampled := make(chan struct{})
	go func() {
		defer close(sampled)
		for {
			select {
			case <-stop:
				return
			case <-time.After(5 * time.Millisecond):
			}
			most = max(most, len(children(t)))
		}
	}()
	for i := range 20 {
		perform(t, s, kinds[i%4], i%4, byte(i))
	}
	await(t, 30*time.Second, "a third cluster", func() bool { return s.State().Cluster.N >= 3 })
	close(stop)
	<-sampled
	st := s.State()
	if most > 3 || st.Totals.Found != 0 {
		t.Fatalf("up to %d ledgerd at once, %d findings", most, st.Totals.Found)
	}
	t.Logf("20 actions over %d clusters, at most %d ledgerd at once", st.Cluster.N, most)
}

func TestResetAtTheLimit(t *testing.T) {
	s := stage(t, func(s *Stage) { s.pace.limit = 300 })
	first := s.State()
	var p Proof
	await(t, 10*time.Second, "a proof", func() bool {
		var err error
		p, err = s.Proof(context.Background())
		return err == nil
	})
	keys := make([][]byte, 3)
	for i, k := range p.Keys {
		keys[i], _ = hex.DecodeString(k)
	}
	raw, _ := hex.DecodeString(p.Receipt)
	id, _ := strconv.ParseUint(p.Cluster, 16, 64)
	if _, err := ledger.VerifyProof(raw, id, keys); err != nil || p.Cluster != first.Cluster.ID {
		t.Fatalf("proof of cluster %s: %v", p.Cluster, err)
	}
	await(t, 60*time.Second, "a second cluster", func() bool {
		st := s.State()
		return st.Cluster.N == 2 && st.Phase == "steady"
	})
	st := s.State()
	switch {
	case st.Cluster.Why != "limit" || st.Cluster.ID == first.Cluster.ID:
		t.Fatalf("cluster %s set up after %s, the first was %s", st.Cluster.ID, st.Cluster.Why, first.Cluster.ID)
	case st.Nodes[0].Key == first.Nodes[0].Key || st.Totals.Clusters != 2 || st.Totals.Found != 0:
		t.Fatalf("key %s, before %s; %d clusters, %d findings", st.Nodes[0].Key, first.Nodes[0].Key, st.Totals.Clusters, st.Totals.Found)
	case st.Totals.Confirmed < 300 || st.Totals.Confirmed < st.Books.Confirmed+300:
		t.Fatalf("%d confirmed since the start, %d in this cluster", st.Totals.Confirmed, st.Books.Confirmed)
	}
	all := s.Lines(0)
	inOrder(t, all, "300 Buchungen erreicht.", "Zählung: alle drei Knoten", "Cluster 2 aufgesetzt (300 Buchungen erreicht)")
	var limit, ready int64
	for _, l := range all {
		if strings.HasPrefix(l.Text, "300 Buchungen erreicht.") {
			limit = l.At
		}
		if strings.HasPrefix(l.Text, "Zählung: alle drei Knoten") && limit != 0 {
			ready = l.At
		}
	}
	t.Logf("from the limit to the first count of the next cluster: %d ms", ready-limit)
}

func TestNodeThatDiesIsAFinding(t *testing.T) {
	s := stage(t, nil)
	pid := s.State().Nodes[1].Pid
	if err := syscall.Kill(pid, syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	await(t, 10*time.Second, "a finding", func() bool { return s.State().Finding != nil })
	st := s.State()
	if f := st.Finding; f.Check != "F1" || !strings.Contains(f.Reason, "n2 hat sich selbst beendet (signal: killed)") || !strings.Contains(f.Reason, time.Now().UTC().Format("2006/")) {
		t.Fatalf("finding %s: %s", f.Check, f.Reason)
	}
	if _, err := s.Act(Kill, 1, addr(1)); !errors.Is(err, ErrRecovering) {
		t.Fatalf("an action while the finding stands: %v", err)
	}
	if _, err := s.Proof(context.Background()); !errors.Is(err, ErrFinding) {
		t.Fatalf("a proof while the finding stands: %v", err)
	}
	await(t, 30*time.Second, "a new cluster", func() bool {
		st := s.State()
		return st.Cluster.N == 2 && st.Phase == "steady"
	})
	st = s.State()
	if st.Finding != nil || st.Cluster.Why != "finding" || st.Totals.Found != 1 || st.Totals.Findings[0].Check != "F1" || st.Totals.Findings[0].Evidence == "" {
		t.Fatalf("after the reset: finding %v, why %s, findings %+v", st.Finding, st.Cluster.Why, st.Totals.Findings)
	}
	dir := filepath.Join(s.cfg.Dir, st.Totals.Findings[0].Evidence)
	for _, name := range []string{"n1", "n2", "n3", "n2.log", "journal.txt", "state.json", "calls.json"} {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			t.Fatalf("evidence: %v", err)
		}
	}
}

func lookups(s *Stage, change func(body []byte) []byte) {
	issue := s.issue
	s.issue = func(ctx context.Context, cl *ledger.Client, op client.Op, body []byte) (client.Reply, error) {
		reply, err := issue(ctx, cl, op, body)
		if err == nil && op == client.LookupTransfers {
			reply.Body = change(reply.Body)
		}
		return reply, err
	}
}

func TestRecountFindsAMissingTransfer(t *testing.T) {
	var armed atomic.Bool
	s := stage(t, func(s *Stage) {
		s.pace.full = time.Second
		lookups(s, func(body []byte) []byte {
			if !armed.Load() || len(body) < state.TransferSize {
				return body
			}
			foreign := state.DecodeTransfer(body)
			foreign.ID = u128.From64(1 << 40)
			return state.AppendTransfer(slices.Clone(body[state.TransferSize:]), foreign)
		})
	})
	armed.Store(true)
	await(t, 10*time.Second, "a finding", func() bool { return s.State().Finding != nil })
	if f := s.State().Finding; f.Check != "books" || !strings.Contains(f.Reason, "bestätigte Buchungen fehlen") {
		t.Fatalf("finding %s: %s", f.Check, f.Reason)
	}
}

func TestRecountSeesAStray(t *testing.T) {
	s := stage(t, func(s *Stage) { s.pace.full = time.Second })
	await(t, 10*time.Second, "a confirmed transfer", func() bool { return s.State().Books.Confirmed > 0 })
	s.mu.Lock()
	r := s.cur
	id := slices.Index(r.marks, confirmed)
	r.marks[id] = refused
	s.mu.Unlock()
	await(t, 10*time.Second, "a finding", func() bool { return s.State().Finding != nil })
	if f := s.State().Finding; f.Check != "books" || !strings.Contains(f.Reason, fmt.Sprintf("abgelehnt oder nie geschickt worden, die erste ist %d", id+1)) {
		t.Fatalf("finding %s: %s", f.Check, f.Reason)
	}
}

func TestViolationBecomesAFinding(t *testing.T) {
	var armed atomic.Bool
	s := stage(t, func(s *Stage) {
		s.pace.full = time.Hour
		issue := s.issue
		s.issue = func(ctx context.Context, cl *ledger.Client, op client.Op, body []byte) (client.Reply, error) {
			reply, err := issue(ctx, cl, op, body)
			if err == nil && op == client.CreateTransfers && len(reply.Body) == 0 && armed.CompareAndSwap(true, false) {
				reply.Body = client.AppendFailures(nil, []state.Failure{{Index: 0, Reason: state.ExceedsCredits}})
			}
			return reply, err
		}
	})
	armed.Store(true)
	await(t, 30*time.Second, "a finding", func() bool { return s.State().Finding != nil })
	st := s.State()
	if st.Finding.Check != "S8" {
		t.Fatalf("finding %s: %s", st.Finding.Check, st.Finding.Reason)
	}
	for _, n := range st.Nodes {
		if n.Proc != "frozen" || procState(n.Pid) != "T" {
			t.Fatalf("n%d is %s, process %d in state %s", n.ID, n.Proc, n.Pid, procState(n.Pid))
		}
	}
	await(t, 10*time.Second, "evidence", func() bool { return s.State().Finding.Evidence != "" })
	dir := filepath.Join(s.cfg.Dir, s.State().Finding.Evidence)
	for id := 1; id <= 3; id++ {
		segs, err := os.ReadDir(filepath.Join(dir, fmt.Sprintf("n%d", id), "wal"))
		if err != nil || len(segs) == 0 {
			t.Fatalf("evidence of n%d: %v", id, err)
		}
	}
}

func TestEvidenceIsCapped(t *testing.T) {
	s := stage(t, func(s *Stage) { s.pace.stand = 200 * time.Millisecond })
	for k := 1; k <= 4; k++ {
		await(t, 30*time.Second, fmt.Sprintf("cluster %d", k), func() bool {
			st := s.State()
			return st.Cluster.N == k && st.Phase == "steady"
		})
		if err := syscall.Kill(s.State().Nodes[0].Pid, syscall.SIGKILL); err != nil {
			t.Fatal(err)
		}
		await(t, 10*time.Second, fmt.Sprintf("finding %d with its evidence", k), func() bool {
			st := s.State()
			return st.Totals.Found == k && st.Totals.Findings[k-1].ResetAt != 0
		})
	}
	kept, _ := filepath.Glob(filepath.Join(s.cfg.Dir, "befund-*"))
	st := s.State()
	if len(kept) != evidenceMax || st.Totals.Findings[3].Evidence != "" || st.Totals.Findings[2].Evidence == "" {
		t.Fatalf("%d evidence folders %v; findings %+v", len(kept), kept, st.Totals.Findings)
	}
}

func TestLoadRestsWithoutViewers(t *testing.T) {
	b := started(t, func(s *Stage) { s.pace.rest = 2 * time.Second })
	s := b.s
	s.Watching(0)
	await(t, 10*time.Second, "a resting load", func() bool { return s.State().Load == "resting" })
	time.Sleep(500 * time.Millisecond)
	resting := s.State()
	time.Sleep(2 * time.Second)
	if st := s.State(); st.Books.Confirmed != resting.Books.Confirmed || st.Load != "resting" || st.Rest.Since == 0 {
		t.Fatalf("%d confirmed while resting, %d before; load %s", st.Books.Confirmed, resting.Books.Confirmed, st.Load)
	}
	if !slices.ContainsFunc(s.Lines(0), func(l Line) bool { return l.Text == sayRest }) {
		t.Fatal("no line about the rest")
	}
	s.Watching(1)
	await(t, 5*time.Second, "the load running again", func() bool {
		st := s.State()
		return st.Load == "running" && st.Books.Confirmed > resting.Books.Confirmed && st.Rest.Until != 0
	})
}

func TestNodesListenOnLoopback(t *testing.T) {
	s := stage(t, nil)
	s.mu.Lock()
	r := s.cur
	s.mu.Unlock()
	below := func(where, a string) {
		t.Helper()
		host, port, _ := strings.Cut(a, ":")
		if p, _ := strconv.Atoi(port); host != "127.0.0.1" || p == 0 || p >= 32768 {
			t.Fatalf("%s: %s", where, a)
		}
	}
	for _, a := range r.c.Addrs() {
		below("client address", a)
	}
	for _, rt := range r.c.Routes() {
		if host, _, _ := strings.Cut(r.c.Link(rt).Addr(), ":"); host != "127.0.0.1" {
			t.Fatalf("link %s on %s", rt, r.c.Link(rt).Addr())
		}
	}
	for id := 1; id <= 3; id++ {
		environ, err := os.ReadFile(fmt.Sprintf("/proc/%d/environ", r.c.Node(id).Pid()))
		if err != nil {
			t.Fatal(err)
		}
		for kv := range bytes.SplitSeq(environ, []byte{0}) {
			list, ok := bytes.CutPrefix(kv, []byte("LEDGER_PEERS="))
			if !ok {
				continue
			}
			for i, a := range strings.Split(string(list), ",") {
				if i == id-1 {
					below(fmt.Sprintf("n%d peer port", id), a)
				} else if host, _, _ := strings.Cut(a, ":"); host != "127.0.0.1" {
					t.Fatalf("n%d reaches n%d on %s", id, i+1, a)
				}
			}
		}
	}
}

func TestSetupFailureIsRetried(t *testing.T) {
	quit, err := exec.LookPath("true")
	if err != nil {
		t.Skip(err)
	}
	s := New(Config{Dir: t.TempDir(), Ledgerd: quit, Log: log.New(testLog{t}, "", 0)})
	s.pace = quick
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	err = s.Run(ctx)
	if err == nil || !strings.Contains(err.Error(), "gave up after 5 attempts") {
		t.Fatalf("Run: %v", err)
	}
	failed := 0
	for _, l := range s.Lines(0) {
		if strings.HasPrefix(l.Text, "Aufsetzen gescheitert") {
			failed++
		}
	}
	if st := s.State(); failed != attempts || st.Phase != "setup" || st.Cluster.Failed == "" || st.Totals.Found != 0 {
		t.Fatalf("%d failed attempts logged, phase %s, reason %q, %d findings", failed, st.Phase, st.Cluster.Failed, st.Totals.Found)
	}
	if kids := children(t); len(kids) > 0 {
		t.Fatalf("ledgerd left: %v", kids)
	}
}
