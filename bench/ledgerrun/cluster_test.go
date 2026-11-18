package ledgerrun

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/Demonisreal/dmn-ledger/bench/load"
	"github.com/Demonisreal/dmn-ledger/ledger"
)

var bin string

var tiny = load.Workload{Name: "tiny", Accounts: 10}

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "ledgerrun-bin-")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	bin, err = Build(context.Background(), "../..", dir)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		_ = os.RemoveAll(dir)
		os.Exit(1)
	}
	code := m.Run()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

func startCluster(t *testing.T, cfg Config) *Cluster {
	t.Helper()
	cfg.Bin, cfg.Dir = bin, t.TempDir()
	c, err := Start(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func ours(t *testing.T) []int {
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
		if exe, err := os.Readlink(filepath.Join("/proc", e.Name(), "exe")); err == nil && exe == bin {
			pids = append(pids, pid)
		}
	}
	return pids
}

func devNull(t *testing.T) *os.File {
	t.Helper()
	f, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Close() })
	return f
}

func leaderNow(t *testing.T, c *Cluster) {
	t.Helper()
	leaders := 0
	for _, n := range c.nodes {
		st, err := ledger.Status(t.Context(), cluster, n.addr)
		if err != nil {
			t.Fatal(err)
		}
		if st.Term == 0 || st.Leader == 0 {
			t.Fatalf("%s right after Start: term %d, leader %d", n.name(), st.Term, st.Leader)
		}
		if st.Role == ledger.Leader {
			leaders++
		}
	}
	if leaders != 1 {
		t.Fatalf("%d leaders right after Start", leaders)
	}
}

func lastStats(t *testing.T, c *Cluster, name string) map[string]uint64 {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(c.run, name+".log"))
	if err != nil {
		t.Fatal(err)
	}
	var last string
	for line := range strings.Lines(string(b)) {
		if strings.Contains(line, " stats ") {
			last = line
		}
	}
	fields, err := parseStats(last)
	if err != nil {
		t.Fatal(err)
	}
	return fields
}

func sameStats(t *testing.T, p load.Part, logged map[string]uint64) {
	t.Helper()
	for _, name := range statsFields {
		if p.Counters[name] != logged[name] {
			t.Fatalf("%s: sampled %s=%d, the last line in its log says %d", p.Name, name, p.Counters[name], logged[name])
		}
	}
}

func gone(t *testing.T, c *Cluster, pids []int) {
	t.Helper()
	for _, pid := range pids {
		if err := syscall.Kill(pid, 0); !errors.Is(err, syscall.ESRCH) {
			t.Errorf("node process %d still there: %v", pid, err)
		}
	}
	if left := ours(t); len(left) > 0 {
		t.Errorf("ledgerd processes %v still run", left)
	}
	if _, err := os.Stat(c.run); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("%s still there: %v", c.run, err)
	}
}

func environ(t *testing.T, pid int) []string {
	t.Helper()
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/environ", pid))
	if err != nil {
		t.Fatal(err)
	}
	return strings.Split(strings.TrimRight(string(b), "\x00"), "\x00")
}

func hasAll(t *testing.T, vars []string, want ...string) {
	t.Helper()
	for _, kv := range want {
		if !slices.Contains(vars, kv) {
			t.Errorf("node environment %v lacks %s", vars, kv)
		}
	}
}

func single(i int) load.Op {
	return load.Op{Kind: load.Single, ID: ledger.From64(uint64(i + 1)), Debit: uint64(1 + i%tiny.Accounts), Credit: uint64(1 + (i+1)%tiny.Accounts), Amount: 5}
}

func TestClusterStartsAndStops(t *testing.T) {
	c := startCluster(t, Config{Nodes: 1, Lanes: 1, Env: []string{"LEDGER_SNAPSHOT_EVERY=20000"}})
	leaderNow(t, c)
	pids := c.Pids()
	vars := environ(t, pids[0])
	hasAll(t, vars, "LEDGER_SNAPSHOT_EVERY=20000", "LEDGER_SWEEP=on", "LEDGER_MAX_CLIENTS=128", "GOMEMLIMIT=4500MiB", "LEDGER_CLUSTER=1", "LEDGER_ID=1", "LEDGER_DIR="+filepath.Join(c.run, "n1"))
	if slices.Contains(vars, "LEDGER_SNAPSHOT_EVERY=4294967296") {
		t.Errorf("node environment %v keeps the default next to the given value", vars)
	}
	head := c.Describe()
	if head.Kind != "ledger" || head.Nodes != 1 || head.Lanes != 1 || head.Cores != "" || head.Env["LEDGER_SNAPSHOT_EVERY"] != "20000" || head.Env["GOMEMLIMIT"] != "4500MiB" || len(head.Env) != 5 {
		t.Errorf("head %+v", head)
	}
	if err := c.Setup(t.Context(), tiny); err != nil {
		t.Fatal(err)
	}
	op := single(0)
	if out, err := c.Do(t.Context(), 0, op); err != nil || out.Existed {
		t.Fatalf("first transfer: %+v, %v", out, err)
	}
	if _, err := c.Do(t.Context(), 0, load.Op{Kind: load.Single, ID: ledger.From64(99), Debit: 1, Credit: 2, Amount: 2 * load.Endowment}); !errors.Is(err, ErrRefused) {
		t.Fatalf("overdraft: got %v, want ErrRefused", err)
	}

	inside, err := c.Sample(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(inside) != 1 {
		t.Fatalf("%d parts, want 1", len(inside))
	}
	p := inside[0]
	if p.Name != "n1" || p.Role != "leader" || p.Term == 0 || p.Requests == 0 || p.Requests != p.Counters["applied"] {
		t.Fatalf("part %+v", p)
	}
	if p.Counters["logsync"] == 0 || p.Counters["metasync"] == 0 || p.Syncs != p.Counters["logsync"]+p.Counters["metasync"]+p.Counters["othersync"] {
		t.Fatalf("syncs %d from counters %v", p.Syncs, p.Counters)
	}
	if p.SyncTime != time.Duration(p.Counters["logsync_ns"]+p.Counters["metasync_ns"]+p.Counters["othersync_ns"]) {
		t.Fatalf("sync time %v from counters %v", p.SyncTime, p.Counters)
	}
	sameStats(t, p, lastStats(t, c, "n1"))

	tally, err := c.Verify(t.Context(), load.Books{Sample: []ledger.Uint128{op.ID}})
	if err != nil {
		t.Fatal(err)
	}
	if tally.Found != 1 || tally.DebitsPosted != op.Amount+tiny.Endowment() {
		t.Fatalf("tally %+v", tally)
	}
	if _, err := c.Verify(t.Context(), load.Books{Sample: []ledger.Uint128{op.ID, ledger.From64(77)}}); !errors.Is(err, ErrBooks) {
		t.Fatalf("a transfer that was never booked: got %v, want ErrBooks", err)
	}

	for seed := range uint64(2) {
		run, err := load.Drive(t.Context(), c, load.NewStream(tiny, 11+seed), load.Plan{Rate: 20_000, Lanes: 1, Window: time.Second})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.Verify(t.Context(), run.Books); err != nil {
			t.Fatalf("drive %d on the same cluster, %d calls cut at its end: %v", seed+1, run.Books.Unsure.Calls(), err)
		}
	}
	burst, cancel := context.WithTimeout(t.Context(), 300*time.Microsecond)
	var calls sync.WaitGroup
	for i := range 200 {
		calls.Go(func() { _, _ = c.Do(burst, 0, single(1000+i)) })
	}
	calls.Wait()
	cancel()
	if c.books.Unsure.Calls() == 0 {
		t.Fatal("200 calls with 300µs to go, none unanswered: the test shows nothing")
	}
	if _, err := c.Verify(t.Context(), load.Books{}); err != nil {
		t.Fatalf("%d calls cut before their answer: %v", c.books.Unsure.Calls(), err)
	}
	if out, err := c.Do(t.Context(), 0, op); err != nil || !out.Existed {
		t.Fatalf("same transfer again: %+v, %v", out, err)
	}

	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	gone(t, c, pids)
}

func TestSampleReadsEveryNode(t *testing.T) {
	c := startCluster(t, Config{Nodes: 3, Lanes: 4, Pin: true})
	leaderNow(t, c)
	pids := c.Pids()
	for i, pid := range pids {
		hasAll(t, environ(t, pid), "LEDGER_SNAPSHOT_EVERY=4294967296", "GOMEMLIMIT=1500MiB", fmt.Sprintf("LEDGER_ID=%d", i+1))
		status, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
		if err != nil {
			t.Fatal(err)
		}
		if want := fmt.Sprintf("Cpus_allowed_list:\t%d-%d\n", 2*i, 2*i+1); !strings.Contains(string(status), want) {
			t.Errorf("n%d runs on other cores than %q:\n%s", i+1, want, status)
		}
	}
	if cores := c.Describe().Cores; cores != "0,1 2,3 4,5" {
		t.Errorf("cores %q in the head", cores)
	}
	if err := c.Setup(t.Context(), tiny); err != nil {
		t.Fatal(err)
	}
	run, err := load.Drive(t.Context(), c, load.NewStream(tiny, 7), load.Plan{Rate: 200, Lanes: 4, Window: time.Second, Drain: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if len(run.Problems) > 0 || run.Books.Confirmed != 200 {
		t.Fatalf("%d confirmed, problems %v", run.Books.Confirmed, run.Problems)
	}
	for i, sec := range run.Seconds {
		if !sec.Sampled || len(sec.Inside) != 3 {
			t.Fatalf("second %d: sampled %v, %d parts", i, sec.Sampled, len(sec.Inside))
		}
	}
	if _, err := c.Verify(t.Context(), run.Books); err != nil {
		t.Fatal(err)
	}

	inside, err := c.Sample(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	var roles []string
	for i, p := range inside {
		if want := fmt.Sprintf("n%d", i+1); p.Name != want {
			t.Fatalf("part %d is %s, want %s", i, p.Name, want)
		}
		if p.Term != inside[0].Term || p.Syncs == 0 {
			t.Fatalf("part %+v next to term %d", p, inside[0].Term)
		}
		if (p.Role == "leader") != (p.Requests > 0) {
			t.Fatalf("%s is %s with %d requests", p.Name, p.Role, p.Requests)
		}
		sameStats(t, p, lastStats(t, c, p.Name))
		roles = append(roles, p.Role)
	}
	slices.Sort(roles)
	if !slices.Equal(roles, []string{"follower", "follower", "leader"}) {
		t.Fatalf("roles %v", roles)
	}

	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	gone(t, c, pids)
}

func names(in load.Inside) []string {
	var out []string
	for _, p := range in {
		out = append(out, p.Name)
	}
	return out
}

func TestStoppedNodesAreLeftOut(t *testing.T) {
	c := startCluster(t, Config{Nodes: 3, Lanes: 2})
	pids := c.Pids()
	if err := c.Setup(t.Context(), tiny); err != nil {
		t.Fatal(err)
	}
	if err := c.Stop(3); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Kill(pids[2], 0); !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("n3 still there after Stop: %v", err)
	}
	if got := c.Pids(); !slices.Equal(got, pids[:2]) {
		t.Fatalf("pids %v after stopping n3, want %v", got, pids[:2])
	}
	run, err := load.Drive(t.Context(), c, load.NewStream(tiny, 3), load.Plan{Rate: 200, Lanes: 2, Window: time.Second, Drain: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	for i, sec := range run.Seconds {
		if !sec.Sampled || !slices.Equal(names(sec.Inside), []string{"n1", "n2"}) {
			t.Fatalf("second %d with n3 stopped: sampled %v, parts %v, problems %v", i, sec.Sampled, names(sec.Inside), run.Problems)
		}
	}
	if _, err := c.Verify(t.Context(), run.Books); err != nil {
		t.Fatal(err)
	}

	restarted := make(chan error, 1)
	run, err = load.Drive(t.Context(), c, load.NewStream(tiny, 4), load.Plan{Rate: 200, Lanes: 2, Window: 3 * time.Second, Drain: time.Second,
		Tick: func(sec int) bool {
			if sec == 0 {
				go func() { restarted <- c.Restart(t.Context(), 3) }()
			}
			return true
		}})
	if err != nil {
		t.Fatal(err)
	}
	if err := <-restarted; err != nil {
		t.Fatal(err)
	}
	for i, sec := range run.Seconds {
		if !sec.Sampled || len(sec.Inside) < 2 {
			t.Fatalf("second %d while n3 came back: sampled %v, parts %v, problems %v", i, sec.Sampled, names(sec.Inside), run.Problems)
		}
	}
	if last := run.Seconds[len(run.Seconds)-1]; !slices.Equal(names(last.Inside), []string{"n1", "n2", "n3"}) {
		t.Fatalf("last second after n3 came back: parts %v", names(last.Inside))
	}
	if _, err := c.Verify(t.Context(), run.Books); err != nil {
		t.Fatal(err)
	}
	inside, err := c.Sample(t.Context())
	if err != nil || !slices.Equal(names(inside), []string{"n1", "n2", "n3"}) {
		t.Fatalf("after n3 came back: parts %v, %v", names(inside), err)
	}
	back := c.Pids()
	if len(back) != 3 || back[2] == pids[2] {
		t.Fatalf("pids %v after the restart of n3, before %v", back, pids)
	}

	if err := syscall.Kill(back[1], syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	for i := 0; ; i++ {
		inside, err = c.Sample(t.Context())
		if err != nil || i == 50 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !errors.Is(err, ErrNode) || !slices.Equal(names(inside), []string{"n1", "n3"}) {
		t.Fatalf("n2 killed behind the back of the cluster: parts %v, %v, want n1, n3 and ErrNode", names(inside), err)
	}
	if err := c.Close(); !errors.Is(err, ErrNode) {
		t.Fatalf("close after n2 died: %v, want ErrNode", err)
	}
	gone(t, c, back)
}

func TestFollowerIsAskedForItsRole(t *testing.T) {
	c := startCluster(t, Config{Nodes: 3, Lanes: 1})
	for range 3 {
		id, err := c.Follower(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		st, err := ledger.Status(t.Context(), cluster, c.nodes[id-1].addr)
		if err != nil || st.Role != ledger.Follower || st.Leader == uint64(id) {
			t.Fatalf("n%d picked as a follower: role %v, leader n%d, %v", id, st.Role, st.Leader, err)
		}
		if err := c.Stop(int(st.Leader)); err != nil {
			t.Fatal(err)
		}
		if err := c.Restart(t.Context(), int(st.Leader)); err != nil {
			t.Fatal(err)
		}
	}
	solo := startCluster(t, Config{Nodes: 1, Lanes: 1})
	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()
	if id, err := solo.Follower(ctx); !errors.Is(err, ErrNoFollower) {
		t.Fatalf("one node: n%d, %v, want ErrNoFollower", id, err)
	}
}

func TestSampleWithoutStatus(t *testing.T) {
	dir := t.TempDir()
	line := "2026/11/17 10:00:00 stats up=1 rounds=2 wait=3 process=4 slowest=5 logsync=6 logsync_ns=7 metasync=8 metasync_ns=9 othersync=10 othersync_ns=11 written=12"
	script := filepath.Join(dir, "stuck")
	if err := os.WriteFile(script, []byte("#!/bin/sh\ntrap 'echo \""+line+"\" >&2' USR1\nwhile :; do sleep 0.02; done\n"), 0o700); err != nil { //nolint:gosec // G306: a script the test runs
		t.Fatal(err)
	}
	mute, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer mute.Close()
	go func() {
		var conns []net.Conn
		for {
			conn, err := mute.Accept()
			if err != nil {
				for _, conn := range conns {
					_ = conn.Close()
				}
				return
			}
			conns = append(conns, conn)
		}
	}()
	n := &node{id: 1, addr: mute.Addr().String(), log: filepath.Join(dir, "n1.log"),
		status: ledger.NodeStatus{ID: 1, Role: ledger.Leader, Term: 3, Leader: 1, Applied: 42}}
	if err := n.start(script); err != nil {
		t.Fatal(err)
	}
	c := &Cluster{nodes: []*node{n}}
	defer func() { _ = c.halt() }()
	time.Sleep(100 * time.Millisecond)
	began := time.Now()
	inside, err := c.Sample(t.Context())
	took := time.Since(began)
	if err != nil || len(inside) != 1 {
		t.Fatalf("a node whose loop does not answer: %v, %d parts", err, len(inside))
	}
	p := inside[0]
	if !p.Stale || p.Role != "leader" || p.Term != 3 || p.Requests != 42 || p.Counters["logsync"] != 6 || p.Syncs != 24 {
		t.Fatalf("part %+v, want the counters of the stats line and role, term and requests from the last status, marked stale", p)
	}
	if took > sampleWithin+200*time.Millisecond {
		t.Errorf("sample took %v with a deadline of %v", took, sampleWithin)
	}
}

func TestFailedStartLeavesNoNode(t *testing.T) {
	for _, tc := range []struct {
		name string
		wait time.Duration
		env  []string
		want error
	}{
		{"no leader in time", 100 * time.Millisecond, nil, ErrNoLeader},
		{"nodes refuse their configuration", 10 * time.Second, []string{"LEDGER_TICK=never"}, ErrNode},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			ctx, cancel := context.WithTimeout(t.Context(), tc.wait)
			defer cancel()
			c, err := Start(ctx, Config{Bin: bin, Dir: dir, Nodes: 3, Lanes: 1, Env: tc.env})
			if !errors.Is(err, tc.want) {
				if c != nil {
					_ = c.Close()
				}
				t.Fatalf("got %v, want %v", err, tc.want)
			}
			if left := ours(t); len(left) > 0 {
				t.Fatalf("ledgerd processes %v still run after: %v", left, err)
			}
			if _, err := os.Stat(filepath.Join(dir, "run")); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("run directory still there: %v", err)
			}
		})
	}
}

func TestStartWantsAnEmptyRunDirectory(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "run", "n1"), 0o750); err != nil {
		t.Fatal(err)
	}
	if _, err := Start(t.Context(), Config{Bin: bin, Dir: dir, Nodes: 1, Lanes: 1}); !errors.Is(err, ErrLeftover) {
		t.Fatalf("got %v, want ErrLeftover", err)
	}
}

func TestBudgetResetsTheCluster(t *testing.T) {
	w := load.Workload{Name: "small", Accounts: 4}
	c := startCluster(t, Config{Nodes: 1, Lanes: 1, Budget: 60})
	if err := c.Setup(t.Context(), w); err != nil {
		t.Fatal(err)
	}
	stage := load.Plan{Rate: 10, Lanes: 1, Warmup: time.Second, Window: 2 * time.Second}
	pids := c.Pids()

	got, err := c.Fit(t.Context(), stage)
	if err != nil || got.Warmup != stage.Warmup || got.Window != stage.Window || got.MaxWindow != 0 {
		t.Fatalf("first stage: %+v, %v", got, err)
	}
	if !slices.Equal(c.Pids(), pids) {
		t.Fatal("cluster set up again before anything was written")
	}
	for i := range 30 {
		op := load.Op{Kind: load.Single, ID: ledger.From64(uint64(i + 1)), Debit: uint64(1 + i%4), Credit: uint64(1 + (i+1)%4), Amount: 1}
		if _, err := c.Do(t.Context(), 0, op); err != nil {
			t.Fatal(err)
		}
	}
	if tally, err := c.Verify(t.Context(), load.Books{}); err != nil || tally.DebitsPosted != 30+w.Endowment() {
		t.Fatalf("books after 30 transfers: %+v, %v", tally, err)
	}

	got, err = c.Fit(t.Context(), stage)
	if err != nil || got.Warmup != stage.Warmup || got.Window != stage.Window || got.MaxWindow != 0 {
		t.Fatalf("second stage: %+v, %v", got, err)
	}
	if slices.Equal(c.Pids(), pids) {
		t.Fatal("4 + 30 + 30 transfers over a budget of 60, and the cluster was not set up again")
	}
	if tally, err := c.Verify(t.Context(), load.Books{}); err != nil || tally.DebitsPosted != w.Endowment() {
		t.Fatalf("books after setting up again: %+v, %v", tally, err)
	}
	if left := ours(t); len(left) != 1 {
		t.Fatalf("ledgerd processes %v after setting up again, want one", left)
	}

	pids = c.Pids()
	got, err = c.Fit(t.Context(), load.Plan{Rate: 8, Lanes: 1, Warmup: time.Second, Window: 9 * time.Second})
	if err != nil || got.Warmup != time.Second || got.Window != MinWindow || got.MaxWindow != MinWindow {
		t.Fatalf("stage over the budget on its own: %+v, %v", got, err)
	}
	if !slices.Equal(c.Pids(), pids) {
		t.Fatal("a cut stage that fits set the cluster up again")
	}
	if _, err := c.Fit(t.Context(), load.Plan{Rate: 100, Lanes: 1, Window: time.Second}); !errors.Is(err, ErrBudget) {
		t.Fatalf("stage without room for a second: got %v, want ErrBudget", err)
	}
}
