//go:build linux

package local

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/Demonisreal/dmn-ledger/internal/disk"
	"github.com/Demonisreal/dmn-ledger/internal/faultnet"
	"github.com/Demonisreal/dmn-ledger/internal/local/localtest"
	"github.com/Demonisreal/dmn-ledger/internal/replica"
	"github.com/Demonisreal/dmn-ledger/internal/wal"
	"github.com/Demonisreal/dmn-ledger/ledger"
)

const clusterID = 7

var bin string

func TestMain(m *testing.M) {
	dir, remove := localtest.Build("", "../../cmd/ledgerd")
	bin = filepath.Join(dir, "ledgerd")
	code := m.Run()
	remove()
	os.Exit(code)
}

func start(t *testing.T, cfg Config) *Cluster {
	t.Helper()
	cfg.Bin, cfg.Dir, cfg.Cluster = bin, t.TempDir(), clusterID
	c, err := Start(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func statuses(c *Cluster) []*ledger.NodeStatus {
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	return c.Statuses(ctx)
}

func leader(c *Cluster) int {
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	return c.Leader(ctx)
}

func peerAddr(t *testing.T, pid, id int) string {
	t.Helper()
	environ, err := os.ReadFile(fmt.Sprintf("/proc/%d/environ", pid))
	if err != nil {
		t.Fatal(err)
	}
	for kv := range bytes.SplitSeq(environ, []byte{0}) {
		if list, ok := bytes.CutPrefix(kv, []byte("LEDGER_PEERS=")); ok {
			return strings.Split(string(list), ",")[id-1]
		}
	}
	t.Fatalf("process %d has no LEDGER_PEERS", pid)
	return ""
}

func TestClusterStartsAndCloses(t *testing.T) {
	c := start(t, Config{Seed: 1, Clients: true})
	localtest.Await(t, 5*time.Second, "a leader", func() bool { return leader(c) != 0 })
	if got := fmt.Sprint(c.Routes()); got != "[c->n1 c->n2 c->n3 n1->n2 n1->n3 n2->n1 n2->n3 n3->n1 n3->n2]" {
		t.Fatalf("routes %s", got)
	}
	var pids []int
	ports := append([]string{}, c.Addrs()...)
	for id := 1; id <= nodes; id++ {
		pid := c.Node(id).Pid()
		pids = append(pids, pid)
		ports = append(ports, peerAddr(t, pid, id))
	}
	for _, addr := range ports {
		_, port, _ := net.SplitHostPort(addr)
		if n, _ := strconv.Atoi(port); n >= 32768 {
			t.Errorf("node port %s is not below 32768", addr)
		}
	}
	for _, r := range c.Routes() {
		ports = append(ports, c.Link(r).Addr())
	}
	if got := localtest.Children(t, os.Getpid()); len(got) != nodes {
		t.Fatalf("%d ledgerd children before Close: %v", len(got), got)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	for _, pid := range pids {
		if err := syscall.Kill(pid, 0); !errors.Is(err, syscall.ESRCH) {
			t.Errorf("process %d still there after Close: %v", pid, err)
		}
	}
	if got := localtest.Children(t, os.Getpid()); len(got) != 0 {
		t.Errorf("ledgerd children after Close: %v", got)
	}
	for _, addr := range ports {
		l, err := net.Listen("tcp", addr)
		if err != nil {
			t.Errorf("port still taken after Close: %v", err)
			continue
		}
		_ = l.Close()
	}
	if err := c.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
	if err := c.Node(1).Start(); err != nil || c.Node(1).Running() {
		t.Errorf("n1 started after Close: %v", err)
	}
}

func TestKillAndStart(t *testing.T) {
	c := start(t, Config{Seed: 2})
	n := c.Node(2)
	localtest.Await(t, 5*time.Second, "n2 answers", func() bool { return statuses(c)[1] != nil })
	pid := n.Pid()
	n.Kill()
	if n.Running() || n.Pid() != 0 {
		t.Fatalf("after Kill: running %t, pid %d", n.Running(), n.Pid())
	}
	if st := statuses(c)[1]; st != nil {
		t.Fatalf("killed n2 answers in term %d", st.Term)
	}
	if err := n.Start(); err != nil {
		t.Fatal(err)
	}
	localtest.Await(t, 5*time.Second, "n2 answers again", func() bool { return statuses(c)[1] != nil })
	if again := n.Pid(); again == pid || again == 0 {
		t.Fatalf("pid %d before Kill, %d after Start", pid, again)
	}
}

func TestStartOfARunningNodeFails(t *testing.T) {
	c := start(t, Config{Seed: 3})
	n := c.Node(1)
	pid := n.Pid()
	if err := n.Start(); !errors.Is(err, ErrRunning) {
		t.Fatalf("Start of a running node: %v, want ErrRunning", err)
	}
	if n.Pid() != pid {
		t.Fatalf("pid %d, was %d", n.Pid(), pid)
	}
	if got := localtest.Children(t, os.Getpid()); len(got) != nodes {
		t.Fatalf("%d ledgerd children: %v", len(got), got)
	}
}

func TestStoppedNodeStaysDown(t *testing.T) {
	c := start(t, Config{Seed: 4})
	n := c.Node(3)
	pid := n.Pid()
	n.Stop()
	if err := n.Start(); err != nil {
		t.Fatal(err)
	}
	n.Signal(syscall.SIGCONT)
	if n.Running() || n.Pid() != 0 {
		t.Fatalf("n3 runs as process %d after Stop and Start", n.Pid())
	}
	if err := syscall.Kill(pid, 0); !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("process %d still there after Stop: %v", pid, err)
	}
	if got := localtest.Children(t, os.Getpid()); len(got) != nodes-1 {
		t.Fatalf("%d ledgerd children: %v", len(got), got)
	}
}

func TestHaltedNodeIgnoresSignals(t *testing.T) {
	c := start(t, Config{Seed: 5})
	n := c.Node(1)
	pid := n.Pid()
	n.Halt()
	localtest.Await(t, 2*time.Second, "n1 stopped", func() bool { return localtest.ProcState(pid) == "T" })
	n.Signal(syscall.SIGCONT)
	n.Kill()
	if err := n.Start(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)
	if st := localtest.ProcState(pid); st != "T" || n.Pid() != pid {
		t.Fatalf("halted n1 is in state %s as process %d, want T as %d", st, n.Pid(), pid)
	}
	n.Stop()
	if err := syscall.Kill(pid, 0); !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("process %d still there after Stop: %v", pid, err)
	}
}

func TestExitedIsReported(t *testing.T) {
	type exit struct {
		node int
		err  error
	}
	exits := make(chan exit, 2*nodes)
	c := start(t, Config{Seed: 6, Exited: func(node int, err error) { exits <- exit{node, err} }})
	if err := syscall.Kill(c.Node(2).Pid(), syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	select {
	case e := <-exits:
		if e.node != 2 || e.err == nil {
			t.Fatalf("Exited(%d, %v), want n2 with an error", e.node, e.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no Exited for n2 after it was killed from outside")
	}
	localtest.Await(t, time.Second, "n2 not running", func() bool { return !c.Node(2).Running() })
	c.Node(1).Kill()
	c.Node(3).Stop()
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case e := <-exits:
		t.Fatalf("Exited(%d, %v) after Kill, Stop and Close", e.node, e.err)
	case <-time.After(300 * time.Millisecond):
	}
}

func TestCutRoutesAroundANode(t *testing.T) {
	c := start(t, Config{Seed: 7})
	localtest.Await(t, 5*time.Second, "a leader", func() bool { return leader(c) != 0 })
	around := c.Around(2)
	if got := fmt.Sprint(around); got != "[n1->n2 n2->n1 n2->n3 n3->n2]" {
		t.Fatalf("around n2: %s", got)
	}
	for _, r := range around {
		c.Link(r).Set(faultnet.Fault{Cut: true})
	}
	localtest.Await(t, 5*time.Second, "n2 alone without a leader, n1 and n3 with one", func() bool {
		sts := statuses(c)
		if sts[0] == nil || sts[1] == nil || sts[2] == nil {
			return false
		}
		lead := sts[0].Leader
		return sts[1].Leader == 0 && lead != 0 && lead != 2 && sts[2].Leader == lead && sts[lead-1].Role == ledger.Leader
	})
	for _, r := range around {
		c.Link(r).Set(faultnet.Fault{})
	}
	localtest.Await(t, 5*time.Second, "three nodes agree after the cut", func() bool {
		sts := statuses(c)
		for _, st := range sts {
			if st == nil || st.Leader == 0 || st.Leader != sts[0].Leader || st.Term != sts[0].Term {
				return false
			}
		}
		return true
	})
}

func TestEnvIsExplicit(t *testing.T) {
	t.Setenv("LEDGER_TICK", "nonsense")
	t.Setenv("LEDGER_FROM_THE_TEST", "1")
	exited := make(chan int, nodes)
	c := start(t, Config{Seed: 8, Env: []string{"LEDGER_SWEEP=off"}, Exited: func(node int, _ error) { exited <- node }})
	localtest.Await(t, 5*time.Second, "a leader", func() bool { return leader(c) != 0 })
	select {
	case id := <-exited:
		t.Fatalf("n%d ended, its log ends with:\n%s", id, logTail(t, c, id))
	default:
	}

	refused := make(chan int, nodes)
	start(t, Config{Seed: 9, Env: []string{"LEDGER_SWEEP=nonsense"}, Exited: func(node int, _ error) { refused <- node }})
	for range nodes {
		select {
		case <-refused:
		case <-time.After(5 * time.Second):
			t.Fatal("a node with LEDGER_SWEEP=nonsense from Env kept running")
		}
	}
}

func TestStartInitialisesEveryDirectory(t *testing.T) {
	c := start(t, Config{Seed: 10})
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	for id := 1; id <= nodes; id++ {
		if err := replica.Init(disk.OS{}, c.Node(id).Dir(), wal.Identity{Cluster: clusterID, Node: uint64(id), Nodes: nodes}); !errors.Is(err, replica.ErrInitialised) {
			t.Errorf("init on the directory of n%d after Close: %v, want replica.ErrInitialised", id, err)
		}
	}
}

func TestStartRefusesAUsedDirectory(t *testing.T) {
	c := start(t, Config{Seed: 11})
	localtest.Await(t, 5*time.Second, "a leader", func() bool { return leader(c) != 0 })
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	again, err := Start(Config{Bin: bin, Dir: c.cfg.Dir, Cluster: clusterID, Seed: 12})
	if err == nil {
		_ = again.Close()
		t.Fatal("a second Start on the same Dir went through")
	}
	if !errors.Is(err, replica.ErrInitialised) {
		t.Fatalf("second Start on the same Dir: %v, want replica.ErrInitialised", err)
	}
}

func TestNodeWithoutMetaDoesNotStart(t *testing.T) {
	exited := make(chan error, nodes)
	c := start(t, Config{Seed: 13, Exited: func(node int, err error) {
		if node == 2 {
			exited <- err
		}
	}})
	n := c.Node(2)
	localtest.Await(t, 5*time.Second, "n2 answers", func() bool { return statuses(c)[1] != nil })
	n.Kill()
	meta := filepath.Join(n.Dir(), wal.MetaName)
	if err := os.Remove(meta); err != nil {
		t.Fatal(err)
	}
	if err := n.Start(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-exited:
		if err == nil {
			t.Fatal("n2 without meta ended without an error")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("n2 without meta kept running")
	}
	if tail := logTail(t, c, 2); !strings.Contains(tail, "wal: no meta") {
		t.Fatalf("log of n2 ends with:\n%s", tail)
	}
	if _, err := os.Stat(meta); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("meta of n2 after the refused start: %v", err)
	}
}

func logTail(t *testing.T, c *Cluster, id int) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(filepath.Dir(c.Node(id).Dir()), fmt.Sprintf("n%d.log", id)))
	if err != nil {
		return err.Error()
	}
	return string(b[max(0, len(b)-2000):])
}
