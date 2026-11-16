//go:build chaos && linux

package chaos

import (
	"bytes"
	"context"
	"encoding/gob"
	"flag"
	"fmt"
	"math/rand/v2"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/Demonisreal/dmn-ledger/internal/faultnet"
	"github.com/Demonisreal/dmn-ledger/internal/sim"
	"github.com/Demonisreal/dmn-ledger/internal/u128"
	"github.com/Demonisreal/dmn-ledger/internal/workload"
	"github.com/Demonisreal/dmn-ledger/ledger"
)

const (
	clusterID = 1
	nodes     = 3
	clients   = 4
)

var (
	duration    = flag.Duration("chaos.duration", 2*time.Minute, "how long TestChaos runs, in rounds of 30s")
	seedFlag    = flag.Uint64("chaos.seed", 0, "seed of the plan and the load, 0 takes one from the clock")
	historyFlag = flag.String("chaos.history", "", "history.bin of a finding, for TestReplayHistory")
)

var bin string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "chaos-bin-")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	build := exec.Command("go", "build", "-tags", ledgerdTags, "-o", dir+string(filepath.Separator), "../../cmd/ledgerd") //nolint:gosec // G204: the directory comes from MkdirTemp
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

func pickSeed() uint64 {
	if *seedFlag != 0 {
		return *seedFlag
	}
	return uint64(time.Now().UnixNano()) //nolint:gosec // G115: the clock is after 1970
}

type finding struct {
	check, reason string
}

type harness struct {
	seed   uint64
	dir    string
	began  time.Time
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	nodes   []*node
	addrs   []string
	links   map[route]*faultnet.Link
	clients []*ledger.Client
	plan    []*window
	begun   []time.Time
	kinds   [len(kindNames)]int

	fmu    sync.Mutex
	active []*window
	sets   map[route]int

	failMu sync.Mutex
	found  *finding

	mu          sync.Mutex
	cond        *sync.Cond
	gen         *workload.Gen
	calls       []sim.Call
	first       int
	open        map[int]uint64
	started     int
	hold        bool
	closed      bool
	pending     [][]sim.Call
	windows     int
	submitted   uint64
	accountIDs  map[u128.Uint128]bool
	transferIDs map[u128.Uint128]bool
	history     *gob.Encoder
	checked     chan struct{}
}

func newHarness(t *testing.T, seed uint64, bug string) *harness {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	h := &harness{
		seed:        seed,
		dir:         t.TempDir(),
		began:       time.Now(),
		ctx:         ctx,
		cancel:      cancel,
		links:       map[route]*faultnet.Link{},
		sets:        map[route]int{},
		open:        map[int]uint64{},
		accountIDs:  map[u128.Uint128]bool{},
		transferIDs: map[u128.Uint128]bool{},
		checked:     make(chan struct{}),
	}
	h.cond = sync.NewCond(&h.mu)
	hist, err := os.Create(filepath.Join(h.dir, "history.bin"))
	if err != nil {
		t.Fatal(err)
	}
	h.history = gob.NewEncoder(hist)
	context.AfterFunc(ctx, func() {
		h.mu.Lock()
		h.cond.Broadcast()
		h.mu.Unlock()
	})
	rng := rand.New(rand.NewPCG(seed, 2))
	h.gen = workload.New(rand.New(rand.NewPCG(rng.Uint64(), rng.Uint64())))
	h.gen.MaxBatch = 1 + rng.IntN(8)
	t.Cleanup(func() {
		h.teardown()
		_ = hist.Close()
	})

	ports := freePorts(t, seed, 2*nodes)
	peers, addrs := ports[:nodes], ports[nodes:]
	h.addrs = addrs
	for i, r := range routes() {
		target := addrs[r.to-1]
		if r.from != 0 {
			target = peers[r.to-1]
		}
		l, err := faultnet.Listen(target, rand.New(rand.NewPCG(seed, 100+uint64(i)))) //nolint:gosec // G115: nine routes
		if err != nil {
			t.Fatal(err)
		}
		h.links[r] = l
	}
	for id := 1; id <= nodes; id++ {
		list := make([]string, nodes)
		for to := 1; to <= nodes; to++ {
			list[to-1] = peers[to-1]
			if to != id {
				list[to-1] = h.links[route{id, to}].Addr()
			}
		}
		env := []string{
			"LEDGER_ID=" + strconv.Itoa(id),
			"LEDGER_CLUSTER=" + strconv.Itoa(clusterID),
			"LEDGER_PEERS=" + strings.Join(list, ","),
			"LEDGER_CLIENT_ADDR=" + addrs[id-1],
			"LEDGER_DIR=" + filepath.Join(h.dir, fmt.Sprintf("n%d", id)),
			"LEDGER_SWEEP=off",
			"LEDGER_SNAPSHOT_EVERY=500",
			"LEDGER_MAX_SESSIONS=64",
		}
		if bug != "" {
			env = append(env, "LEDGER_BUG="+bug)
		}
		n := &node{h: h, id: id, env: env, log: filepath.Join(h.dir, fmt.Sprintf("n%d.log", id))}
		h.nodes = append(h.nodes, n)
		if err := n.start(); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("seed %d, data in %s on %s", seed, h.dir, fsName(h.dir))
	return h
}

func freePorts(t *testing.T, seed uint64, n int) []string {
	t.Helper()
	var addrs []string
	for port := 20000 + int(seed%10000); len(addrs) < n && port < 32768; port++ {
		addr := "127.0.0.1:" + strconv.Itoa(port)
		if l, err := net.Listen("tcp", addr); err == nil {
			_ = l.Close()
			addrs = append(addrs, addr)
		}
	}
	if len(addrs) < n {
		t.Fatalf("found %d free ports below 32768, want %d", len(addrs), n)
	}
	return addrs
}

func fsName(dir string) string {
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		return err.Error()
	}
	switch st.Type {
	case 0xef53:
		return "ext4"
	case 0x01021994:
		return "tmpfs, fsync costs nothing"
	}
	return fmt.Sprintf("file system %#x", st.Type)
}

func (h *harness) since() uint64 {
	return uint64(time.Since(h.began)) //nolint:gosec // G115: a run lasts minutes
}

func (h *harness) fail(check, format string, args ...any) {
	h.failMu.Lock()
	defer h.failMu.Unlock()
	if h.found != nil || h.ctx.Err() != nil {
		return
	}
	h.found = &finding{check: check, reason: fmt.Sprintf(format, args...)}
	for _, n := range h.nodes {
		n.halt()
	}
	h.cancel()
}

func (h *harness) finding() *finding {
	h.failMu.Lock()
	defer h.failMu.Unlock()
	return h.found
}

func (h *harness) teardown() {
	h.failMu.Lock()
	h.cancel()
	h.failMu.Unlock()
	for _, n := range h.nodes {
		n.stop()
	}
	for _, l := range h.links {
		_ = l.Close()
	}
	for _, cl := range h.clients {
		_ = cl.Close()
	}
	h.wg.Wait()
}

func (h *harness) keep() string {
	dst := filepath.Join(os.TempDir(), fmt.Sprintf("chaos-%d-%s", h.seed, time.Now().UTC().Format("20060102T150405")))
	if err := os.MkdirAll(dst, 0o750); err != nil {
		return err.Error()
	}
	var b strings.Builder
	for k, at := range h.begun {
		fmt.Fprintf(&b, "round %d began %s\n", k+1, at.UTC().Format(time.RFC3339Nano))
	}
	b.WriteString(planText(h.plan))
	if err := os.WriteFile(filepath.Join(dst, "plan.txt"), []byte(b.String()), 0o600); err != nil {
		return err.Error()
	}
	if err := os.CopyFS(dst, os.DirFS(h.dir)); err != nil {
		return err.Error()
	}
	return dst
}

func (h *harness) statuses() []*ledger.NodeStatus {
	sts := make([]*ledger.NodeStatus, nodes)
	var wg sync.WaitGroup
	for i, addr := range h.addrs {
		wg.Go(func() {
			ctx, cancel := context.WithTimeout(h.ctx, 500*time.Millisecond)
			defer cancel()
			if st, err := ledger.Status(ctx, clusterID, addr); err == nil {
				sts[i] = &st
			}
		})
	}
	wg.Wait()
	return sts
}

func (h *harness) leader() int {
	id, term := 0, uint64(0)
	for _, st := range h.statuses() {
		if st != nil && st.Role == ledger.Leader && st.Term >= term {
			id, term = int(st.ID), st.Term //nolint:gosec // G115: a node ID from 1 to 3
		}
	}
	return id
}

func (h *harness) logged(line string) int {
	seen := 0
	for _, n := range h.nodes {
		b, _ := os.ReadFile(n.log)
		seen += bytes.Count(b, []byte(line))
	}
	return seen
}

type node struct {
	h   *harness
	id  int
	env []string
	log string

	mu     sync.Mutex
	cmd    *exec.Cmd
	exited chan struct{}
	ending bool
	held   bool
}

func (n *node) start() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.held {
		return nil
	}
	logf, err := os.OpenFile(n.log, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	cmd := exec.Command(bin) //nolint:gosec // G204: the binary TestMain built
	cmd.Env = n.env
	cmd.Stderr = logf
	cmd.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGKILL}
	if err := cmd.Start(); err != nil {
		_ = logf.Close()
		return err
	}
	exited := make(chan struct{})
	n.cmd, n.exited, n.ending = cmd, exited, false
	go func() {
		err := cmd.Wait()
		_ = logf.Close()
		n.mu.Lock()
		ending := n.ending
		n.mu.Unlock()
		if !ending {
			n.h.fail("F1", "n%d ended on its own (%v), its log ends with:\n%s", n.id, err, tail(n.log, 20))
		}
		close(exited)
	}()
	return nil
}

func (n *node) running() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.cmd != nil
}

func (n *node) signal(sig syscall.Signal) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.cmd != nil && !n.held {
		_ = n.cmd.Process.Signal(sig)
	}
}

func (n *node) kill() {
	n.end(false)
}

func (n *node) stop() {
	n.end(true)
}

func (n *node) end(hold bool) {
	n.mu.Lock()
	if n.cmd == nil || n.held && !hold {
		n.held = n.held || hold
		n.mu.Unlock()
		return
	}
	n.held = n.held || hold
	n.ending = true
	_ = n.cmd.Process.Kill()
	exited := n.exited
	n.cmd = nil
	n.mu.Unlock()
	<-exited
}

func (n *node) halt() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.held = true
	if n.cmd != nil {
		_ = n.cmd.Process.Signal(syscall.SIGSTOP)
	}
}

func tail(path string, lines int) string {
	b, err := os.ReadFile(path)
	if err != nil {
		return err.Error()
	}
	all := strings.Split(strings.TrimRight(string(b), "\n"), "\n")
	return strings.Join(all[max(0, len(all)-lines):], "\n")
}
