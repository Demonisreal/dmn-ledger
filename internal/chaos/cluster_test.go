//go:build chaos && linux

package chaos

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/gob"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/Demonisreal/dmn-ledger/internal/history"
	"github.com/Demonisreal/dmn-ledger/internal/local"
	"github.com/Demonisreal/dmn-ledger/internal/local/localtest"
	"github.com/Demonisreal/dmn-ledger/internal/u128"
	"github.com/Demonisreal/dmn-ledger/internal/wal"
	"github.com/Demonisreal/dmn-ledger/internal/workload"
	"github.com/Demonisreal/dmn-ledger/ledger"
)

const (
	clusterID   = 1
	nodes       = 3
	clients     = 4
	segmentSize = 2 << 20
)

var (
	duration    = flag.Duration("chaos.duration", 2*time.Minute, "how long TestChaos runs, in rounds of 30s")
	seedFlag    = flag.Uint64("chaos.seed", 0, "seed of the plan and the load, 0 takes one from the clock")
	historyFlag = flag.String("chaos.history", "", "history.bin of a finding, for TestReplayHistory")
)

var bin string

func TestMain(m *testing.M) {
	dir, remove := localtest.Build(ledgerdTags, "../../cmd/ledgerd")
	bin = filepath.Join(dir, "ledgerd")
	code := m.Run()
	remove()
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

	c       *local.Cluster
	clients []*ledger.Client
	plan    []*window
	begun   []time.Time
	kinds   [len(kindNames)]int

	fmu    sync.Mutex
	active []*window
	sets   map[local.Route]int

	failMu sync.Mutex
	found  *finding

	rec      *history.Recorder
	history  *gob.Encoder
	checked  chan struct{}
	receipts int

	mu          sync.Mutex
	gen         *workload.Gen
	open        map[int]uint64
	submitted   uint64
	accountIDs  map[u128.Uint128]bool
	transferIDs map[u128.Uint128]bool
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
		sets:        map[local.Route]int{},
		open:        map[int]uint64{},
		accountIDs:  map[u128.Uint128]bool{},
		transferIDs: map[u128.Uint128]bool{},
		checked:     make(chan struct{}),
	}
	hist, err := os.Create(filepath.Join(h.dir, "history.bin"))
	if err != nil {
		t.Fatal(err)
	}
	h.history = gob.NewEncoder(hist)
	t.Cleanup(func() { _ = hist.Close() })
	rng := rand.New(rand.NewPCG(seed, 2))
	h.gen = workload.New(rand.New(rand.NewPCG(rng.Uint64(), rng.Uint64())))
	h.gen.MaxBatch = 1 + rng.IntN(8)

	var keys []ed25519.PrivateKey
	for id := 1; id <= nodes; id++ {
		keys = append(keys, ed25519.NewKeyFromSeed(bytes.Repeat([]byte{byte(id)}, ed25519.SeedSize)))
	}
	env := []string{
		"LEDGER_SWEEP=off",
		"LEDGER_SNAPSHOT_EVERY=500",
		"LEDGER_SEGMENT_SIZE=" + strconv.Itoa(segmentSize),
		"LEDGER_MAX_SESSIONS=64",
	}
	if bug != "" {
		env = append(env, "LEDGER_BUG="+bug)
	}
	c, err := local.Start(local.Config{
		Bin:     bin,
		Dir:     h.dir,
		Cluster: clusterID,
		Env:     env,
		Keys:    keys,
		Clients: true,
		Seed:    seed,
		Exited: func(id int, err error) {
			h.fail("F1", "n%d ended on its own (%v), its log ends with:\n%s", id, err, tail(h.log(id), 20))
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	h.failMu.Lock()
	h.c = c
	h.failMu.Unlock()
	t.Cleanup(h.teardown)
	t.Logf("seed %d, data in %s on %s", seed, h.dir, fsName(h.dir))
	return h
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
	if h.c != nil {
		for id := 1; id <= nodes; id++ {
			h.c.Node(id).Halt()
		}
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
	_ = h.c.Close()
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
		fmt.Fprintf(&b, "storm %d began %s\n", k+1, at.UTC().Format(time.RFC3339Nano))
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
	ctx, cancel := context.WithTimeout(h.ctx, 500*time.Millisecond)
	defer cancel()
	return h.c.Statuses(ctx)
}

func (h *harness) leader() int {
	ctx, cancel := context.WithTimeout(h.ctx, 500*time.Millisecond)
	defer cancel()
	return h.c.Leader(ctx)
}

func (h *harness) log(id int) string {
	return filepath.Join(h.dir, fmt.Sprintf("n%d.log", id))
}

func (h *harness) logged(line string) int {
	seen := 0
	for id := 1; id <= nodes; id++ {
		b, _ := os.ReadFile(h.log(id))
		seen += bytes.Count(b, []byte(line))
	}
	return seen
}

func segments(dir string) (first uint64, tail int64) {
	segs, _ := os.ReadDir(filepath.Join(dir, "wal"))
	for _, seg := range segs {
		num, ok := strings.CutPrefix(seg.Name(), wal.SegmentPrefix)
		num, done := strings.CutSuffix(num, ".log")
		i, err := strconv.ParseUint(num, 10, 64)
		if !ok || !done || err != nil {
			continue
		}
		if first == 0 {
			first = i
		}
		if fi, err := seg.Info(); err == nil {
			tail = fi.Size()
		}
	}
	return first, tail
}

func tail(path string, lines int) string {
	b, err := os.ReadFile(path)
	if err != nil {
		return err.Error()
	}
	all := strings.Split(strings.TrimRight(string(b), "\n"), "\n")
	return strings.Join(all[max(0, len(all)-lines):], "\n")
}
