package sim

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/Demonisreal/dmn-ledger/internal/disk"
)

const (
	echoDir   = "/echo"
	echoLog   = "/echo/log"
	echoRetry = 10
)

type echo struct {
	Seq   uint64
	Reply bool
	Count int64
	At    uint64
	Note  []byte
}

func (e *echo) Clone() Body {
	c := *e
	c.Note = slices.Clone(e.Note)
	return &c
}

func (e *echo) String() string {
	if e.Reply {
		return fmt.Sprintf("echo reply %d count %d at %d note %x", e.Seq, e.Count, e.At, e.Note)
	}
	return fmt.Sprintf("echo req %d note %x", e.Seq, e.Note)
}

type echoNode struct {
	f       disk.File
	size    int64
	clock   func() uint64
	pending []Msg
}

func startEchoNode(env NodeEnv) (Process, error) {
	if err := env.FS.MkdirAll(echoDir); err != nil {
		return nil, err
	}
	names, err := env.FS.List(echoDir)
	if err != nil {
		return nil, err
	}
	var f disk.File
	if slices.Contains(names, "log") {
		f, err = env.FS.Open(echoLog)
	} else {
		f, err = env.FS.Create(echoLog)
		if err == nil {
			err = env.FS.SyncDir(echoDir)
		}
	}
	if err != nil {
		return nil, err
	}
	size, err := f.Size()
	if err != nil {
		return nil, err
	}
	return &echoNode{f: f, size: size / 8 * 8, clock: env.Clock}, nil
}

func (n *echoNode) Tick() {}

func (n *echoNode) Receive(m Msg) {
	n.pending = append(n.pending, m)
}

func (n *echoNode) Run() ([]Msg, error) {
	if len(n.pending) == 0 {
		return nil, nil
	}
	var buf []byte
	for _, m := range n.pending {
		buf = binary.BigEndian.AppendUint64(buf, m.Body.(*echo).Seq)
	}
	if _, err := n.f.WriteAt(buf, n.size); err != nil {
		return nil, err
	}
	if err := n.f.Sync(); err != nil {
		return nil, err
	}
	n.size += int64(len(buf))
	replies := make([]Msg, 0, len(n.pending))
	for _, m := range n.pending {
		req := m.Body.(*echo)
		replies = append(replies, Msg{To: m.From, Body: &echo{Seq: req.Seq, Reply: true, Count: n.size / 8, At: n.clock(), Note: req.Note}})
	}
	n.pending = n.pending[:0]
	return replies, nil
}

type echoClient struct {
	nodes   []uint64
	rng     *rand.Rand
	now     func() uint64
	target  int
	seq     uint64
	waiting bool
	ticks   int
	sent    []uint64
	got     []uint64
	outbox  []Msg
}

func newEchoClient(env ClientEnv) *echoClient {
	return &echoClient{nodes: env.Nodes, rng: env.Rand, now: env.Now, target: env.Rand.IntN(len(env.Nodes))}
}

func (c *echoClient) Tick() {
	c.ticks++
	switch {
	case c.waiting && c.ticks >= echoRetry:
		c.target = (c.target + 1) % len(c.nodes)
		c.request()
	case !c.waiting && c.ticks > 0:
		c.seq++
		c.sent = append(c.sent, c.now())
		c.waiting = true
		c.request()
	}
}

func (c *echoClient) request() {
	c.ticks = 0
	note := binary.BigEndian.AppendUint64(nil, c.rng.Uint64())
	c.outbox = append(c.outbox, Msg{To: c.nodes[c.target], Body: &echo{Seq: c.seq, Note: note}})
}

func (c *echoClient) Receive(m Msg) {
	reply := m.Body.(*echo)
	if !c.waiting || reply.Seq != c.seq {
		return
	}
	c.got = append(c.got, c.now())
	c.waiting = false
	c.ticks = -c.rng.IntN(20)
}

func (c *echoClient) Run() ([]Msg, error) {
	out := c.outbox
	c.outbox = nil
	return out, nil
}

type echoRun struct {
	w       *World
	clients []*echoClient
}

func echoConfig(seed uint64, p Profile, trace io.Writer) (Config, *echoRun) {
	r := &echoRun{}
	nodes := uint64(3)
	if seed/5%3 == 0 {
		nodes = 5
	}
	return Config{
		Seed:    seed,
		Profile: p,
		Nodes:   nodes,
		Clients: 3 + seed%4,
		Node:    startEchoNode,
		Client: func(env ClientEnv) Process {
			c := newEchoClient(env)
			r.clients = append(r.clients, c)
			return c
		},
		Trace: trace,
	}, r
}

func newEchoRun(t testing.TB, seed uint64, p Profile, trace io.Writer) *echoRun {
	t.Helper()
	cfg, r := echoConfig(seed, p, trace)
	w, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	r.w = w
	if err := w.Run(); err != nil {
		t.Fatal(err)
	}
	return r
}

var profiles = []Profile{ProfileCalm, ProfileNet, ProfileCrash, ProfileClock, ProfileAll}

func seedCount(full uint64) uint64 {
	if testing.Short() {
		return full / 5
	}
	return full
}

func TestTraceHashDeterministic(t *testing.T) {
	seeds := seedCount(200)
	hashes := make([][32]byte, seeds)
	t.Cleanup(func() {
		seen := make(map[[32]byte]int)
		for seed, h := range hashes {
			if prev, ok := seen[h]; ok {
				t.Errorf("seeds %d and %d share trace hash %x", prev, seed, h)
			}
			seen[h] = seed
		}
	})
	for seed := range seeds {
		t.Run(strconv.FormatUint(seed, 10), func(t *testing.T) {
			t.Parallel()
			p := profiles[seed%uint64(len(profiles))]
			a := newEchoRun(t, seed, p, nil).w.TraceHash()
			b := newEchoRun(t, seed, p, nil).w.TraceHash()
			if a != b {
				t.Fatalf("profile %s: trace hash %x then %x", p, a, b)
			}
			hashes[seed] = a
		})
	}
}

func TestTraceHashPinned(t *testing.T) {
	pinned := []struct {
		seed uint64
		p    Profile
		hash string
	}{
		{1, ProfileAll, "dc8de919bf0713e2ec773867716ba5a46373601b6b1e7e7afe4c118f09c509fa"},
		{2, ProfileCrash, "4805208b888fa8f2e6b7b6fe0d7e4905fcff11989af9c4867929d89cdaf3b0e6"},
		{15, ProfileNet, "6fc34c840168347553b46404434714a2db49b772c4e1858f2e34f2f9d24f77c4"},
	}
	for _, pin := range pinned {
		sum := newEchoRun(t, pin.seed, pin.p, nil).w.TraceHash()
		if got := hex.EncodeToString(sum[:]); got != pin.hash {
			t.Errorf("seed %d profile %s: trace hash %s, pinned %s; the trace changed, update the pin if that was intended", pin.seed, pin.p, got, pin.hash)
		}
	}
}

func TestEchoAllAnswersInQuiet(t *testing.T) {
	const bound = second
	for seed := range seedCount(200) {
		t.Run(strconv.FormatUint(seed, 10), func(t *testing.T) {
			t.Parallel()
			r := newEchoRun(t, seed, ProfileAll, nil)
			quiet, end := r.w.Quiet(), r.w.plan.end
			for i, c := range r.clients {
				if len(c.got) == 0 || c.got[len(c.got)-1] < quiet {
					t.Fatalf("client %d got no reply in the quiet phase", i)
				}
				for k, sent := range c.sent {
					if k == len(c.got) {
						if sent < end-bound {
							t.Fatalf("client %d request %d sent at %d never answered", i, k+1, sent)
						}
						break
					}
					if at, from := c.got[k], max(quiet, sent); at > quiet && at-from > bound {
						t.Fatalf("client %d request %d open %d ns into the quiet phase", i, k+1, at-from)
					}
				}
			}
		})
	}
}

func TestQuietPhaseHasNoFaults(t *testing.T) {
	for seed := range uint64(50) {
		var buf bytes.Buffer
		newEchoRun(t, seed, ProfileAll, &buf)
		_, after, ok := strings.Cut(buf.String(), " quiet\n")
		if !ok {
			t.Fatalf("seed %d: no quiet phase", seed)
		}
		for line := range strings.Lines(after) {
			switch strings.Fields(line)[1] {
			case "lose", "cut", "hold", "drop", "fail", "kill", "arm", "fault":
				t.Fatalf("seed %d: fault in the quiet phase: %s", seed, line)
			}
		}
	}
}

type tally map[string]int

func countTrace(t *testing.T, p Profile, seeds uint64) tally {
	t.Helper()
	counts := make(tally)
	for seed := range seeds {
		var buf bytes.Buffer
		newEchoRun(t, seed, p, &buf)
		lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
		inStart := 0
		for i, line := range lines {
			f := strings.Fields(line)
			kind := f[1]
			switch kind {
			case "fail", "kill":
				kind += " " + strings.TrimSuffix(f[3], ":")
			case "fault", "arm":
				kind += " " + f[2]
			}
			counts[kind]++
			if f[1] == "fail" && i >= 2 && strings.Fields(lines[i-2])[1] == "start" && strings.Fields(lines[i-1])[1] == "arm" {
				inStart++
			}
		}
		counts["fail in start"] += inStart
		if inStart > 0 {
			counts["seeds with fail in start"]++
		}
	}
	return counts
}

func TestCrashDuringRecovery(t *testing.T) {
	counts := countTrace(t, ProfileCrash, 100)
	if counts["seeds with fail in start"] < 20 {
		t.Fatalf("only %d of 100 seeds crash while a node restarts", counts["seeds with fail in start"])
	}
}

func TestOutageCoinHasBothSides(t *testing.T) {
	counts := countTrace(t, ProfileCrash, 50)
	for _, kind := range []string{"fail power", "fail process", "kill power", "kill process"} {
		if counts[kind] == 0 {
			t.Errorf("no %q in 50 seeds: %v", kind, counts)
		}
	}
}

func BenchmarkEchoRun(b *testing.B) {
	b.ReportAllocs()
	var events uint64
	for i := range b.N {
		r := newEchoRun(b, uint64(i%50), ProfileAll, nil)
		events += r.w.seq
	}
	b.ReportMetric(float64(events)/float64(b.N), "events/run")
}

func BenchmarkQueue(b *testing.B) {
	b.ReportAllocs()
	var q queue
	rng := rand.New(rand.NewPCG(1, 2))
	for range 1024 {
		q.push(event{at: rng.Uint64N(1 << 20)})
	}
	for b.Loop() {
		ev := q.pop()
		ev.at += rng.Uint64N(1 << 10)
		q.push(ev)
	}
}
