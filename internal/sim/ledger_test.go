package sim

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/Demonisreal/dmn-ledger/internal/client"
	"github.com/Demonisreal/dmn-ledger/internal/replica"
)

type ledgerWatch struct {
	hashes  map[uint64][32]byte
	terms   []uint64
	commits map[*replica.Node]uint64
	checked map[*replica.Node]uint64
}

func newLedgerWatch(nodes uint64) *ledgerWatch {
	return &ledgerWatch{
		hashes:  make(map[uint64][32]byte),
		terms:   make([]uint64, nodes),
		commits: make(map[*replica.Node]uint64),
		checked: make(map[*replica.Node]uint64),
	}
}

func (lw *ledgerWatch) check(c *Cluster) error {
	for id := uint64(1); id <= c.Nodes(); id++ {
		n := c.Node(id)
		if n == nil {
			continue
		}
		st := n.Status()
		if h, ok := lw.hashes[st.Applied]; !ok {
			lw.hashes[st.Applied] = st.ApplyHash
		} else if h != st.ApplyHash {
			return fmt.Errorf("S3: n%d applied %d with hash %x, seen %x", id, st.Applied, st.ApplyHash[:4], h[:4])
		}
		if st.Term < lw.terms[id-1] {
			return fmt.Errorf("S4: n%d term %d after %d", id, st.Term, lw.terms[id-1])
		}
		lw.terms[id-1] = st.Term
		if st.Commit < lw.commits[n] {
			return fmt.Errorf("S4: n%d commit %d after %d", id, st.Commit, lw.commits[n])
		}
		lw.commits[n] = st.Commit
		if last, ok := lw.checked[n]; !ok || st.Applied >= last+64 {
			if err := n.CheckInvariants(); err != nil {
				return fmt.Errorf("S6: n%d at %d: %w", id, st.Applied, err)
			}
			lw.checked[n] = st.Applied
		}
	}
	return nil
}

const quietBound = 2 * second

func runLedger(t testing.TB, cfg ClusterConfig, lw *ledgerWatch) *Cluster {
	t.Helper()
	if lw != nil {
		cfg.Check = lw.check
	}
	c, err := NewCluster(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Run(); err != nil {
		t.Fatal(err)
	}
	return c
}

func checkLedgerEnd(t *testing.T, c *Cluster) {
	t.Helper()
	var applied uint64
	var digest [32]byte
	for id := uint64(1); id <= c.Nodes(); id++ {
		n := c.Node(id)
		if n == nil {
			t.Fatalf("n%d is down at the end", id)
		}
		if err := n.CheckInvariants(); err != nil {
			t.Fatalf("S6: n%d at the end: %v", id, err)
		}
		st, d := n.Status(), n.Digest()
		if id == 1 {
			applied, digest = st.Applied, d
			continue
		}
		if st.Applied != applied || d != digest {
			t.Fatalf("S7: n%d applied %d digest %x, n1 applied %d digest %x", id, st.Applied, d[:4], applied, digest[:4])
		}
	}
	quiet := c.World().Quiet()
	for i, call := range c.Calls() {
		switch {
		case call.End == 0:
			t.Fatalf("call %d of c%d (%s) started at %d never answered", i, call.Client, call.Op, call.Start)
		case call.Reply.Status == client.SessionUnknown || call.Reply.Status == client.Stale:
			t.Fatalf("call %d of c%d (%s) ended with %s", i, call.Client, call.Op, call.Reply.Status)
		case call.End > quiet && call.End-max(quiet, call.Start) > quietBound:
			t.Fatalf("call %d of c%d (%s) open %d ns into the quiet phase", i, call.Client, call.Op, call.End-max(quiet, call.Start))
		}
	}
	if len(c.Calls()) == 0 {
		t.Fatal("no calls")
	}
}

func TestLedgerRuns(t *testing.T) {
	for _, p := range []Profile{ProfileCalm, ProfileAll} {
		t.Run(string(p), func(t *testing.T) {
			t.Parallel()
			for seed := range seedCount(200) {
				t.Run(strconv.FormatUint(seed, 10), func(t *testing.T) {
					t.Parallel()
					lw := newLedgerWatch(3)
					c := runLedger(t, ClusterConfig{Seed: seed, Profile: p, Nodes: 3}, lw)
					checkLedgerEnd(t, c)
				})
			}
		})
	}
}

func TestLedgerTraceHashDeterministic(t *testing.T) {
	for seed := range seedCount(200) {
		t.Run(strconv.FormatUint(seed, 10), func(t *testing.T) {
			t.Parallel()
			cfg := ClusterConfig{Seed: seed, Profile: profiles[seed%uint64(len(profiles))]}
			a := runLedger(t, cfg, nil).World().TraceHash()
			b := runLedger(t, cfg, nil).World().TraceHash()
			if a != b {
				t.Fatalf("profile %s: trace hash %x then %x", cfg.Profile, a, b)
			}
		})
	}
}

func TestClusterKnobsComeFromTheSeed(t *testing.T) {
	appends, retries := make(map[int64]bool), make(map[int]bool)
	for seed := range uint64(100) {
		c, err := NewCluster(ClusterConfig{Seed: seed})
		if err != nil {
			t.Fatal(err)
		}
		if c.appendMax < 0 || c.appendMax > 8<<10 || c.retry < 2 || c.retry > 20 {
			t.Fatalf("seed %d: MaxAppendBytes %d, retryTicks %d", seed, c.appendMax, c.retry)
		}
		again, _ := NewCluster(ClusterConfig{Seed: seed})
		if again.appendMax != c.appendMax || again.retry != c.retry {
			t.Fatalf("seed %d drew %d/%d, then %d/%d", seed, c.appendMax, c.retry, again.appendMax, again.retry)
		}
		appends[c.appendMax], retries[c.retry] = true, true
	}
	if len(appends) < 90 || len(retries) < 15 {
		t.Fatalf("%d distinct MaxAppendBytes and %d distinct retryTicks in 100 seeds", len(appends), len(retries))
	}
}
