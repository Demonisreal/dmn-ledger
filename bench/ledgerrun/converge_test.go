package ledgerrun

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Demonisreal/dmn-ledger/bench/load"
	"github.com/Demonisreal/dmn-ledger/ledger"
)

func TestFillAndConverge(t *testing.T) {
	c := startCluster(t, Config{Nodes: 3, Lanes: 1, Env: []string{"LEDGER_SNAPSHOT_EVERY=3"}})
	if err := c.Setup(t.Context(), tiny); err != nil {
		t.Fatal(err)
	}
	if err := c.Fill(t.Context(), 25_000); err != nil {
		t.Fatal(err)
	}
	if err := c.Fill(t.Context(), 7); err != nil {
		t.Fatal(err)
	}
	if got, want := c.written.Load(), uint64(tiny.Accounts+25_007); got != want {
		t.Fatalf("%d transfers counted for the budget, want %d", got, want)
	}
	if err := c.Converge(t.Context()); err != nil {
		t.Fatal(err)
	}
	tally, err := c.Verify(t.Context(), load.Books{})
	if err != nil {
		t.Fatal(err)
	}
	if tally.DebitsPosted != tiny.Endowment()+25_007 {
		t.Fatalf("%d posted, want the endowment and 25007", tally.DebitsPosted)
	}
	inside, err := c.Sample(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range inside {
		dir := filepath.Join(c.run, p.Name, "snap")
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		var newest string
		for _, e := range entries {
			if strings.HasSuffix(e.Name(), ".snap") {
				newest = max(newest, e.Name())
			}
		}
		fi, err := os.Stat(filepath.Join(dir, newest))
		if err != nil {
			t.Fatalf("%s has no snapshot after 25 000 transfers with a snapshot every 3 entries: %v", p.Name, err)
		}
		if got := p.Counters["snapshot_bytes"]; got != uint64(fi.Size()) {
			t.Errorf("%s: snapshot_bytes %d, %s holds %d", p.Name, got, newest, fi.Size())
		}
		if p.Counters["peak_rss"] < 1<<20 {
			t.Errorf("%s: peak_rss %d", p.Name, p.Counters["peak_rss"])
		}
	}
	if err := c.Stop(3); err != nil {
		t.Fatal(err)
	}
	if err := c.Fill(t.Context(), 10); err != nil {
		t.Fatal(err)
	}
	if err := c.Converge(t.Context()); err != nil {
		t.Fatalf("two of three nodes after a stop: %v", err)
	}
}

func TestStoppedFollowerGetsASnapshot(t *testing.T) {
	c := startCluster(t, Config{Nodes: 3, Lanes: 1, Env: []string{"LEDGER_SNAPSHOT_EVERY=2", "LEDGER_SEGMENT_SIZE=2097152"}})
	if err := c.Setup(t.Context(), tiny); err != nil {
		t.Fatal(err)
	}
	bounce := func(behind uint64) (*node, uint64) {
		t.Helper()
		id, err := c.Follower(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if err := c.Stop(id); err != nil {
			t.Fatal(err)
		}
		if err := c.Fill(t.Context(), behind); err != nil {
			t.Fatal(err)
		}
		if err := c.Restart(t.Context(), id); err != nil {
			t.Fatal(err)
		}
		if err := c.Converge(t.Context()); err != nil {
			t.Fatal(err)
		}
		size, err := c.Received(id)
		if err != nil {
			t.Fatal(err)
		}
		return c.nodes[id-1], size
	}
	if n, size := bounce(100_000); size == 0 {
		t.Fatalf("%s came back 100 000 transfers behind, segments of 2 MiB, a snapshot every 2 entries: its log names no snapshot:\n%s", n.name(), tail(n.log, 6))
	}
	if n, size := bounce(3); size != 0 {
		t.Fatalf("%s came back 3 transfers behind and its log names a snapshot of %d bytes; an earlier one must not count", n.name(), size)
	}
}

func TestReceivedReadsTheLogOfTheNode(t *testing.T) {
	path := filepath.Join(t.TempDir(), "n2.log")
	early := "2026/11/18 21:00:00.000001 receiving snapshot 7012/3 from node 1\n2026/11/18 21:00:00.400000 received snapshot 7012/3 from node 1, 290123456 bytes\n"
	late := "2026/11/18 21:00:09.000000 ledgerd: node 2 listening\n2026/11/18 21:00:11.000000 received snapshot 14020/3 from node 1, 301000000 bytes\n"
	if err := os.WriteFile(path, []byte(early+late), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		from int64
		want uint64
	}{{0, 301000000}, {int64(len(early)), 301000000}, {int64(len(early + late)), 0}, {1 << 20, 0}} {
		if got, err := received(path, c.from); err != nil || got != c.want {
			t.Errorf("from byte %d: %d, %v, want %d", c.from, got, err, c.want)
		}
	}
	if err := os.WriteFile(path, []byte("x received snapshot of some kind\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := received(path, 0); err == nil {
		t.Error("a line that does not parse passed as no snapshot")
	}
}

func TestApart(t *testing.T) {
	names := []string{"n1", "n2", "n3"}
	level := func() []ledger.NodeStatus {
		st := ledger.NodeStatus{Applied: 40, ApplyHash: [32]byte{1}, HeadSeq: 9, Head: [32]byte{2}}
		return []ledger.NodeStatus{st, st, st}
	}
	if err := apart(names, level()); err != nil {
		t.Fatalf("three equal nodes: %v", err)
	}
	for _, c := range []struct {
		name  string
		spoil func(sts []ledger.NodeStatus)
		want  error
	}{
		{"behind", func(sts []ledger.NodeStatus) { sts[2].Applied = 39 }, ErrBehind},
		{"apply hash", func(sts []ledger.NodeStatus) { sts[1].ApplyHash[5] = 7 }, ErrDiverged},
		{"head", func(sts []ledger.NodeStatus) { sts[2].Head[0] = 3 }, ErrDiverged},
		{"head sequence", func(sts []ledger.NodeStatus) { sts[1].HeadSeq = 8 }, ErrDiverged},
	} {
		sts := level()
		c.spoil(sts)
		if err := apart(names, sts); !errors.Is(err, c.want) {
			t.Errorf("%s: got %v, want %v", c.name, err, c.want)
		}
	}
}
