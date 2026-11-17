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
