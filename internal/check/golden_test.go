package check

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/Demonisreal/dmn-ledger/internal/sim"
)

var update = flag.Bool("update", false, "rewrite testdata/traces.golden")

const goldenSeeds = 8

func TestTracesStayPinned(t *testing.T) {
	var traces strings.Builder
	for _, p := range sim.Profiles() {
		for seed := range uint64(goldenSeeds) {
			rep, err := Run(Config{Seed: seed, Profile: p})
			if err != nil {
				t.Fatalf("profile %s seed %d: %v", p, seed, err)
			}
			fmt.Fprintf(&traces, "%s %d %x\n", p, seed, rep.Trace)
		}
	}
	const path = "testdata/traces.golden"
	if *update {
		if err := os.WriteFile(path, []byte(traces.String()), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	got, pinned := strings.Split(traces.String(), "\n"), strings.Split(string(want), "\n")
	if len(got) != len(pinned) {
		t.Fatalf("%d traces, %d pinned", len(got), len(pinned))
	}
	for i := range got {
		if got[i] != pinned[i] {
			t.Errorf("trace %q, pinned %q", got[i], pinned[i])
		}
	}
}
