//go:build simbug

package check

import (
	"errors"
	"runtime"
	"testing"

	"github.com/Demonisreal/dmn-ledger/internal/sim"
	"github.com/Demonisreal/dmn-ledger/internal/simbug"
)

var masked = map[string]string{
	"B6":  "the leader's empty entry travels with every old entry, raft's TestCommitOnlyOwnTerm and TestFigure8 catch it",
	"B10": "check-quorum deposes a cut-off leader before a client reaches the new one, raft's TestReadConfirmedByQuorum catches it",
}

func TestBugsAreFound(t *testing.T) {
	t.Cleanup(func() { _ = simbug.Set("") })
	for _, name := range simbug.Names {
		t.Run(name, func(t *testing.T) {
			if why, ok := masked[name]; ok {
				t.Skip("not found in 1000 seeds: " + why)
			}
			if err := simbug.Set(name); err != nil {
				t.Fatal(err)
			}
			seed, err := Sweep(0, 1000, runtime.GOMAXPROCS(0), func(seed uint64) error {
				_, err := Run(Config{Seed: seed, Profile: sim.ProfileAll})
				return err
			})
			v, ok := errors.AsType[*Violation](err)
			if !ok {
				t.Fatalf("no violation in 1000 seeds, run error %v", err)
			}
			t.Logf("found by %s in seed %d: %v", v.Check, seed, v)
		})
	}
}
