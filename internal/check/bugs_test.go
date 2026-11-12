//go:build simbug

package check

import (
	"errors"
	"runtime"
	"testing"

	"github.com/Demonisreal/dmn-ledger/internal/sim"
	"github.com/Demonisreal/dmn-ledger/internal/simbug"
)

const masked = "B6"

func TestBugsAreFound(t *testing.T) {
	t.Cleanup(func() { _ = simbug.Set("") })
	type hunt struct {
		bug     string
		profile sim.Profile
		seeds   uint64
	}
	var hunts []hunt
	for _, name := range simbug.Names {
		seeds := uint64(1000)
		if name == "B10" {
			seeds = 2000
		}
		hunts = append(hunts, hunt{name, sim.ProfileAll, seeds})
	}
	hunts = append(hunts, hunt{"B11", ProfileEvict, 1000})
	for _, h := range hunts {
		t.Run(h.bug+"/"+string(h.profile), func(t *testing.T) {
			if h.bug == masked {
				t.Skip("not found in 10 000 seeds: the old entries need two leaders to die within one round trip each, raft's TestCommitOnlyOwnTerm and TestFigure8 catch it")
			}
			if err := simbug.Set(h.bug); err != nil {
				t.Fatal(err)
			}
			seed, err := Sweep(0, h.seeds, runtime.GOMAXPROCS(0), func(seed uint64) error {
				_, err := Run(Config{Seed: seed, Profile: h.profile})
				return err
			})
			v, ok := errors.AsType[*Violation](err)
			if !ok {
				t.Fatalf("no violation in %d seeds, run error %v", h.seeds, err)
			}
			t.Logf("found by %s in seed %d: %v", v.Check, seed, v)
		})
	}
}
