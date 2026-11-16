//go:build simbug

package check

import (
	"errors"
	"runtime"
	"slices"
	"testing"

	"github.com/Demonisreal/dmn-ledger/internal/sim"
	"github.com/Demonisreal/dmn-ledger/internal/simbug"
)

func TestBugsAreFound(t *testing.T) {
	t.Cleanup(func() { _ = simbug.Set("") })
	type hunt struct {
		bug     string
		profile sim.Profile
		seeds   uint64
		by      []string
	}
	hunts := []hunt{
		{"B1", sim.ProfileAll, 500, []string{"S5", "F1"}},
		{"B2", sim.ProfileAll, 500, []string{"F2", "S5"}},
		{"B3", sim.ProfileAll, 500, []string{"F2", "S5"}},
		{"B4", sim.ProfileAll, 500, []string{"F2", "S5"}},
		{"B5", sim.ProfileAll, 1000, []string{"S5", "F1"}},
		{"B6", sim.ProfileLeaders, 6000, []string{"S3"}},
		{"B7", sim.ProfileAll, 500, []string{"S3", "S7"}},
		{"B8", sim.ProfileAll, 500, []string{"S3"}},
		{"B9", sim.ProfileAll, 1500, []string{"S3"}},
		{"B10", sim.ProfileSplit, 8000, []string{"S9", "S13"}},
		{"B11", sim.ProfileAll, 500, []string{"S8", "S9", "S11"}},
		{"B11", sim.ProfileEvict, 500, []string{"S11", "S13"}},
		{"B11", sim.ProfileBeyond, 500, []string{"S11", "S13"}},
		{"B12", sim.ProfileAll, 500, []string{"S7", "S12"}},
		{"B12", sim.ProfileBeyond, 500, []string{"S11", "S12"}},
		{"B13", sim.ProfileRot, 500, []string{"S5"}},
		{"B14", sim.ProfileCrash, 500, []string{"F2"}},
		{"B15", sim.ProfileRot, 500, []string{"S10"}},
		{"B17", sim.ProfileRot, 4000, []string{"S5"}},
		{"B18", sim.ProfileRot, 500, []string{"S5"}},
		{"B19", sim.ProfileAll, 100, []string{"S5", "S6"}},
		{"B19", sim.ProfileCalm, 100, []string{"S5", "S6"}},
		{"B20", sim.ProfileAll, 100, []string{"S12"}},
		{"B20", sim.ProfileCalm, 100, []string{"S12"}},
		{"B21", sim.ProfileAll, 100, []string{"S5", "S6"}},
		{"B21", sim.ProfileCalm, 100, []string{"S5", "S6"}},
		{"B22", sim.ProfileAll, 100, []string{"S5", "S6"}},
		{"B22", sim.ProfileCalm, 100, []string{"S5", "S6"}},
	}
	huntedInRaft := []string{"B16"}
	for _, name := range simbug.Names() {
		if !slices.Contains(huntedInRaft, name) && !slices.ContainsFunc(hunts, func(h hunt) bool { return h.bug == name }) {
			t.Errorf("%s is not hunted", name)
		}
	}
	for _, h := range hunts {
		t.Run(h.bug+"/"+string(h.profile), func(t *testing.T) {
			if testing.Short() && h.seeds > 1000 {
				t.Skipf("hunts %d seeds", h.seeds)
			}
			if err := simbug.Set(h.bug); err != nil {
				t.Fatal(err)
			}
			seed, err := Sweep(0, h.seeds, runtime.GOMAXPROCS(0), func(seed uint64) error {
				_, err := Run(Config{Seed: seed, Profile: h.profile})
				return err
			})
			v, ok := errors.AsType[*Violation](err)
			switch {
			case !ok:
				t.Fatalf("no violation in %d seeds, run error %v", h.seeds, err)
			case !slices.Contains(h.by, v.Check):
				t.Fatalf("found by %s in seed %d, want one of %v: %v", v.Check, seed, h.by, v)
			}
			t.Logf("found by %s in seed %d of %d: %v", v.Check, seed, h.seeds, v)
		})
	}
}
