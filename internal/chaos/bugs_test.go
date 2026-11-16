//go:build chaos && linux && simbug

package chaos

import (
	"testing"
	"time"
)

const ledgerdTags = "simbug"

func TestChaosFindsPlantedBugs(t *testing.T) {
	cases := []struct {
		bug    string
		budget time.Duration
		want   string
	}{
		{"B11", 2 * time.Minute, "S8"},
		{"B10", 5 * time.Minute, ""},
		{"B12", 5 * time.Minute, ""},
	}
	seed := pickSeed()
	for i, tc := range cases {
		t.Run(tc.bug, func(t *testing.T) {
			h := newHarness(t, seed+uint64(i), tc.bug) //nolint:gosec // G115: three cases
			f := h.run(tc.budget)
			t.Log(h.summary())
			switch {
			case f == nil && tc.want != "":
				t.Fatalf("seed %d: %s stayed hidden for %v", h.seed, tc.bug, tc.budget)
			case f == nil:
				t.Logf("seed %d: %s stayed hidden for %v; a finding is wanted, not required", h.seed, tc.bug, tc.budget)
			case tc.want != "" && f.check != tc.want:
				t.Fatalf("seed %d: %s showed as %s, want %s: %s", h.seed, tc.bug, f.check, tc.want, f.reason)
			default:
				t.Logf("seed %d: %s found as %s after %v: %s", h.seed, tc.bug, f.check, time.Since(h.began).Round(time.Second), f.reason)
			}
		})
	}
}
