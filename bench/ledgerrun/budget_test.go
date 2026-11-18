package ledgerrun

import (
	"errors"
	"testing"
	"time"

	"github.com/Demonisreal/dmn-ledger/bench/load"
)

func TestStagesAreCutToTheBudget(t *testing.T) {
	const s = time.Second
	uniform, _ := load.Named("uniform-10k")
	twophase, _ := load.Named("twophase")
	stage := func(rate int, warm, window, longest time.Duration) load.Plan {
		return load.Plan{Rate: rate, Lanes: 32, Warmup: warm, Window: window, MaxWindow: longest, Drain: 5 * s}
	}
	for _, tc := range []struct {
		name string
		in   load.Plan
		w    load.Workload
		room uint64
		want load.Plan
	}{
		{"three nodes hold 64 000/s", stage(64_000, 5*s, 15*s, 0), uniform, 2_000_000 - 10_000, stage(64_000, 5*s, 15*s, 0)},
		{"three nodes cut 128 000/s", stage(128_000, 5*s, 15*s, 0), uniform, 2_000_000 - 10_000, stage(128_000, 3*s, 12*s, 12*s)},
		{"one node holds 256 000/s", stage(256_000, 5*s, 15*s, 0), uniform, 6_000_000 - 10_000, stage(256_000, 5*s, 15*s, 0)},
		{"one node cuts 512 000/s", stage(512_000, 5*s, 15*s, 0), uniform, 6_000_000 - 10_000, stage(512_000, 2*s, 9*s, 9*s)},
		{"two transfers to a payment", stage(64_000, 5*s, 15*s, 0), twophase, 2_000_000 - 10_000, stage(64_000, 3*s, 12*s, 12*s)},
		{"a window that may grow counts at its longest", stage(64_000, 5*s, 15*s, 30*s), uniform, 2_000_000 - 10_000, stage(64_000, 7*s, 24*s, 24*s)},
		{"a window that may grow and still fits", stage(32_000, 5*s, 15*s, 30*s), uniform, 2_000_000 - 10_000, stage(32_000, 5*s, 15*s, 30*s)},
		{"exactly the room", stage(1000, 1*s, 9*s, 0), uniform, 10_000, stage(1000, 1*s, 9*s, 0)},
		{"three nodes cut 256 000/s to the shortest window", stage(256_000, 2*s, 6*s, 6*s), uniform, 2_000_000 - 10_000, stage(256_000, 1*s, 6*s, 6*s)},
		{"seven seconds are one to warm up and six to measure", stage(1000, 5*s, 15*s, 0), uniform, 7_000, stage(1000, 1*s, 6*s, 6*s)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := fit(tc.in, tc.w, tc.room)
			if err != nil {
				t.Fatal(err)
			}
			if got.Warmup != tc.want.Warmup || got.Window != tc.want.Window || got.MaxWindow != tc.want.MaxWindow || got.Rate != tc.in.Rate || got.Drain != tc.in.Drain {
				t.Fatalf("got %+v, want %+v", got, tc.want)
			}
			if planned(got, tc.w) > tc.room {
				t.Fatalf("%+v writes %d transfers, room for %d", got, planned(got, tc.w), tc.room)
			}
		})
	}
	if _, err := fit(stage(512_000, 5*s, 15*s, 0), twophase, 1_000_000); !errors.Is(err, ErrBudget) {
		t.Fatalf("a stage without room for one second: got %v, want ErrBudget", err)
	}
	if _, err := fit(stage(1000, 5*s, 15*s, 0), uniform, 1_999); !errors.Is(err, ErrBudget) {
		t.Fatalf("a stage with room for one second and none to warm up: got %v, want ErrBudget", err)
	}
	for _, tc := range []struct {
		nodes int
		rate  int
		fits  bool
	}{{3, 256_000, true}, {3, 512_000, false}, {1, 512_000, true}} {
		err := Fits(stage(tc.rate, 5*s, 15*s, 30*s), uniform, Budget(tc.nodes))
		if fits := !errors.Is(err, ErrBudget); fits != tc.fits || fits && err != nil {
			t.Errorf("%d nodes at %d/s: %v, want fits %v", tc.nodes, tc.rate, err, tc.fits)
		}
	}
	for _, tc := range []struct {
		name string
		in   load.Plan
		room uint64
	}{
		{"three nodes at 512 000/s keep a window of 2 s", stage(512_000, 5*s, 15*s, 0), 2_000_000 - 10_000},
		{"six seconds leave five to measure", stage(1000, 5*s, 15*s, 0), 6_999},
	} {
		if p, err := fit(tc.in, uniform, tc.room); !errors.Is(err, ErrBudget) {
			t.Errorf("%s: got %+v, %v, want ErrBudget: a window under %v is not a stage", tc.name, p, err, MinWindow)
		}
	}
}
