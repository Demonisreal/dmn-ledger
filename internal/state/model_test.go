package state_test

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/Demonisreal/dmn-ledger/internal/oracle"
	"github.com/Demonisreal/dmn-ledger/internal/state"
)

const eventsPerRun = 2000

func TestModel(t *testing.T) {
	t.Parallel()
	total := 1_000_000
	if testing.Short() {
		total = 40_000
	}
	var seen [state.ExceedsDebits + 1]atomic.Int64
	t.Run("seeds", func(t *testing.T) {
		for seed := range uint64(total / eventsPerRun) {
			t.Run(fmt.Sprint(seed), func(t *testing.T) {
				t.Parallel()
				r := rand.New(rand.NewPCG(seed, 0x1ed6e5))
				run(t, r, eventsPerRun, nil, func(c state.Code) { seen[c].Add(1) })
			})
		}
	})
	if testing.Short() || t.Failed() {
		return
	}
	for c := state.LinkedEventFailed; c <= state.ExceedsDebits; c++ {
		if seen[c].Load() == 0 {
			t.Errorf("generator never produced %v", c)
		}
	}
}

func FuzzApply(f *testing.F) {
	r := rand.New(rand.NewPCG(7, 7))
	for range 8 {
		seed := make([]byte, 64+r.IntN(4096))
		for i := range seed {
			seed[i] = byte(r.Uint32())
		}
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, seed []byte) {
		src := &fuzzSource{b: seed}
		run(t, rand.New(src), eventsPerRun, src.exhausted, nil)
	})
}

func run(t *testing.T, r *rand.Rand, events int, stop func() bool, observe func(state.Code)) {
	t.Helper()
	g := newGen(r)
	m, o := state.New(), oracle.New()
	for n := 0; events > 0 && (stop == nil || !stop()); n++ {
		b := g.batch()
		got, err := m.Apply(b.op, b.ts, b.body)
		if err != nil {
			t.Fatalf("batch %d: %v", n, err)
		}
		want, err := o.Apply(b.op, b.ts, b.body)
		if err != nil {
			t.Fatalf("batch %d: oracle: %v", n, err)
		}
		if !slices.Equal(got, want) {
			t.Fatalf("batch %d: results differ\n got %v\nwant %v\n%s", n, got, want, dump(b))
		}
		if got, want := m.LookupAccounts(b.ids), o.LookupAccounts(b.ids); !slices.Equal(got, want) {
			t.Fatalf("batch %d: accounts differ\n got %+v\nwant %+v\n%s", n, got, want, dump(b))
		}
		if got, want := m.LookupTransfers(b.ids), o.LookupTransfers(b.ids); !slices.Equal(got, want) {
			t.Fatalf("batch %d: transfers differ\n got %+v\nwant %+v\n%s", n, got, want, dump(b))
		}
		if m.Digest() != o.Digest() {
			t.Fatalf("batch %d: digests differ\n%s", n, dump(b))
		}
		if err := m.CheckInvariants(); err != nil {
			t.Fatalf("batch %d: %v", n, err)
		}
		if err := o.CheckInvariants(); err != nil {
			t.Fatalf("batch %d: oracle: %v", n, err)
		}
		g.settle(b, got)
		if observe != nil {
			for _, fail := range got {
				observe(fail.Code)
			}
		}
		if b.op == state.CreateAccounts {
			events -= len(b.body) / state.AccountSize
		} else {
			events -= len(b.body) / state.TransferSize
		}
	}
}

func dump(b batch) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "op %d at %d:\n", b.op, b.ts)
	if b.op == state.CreateAccounts {
		for i := 0; i < len(b.body); i += state.AccountSize {
			fmt.Fprintf(&sb, "  %d %+v\n", i/state.AccountSize, state.DecodeAccount(b.body[i:]))
		}
		return sb.String()
	}
	for i := 0; i < len(b.body); i += state.TransferSize {
		fmt.Fprintf(&sb, "  %d %+v\n", i/state.TransferSize, state.DecodeTransfer(b.body[i:]))
	}
	return sb.String()
}
