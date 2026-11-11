package workload

import (
	"bytes"
	"math/rand/v2"
	"testing"

	"github.com/Demonisreal/dmn-ledger/internal/state"
)

func TestSameSeedSameBatches(t *testing.T) {
	a, b := New(rand.New(rand.NewPCG(5, 6))), New(rand.New(rand.NewPCG(5, 6)))
	for i := range 500 {
		x, y := a.Batch(), b.Batch()
		if x.Op != y.Op || x.TS != y.TS || !bytes.Equal(x.Body, y.Body) {
			t.Fatalf("batch %d differs", i)
		}
		a.Settle(x, nil)
		b.Settle(y, nil)
	}
}

func TestSettleDropsFailedPendings(t *testing.T) {
	g := New(rand.New(rand.NewPCG(1, 2)))
	g.PendingPct, g.MaxBatch = 50, 8
	for len(g.pendings) == 0 {
		b := g.Batch()
		g.Settle(b, nil)
	}
	var b Batch
	for len(b.pendings) == 0 {
		b = g.Batch()
	}
	before := len(g.pendings)
	g.Settle(b, []state.Failure{{Index: b.pendings[0], Reason: state.ExceedsCredits}})
	if len(g.pendings) != before-1 {
		t.Fatalf("pendings %d, want %d", len(g.pendings), before-1)
	}
	for _, p := range g.pendings {
		if p.ID == b.pendingIDs[0] {
			t.Fatal("failed pending transfer is still offered for post and void")
		}
	}
}

func TestLookupPicksKnownIDs(t *testing.T) {
	g := New(rand.New(rand.NewPCG(3, 4)))
	for range 50 {
		g.Settle(g.Batch(), nil)
	}
	known := 0
	for range 200 {
		accounts, ids := g.Lookup()
		for _, id := range ids {
			if accounts {
				for _, a := range g.accts {
					if a.ID == id {
						known++
						break
					}
				}
			}
		}
	}
	if known == 0 {
		t.Fatal("lookups never ask for a known account")
	}
}
