package workload

import (
	"bytes"
	"math/rand/v2"
	"testing"

	"github.com/Demonisreal/dmn-ledger/internal/state"
	"github.com/Demonisreal/dmn-ledger/internal/u128"
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
	accounts, transfers := make(map[u128.Uint128]bool), make(map[u128.Uint128]bool)
	for _, a := range g.accts {
		accounts[a.ID] = true
	}
	for _, tr := range g.sent {
		transfers[tr.ID] = true
	}
	type tally struct{ known, unknown int }
	var acct, xfer tally
	for range 400 {
		forAccounts, ids := g.Lookup()
		for _, id := range ids {
			switch {
			case forAccounts && accounts[id]:
				acct.known++
			case forAccounts:
				acct.unknown++
			case transfers[id]:
				xfer.known++
			default:
				xfer.unknown++
			}
		}
	}
	for _, c := range []struct {
		kind string
		n    tally
	}{{"account", acct}, {"transfer", xfer}} {
		if c.n.unknown == 0 || c.n.known < 4*c.n.unknown {
			t.Errorf("%s lookups: %d known and %d unknown ids, want mostly known and some unknown", c.kind, c.n.known, c.n.unknown)
		}
	}
}
