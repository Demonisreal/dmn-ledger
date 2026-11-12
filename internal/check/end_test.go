package check

import (
	"crypto/sha256"
	"errors"
	"math/rand/v2"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/Demonisreal/dmn-ledger/internal/client"
	"github.com/Demonisreal/dmn-ledger/internal/replica"
	"github.com/Demonisreal/dmn-ledger/internal/sim"
	"github.com/Demonisreal/dmn-ledger/internal/state"
)

func finished(t *testing.T) (*watch, []*replica.Node) {
	t.Helper()
	c, err := sim.NewCluster(sim.ClusterConfig{Seed: 2, Profile: sim.ProfileCalm, Nodes: 3})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Run(); err != nil {
		t.Fatal(err)
	}
	w := newWatch(Config{Seed: 2, Profile: sim.ProfileCalm})
	w.c, w.over = c, true
	nodes := []*replica.Node{c.Node(1), c.Node(2), c.Node(3)}
	if err := w.owners(nodes); err != nil {
		t.Fatalf("a clean run: %v", err)
	}
	if err := w.replay(nodes[0]); err != nil {
		t.Fatalf("a clean run: %v", err)
	}
	return w, nodes
}

func wantViolation(t *testing.T, err error, check, reason string) {
	t.Helper()
	v, ok := errors.AsType[*Violation](err)
	switch {
	case reason == "" && err != nil:
		t.Fatalf("unexpected %v", err)
	case reason != "" && (!ok || v.Check != check || !strings.Contains(v.Reason, reason)):
		t.Fatalf("want %s with %q, got %v", check, reason, err)
	}
}

func TestOwnersComparesTheOp(t *testing.T) {
	w, nodes := finished(t)
	call := &w.c.Calls()[len(w.c.Calls())-1]
	call.Reply.Op = client.LookupTransfers
	if call.Op == client.LookupTransfers {
		call.Reply.Op = client.CreateAccounts
	}
	wantViolation(t, w.owners(nodes), "E1", "got the reply to")
}

func TestOwnersFindsAHeldSessionCalledUnknown(t *testing.T) {
	w, nodes := finished(t)
	call := &w.c.Calls()[len(w.c.Calls())-1]
	if _, held := nodes[0].Session(call.ID); !held {
		t.Fatalf("session of %v is gone at the end", call.ID)
	}
	call.Reply.Status = client.SessionUnknown
	wantViolation(t, w.owners(nodes), "E2", "still holds session")
}

func TestReplayFindsATransferBookedTwice(t *testing.T) {
	w, nodes := finished(t)
	calls := w.c.Calls()
	k := slices.IndexFunc(calls, func(c sim.Call) bool {
		return c.Op == client.CreateTransfers && len(c.Reply.Body) == 0 && len(c.Body) == state.TransferSize
	})
	if k < 0 {
		t.Fatal("no single transfer booked without failures")
	}
	for i := k + 1; i < len(calls); i++ {
		if calls[i].Op == client.CreateTransfers && len(calls[i].Reply.Body) == 0 {
			calls[i].Body = slices.Clone(calls[k].Body)
			wantViolation(t, w.replay(nodes[0]), "S8", "the oracle has")
			return
		}
	}
	t.Fatal("no second transfer to book twice")
}

func checked(t *testing.T) *watch {
	t.Helper()
	w, _ := finished(t)
	w.over, w.terms = false, make([]uint64, 3)
	if err := w.check(w.c); err != nil {
		t.Fatalf("a clean run: %v", err)
	}
	return w
}

func TestCheckComparesTheApplyHash(t *testing.T) {
	w := checked(t)
	w.hashes[w.c.Node(1).Status().Applied] = [sha256.Size]byte{1}
	wantViolation(t, w.check(w.c), "S3", "with hash")
}

func TestCheckComparesTheTermOfEveryCommittedIndex(t *testing.T) {
	w := checked(t)
	clear(w.commits)
	w.committed[w.c.Node(1).Status().Commit-1]++
	wantViolation(t, w.check(w.c), "S3", "committed")
}

func TestSameTermsLooksPastAnEmptyLog(t *testing.T) {
	w := checked(t)
	d := w.c.World().Disk(1).Clone(rand.New(rand.NewPCG(1, 1)))
	segs, err := d.List("/ledger/wal")
	if err != nil {
		t.Fatal(err)
	}
	for _, seg := range segs {
		if err := d.Remove(filepath.Join("/ledger/wal", seg)); err != nil {
			t.Fatal(err)
		}
	}
	n, err := w.c.Open(1, d)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = n.Close() })
	st := n.Status()
	if st.Commit == 0 || st.Last != st.Commit {
		t.Fatalf("status %+v, want a node that holds only its snapshot", st)
	}
	w.committed[st.Commit]++
	wantViolation(t, w.sameTerms(1, n, 1, st.Commit), "S3", "committed")
}
