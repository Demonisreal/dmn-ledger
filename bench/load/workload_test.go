package load

import (
	"errors"
	"testing"
	"time"
)

func arrivals(w Workload, seed uint64, n int) []Arrival {
	s := NewStream(w, seed)
	out := make([]Arrival, n)
	for i := range out {
		out[i] = s.next(time.Duration(i) * 50 * time.Microsecond)
	}
	return out
}

func TestSameSeedSameStream(t *testing.T) {
	for _, w := range Workloads() {
		a, b, c := arrivals(w, 11, 5000), arrivals(w, 11, 5000), arrivals(w, 12, 5000)
		same := 0
		for i := range a {
			if a[i] != b[i] {
				t.Fatalf("%s: arrival %d differs for the same seed: %+v, %+v", w.Name, i, a[i], b[i])
			}
			if a[i] == c[i] {
				same++
			}
		}
		if same > 0 {
			t.Errorf("%s: %d arrivals equal under another seed", w.Name, same)
		}
	}
}

func TestIDsFollowThePlan(t *testing.T) {
	s := NewStream(workloads[0], 5)
	seen := make(map[[2]uint64]bool)
	var prev uint64
	for run := range 3 {
		var last time.Duration
		for i := range 4000 {
			last = time.Duration(i) * 500 * time.Microsecond
			a := s.next(last)
			ms := a.First.ID.Hi >> 16
			if ms < prev {
				t.Fatalf("run %d, arrival %d: id time %dms after %dms", run, i, ms, prev)
			}
			prev = ms
			key := [2]uint64{a.First.ID.Hi, a.First.ID.Lo}
			if seen[key] || a.First.ID.Hi == 0 && a.First.ID.Lo == 0 {
				t.Fatalf("run %d, arrival %d: id %v repeats or is zero", run, i, a.First.ID)
			}
			seen[key] = true
		}
		s.clock += uint64(last/time.Millisecond) + 1
	}
}

func TestWorkloads(t *testing.T) {
	const n = 100_000
	for _, w := range Workloads() {
		hot, debits := 0, make(map[uint64]int)
		for i, a := range arrivals(w, 1, n) {
			op := a.First
			if op.Debit == op.Credit || op.Debit < 1 || op.Credit < 1 || op.Debit > uint64(w.Accounts) || op.Credit > uint64(w.Accounts) {
				t.Fatalf("%s, arrival %d: accounts %d and %d out of 1..%d", w.Name, i, op.Debit, op.Credit, w.Accounts)
			}
			if op.Amount < 1 || op.Amount > 100 {
				t.Fatalf("%s, arrival %d: amount %d", w.Name, i, op.Amount)
			}
			if op.Credit <= 8 {
				hot++
			}
			debits[op.Debit]++
			switch {
			case w.TwoPhase && op.Kind != Reserve:
				t.Fatalf("%s, arrival %d starts with %v", w.Name, i, op.Kind)
			case w.TwoPhase:
				then := a.Then
				if then.Kind != Post && then.Kind != Void || then.Pending != op.ID || then.ID == op.ID ||
					then.Debit != op.Debit || then.Credit != op.Credit || then.Amount != op.Amount {
					t.Fatalf("%s, arrival %d: %+v does not settle %+v", w.Name, i, then, op)
				}
			case op.Kind != Single || a.Then.Kind != 0:
				t.Fatalf("%s, arrival %d: %v then %v", w.Name, i, op.Kind, a.Then.Kind)
			}
		}
		share := float64(hot) / n
		switch {
		case w.Hot > 0 && (share < 0.79 || share > 0.81):
			t.Errorf("%s: %.3f of the credits on the eight hot accounts, want 0.8", w.Name, share)
		case w.Hot == 0 && share > 0.01:
			t.Errorf("%s: %.3f of the credits on accounts 1 to 8", w.Name, share)
		}
		if w.Accounts == 10_000 && len(debits) < 9_900 {
			t.Errorf("%s: only %d accounts pay", w.Name, len(debits))
		}
	}
	w, _ := Named("twophase")
	posts := 0
	for _, a := range arrivals(w, 2, n) {
		if a.Then.Kind == Post {
			posts++
		}
	}
	if share := float64(posts) / n; share < 0.89 || share > 0.91 {
		t.Errorf("%.3f of the reservations posted, want 0.9", share)
	}
	if _, err := Named("uniform"); !errors.Is(err, ErrWorkload) {
		t.Errorf("unknown workload: %v", err)
	}
	if w, _ := Named("uniform-1m"); w.Bank() != 1_000_001 || w.Endowment() != 1_000_000*Endowment {
		t.Errorf("uniform-1m: bank %d, endowment %d", w.Bank(), w.Endowment())
	}
}
