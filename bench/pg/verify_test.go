package pg

import (
	"errors"
	"testing"

	"github.com/Demonisreal/dmn-ledger/bench/load"
	"github.com/Demonisreal/dmn-ledger/ledger"
)

func TestVerifyComparesEverything(t *testing.T) {
	w := load.Workload{Name: "test", Accounts: 25_000}
	want := load.Books{Confirmed: 40, Posted: 1234, Pending: 56, Sample: []ledger.Uint128{ledger.From64(1), ledger.From64(2), ledger.From64(3)}}
	books := func() (load.Tally, uint64) {
		posted := want.Posted + w.Endowment()
		return load.Tally{Found: 3, DebitsPosted: posted, CreditsPosted: posted, DebitsPending: 56, CreditsPending: 56}, w.Bank()
	}
	tally, accounts := books()
	if err := compare(tally, accounts, w, want); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name  string
		spoil func(t *load.Tally, accounts *uint64)
	}{
		{"an account is missing", func(_ *load.Tally, accounts *uint64) { *accounts-- }},
		{"debits and credits disagree", func(t *load.Tally, _ *uint64) { t.DebitsPosted++ }},
		{"a confirmed amount never landed", func(t *load.Tally, _ *uint64) { t.DebitsPosted -= 7; t.CreditsPosted -= 7 }},
		{"an amount landed twice", func(t *load.Tally, _ *uint64) { t.DebitsPosted += 7; t.CreditsPosted += 7 }},
		{"pending sides disagree", func(t *load.Tally, _ *uint64) { t.CreditsPending++ }},
		{"a reservation went missing", func(t *load.Tally, _ *uint64) { t.DebitsPending -= 5; t.CreditsPending -= 5 }},
		{"a sampled transfer is gone", func(t *load.Tally, _ *uint64) { t.Found-- }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tally, accounts := books()
			tc.spoil(&tally, &accounts)
			if err := compare(tally, accounts, w, want); !errors.Is(err, ErrBooks) {
				t.Fatalf("got %v, want ErrBooks", err)
			}
		})
	}
}
