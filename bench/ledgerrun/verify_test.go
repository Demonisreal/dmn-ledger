package ledgerrun

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/Demonisreal/dmn-ledger/bench/load"
	"github.com/Demonisreal/dmn-ledger/ledger"
)

type fakeBooks struct {
	accounts  map[uint64]*ledger.Account
	transfers map[ledger.Uint128]ledger.Transfer
}

func newFakeBooks(w load.Workload) *fakeBooks {
	f := &fakeBooks{accounts: map[uint64]*ledger.Account{}, transfers: map[ledger.Uint128]ledger.Transfer{}}
	for id := uint64(1); id <= w.Bank(); id++ {
		f.accounts[id] = &ledger.Account{ID: ledger.From64(id), Ledger: load.Ledger, Code: load.Code}
	}
	for id := uint64(1); id < w.Bank(); id++ {
		f.post(ledger.Transfer{ID: ledger.Uint128{Hi: endowed, Lo: id}, DebitAccountID: ledger.From64(w.Bank()), CreditAccountID: ledger.From64(id), Amount: ledger.From64(load.Endowment)})
	}
	return f
}

func (f *fakeBooks) post(tr ledger.Transfer) {
	f.accounts[tr.DebitAccountID.Lo].DebitsPosted.Lo += tr.Amount.Lo
	f.accounts[tr.CreditAccountID.Lo].CreditsPosted.Lo += tr.Amount.Lo
	f.transfers[tr.ID] = tr
}

func (f *fakeBooks) undo(tr ledger.Transfer) {
	f.accounts[tr.DebitAccountID.Lo].DebitsPosted.Lo -= tr.Amount.Lo
	f.accounts[tr.CreditAccountID.Lo].CreditsPosted.Lo -= tr.Amount.Lo
	delete(f.transfers, tr.ID)
}

func (f *fakeBooks) LookupAccounts(_ context.Context, ids []ledger.Uint128) ([]ledger.Account, error) {
	if len(ids) > 10922 {
		return nil, fmt.Errorf("%w: %d accounts", ledger.ErrTooLarge, len(ids))
	}
	var found []ledger.Account
	for _, id := range ids {
		if a, ok := f.accounts[id.Lo]; ok {
			found = append(found, *a)
		}
	}
	return found, nil
}

func (f *fakeBooks) LookupTransfers(_ context.Context, ids []ledger.Uint128) ([]ledger.Transfer, error) {
	var found []ledger.Transfer
	for _, id := range ids {
		if tr, ok := f.transfers[id]; ok {
			found = append(found, tr)
		}
	}
	return found, nil
}

func TestVerifyFindsAMissingTransfer(t *testing.T) {
	w := load.Workload{Name: "test", Accounts: 25_000}
	booked := func() (*fakeBooks, load.Books, []ledger.Transfer) {
		f := newFakeBooks(w)
		var want load.Books
		var trs []ledger.Transfer
		for i := range 20 {
			tr := ledger.Transfer{ID: ledger.From64(uint64(i + 1)), DebitAccountID: ledger.From64(uint64(1 + i)), CreditAccountID: ledger.From64(uint64(24_000 - i)), Amount: ledger.From64(uint64(1 + i%7))}
			f.post(tr)
			trs = append(trs, tr)
			want.Confirmed++
			want.Posted += tr.Amount.Lo
			if i%3 == 0 {
				want.Sample = append(want.Sample, tr.ID)
			}
		}
		return f, want, trs
	}

	f, want, _ := booked()
	tally, err := count(context.Background(), f, w, want)
	if err != nil {
		t.Fatal(err)
	}
	if tally.Found != len(want.Sample) || tally.DebitsPosted != want.Posted+w.Endowment() || tally.CreditsPosted != tally.DebitsPosted || tally.DebitsPending != 0 {
		t.Fatalf("tally %+v of books %+v", tally, want)
	}

	for _, tc := range []struct {
		name  string
		spoil func(f *fakeBooks, want *load.Books, trs []ledger.Transfer)
	}{
		{"a sampled transfer never applied", func(f *fakeBooks, _ *load.Books, trs []ledger.Transfer) {
			f.undo(trs[0])
		}},
		{"a transfer outside the sample never applied", func(f *fakeBooks, _ *load.Books, trs []ledger.Transfer) {
			f.undo(trs[1])
		}},
		{"a sampled transfer is gone, the balances still add up", func(f *fakeBooks, _ *load.Books, trs []ledger.Transfer) {
			delete(f.transfers, trs[3].ID)
		}},
		{"debits and credits disagree", func(f *fakeBooks, _ *load.Books, _ []ledger.Transfer) {
			f.accounts[7].DebitsPosted.Lo++
		}},
		{"an amount landed twice", func(f *fakeBooks, _ *load.Books, trs []ledger.Transfer) {
			f.post(trs[2])
		}},
		{"pending amounts are left over", func(f *fakeBooks, _ *load.Books, _ []ledger.Transfer) {
			f.accounts[3].DebitsPending.Lo += 5
			f.accounts[4].CreditsPending.Lo += 5
		}},
		{"a pending amount went missing", func(_ *fakeBooks, want *load.Books, _ []ledger.Transfer) {
			want.Pending = 5
		}},
		{"an account is missing", func(f *fakeBooks, _ *load.Books, _ []ledger.Transfer) {
			delete(f.accounts, 12_345)
			f.accounts[1].CreditsPosted.Lo += load.Endowment
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, want, trs := booked()
			tc.spoil(f, &want, trs)
			if _, err := count(context.Background(), f, w, want); !errors.Is(err, ErrBooks) {
				t.Fatalf("got %v, want ErrBooks", err)
			}
		})
	}
}

func TestVerifyAllowsUnansweredCalls(t *testing.T) {
	w := load.Workload{Name: "test", Accounts: 100}
	transfer := func(id, amount uint64) ledger.Transfer {
		return ledger.Transfer{ID: ledger.From64(id), DebitAccountID: ledger.From64(1 + id%50), CreditAccountID: ledger.From64(51 + id%50), Amount: ledger.From64(amount)}
	}
	reserve := func(f *fakeBooks, id, amount uint64) {
		tr := transfer(id, amount)
		f.accounts[tr.DebitAccountID.Lo].DebitsPending.Lo += amount
		f.accounts[tr.CreditAccountID.Lo].CreditsPending.Lo += amount
		f.transfers[tr.ID] = tr
	}
	for _, tc := range []struct {
		name   string
		unsure load.Unsure
		landed func(f *fakeBooks)
		ok     bool
	}{
		{"nothing unsure, nothing more", load.Unsure{}, func(*fakeBooks) {}, true},
		{"nothing unsure, one more transfer", load.Unsure{}, func(f *fakeBooks) { f.post(transfer(90, 5)) }, false},
		{"unsure transfers that did not land", load.Unsure{Single: load.Doubt{Calls: 2, Amount: 7}}, func(*fakeBooks) {}, true},
		{"an unsure transfer landed", load.Unsure{Single: load.Doubt{Calls: 2, Amount: 7}}, func(f *fakeBooks) { f.post(transfer(90, 5)) }, true},
		{"both landed", load.Unsure{Single: load.Doubt{Calls: 2, Amount: 7}}, func(f *fakeBooks) { f.post(transfer(90, 5)); f.post(transfer(91, 2)) }, true},
		{"more landed than was unsure", load.Unsure{Single: load.Doubt{Calls: 2, Amount: 7}}, func(f *fakeBooks) { f.post(transfer(90, 8)) }, false},
		{"an unsure reserve landed", load.Unsure{Reserve: load.Doubt{Calls: 1, Amount: 9}}, func(f *fakeBooks) { reserve(f, 92, 9) }, true},
		{"a reserve landed that nobody sent", load.Unsure{Single: load.Doubt{Calls: 1, Amount: 9}}, func(f *fakeBooks) { reserve(f, 92, 9) }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFakeBooks(w)
			want := load.Books{Unsure: tc.unsure}
			for i := range 10 {
				tr := transfer(uint64(i+1), uint64(1+i))
				f.post(tr)
				want.Confirmed++
				want.Posted += tr.Amount.Lo
				want.Sample = append(want.Sample, tr.ID)
			}
			tc.landed(f)
			_, err := count(context.Background(), f, w, want)
			if tc.ok && err != nil || !tc.ok && !errors.Is(err, ErrBooks) {
				t.Fatalf("got %v, want ok %v", err, tc.ok)
			}
		})
	}
}
