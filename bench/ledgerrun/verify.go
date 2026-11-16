package ledgerrun

import (
	"context"
	"fmt"

	"github.com/Demonisreal/dmn-ledger/bench/load"
	"github.com/Demonisreal/dmn-ledger/ledger"
)

type lookups interface {
	LookupAccounts(ctx context.Context, ids []ledger.Uint128) ([]ledger.Account, error)
	LookupTransfers(ctx context.Context, ids []ledger.Uint128) ([]ledger.Transfer, error)
}

func (c *Cluster) Verify(ctx context.Context, want load.Books) (load.Tally, error) {
	return count(ctx, c.sessions[0], c.w, want)
}

func count(ctx context.Context, books lookups, w load.Workload, want load.Books) (load.Tally, error) {
	var t load.Tally
	bank := w.Bank()
	found := uint64(0)
	ids := make([]ledger.Uint128, 0, chunk)
	for id := uint64(1); id <= bank; id++ {
		ids = append(ids, ledger.From64(id))
		if len(ids) < chunk && id < bank {
			continue
		}
		accounts, err := books.LookupAccounts(ctx, ids)
		if err != nil {
			return t, fmt.Errorf("look up accounts: %w", err)
		}
		for _, a := range accounts {
			if a.DebitsPosted.Hi|a.CreditsPosted.Hi|a.DebitsPending.Hi|a.CreditsPending.Hi != 0 {
				return t, fmt.Errorf("%w: account %v holds more than 64 bits", ErrBooks, a.ID)
			}
			t.DebitsPosted += a.DebitsPosted.Lo
			t.CreditsPosted += a.CreditsPosted.Lo
			t.DebitsPending += a.DebitsPending.Lo
			t.CreditsPending += a.CreditsPending.Lo
		}
		found += uint64(len(accounts))
		ids = ids[:0]
	}
	if len(want.Sample) > 0 {
		transfers, err := books.LookupTransfers(ctx, want.Sample)
		if err != nil {
			return t, fmt.Errorf("look up transfers: %w", err)
		}
		t.Found = len(transfers)
	}
	switch posted := want.Posted + w.Endowment(); {
	case found != bank:
		return t, fmt.Errorf("%w: found %d of %d accounts", ErrBooks, found, bank)
	case t.DebitsPosted != t.CreditsPosted:
		return t, fmt.Errorf("%w: %d debits posted, %d credits posted", ErrBooks, t.DebitsPosted, t.CreditsPosted)
	case t.DebitsPosted != posted:
		return t, fmt.Errorf("%w: %d posted, want %d confirmed and %d endowment", ErrBooks, t.DebitsPosted, want.Posted, w.Endowment())
	case t.DebitsPending != t.CreditsPending || t.DebitsPending != want.Pending:
		return t, fmt.Errorf("%w: %d debits pending, %d credits pending, want %d", ErrBooks, t.DebitsPending, t.CreditsPending, want.Pending)
	case t.Found != len(want.Sample):
		return t, fmt.Errorf("%w: found %d of %d sampled transfers", ErrBooks, t.Found, len(want.Sample))
	}
	return t, nil
}
