package ledgerrun

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Demonisreal/dmn-ledger/bench/load"
	"github.com/Demonisreal/dmn-ledger/ledger"
)

type lookups interface {
	LookupAccounts(ctx context.Context, ids []ledger.Uint128) ([]ledger.Account, error)
	LookupTransfers(ctx context.Context, ids []ledger.Uint128) ([]ledger.Transfer, error)
}

func (c *Cluster) Verify(ctx context.Context, want load.Books) (load.Tally, error) {
	c.mu.Lock()
	books := c.books
	c.mu.Unlock()
	books.Sample = want.Sample
	return count(ctx, c.sessions[0], c.w, books)
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
	u := want.Unsure
	posted, maybePosted := want.Posted+w.Endowment(), u.Single.Amount+u.Post.Amount
	pending, released := want.Pending+u.Reserve.Amount, u.Post.Amount+u.Void.Amount
	unsure := ""
	if u.Calls() > 0 {
		unsure = fmt.Sprintf(", %d calls unanswered", u.Calls())
	}
	switch {
	case found != bank:
		return t, fmt.Errorf("%w: found %d of %d accounts", ErrBooks, found, bank)
	case t.DebitsPosted != t.CreditsPosted:
		return t, fmt.Errorf("%w: %d debits posted, %d credits posted", ErrBooks, t.DebitsPosted, t.CreditsPosted)
	case t.DebitsPosted < posted || t.DebitsPosted > posted+maybePosted:
		return t, fmt.Errorf("%w: %d posted, want %d confirmed and %d endowment%s", ErrBooks, t.DebitsPosted, want.Posted, w.Endowment(), unsure)
	case t.DebitsPending != t.CreditsPending || t.DebitsPending > pending || t.DebitsPending < want.Pending-released:
		return t, fmt.Errorf("%w: %d debits pending, %d credits pending, want %d%s", ErrBooks, t.DebitsPending, t.CreditsPending, want.Pending, unsure)
	case t.Found != len(want.Sample):
		return t, fmt.Errorf("%w: found %d of %d sampled transfers", ErrBooks, t.Found, len(want.Sample))
	}
	return t, nil
}

func (c *Cluster) Converge(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, levelWithin)
	defer cancel()
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	nodes := c.running()
	names := make([]string, len(nodes))
	for i, n := range nodes {
		names[i] = n.name()
	}
	for {
		sts, err := c.statuses(ctx)
		if err == nil {
			if err := apart(names, sts); !errors.Is(err, ErrBehind) {
				return err
			}
		}
		select {
		case <-ctx.Done():
			return errors.Join(apart(names, sts), err)
		case <-tick.C:
		}
	}
}

func apart(names []string, sts []ledger.NodeStatus) error {
	var at []string
	for i, st := range sts {
		at = append(at, fmt.Sprintf("%s at %d", names[i], st.Applied))
	}
	for i, st := range sts {
		if st.Applied != sts[0].Applied {
			return fmt.Errorf("%w: %s", ErrBehind, strings.Join(at, ", "))
		}
		if st.ApplyHash != sts[0].ApplyHash || st.HeadSeq != sts[0].HeadSeq || st.Head != sts[0].Head {
			return fmt.Errorf("%w: %s and %s at %d", ErrDiverged, names[0], names[i], st.Applied)
		}
	}
	return nil
}
