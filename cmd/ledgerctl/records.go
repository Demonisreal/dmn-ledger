package main

import (
	"context"
	"flag"
	"fmt"

	"github.com/Demonisreal/dmn-ledger/ledger"
)

func (c *ctl) createAccount(args []string) int {
	fs := c.flags("account create")
	var a ledger.Account
	fs.Func("id", "account ID, random if missing", number128(&a.ID))
	fs.Func("ledger", "ledger of the account", number(&a.Ledger))
	fs.Func("code", "account code", number(&a.Code))
	limit := fs.String("limit", "", "debits: debits must not exceed credits, credits: the other way round")
	if code, ok := c.parse(fs, args, false); !ok {
		return code
	}
	switch *limit {
	case "":
	case "debits":
		a.Flags = ledger.DebitsMustNotExceedCredits
	case "credits":
		a.Flags = ledger.CreditsMustNotExceedDebits
	default:
		fmt.Fprintf(c.stderr, "%s: -limit %q, want debits or credits\n", fs.Name(), *limit)
		return 2
	}
	if !given(fs, "id") {
		a.ID = ledger.NewID()
	}
	return c.call(a.ID.String(), func(ctx context.Context, cl *ledger.Client) int {
		booked, err := cl.CreateAccounts(ctx, []ledger.Account{a})
		return c.written(a.ID, booked, err)
	})
}

func (c *ctl) createTransfer(args []string) int {
	fs := c.flags("transfer create")
	var tr ledger.Transfer
	fs.Func("id", "transfer ID, random if missing", number128(&tr.ID))
	fs.Func("debit", "ID of the debit account", number128(&tr.DebitAccountID))
	fs.Func("credit", "ID of the credit account", number128(&tr.CreditAccountID))
	fs.Func("amount", "amount", number128(&tr.Amount))
	fs.Func("ledger", "ledger of both accounts", number(&tr.Ledger))
	fs.Func("code", "transfer code", number(&tr.Code))
	pending := fs.Bool("pending", false, "reserve the amount instead of posting it")
	fs.Func("timeout", "seconds until a pending transfer expires, 0 never", number(&tr.Timeout))
	if code, ok := c.parse(fs, args, false); !ok {
		return code
	}
	if *pending {
		tr.Flags = ledger.Pending
	}
	return c.transfer(fs, tr)
}

func (c *ctl) settle(name string, flags ledger.TransferFlags, args []string) int {
	fs := c.flags(name)
	tr := ledger.Transfer{Flags: flags}
	fs.Func("id", "transfer ID, random if missing", number128(&tr.ID))
	fs.Func("pending-id", "ID of the pending transfer", number128(&tr.PendingID))
	if flags == ledger.PostPending {
		fs.Func("amount", "amount to post, the full pending amount if missing", number128(&tr.Amount))
	}
	if code, ok := c.parse(fs, args, false); !ok {
		return code
	}
	return c.transfer(fs, tr)
}

func (c *ctl) transfer(fs *flag.FlagSet, tr ledger.Transfer) int {
	if !given(fs, "id") {
		tr.ID = ledger.NewID()
	}
	return c.call(tr.ID.String(), func(ctx context.Context, cl *ledger.Client) int {
		booked, err := cl.CreateTransfers(ctx, []ledger.Transfer{tr})
		return c.written(tr.ID, booked, err)
	})
}

func (c *ctl) getAccounts(args []string) int {
	fs := c.flags("account get")
	if code, ok := c.parse(fs, args, true); !ok {
		return code
	}
	want, err := ids(fs)
	if err != nil {
		fmt.Fprintf(c.stderr, "%s: %v\n", fs.Name(), err)
		return 2
	}
	return c.call("account get", func(ctx context.Context, cl *ledger.Client) int {
		accounts, err := cl.LookupAccounts(ctx, want)
		if err != nil {
			return c.fail("account get", err)
		}
		found := make(map[ledger.Uint128]bool)
		for _, a := range accounts {
			found[a.ID] = true
			fmt.Fprintf(c.stdout, "id=%s ledger=%d code=%d flags=%d debits_pending=%s debits_posted=%s credits_pending=%s credits_posted=%s timestamp=%s\n",
				a.ID, a.Ledger, a.Code, a.Flags, a.DebitsPending, a.DebitsPosted, a.CreditsPending, a.CreditsPosted, stamp(a.Timestamp))
		}
		return c.missing(want, found)
	})
}

func (c *ctl) getTransfers(args []string) int {
	fs := c.flags("transfer get")
	if code, ok := c.parse(fs, args, true); !ok {
		return code
	}
	want, err := ids(fs)
	if err != nil {
		fmt.Fprintf(c.stderr, "%s: %v\n", fs.Name(), err)
		return 2
	}
	return c.call("transfer get", func(ctx context.Context, cl *ledger.Client) int {
		transfers, err := cl.LookupTransfers(ctx, want)
		if err != nil {
			return c.fail("transfer get", err)
		}
		found := make(map[ledger.Uint128]bool)
		for _, tr := range transfers {
			found[tr.ID] = true
			fmt.Fprintf(c.stdout, "id=%s debit_account_id=%s credit_account_id=%s amount=%s pending_id=%s timeout=%d ledger=%d code=%d flags=%d timestamp=%s\n",
				tr.ID, tr.DebitAccountID, tr.CreditAccountID, tr.Amount, tr.PendingID, tr.Timeout, tr.Ledger, tr.Code, tr.Flags, stamp(tr.Timestamp))
		}
		return c.missing(want, found)
	})
}

func (c *ctl) missing(want []ledger.Uint128, found map[ledger.Uint128]bool) int {
	code := 0
	for _, id := range want {
		if !found[id] {
			fmt.Fprintf(c.stdout, "not found %s\n", id)
			code = 1
		}
	}
	return code
}
