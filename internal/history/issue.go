package history

import (
	"context"

	"github.com/Demonisreal/dmn-ledger/internal/client"
	"github.com/Demonisreal/dmn-ledger/internal/state"
	"github.com/Demonisreal/dmn-ledger/internal/u128"
	"github.com/Demonisreal/dmn-ledger/ledger"
)

func Issue(ctx context.Context, cl *ledger.Client, op client.Op, body []byte) (client.Reply, error) {
	var res ledger.Result
	var err error
	switch op {
	case client.CreateAccounts:
		res, err = cl.CreateAccounts(ctx, accountsOf(body))
	case client.CreateTransfers:
		res, err = cl.CreateTransfers(ctx, transfersOf(body))
	case client.LookupAccounts:
		accts, err := cl.LookupAccounts(ctx, idsOf(body))
		var found []byte
		for _, a := range accts {
			found = state.AppendAccount(found, account(a))
		}
		return client.Reply{Status: client.OK, Body: found}, err
	case client.LookupTransfers:
		trs, err := cl.LookupTransfers(ctx, idsOf(body))
		var found []byte
		for _, tr := range trs {
			found = state.AppendTransfer(found, transfer(tr))
		}
		return client.Reply{Status: client.OK, Body: found}, err
	}
	fails := make([]state.Failure, len(res.Failures))
	for i, f := range res.Failures {
		fails[i] = state.Failure{Index: f.Index, Reason: state.Reason(f.Reason)}
	}
	return client.Reply{Status: client.OK, Time: res.Timestamp, Body: client.AppendFailures(nil, fails)}, err
}

func idsOf(body []byte) []ledger.Uint128 {
	var ids []ledger.Uint128
	for _, id := range client.DecodeIDs(body) {
		ids = append(ids, ledger.Uint128(id))
	}
	return ids
}

func accountsOf(body []byte) []ledger.Account {
	var accts []ledger.Account
	for ; len(body) > 0; body = body[state.AccountSize:] {
		a := state.DecodeAccount(body)
		accts = append(accts, ledger.Account{
			ID:             ledger.Uint128(a.ID),
			DebitsPending:  ledger.Uint128(a.DebitsPending),
			DebitsPosted:   ledger.Uint128(a.DebitsPosted),
			CreditsPending: ledger.Uint128(a.CreditsPending),
			CreditsPosted:  ledger.Uint128(a.CreditsPosted),
			Ledger:         a.Ledger,
			Code:           a.Code,
			Flags:          ledger.AccountFlags(a.Flags),
			Timestamp:      a.Timestamp,
		})
	}
	return accts
}

func transfersOf(body []byte) []ledger.Transfer {
	var trs []ledger.Transfer
	for ; len(body) > 0; body = body[state.TransferSize:] {
		tr := state.DecodeTransfer(body)
		trs = append(trs, ledger.Transfer{
			ID:              ledger.Uint128(tr.ID),
			DebitAccountID:  ledger.Uint128(tr.DebitAccountID),
			CreditAccountID: ledger.Uint128(tr.CreditAccountID),
			Amount:          ledger.Uint128(tr.Amount),
			PendingID:       ledger.Uint128(tr.PendingID),
			Timeout:         tr.Timeout,
			Ledger:          tr.Ledger,
			Code:            tr.Code,
			Flags:           ledger.TransferFlags(tr.Flags),
			Timestamp:       tr.Timestamp,
		})
	}
	return trs
}

func account(a ledger.Account) state.Account {
	return state.Account{
		ID:             u128.Uint128(a.ID),
		DebitsPending:  u128.Uint128(a.DebitsPending),
		DebitsPosted:   u128.Uint128(a.DebitsPosted),
		CreditsPending: u128.Uint128(a.CreditsPending),
		CreditsPosted:  u128.Uint128(a.CreditsPosted),
		Ledger:         a.Ledger,
		Code:           a.Code,
		Flags:          state.AccountFlags(a.Flags),
		Timestamp:      a.Timestamp,
	}
}

func transfer(tr ledger.Transfer) state.Transfer {
	return state.Transfer{
		ID:              u128.Uint128(tr.ID),
		DebitAccountID:  u128.Uint128(tr.DebitAccountID),
		CreditAccountID: u128.Uint128(tr.CreditAccountID),
		Amount:          u128.Uint128(tr.Amount),
		PendingID:       u128.Uint128(tr.PendingID),
		Timeout:         tr.Timeout,
		Ledger:          tr.Ledger,
		Code:            tr.Code,
		Flags:           state.TransferFlags(tr.Flags),
		Timestamp:       tr.Timestamp,
	}
}
