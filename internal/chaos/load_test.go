//go:build chaos && linux

package chaos

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"math/big"
	"math/rand/v2"
	"slices"
	"time"

	"github.com/Demonisreal/dmn-ledger/internal/check"
	"github.com/Demonisreal/dmn-ledger/internal/client"
	"github.com/Demonisreal/dmn-ledger/internal/sim"
	"github.com/Demonisreal/dmn-ledger/internal/state"
	"github.com/Demonisreal/dmn-ledger/internal/u128"
	"github.com/Demonisreal/dmn-ledger/internal/workload"
	"github.com/Demonisreal/dmn-ledger/ledger"
)

const (
	windowSize = 32
	lookupOdds = 5
	backlog    = 8
)

type turn struct {
	i, n  int
	call  sim.Call
	batch workload.Batch
}

func (h *harness) drive(k int, cl *ledger.Client, rng *rand.Rand) {
	for {
		tr, ok := h.admit(k, rng)
		if !ok {
			return
		}
		reply, err := issue(cl, tr.call.Op, tr.call.Body)
		if err != nil {
			h.fail("call", "c%d call %d (%s) returned %v", k, tr.n, tr.call.Op, err)
			return
		}
		h.finish(tr, reply)
	}
}

func (h *harness) admit(k int, rng *rand.Rand) (turn, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for h.ctx.Err() == nil && (h.hold || len(h.pending) >= backlog || h.started == windowSize && len(h.open) > 0) {
		h.cond.Wait()
	}
	if h.ctx.Err() != nil {
		return turn{}, false
	}
	if h.started == windowSize {
		h.flush()
	}
	h.started++

	tr := turn{i: len(h.calls), n: h.first + len(h.calls)}
	tr.call = sim.Call{Client: uint64(k), Start: h.since()} //nolint:gosec // G115: four clients
	if rng.IntN(lookupOdds) == 0 {
		accounts, ids := h.gen.Lookup()
		tr.call.Op = client.LookupTransfers
		if accounts {
			tr.call.Op = client.LookupAccounts
		}
		tr.call.Body = client.AppendIDs(nil, ids)
	} else {
		tr.batch = h.gen.Batch()
		tr.call.Op, tr.call.Body = client.CreateTransfers, tr.batch.Body
		size := state.TransferSize
		if tr.batch.Op == state.CreateAccounts {
			tr.call.Op, size = client.CreateAccounts, state.AccountSize
		}
		h.learn(tr.batch)
		h.submitted += uint64(max(1, len(tr.batch.Body)/size)) //nolint:gosec // G115: a count
	}
	h.calls = append(h.calls, tr.call)
	h.open[tr.i] = tr.call.Start
	return tr, true
}

func (h *harness) learn(b workload.Batch) {
	if b.Op == state.CreateAccounts {
		for _, id := range b.IDs {
			h.accountIDs[id] = true
		}
		return
	}
	for k := 0; k < len(b.IDs); k += 4 {
		h.transferIDs[b.IDs[k]] = true
		h.accountIDs[b.IDs[k+1]] = true
		h.accountIDs[b.IDs[k+2]] = true
		h.transferIDs[b.IDs[k+3]] = true
	}
}

func (h *harness) finish(tr turn, reply client.Reply) {
	wall := uint64(time.Now().UnixNano()) //nolint:gosec // G115: the clock is after 1970
	end := h.since()
	h.mu.Lock()
	defer h.mu.Unlock()
	h.calls[tr.i].End, h.calls[tr.i].Reply = end, reply
	delete(h.open, tr.i)
	h.cond.Broadcast()
	if tr.call.Op.Lookup() {
		return
	}
	h.gen.Settle(tr.batch, client.DecodeFailures(reply.Body))
	if reply.Time > wall+h.submitted {
		h.fail("clock", "c%d call %d has time %d, the wall clock at its end was %d and %d records were sent", tr.call.Client, tr.n, reply.Time, wall, h.submitted)
	}
}

func (h *harness) flush() {
	h.seal(h.calls)
	h.calls, h.started = nil, 0
}

func (h *harness) seal(calls []sim.Call) {
	h.first += len(calls)
	h.cond.Broadcast()
	if len(calls) == 0 {
		return
	}
	h.pending = append(h.pending, calls)
	h.windows++
	if err := h.history.Encode(calls); err != nil {
		h.fail("history", "%v", err)
	}
}

func (h *harness) idle(within time.Duration) bool {
	deadline := time.Now().Add(within)
	h.mu.Lock()
	defer h.mu.Unlock()
	h.hold = true
	for len(h.open) > 0 {
		if h.ctx.Err() != nil {
			return false
		}
		if time.Now().After(deadline) {
			h.fail("L1", "%d calls still open %v after the load was held", len(h.open), within)
			return false
		}
		h.mu.Unlock()
		time.Sleep(20 * time.Millisecond)
		h.mu.Lock()
	}
	h.flush()
	return true
}

func (h *harness) resume() {
	h.mu.Lock()
	h.hold = false
	h.cond.Broadcast()
	h.mu.Unlock()
}

func (h *harness) replay(r *check.Replayer) {
	defer close(h.checked)
	for {
		h.mu.Lock()
		for len(h.pending) == 0 && !h.closed && h.ctx.Err() == nil {
			h.cond.Wait()
		}
		if len(h.pending) == 0 || h.ctx.Err() != nil {
			h.mu.Unlock()
			return
		}
		calls := h.pending[0]
		h.pending = h.pending[1:]
		h.cond.Broadcast()
		h.mu.Unlock()

		if err := r.Window(calls); err != nil {
			if v, ok := errors.AsType[*check.Violation](err); ok {
				h.fail(v.Check, "%s", v.Reason)
			} else {
				h.fail("replay", "%v", err)
			}
			return
		}
	}
}

func issue(cl *ledger.Client, op client.Op, body []byte) (client.Reply, error) {
	ctx := context.Background()
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

func (h *harness) lookups(cl *ledger.Client) ([]state.Account, bool) {
	var accts []state.Account
	var calls []sim.Call
	ask := func(op client.Op, ids map[u128.Uint128]bool, per int) bool {
		sorted := slices.SortedFunc(maps.Keys(ids), u128.Uint128.Cmp)
		for chunk := range slices.Chunk(sorted, per) {
			call := sim.Call{Client: 1, Op: op, Body: client.AppendIDs(nil, chunk), Start: h.since()}
			reply, err := issue(cl, op, call.Body)
			if err != nil {
				h.fail("call", "lookup at the end returned %v", err)
				return false
			}
			call.End, call.Reply = h.since(), reply
			calls = append(calls, call)
			for rec := reply.Body; op == client.LookupAccounts && len(rec) > 0; rec = rec[state.AccountSize:] {
				accts = append(accts, state.DecodeAccount(rec))
			}
			if len(calls) == windowSize {
				h.push(calls)
				calls = nil
			}
		}
		return true
	}
	ok := ask(client.LookupAccounts, h.accountIDs, client.MaxBody/state.AccountSize) &&
		ask(client.LookupTransfers, h.transferIDs, client.MaxBody/state.TransferSize)
	h.push(calls)
	return accts, ok
}

func (h *harness) push(calls []sim.Call) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.seal(calls)
}

func balance(accts []state.Account) error {
	type sides struct{ debitsPending, debitsPosted, creditsPending, creditsPosted big.Int }
	ledgers := map[uint32]*sides{}
	add := func(sum *big.Int, u u128.Uint128) {
		sum.Add(sum, new(big.Int).SetBytes(u.AppendBytes(nil)))
	}
	for _, a := range accts {
		s := ledgers[a.Ledger]
		if s == nil {
			s = &sides{}
			ledgers[a.Ledger] = s
		}
		add(&s.debitsPending, a.DebitsPending)
		add(&s.debitsPosted, a.DebitsPosted)
		add(&s.creditsPending, a.CreditsPending)
		add(&s.creditsPosted, a.CreditsPosted)
	}
	for _, id := range slices.Sorted(maps.Keys(ledgers)) {
		s := ledgers[id]
		if s.debitsPosted.Cmp(&s.creditsPosted) != 0 {
			return fmt.Errorf("ledger %d has %v posted debits and %v posted credits", id, &s.debitsPosted, &s.creditsPosted)
		}
		if s.debitsPending.Cmp(&s.creditsPending) != 0 {
			return fmt.Errorf("ledger %d has %v pending debits and %v pending credits", id, &s.debitsPending, &s.creditsPending)
		}
	}
	return nil
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
