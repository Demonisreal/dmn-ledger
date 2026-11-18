//go:build chaos && linux

package chaos

import (
	"errors"
	"fmt"
	"maps"
	"math/big"
	"math/rand/v2"
	"slices"
	"time"

	"github.com/Demonisreal/dmn-ledger/internal/check"
	"github.com/Demonisreal/dmn-ledger/internal/client"
	"github.com/Demonisreal/dmn-ledger/internal/history"
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

func (h *harness) drive(k int, cl *ledger.Client, rng *rand.Rand) {
	for {
		var batch workload.Batch
		tr, ok := h.rec.Begin(h.ctx, uint64(k), func() (op client.Op, body []byte) { //nolint:gosec // G115: four clients
			op, body, batch = h.pick(rng)
			return op, body
		})
		if !ok {
			return
		}
		h.mu.Lock()
		h.open[tr.N] = tr.Call.Start
		h.mu.Unlock()
		reply, err := history.Issue(h.ctx, cl, tr.Call.Op, tr.Call.Body)
		if err != nil {
			h.fail("call", "c%d call %d (%s) returned %v", k, tr.N, tr.Call.Op, err)
			return
		}
		h.finish(tr, batch, reply)
	}
}

func (h *harness) pick(rng *rand.Rand) (client.Op, []byte, workload.Batch) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if rng.IntN(lookupOdds) == 0 {
		accounts, ids := h.gen.Lookup()
		op := client.LookupTransfers
		if accounts {
			op = client.LookupAccounts
		}
		return op, client.AppendIDs(nil, ids), workload.Batch{}
	}
	batch := h.gen.Batch()
	op, size := client.CreateTransfers, state.TransferSize
	if batch.Op == state.CreateAccounts {
		op, size = client.CreateAccounts, state.AccountSize
	}
	h.learn(batch)
	h.submitted += uint64(max(1, len(batch.Body)/size)) //nolint:gosec // G115: a count
	return op, batch.Body, batch
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

func (h *harness) finish(tr history.Turn, batch workload.Batch, reply client.Reply) {
	wall := uint64(time.Now().UnixNano()) //nolint:gosec // G115: the clock is after 1970
	write := !tr.Call.Op.Lookup()
	h.mu.Lock()
	delete(h.open, tr.N)
	if write {
		h.gen.Settle(batch, client.DecodeFailures(reply.Body))
	}
	submitted := h.submitted
	h.mu.Unlock()
	h.rec.End(tr, reply)
	if write && reply.Time > wall+submitted {
		h.fail("clock", "c%d call %d has time %d, the wall clock at its end was %d and %d records were sent", tr.Call.Client, tr.N, reply.Time, wall, submitted)
	}
}

func (h *harness) sink(calls []sim.Call) error {
	err := h.history.Encode(calls)
	if err != nil {
		h.fail("history", "%v", err)
	}
	return err
}

func (h *harness) replay() {
	defer close(h.checked)
	err := h.rec.Judge(h.ctx)
	if v, ok := errors.AsType[*check.Violation](err); ok {
		h.fail(v.Check, "%s", v.Reason)
	} else if err != nil {
		h.fail("replay", "%v", err)
	}
}

func (h *harness) idle(within time.Duration) bool {
	err := h.rec.Hold(h.ctx, within)
	if errors.Is(err, history.ErrOpen) {
		h.mu.Lock()
		open := len(h.open)
		h.mu.Unlock()
		h.fail("L1", "%d calls still open %v after the load was held", open, within)
	}
	return err == nil
}

func (h *harness) lookups(cl *ledger.Client) ([]state.Account, bool) {
	var accts []state.Account
	var calls []sim.Call
	ask := func(op client.Op, ids map[u128.Uint128]bool, per int) bool {
		sorted := slices.SortedFunc(maps.Keys(ids), u128.Uint128.Cmp)
		for chunk := range slices.Chunk(sorted, per) {
			call := sim.Call{Client: 1, Op: op, Body: client.AppendIDs(nil, chunk), Start: h.since()}
			reply, err := history.Issue(h.ctx, cl, op, call.Body)
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
				h.rec.Push(calls)
				calls = nil
			}
		}
		return true
	}
	ok := ask(client.LookupAccounts, h.accountIDs, client.MaxBody/state.AccountSize) &&
		ask(client.LookupTransfers, h.transferIDs, client.MaxBody/state.TransferSize)
	h.rec.Push(calls)
	return accts, ok
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
