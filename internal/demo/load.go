//go:build linux

package demo

import (
	"fmt"
	"math/rand/v2"
	"strconv"
	"time"

	"github.com/Demonisreal/dmn-ledger/internal/client"
	"github.com/Demonisreal/dmn-ledger/internal/history"
	"github.com/Demonisreal/dmn-ledger/internal/sim"
	"github.com/Demonisreal/dmn-ledger/internal/state"
	"github.com/Demonisreal/dmn-ledger/internal/u128"
	"github.com/Demonisreal/dmn-ledger/ledger"
)

const (
	clients   = 4
	window    = 16
	backlog   = 4
	accounts  = 6
	spring    = 9
	funding   = 1_000_000
	want      = accounts * funding
	least     = 100
	most      = 25_000
	batchMost = 4
	recentMax = 5
)

var owned = []uint64{1, 2, 3, 4, 5, 6, spring}

type sent struct {
	debit, credit uint8
	amount        uint32
}

type mark uint8

const (
	unanswered mark = iota
	confirmed
	refused
)

type sealed struct {
	calls []sim.Call
	upto  uint64
}

func (s *Stage) drive(r *run, k int, cl *ledger.Client, rng *rand.Rand) {
	for s.pause(r, rng) {
		var first, n uint64
		t, ok := r.rec.Begin(r.ctx, uint64(k), func() (client.Op, []byte) { //nolint:gosec // G115: four clients
			body := s.deal(r, rng)
			n = uint64(len(body) / state.TransferSize)
			first = state.DecodeTransfer(body).ID.Lo
			return client.CreateTransfers, body
		})
		if !ok {
			return
		}
		s.opened(r, t)
		reply, err := s.issue(r.ctx, cl, t.Call.Op, t.Call.Body)
		if err != nil {
			if r.ctx.Err() == nil {
				s.fail(r, "call", fmt.Sprintf(failCall, k, t.N, t.Call.Op, err))
			}
			return
		}
		s.settle(r, t, first, n, reply)
		r.rec.End(t, reply)
	}
}

func (s *Stage) pause(r *run, rng *rand.Rand) bool {
	if !sleep(r.ctx, s.pace.pause*3/4+time.Duration(rng.Int64N(int64(s.pace.pause/2)))) {
		return false
	}
	s.mu.Lock()
	awake := r.awake
	s.mu.Unlock()
	select {
	case <-r.ctx.Done():
		return false
	case <-awake:
		return true
	}
}

func (s *Stage) deal(r *run, rng *rand.Rand) []byte {
	var body []byte
	s.mu.Lock()
	defer s.mu.Unlock()
	for range 1 + rng.IntN(batchMost) {
		debit := 1 + rng.IntN(accounts)
		credit := 1 + rng.IntN(accounts-1)
		if credit >= debit {
			credit++
		}
		amount := least + rng.IntN(most-least+1)
		r.sent = append(r.sent, sent{uint8(debit), uint8(credit), uint32(amount)}) //nolint:gosec // G115: accounts 1 to 6, amounts up to 25 000
		r.marks = append(r.marks, unanswered)
		body = state.AppendTransfer(body, state.Transfer{
			ID:              u128.From64(uint64(len(r.sent))),
			DebitAccountID:  u128.From64(uint64(debit)),  //nolint:gosec // G115: 1 to 6
			CreditAccountID: u128.From64(uint64(credit)), //nolint:gosec // G115: 1 to 6
			Amount:          u128.From64(uint64(amount)), //nolint:gosec // G115: 100 to 25 000
			Ledger:          1,
			Code:            1,
		})
	}
	return body
}

func (s *Stage) opened(r *run, t history.Turn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r.calls[t.N] = t.Call
	s.tally(r)
}

func (s *Stage) settle(r *run, t history.Turn, first, n uint64, reply client.Reply) {
	now := time.Now()
	failed := map[int]state.Reason{}
	for _, f := range client.DecodeFailures(reply.Body) {
		failed[f.Index] = f.Reason
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.answer(r, t, reply)
	for i := range n {
		id := first + i
		b := Booking{ID: strconv.FormatUint(id, 10), Debit: int(r.sent[id-1].debit), Credit: int(r.sent[id-1].credit), Amount: strconv.FormatUint(uint64(r.sent[id-1].amount), 10), At: now.UnixMilli()}
		if why, ok := failed[int(i)]; ok {
			r.marks[id-1] = refused
			b.Refused = why.String()
			s.st.Books.Refused++
		} else {
			r.marks[id-1] = confirmed
			r.latest = max(r.latest, id)
			if n > 1 {
				r.provable = max(r.provable, id)
			}
			s.st.Books.Confirmed++
			s.st.Totals.Confirmed++
		}
		s.st.Recent = append([]Booking{b}, s.st.Recent[:min(len(s.st.Recent), recentMax-1)]...)
	}
	if s.st.Books.Confirmed >= s.pace.limit {
		select {
		case r.limit <- struct{}{}:
		default:
		}
	}
}

func (s *Stage) answer(r *run, t history.Turn, reply client.Reply) {
	call := r.calls[t.N]
	call.End, call.Reply = r.since(), reply
	r.calls[t.N] = call
	s.tally(r)
}

func (s *Stage) fund(r *run) error {
	var accts, trs []byte
	for _, id := range owned {
		a := state.Account{ID: u128.From64(id), Ledger: 1, Code: 1}
		if id != spring {
			a.Flags = state.DebitsMustNotExceedCredits
			trs = state.AppendTransfer(trs, state.Transfer{
				ID:              u128.Uint128{Hi: 1, Lo: id},
				DebitAccountID:  u128.From64(spring),
				CreditAccountID: u128.From64(id),
				Amount:          u128.From64(funding),
				Ledger:          1,
				Code:            1,
			})
		}
		accts = state.AppendAccount(accts, a)
	}
	for _, batch := range []struct {
		op   client.Op
		body []byte
	}{{client.CreateAccounts, accts}, {client.CreateTransfers, trs}} {
		t, ok := r.rec.Begin(r.ctx, clients+1, func() (client.Op, []byte) { return batch.op, batch.body })
		if !ok {
			return r.ctx.Err()
		}
		s.opened(r, t)
		reply, err := s.issue(r.ctx, r.teller, batch.op, batch.body)
		if err != nil {
			return err
		}
		s.mu.Lock()
		s.answer(r, t, reply)
		s.mu.Unlock()
		r.rec.End(t, reply)
		if fails := client.DecodeFailures(reply.Body); len(fails) > 0 {
			return fmt.Errorf("demo: %s for the funding failed with %v", batch.op, fails)
		}
	}
	return nil
}

func (s *Stage) tally(r *run) {
	s.st.Books.Open = 0
	for _, call := range r.calls {
		if call.End == 0 {
			s.st.Books.Open++
		}
	}
	s.st.Rev++
}

func (s *Stage) sink(r *run, calls []sim.Call) {
	var upto uint64
	for _, call := range calls {
		if call.Op != client.CreateTransfers {
			continue
		}
		for body := call.Body; len(body) > 0; body = body[state.TransferSize:] {
			if id := state.DecodeTransfer(body).ID; id.Hi == 0 {
				upto = max(upto, id.Lo)
			}
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r.sunk += len(calls)
	for n := range r.calls {
		if n < r.sunk {
			delete(r.calls, n)
		}
	}
	r.sealed = append(r.sealed, sealed{calls, upto})
}
