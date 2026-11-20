//go:build linux

package demo

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/Demonisreal/dmn-ledger/internal/client"
	"github.com/Demonisreal/dmn-ledger/internal/history"
	"github.com/Demonisreal/dmn-ledger/internal/sim"
	"github.com/Demonisreal/dmn-ledger/internal/state"
	"github.com/Demonisreal/dmn-ledger/internal/u128"
	"github.com/Demonisreal/dmn-ledger/ledger"
)

const (
	holdWithin  = 15 * time.Second
	agreeWithin = 10 * time.Second
	askWithin   = 5 * time.Second
	agreeEvery  = 50 * time.Millisecond
	perLookup   = client.MaxBody / state.TransferSize
)

func (s *Stage) count(r *run, full bool, after Kind) bool {
	s.change(func(st *State) { st.Load = "held" })
	defer s.change(func(st *State) {
		switch {
		case r.found != nil || r.last:
		case s.resting:
			st.Load = "resting"
		default:
			st.Load = "running"
		}
	})
	if err := r.rec.Hold(r.ctx, holdWithin); err != nil {
		if errors.Is(err, history.ErrOpen) {
			s.mu.Lock()
			open := s.st.Books.Open
			s.mu.Unlock()
			why := fmt.Sprintf(failOpen, open, holdWithin)
			if open == 1 {
				why = fmt.Sprintf(failOpenOne, holdWithin)
			}
			s.fail(r, "L1", why)
		}
		return false
	}
	if !r.last {
		defer r.rec.Resume()
	}
	var sts []*ledger.NodeStatus
	if full {
		var ok bool
		if sts, ok = s.agree(r); !ok {
			return false
		}
	}
	ctx, cancel := context.WithTimeout(r.ctx, askWithin)
	defer cancel()
	accts, trs, calls, err := s.ask(ctx, r, full)
	if err != nil {
		if r.ctx.Err() == nil {
			s.fail(r, "call", fmt.Sprintf(failAsk, err))
		}
		return false
	}
	sum, why := balance(accts)
	var rc Recount
	if full && why == "" {
		rc, why = s.compare(r, trs)
	}
	if why != "" {
		s.fail(r, "books", why)
		return false
	}
	r.rec.Push(calls)
	now := time.Now()
	s.change(func(st *State) {
		st.Books.Sum = sum
		st.Books.Sum.At = now.UnixMilli()
	})
	if !full {
		s.say("check", fmt.Sprintf(sayLight, money(sum.Value)))
		return true
	}
	rc.At, rc.After = now.UnixMilli(), after
	rc.Index, rc.Hash = sts[0].Applied, hex.EncodeToString(sts[0].ApplyHash[:8])
	s.change(func(st *State) { st.Books.Recount = rc })
	line := fmt.Sprintf(sayFull, rc.Index, rc.Hash, num(rc.Confirmed), num(rc.Found), num(rc.Refused), num(rc.Strays), money(sum.Value))
	if after != 0 {
		line = sayAfter + strings.TrimPrefix(line, "Zählung: ")
	}
	s.say("check", line)
	return true
}

func (s *Stage) agree(r *run) ([]*ledger.NodeStatus, bool) {
	deadline := time.Now().Add(agreeWithin)
	for {
		ctx, cancel := context.WithTimeout(r.ctx, sightWithin)
		sts := r.c.Statuses(ctx)
		cancel()
		why := s.apart(r, sts)
		switch {
		case r.ctx.Err() != nil:
			return nil, false
		case why == "":
			return sts, s.keyed(r, sts)
		case time.Now().After(deadline):
			s.fail(r, "L2", fmt.Sprintf(failApart, agreeWithin, why))
			return nil, false
		case !sleep(r.ctx, agreeEvery):
			return nil, false
		}
	}
}

func (s *Stage) apart(r *run, sts []*ledger.NodeStatus) string {
	var rows []string
	leaders, agreed := 0, true
	for i, st := range sts {
		if st == nil {
			rows = append(rows, fmt.Sprintf("n%d %s", i+1, silent))
			agreed = false
			continue
		}
		if st.Repair {
			s.fail(r, "L4", fmt.Sprintf(failRepair, st.ID, st.Term))
		}
		for _, o := range sts[:i] {
			if o != nil && o.Applied == st.Applied && o.ApplyHash != st.ApplyHash {
				s.fail(r, "S7", fmt.Sprintf(failHashes, o.ID, st.ID, st.Applied, o.ApplyHash[:8], st.ApplyHash[:8]))
			}
		}
		if st.Role == ledger.Leader {
			leaders++
		}
		first := sts[0]
		if first == nil || st.Commit != first.Commit || st.Applied != st.Commit || st.Applied != first.Applied || st.HeadSeq != first.HeadSeq || st.Head != first.Head {
			agreed = false
		}
		rows = append(rows, fmt.Sprintf(failNode, st.ID, standing(st), st.Commit, st.Applied, st.HeadSeq, st.Head[:8]))
	}
	if agreed && leaders == 1 {
		return ""
	}
	return fmt.Sprintf(failLeaders, leaders, strings.Join(rows, "; "))
}

func (s *Stage) keyed(r *run, sts []*ledger.NodeStatus) bool {
	keys := r.c.Keys()
	for i, st := range sts {
		if !bytes.Equal(st.Key, keys[i]) {
			s.fail(r, "key", fmt.Sprintf(failKey, st.ID, st.Key, keys[i]))
			return false
		}
	}
	return true
}

func (s *Stage) ask(ctx context.Context, r *run, full bool) ([]state.Account, []state.Transfer, []sim.Call, error) {
	var accts []state.Account
	var trs []state.Transfer
	var calls []sim.Call
	issue := func(op client.Op, ids []u128.Uint128) error {
		call := sim.Call{Client: clients + 1, Op: op, Body: client.AppendIDs(nil, ids), Start: r.since()}
		reply, err := s.issue(ctx, r.teller, op, call.Body)
		if err != nil {
			return err
		}
		call.End, call.Reply = r.since(), reply
		calls = append(calls, call)
		for rec := reply.Body; op == client.LookupAccounts && len(rec) > 0; rec = rec[state.AccountSize:] {
			accts = append(accts, state.DecodeAccount(rec))
		}
		for rec := reply.Body; op == client.LookupTransfers && len(rec) > 0; rec = rec[state.TransferSize:] {
			trs = append(trs, state.DecodeTransfer(rec))
		}
		return nil
	}
	var ids []u128.Uint128
	for _, id := range owned {
		ids = append(ids, u128.From64(id))
	}
	if err := issue(client.LookupAccounts, ids); err != nil || !full {
		return accts, trs, calls, err
	}
	s.mu.Lock()
	sent := uint64(len(r.sent))
	s.mu.Unlock()
	for from := uint64(1); from <= sent; from += perLookup {
		ids = ids[:0]
		for id := from; id <= min(sent, from+perLookup-1); id++ {
			ids = append(ids, u128.From64(id))
		}
		if err := issue(client.LookupTransfers, ids); err != nil {
			return accts, trs, calls, err
		}
	}
	return accts, trs, calls, nil
}

func balance(accts []state.Account) (Sum, string) {
	sum := Sum{Want: fmt.Sprint(want)}
	if len(accts) != accounts+1 {
		return sum, fmt.Sprintf(failAccounts, len(accts))
	}
	total, debits, credits := new(big.Int), new(big.Int), new(big.Int)
	for i, a := range accts {
		if a.ID != u128.From64(owned[i]) {
			return sum, fmt.Sprintf(failAccounts, i)
		}
		if !a.DebitsPending.IsZero() || !a.CreditsPending.IsZero() {
			return sum, fmt.Sprintf(failPending, a.ID, a.DebitsPending, a.CreditsPending)
		}
		debits.Add(debits, wide(a.DebitsPosted))
		credits.Add(credits, wide(a.CreditsPosted))
		if i < accounts {
			bal := new(big.Int).Sub(wide(a.CreditsPosted), wide(a.DebitsPosted))
			total.Add(total, bal)
			sum.Accounts[i] = Account{ID: int(a.ID.Lo), Balance: bal.String()} //nolint:gosec // G115: accounts 1 to 6
		}
	}
	sum.Value = total.String()
	switch spent := accts[accounts].DebitsPosted; {
	case sum.Value != sum.Want:
		return sum, fmt.Sprintf(failSum, money(sum.Value), money(sum.Want))
	case spent != u128.From64(want):
		return sum, fmt.Sprintf(failSpent, money(spent.String()), money(sum.Want))
	case debits.Cmp(credits) != 0:
		return sum, fmt.Sprintf(failSides, money(debits.String()), money(credits.String()))
	}
	return sum, ""
}

func wide(u u128.Uint128) *big.Int {
	return new(big.Int).SetBytes(u.AppendBytes(nil))
}

func (s *Stage) compare(r *run, trs []state.Transfer) (Recount, string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var rc Recount
	found := make([]bool, len(r.marks))
	var stray *u128.Uint128
	for _, tr := range trs {
		rc.Found++
		id := tr.ID
		if id.Hi == 0 && id.Lo >= 1 && id.Lo <= uint64(len(r.marks)) && r.marks[id.Lo-1] == confirmed && !found[id.Lo-1] {
			found[id.Lo-1] = true
			continue
		}
		if id.Hi == 0 && id.Lo >= 1 && id.Lo <= uint64(len(r.marks)) && r.marks[id.Lo-1] == refused {
			rc.Strays++
		}
		if stray == nil {
			stray = &id
		}
	}
	missing, first := 0, 0
	for i, m := range r.marks {
		switch m {
		case confirmed:
			rc.Confirmed++
			if !found[i] {
				missing++
				if first == 0 {
					first = i + 1
				}
			}
		case refused:
			rc.Refused++
		}
	}
	extra := len(trs) - rc.Confirmed
	switch {
	case missing == 1:
		return rc, fmt.Sprintf(failMissingOne, first)
	case missing > 0:
		return rc, fmt.Sprintf(failMissing, missing, first)
	case stray != nil && extra == 1:
		return rc, fmt.Sprintf(failStraysOne, *stray)
	case stray != nil:
		return rc, fmt.Sprintf(failStrays, extra, *stray)
	}
	return rc, ""
}
