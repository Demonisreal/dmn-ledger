package check

import (
	"bytes"
	"cmp"
	"slices"

	"github.com/anishathalye/porcupine"

	"github.com/Demonisreal/dmn-ledger/internal/client"
	"github.com/Demonisreal/dmn-ledger/internal/oracle"
	"github.com/Demonisreal/dmn-ledger/internal/replica"
	"github.com/Demonisreal/dmn-ledger/internal/sim"
	"github.com/Demonisreal/dmn-ledger/internal/state"
)

func (w *watch) replay(n *replica.Node) error {
	calls, bounds := w.c.Calls(), w.c.Windows()
	window := make([]int, len(calls))
	var writes []int
	unsure := false
	k := 0
	for i, call := range calls {
		for k < len(bounds) && bounds[k] <= i {
			k++
		}
		window[i] = k
		switch {
		case call.Reply.Status == client.SessionUnknown && w.cfg.Profile == sim.ProfileEvict:
			unsure = true
			continue
		case call.Reply.Status != client.OK:
			return w.fail("S8", 0, "c%d call %d (%s) ended with %s", call.Client, i, call.Op, call.Reply.Status)
		}
		if !call.Op.Lookup() {
			writes = append(writes, i)
		}
	}
	if unsure {
		return nil
	}
	slices.SortFunc(writes, func(a, b int) int { return cmp.Compare(calls[a].Reply.Time, calls[b].Reply.Time) })

	l := oracle.New()
	starts := make([]*oracle.Ledger, len(bounds)+1)
	next, early := 0, -1
	for _, i := range writes {
		call := &calls[i]
		if call.Reply.Time <= l.Last() {
			return w.fail("S8", 0, "c%d call %d has time %d, the ledger is already at %d", call.Client, i, call.Reply.Time, l.Last())
		}
		for ; next <= window[i]; next++ {
			starts[next] = l.Clone()
		}
		if window[i] < next-1 && early < 0 {
			early = i
		}
		fails, err := l.Apply(stateOp(call.Op), call.Reply.Time, call.Body)
		if err != nil {
			return w.fail("S8", 0, "c%d call %d: %v", call.Client, i, err)
		}
		if got := client.AppendFailures(nil, fails); !bytes.Equal(got, call.Reply.Body) {
			return w.fail("S8", 0, "c%d call %d (%s at %d) got failures %x, the oracle has %x", call.Client, i, call.Op, call.Reply.Time, call.Reply.Body, got)
		}
	}
	for ; next < len(starts); next++ {
		starts[next] = l.Clone()
	}
	if d, want := n.LedgerDigest(), l.Digest(); d != want {
		return w.fail("S8", 1, "ledger digest %x, the oracle has %x", d[:4], want[:4])
	}

	if early >= 0 {
		return w.fail("S9", 0, "call %d of window %d has a later time than a call of a later window", early, window[early])
	}
	return w.linearizable(calls, bounds, starts)
}

func (w *watch) linearizable(calls []sim.Call, bounds []int, starts []*oracle.Ledger) error {
	lo := 0
	for k, start := range starts {
		hi := len(calls)
		if k < len(bounds) {
			hi = bounds[k]
		}
		ops := make([]porcupine.Operation, 0, hi-lo)
		for i := lo; i < hi; i++ {
			ops = append(ops, porcupine.Operation{Input: &calls[i], Call: int64(calls[i].Start), Return: int64(calls[i].End)}) //nolint:gosec // G115: virtual time stays far below 2^63
		}
		if !porcupine.CheckOperations(model(start), ops) {
			return w.fail("S9", 0, "window %d with calls %d to %d is not linearizable", k, lo, hi-1)
		}
		lo = hi
	}
	return nil
}

func model(start *oracle.Ledger) porcupine.Model {
	return porcupine.Model{
		Init: func() any { return start },
		Step: func(st, in, _ any) (bool, any) {
			l, call := st.(*oracle.Ledger), in.(*sim.Call)
			if call.Op.Lookup() {
				return call.Reply.Time == l.Last() && bytes.Equal(lookup(l, call), call.Reply.Body), l
			}
			if call.Reply.Time <= l.Last() {
				return false, l
			}
			next := l.Clone()
			fails, err := next.Apply(stateOp(call.Op), call.Reply.Time, call.Body)
			return err == nil && bytes.Equal(client.AppendFailures(nil, fails), call.Reply.Body), next
		},
		Equal: func(a, b any) bool { return a.(*oracle.Ledger).Last() == b.(*oracle.Ledger).Last() },
		Hash:  func(st any) uint64 { return st.(*oracle.Ledger).Last() },
	}
}

func lookup(l *oracle.Ledger, call *sim.Call) []byte {
	ids := client.DecodeIDs(call.Body)
	var body []byte
	if call.Op == client.LookupAccounts {
		for _, a := range l.LookupAccounts(ids) {
			body = state.AppendAccount(body, a)
		}
		return body
	}
	for _, tr := range l.LookupTransfers(ids) {
		body = state.AppendTransfer(body, tr)
	}
	return body
}

func stateOp(op client.Op) state.Op {
	if op == client.CreateAccounts {
		return state.CreateAccounts
	}
	return state.CreateTransfers
}
