package check

import (
	"bytes"
	"cmp"
	"errors"
	"fmt"
	"slices"

	"github.com/anishathalye/porcupine"

	"github.com/Demonisreal/dmn-ledger/internal/client"
	"github.com/Demonisreal/dmn-ledger/internal/oracle"
	"github.com/Demonisreal/dmn-ledger/internal/replica"
	"github.com/Demonisreal/dmn-ledger/internal/sim"
	"github.com/Demonisreal/dmn-ledger/internal/state"
)

type Replayer struct {
	ledger  *oracle.Ledger
	calls   int
	windows int
	wrote   uint64
	spent   bool
	timed   bool
}

func NewReplayer() *Replayer {
	return &Replayer{ledger: oracle.New()}
}

func (r *Replayer) Ledger() *oracle.Ledger {
	return r.ledger
}

func (r *Replayer) Window(calls []sim.Call) error {
	first, k := r.calls, r.windows
	r.calls += len(calls)
	r.windows++
	var writes []int
	for i, call := range calls {
		if call.Reply.Status != client.OK {
			return breach("S8", "c%d call %d (%s) ended with %s", call.Client, first+i, call.Op, call.Reply.Status)
		}
		if !call.Op.Lookup() {
			writes = append(writes, i)
		}
	}
	slices.SortFunc(writes, func(a, b int) int { return cmp.Compare(calls[a].Reply.Time, calls[b].Reply.Time) })

	start := r.ledger.Clone()
	for _, i := range writes {
		call := &calls[i]
		switch {
		case call.Reply.Time < r.wrote:
			r.spent = true
			return breach("S9", "c%d call %d has time %d, a call of an earlier window has %d", call.Client, first+i, call.Reply.Time, r.wrote)
		case call.Reply.Time <= r.ledger.Last():
			return breach("S8", "c%d call %d has time %d, the ledger is already at %d", call.Client, first+i, call.Reply.Time, r.ledger.Last())
		}
		fails, err := r.ledger.Apply(stateOp(call.Op), call.Reply.Time, call.Body)
		if err != nil {
			return breach("S8", "c%d call %d: %v", call.Client, first+i, err)
		}
		if got := client.AppendFailures(nil, fails); !bytes.Equal(got, call.Reply.Body) {
			return breach("S8", "c%d call %d (%s at %d) got failures %x, the oracle has %x", call.Client, first+i, call.Op, call.Reply.Time, call.Reply.Body, got)
		}
		r.wrote = call.Reply.Time
	}

	ops := make([]porcupine.Operation, 0, len(calls))
	for i := range calls {
		ops = append(ops, porcupine.Operation{Input: &calls[i], Call: int64(calls[i].Start), Return: int64(calls[i].End)}) //nolint:gosec // G115: virtual time stays far below 2^63
	}
	if !porcupine.CheckOperations(model(start, r.timed), ops) {
		return breach("S9", "window %d with calls %d to %d is not linearizable", k, first, first+len(calls)-1)
	}
	return nil
}

func breach(check, format string, args ...any) error {
	return &Violation{Check: check, Reason: fmt.Sprintf(format, args...)}
}

func (w *watch) replay(n *replica.Node) error {
	calls := w.c.Calls()
	unsure := false
	for i, call := range calls {
		switch {
		case call.Reply.Status == client.SessionUnknown && w.cfg.Profile == sim.ProfileEvict:
			unsure = true
		case call.Reply.Status != client.OK:
			return w.fail("S8", 0, "c%d call %d (%s) ended with %s", call.Client, i, call.Op, call.Reply.Status)
		}
	}
	if unsure {
		return nil
	}

	r := NewReplayer()
	r.timed = true
	var late *Violation
	lo := 0
	for _, hi := range append(w.c.Windows(), len(calls)) {
		v, _ := errors.AsType[*Violation](r.Window(calls[lo:hi]))
		lo = hi
		if v != nil && v.Check == "S8" {
			return w.fail("S8", 0, "%s", v.Reason)
		}
		if late == nil {
			late = v
		}
		if r.spent {
			return w.fail("S9", 0, "%s", late.Reason)
		}
	}
	if d, want := n.LedgerDigest(), r.Ledger().Digest(); d != want {
		return w.fail("S8", 1, "ledger digest %x, the oracle has %x", d[:4], want[:4])
	}
	if late != nil {
		return w.fail("S9", 0, "%s", late.Reason)
	}
	return nil
}

func model(start *oracle.Ledger, timed bool) porcupine.Model {
	return porcupine.Model{
		Init: func() any { return start },
		Step: func(st, in, _ any) (bool, any) {
			l, call := st.(*oracle.Ledger), in.(*sim.Call)
			if call.Op.Lookup() {
				return (!timed && call.Reply.Time == 0 || call.Reply.Time == l.Last()) && bytes.Equal(lookup(l, call), call.Reply.Body), l
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
