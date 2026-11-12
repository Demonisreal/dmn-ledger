package check

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"math"
	"slices"

	"github.com/anishathalye/porcupine"

	"github.com/Demonisreal/dmn-ledger/internal/client"
	"github.com/Demonisreal/dmn-ledger/internal/oracle"
	"github.com/Demonisreal/dmn-ledger/internal/replica"
	"github.com/Demonisreal/dmn-ledger/internal/sim"
	"github.com/Demonisreal/dmn-ledger/internal/state"
	"github.com/Demonisreal/dmn-ledger/internal/u128"
)

func (w *watch) replay(n *replica.Node) error {
	calls, bounds := w.c.Calls(), w.c.Windows()
	window := make([]int, len(calls))
	var writes []int
	doubt := uint64(math.MaxUint64)
	k := 0
	for i, call := range calls {
		for k < len(bounds) && bounds[k] <= i {
			k++
		}
		window[i] = k
		switch {
		case call.Reply.Status == client.SessionUnknown && w.cfg.Profile == ProfileEvict:
			doubt = min(doubt, call.Start)
			continue
		case call.Reply.Status != client.OK:
			return w.fail("S8", 0, "c%d call %d (%s) ended with %s", call.Client-w.c.Nodes(), i, call.Op, call.Reply.Status)
		}
		if !call.Op.Lookup() {
			writes = append(writes, i)
		}
	}
	slices.SortFunc(writes, func(a, b int) int { return cmp.Compare(calls[a].Reply.Time, calls[b].Reply.Time) })
	if doubt != math.MaxUint64 {
		writes = settled(calls, writes, doubt)
	}

	l := oracle.New()
	starts := make([]*oracle.Ledger, len(bounds)+1)
	next, early := 0, -1
	booked := make(map[u128.Uint128]int)
	for _, i := range writes {
		call := &calls[i]
		who := call.Client - w.c.Nodes()
		if call.Reply.Time <= l.Last() {
			return w.fail("S8", 0, "c%d call %d has time %d, the ledger is already at %d", who, i, call.Reply.Time, l.Last())
		}
		for ; next <= window[i]; next++ {
			starts[next] = l.Clone()
		}
		if window[i] < next-1 && early < 0 {
			early = i
		}
		fails, err := l.Apply(stateOp(call.Op), call.Reply.Time, call.Body)
		if err != nil {
			return w.fail("S8", 0, "c%d call %d: %v", who, i, err)
		}
		if got := appendFailures(nil, fails); !bytes.Equal(got, call.Reply.Body) {
			return w.fail("S8", 0, "c%d call %d (%s at %d) got failures %x, the oracle has %x", who, i, call.Op, call.Reply.Time, call.Reply.Body, got)
		}
		if call.Op != client.CreateTransfers {
			continue
		}
		for k := range len(call.Body) / state.TransferSize {
			if slices.ContainsFunc(fails, func(f state.Failure) bool { return f.Index == k }) {
				continue
			}
			id := u128.FromBytes(call.Body[k*state.TransferSize:])
			if prev, ok := booked[id]; ok {
				return w.fail("S8", 0, "transfer %v booked by call %d and call %d", id, prev, i)
			}
			booked[id] = i
		}
	}
	if doubt != math.MaxUint64 {
		return nil
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
	lo := 0
	for k, start := range starts {
		hi := len(calls)
		if k < len(bounds) {
			hi = bounds[k]
		}
		ops := make([]porcupine.Operation, 0, hi-lo)
		for i := lo; i < hi; i++ {
			ops = append(ops, porcupine.Operation{Input: &calls[i], Call: int64(calls[i].Start), Return: int64(calls[i].End)}) //nolint:gosec
		}
		if !porcupine.CheckOperations(model(start), ops) {
			return w.fail("S9", 0, "window %d with calls %d to %d is not linearizable", k, lo, hi-1)
		}
		lo = hi
	}
	return nil
}

func settled(calls []sim.Call, writes []int, doubt uint64) []int {
	var horizon uint64
	for _, i := range writes {
		if calls[i].End < doubt {
			horizon = max(horizon, calls[i].Reply.Time)
		}
	}
	k := 0
	for k < len(writes) && calls[writes[k]].Reply.Time <= horizon {
		k++
	}
	return writes[:k]
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
			return err == nil && bytes.Equal(appendFailures(nil, fails), call.Reply.Body), next
		},
		Equal: func(a, b any) bool { return a.(*oracle.Ledger).Last() == b.(*oracle.Ledger).Last() },
		Hash:  func(st any) uint64 { return st.(*oracle.Ledger).Last() },
	}
}

func lookup(l *oracle.Ledger, call *sim.Call) []byte {
	var ids []u128.Uint128
	for b := call.Body; len(b) >= 16; b = b[16:] {
		ids = append(ids, u128.FromBytes(b))
	}
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

func appendFailures(dst []byte, fails []state.Failure) []byte {
	for _, f := range fails {
		dst = binary.BigEndian.AppendUint32(dst, uint32(f.Index)) //nolint:gosec
		dst = binary.BigEndian.AppendUint32(dst, uint32(f.Reason))
	}
	return dst
}
