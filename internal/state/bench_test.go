package state_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/Demonisreal/dmn-ledger/internal/state"
)

const (
	benchAccounts = 1000
	benchExisting = 100_000
	benchWindow   = 1 << 18
)

type benchLedger struct {
	m       *state.Machine
	id, ts  uint64
	applied int
	fails   []state.Failure
}

func newBenchLedger(b *testing.B) *benchLedger {
	b.Helper()
	l := &benchLedger{m: state.New(benchAccounts, benchExisting+benchWindow), id: benchAccounts, ts: 1e9}
	var as []byte
	for i := range uint64(benchAccounts) {
		as = state.AppendAccount(as, state.Account{ID: id128(i + 1), Ledger: 1, Code: 1})
	}
	if fails, err := l.m.Apply(state.CreateAccounts, 1, as, nil); err != nil || len(fails) > 0 {
		b.Fatal(fails, err)
	}
	trs := benchTransfers(1000, 0)
	for range benchExisting / len(trs) {
		l.apply(b, trs, encodeTransfers(trs...), 0)
	}
	l.applied = 0
	return l
}

func benchTransfers(n int, flags state.TransferFlags) []state.Transfer {
	trs := make([]state.Transfer, n)
	for i := range trs {
		trs[i] = state.Transfer{
			DebitAccountID:  id128(uint64(i*7%benchAccounts + 1)),
			CreditAccountID: id128(uint64((i*7+1)%benchAccounts + 1)),
			Amount:          id128(uint64(1 + i%100)),
			Ledger:          1,
			Code:            1,
			Flags:           flags,
		}
	}
	return trs
}

func (l *benchLedger) apply(b *testing.B, trs []state.Transfer, body []byte, wantFails int) {
	for i := range trs {
		l.id++
		id128(l.id).PutBytes(body[i*state.TransferSize:])
		if trs[i].Flags&state.PostPending != 0 {
			id128(l.id - 1).PutBytes(body[i*state.TransferSize+64:])
		}
	}
	var err error
	if l.fails, err = l.m.Apply(state.CreateTransfers, l.ts, body, l.fails[:0]); err != nil || len(l.fails) != wantFails {
		b.Fatal(l.fails, err)
	}
	l.ts += 1e6
	l.applied += len(trs)
}

func benchApply(b *testing.B, trs []state.Transfer, wantFails int) {
	b.ReportAllocs()
	body := encodeTransfers(trs...)
	l := newBenchLedger(b)
	b.ResetTimer()
	for range b.N {
		if l.applied+len(trs) > benchWindow {
			b.StopTimer()
			l = newBenchLedger(b)
			b.StartTimer()
		}
		l.apply(b, trs, body, wantFails)
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*len(trs)), "ns/transfer")
}

func BenchmarkApply(b *testing.B) {
	for _, size := range []int{1, 128, 8000} {
		b.Run(fmt.Sprintf("batch=%d", size), func(b *testing.B) {
			benchApply(b, benchTransfers(size, 0), 0)
		})
	}
}

func BenchmarkApplyPendingPost(b *testing.B) {
	trs := benchTransfers(128, state.Pending)
	for i := 1; i < len(trs); i += 2 {
		trs[i] = state.Transfer{Flags: state.PostPending}
	}
	for i := 0; i < len(trs); i += 4 {
		trs[i].Timeout = 60
	}
	benchApply(b, trs, 0)
}

func BenchmarkApplyFailedChain(b *testing.B) {
	trs := benchTransfers(128, state.Linked)
	trs[len(trs)-1].Flags, trs[len(trs)-1].Amount = 0, id128(0)
	benchApply(b, trs, len(trs))
}

func BenchmarkProve(b *testing.B) {
	for _, size := range []int{1, 128, 8000} {
		b.Run(fmt.Sprintf("batch=%d", size), func(b *testing.B) {
			b.ReportAllocs()
			l := newBenchLedger(b)
			trs := benchTransfers(size, 0)
			l.apply(b, trs, encodeTransfers(trs...), 0)
			id := id128(l.id - uint64(size)/2)
			b.ResetTimer()
			for range b.N {
				if _, ok := l.m.ProveTransfer(id); !ok {
					b.Fatal("no receipt")
				}
			}
		})
	}
}

func BenchmarkDigest(b *testing.B) {
	b.ReportAllocs()
	l := newBenchLedger(b)
	l.m.Digest()
	b.ResetTimer()
	for range b.N {
		l.m.Digest()
	}
}

func BenchmarkSnapshot(b *testing.B) {
	l := newBenchLedger(b)
	snap := snapshot(b, l.m)
	b.Run("write", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(snap)))
		var buf bytes.Buffer
		buf.Grow(len(snap))
		for range b.N {
			buf.Reset()
			if err := l.m.WriteSnapshot(&buf); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("read", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(snap)))
		for range b.N {
			if _, err := state.ReadSnapshot(bytes.NewReader(snap)); err != nil {
				b.Fatal(err)
			}
		}
	})
}
