package state_test

import (
	"fmt"
	"testing"

	"github.com/Demonisreal/dmn-ledger/internal/state"
	"github.com/Demonisreal/dmn-ledger/internal/u128"
)

func BenchmarkApply(b *testing.B) {
	const accounts, existing = 1000, 100_000
	for _, size := range []int{1, 128, 8000} {
		b.Run(fmt.Sprintf("batch=%d", size), func(b *testing.B) {
			b.ReportAllocs()
			m := state.New(accounts, existing+(b.N+1)*size)
			var as []byte
			for i := range uint64(accounts) {
				as = state.AppendAccount(as, state.Account{ID: id128(i + 1), Ledger: 1, Code: 1})
			}
			if fails, err := m.Apply(state.CreateAccounts, 1, as, nil); err != nil || len(fails) > 0 {
				b.Fatal(fails, err)
			}

			var body []byte
			for i := range uint64(size) {
				body = state.AppendTransfer(body, state.Transfer{
					DebitAccountID:  id128(i*7%accounts + 1),
					CreditAccountID: id128((i*7+1)%accounts + 1),
					Amount:          id128(1 + i%100),
					Ledger:          1,
					Code:            1,
				})
			}
			id, ts := uint64(accounts), uint64(1e9)
			var fails []state.Failure
			apply := func() {
				for i := range size {
					id++
					u128.From64(id).PutBytes(body[i*state.TransferSize:])
				}
				var err error
				if fails, err = m.Apply(state.CreateTransfers, ts, body, fails[:0]); err != nil || len(fails) > 0 {
					b.Fatal(fails, err)
				}
				ts += 1e6
			}
			for range (existing + size - 1) / size {
				apply()
			}

			b.ResetTimer()
			for range b.N {
				apply()
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*size), "ns/transfer")
		})
	}
}
