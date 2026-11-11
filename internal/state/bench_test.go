package state_test

import (
	"fmt"
	"testing"

	"github.com/Demonisreal/dmn-ledger/internal/state"
	"github.com/Demonisreal/dmn-ledger/internal/u128"
)

func BenchmarkApply(b *testing.B) {
	const accounts = 1000
	for _, size := range []int{1, 128, 8000} {
		b.Run(fmt.Sprintf("batch=%d", size), func(b *testing.B) {
			m := state.New()
			var as []byte
			for i := range uint64(accounts) {
				as = state.AppendAccount(as, state.Account{ID: n(i + 1), Ledger: 1, Code: 1})
			}
			if fails, err := m.Apply(state.CreateAccounts, 1, as); err != nil || fails != nil {
				b.Fatal(fails, err)
			}

			var body []byte
			for i := range uint64(size) {
				body = state.AppendTransfer(body, state.Transfer{
					DebitAccountID:  n(i*7%accounts + 1),
					CreditAccountID: n((i*7+1)%accounts + 1),
					Amount:          n(1 + i%100),
					Ledger:          1,
					Code:            1,
				})
			}

			id, ts := uint64(accounts), uint64(1e9)
			for b.Loop() {
				for i := range size {
					id++
					u128.From64(id).PutBytes(body[i*state.TransferSize:])
				}
				if fails, _ := m.Apply(state.CreateTransfers, ts, body); fails != nil {
					b.Fatal(fails[0])
				}
				ts += 1e6
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*size), "ns/transfer")
		})
	}
}
