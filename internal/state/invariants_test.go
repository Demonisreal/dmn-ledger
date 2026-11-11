package state

import (
	"strings"
	"testing"

	"github.com/Demonisreal/dmn-ledger/internal/u128"
)

func TestCheckInvariantsCatches(t *testing.T) {
	t.Parallel()
	id := u128.From64
	build := func(t *testing.T) *Machine {
		m := New(0, 0)
		var accts, trs []byte
		for i, flags := range []AccountFlags{0, 0, DebitsMustNotExceedCredits, CreditsMustNotExceedDebits, 0, 0} {
			accts = AppendAccount(accts, Account{ID: id(uint64(i + 1)), Ledger: 1, Code: 1, Flags: flags})
		}
		for _, tr := range []Transfer{
			{ID: id(10), DebitAccountID: id(1), CreditAccountID: id(2), Amount: id(10)},
			{ID: id(11), DebitAccountID: id(1), CreditAccountID: id(3), Amount: id(5)},
			{ID: id(12), DebitAccountID: id(4), CreditAccountID: id(2), Amount: id(5)},
			{ID: id(13), DebitAccountID: id(5), CreditAccountID: id(6), Amount: id(7), Flags: Pending},
		} {
			tr.Ledger, tr.Code = 1, 1
			trs = AppendTransfer(trs, tr)
		}
		for _, b := range []struct {
			op   Op
			body []byte
		}{{CreateAccounts, accts}, {CreateTransfers, trs}} {
			if fails, err := m.Apply(b.op, 1, b.body, nil); err != nil || len(fails) > 0 {
				t.Fatal(fails, err)
			}
		}
		if err := m.CheckInvariants(); err != nil {
			t.Fatal(err)
		}
		return m
	}

	tests := []struct {
		name    string
		corrupt func(m *Machine)
		want    string
	}{
		{"pending without transfer", func(m *Machine) {
			m.accounts[0].DebitsPending = id(1)
		}, "open transfers sum"},
		{"posted plus pending overflows", func(m *Machine) {
			m.accounts[4].DebitsPosted = u128.Max()
		}, "overflows"},
		{"debits exceed credits", func(m *Machine) {
			m.accounts[2].DebitsPosted = id(6)
		}, "exceed credits"},
		{"credits exceed debits", func(m *Machine) {
			m.accounts[3].CreditsPosted = id(6)
		}, "exceed debits"},
		{"posted sides differ", func(m *Machine) {
			m.accounts[1].CreditsPosted = id(16)
		}, "debits posted and credits posted differ"},
		{"pending sides differ", func(m *Machine) {
			m.accounts[5].Ledger = 2
		}, "debits pending and credits pending differ"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			m := build(t)
			tt.corrupt(m)
			if err := m.CheckInvariants(); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Errorf("err = %v, want %q", err, tt.want)
			}
		})
	}
}
