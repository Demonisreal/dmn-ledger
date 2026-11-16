package state

import (
	"strings"
	"testing"

	"github.com/Demonisreal/dmn-ledger/internal/audit"
	"github.com/Demonisreal/dmn-ledger/internal/u128"
)

func TestCheckInvariantsCatches(t *testing.T) {
	t.Parallel()
	id := u128.From64
	build := func(t *testing.T) *Machine {
		m := New(0, 0)
		var accts []byte
		for i, flags := range []AccountFlags{0, 0, DebitsMustNotExceedCredits, CreditsMustNotExceedDebits, 0, 0} {
			accts = AppendAccount(accts, Account{ID: id(uint64(i + 1)), Ledger: 1, Code: 1, Flags: flags})
		}
		var trs []byte
		for _, tr := range []Transfer{
			{ID: id(10), DebitAccountID: id(1), CreditAccountID: id(2), Amount: id(10)},
			{ID: id(11), DebitAccountID: id(1), CreditAccountID: id(3), Amount: id(5)},
			{ID: id(12), DebitAccountID: id(4), CreditAccountID: id(2), Amount: id(5)},
			{ID: id(13), DebitAccountID: id(5), CreditAccountID: id(6), Amount: id(7), Flags: Pending, Timeout: 100},
			{ID: id(14), DebitAccountID: id(5), CreditAccountID: id(6), Amount: id(2), Flags: Pending, Timeout: 50},
			{ID: id(15), DebitAccountID: id(1), CreditAccountID: id(2), Amount: id(4), Flags: Pending},
			{ID: id(16), DebitAccountID: id(1), CreditAccountID: id(2), Amount: id(3), Flags: Pending},
			{ID: id(17), DebitAccountID: id(1), CreditAccountID: id(2), Amount: id(1), Flags: Pending, Timeout: 1},
			{ID: id(18), DebitAccountID: id(1), CreditAccountID: id(2), Amount: id(1), Flags: Pending, Timeout: 1},
			{ID: id(20), PendingID: id(15), Amount: id(3), Flags: PostPending},
			{ID: id(21), PendingID: id(16), Flags: VoidPending},
		} {
			if tr.PendingID.IsZero() {
				tr.Ledger, tr.Code = 1, 1
			}
			trs = AppendTransfer(trs, tr)
		}
		for _, b := range []struct {
			op   Op
			ts   uint64
			body []byte
		}{{CreateAccounts, 1, accts}, {CreateTransfers, 1e9, trs}, {CreateTransfers, 3e9, nil}} {
			if fails, err := m.Apply(b.op, b.ts, b.body, nil); err != nil || len(fails) > 0 {
				t.Fatal(fails, err)
			}
		}
		if err := m.CheckInvariants(); err != nil {
			t.Fatal(err)
		}
		return m
	}
	tr := func(m *Machine, n uint64) *stored {
		return m.transfer(m.transferIdx[id(n)])
	}
	link := func(m *Machine, seq uint64) *sealed {
		return m.sealedAt(seq - 1)
	}
	rename := func(m *Machine, n uint64, to u128.Uint128) {
		i := m.transferIdx[id(n)]
		delete(m.transferIdx, id(n))
		m.transferIdx[to] = i
		m.transfer(i).ID = to
	}

	tests := []struct {
		name    string
		corrupt func(m *Machine)
		want    string
	}{
		{"last out of range", func(m *Machine) { m.last = tsLimit }, "out of range"},
		{"undo left", func(m *Machine) { m.undo = append(m.undo, undo{}) }, "undo entries"},
		{"index too small", func(m *Machine) { delete(m.transferIdx, id(10)) }, "indexes hold"},
		{"account index", func(m *Machine) { m.accountIdx[id(1)], m.accountIdx[id(2)] = 1, 0 }, "account 1: not indexed"},
		{"account id", func(m *Machine) {
			delete(m.accountIdx, id(1))
			m.accountIdx[u128.Max()] = 0
			m.accounts[0].ID = u128.Max()
		}, "id out of range"},
		{"account flags", func(m *Machine) { m.accounts[0].Flags = accountFlagsKnown }, "flags"},
		{"account code", func(m *Machine) { m.accounts[0].Code = 0 }, "ledger or code"},
		{"posted plus pending overflows", func(m *Machine) { m.accounts[4].DebitsPosted = u128.Max() }, "overflows"},
		{"debits exceed credits", func(m *Machine) { m.accounts[2].DebitsPosted = id(6) }, "exceed credits"},
		{"credits exceed debits", func(m *Machine) { m.accounts[3].CreditsPosted = id(6) }, "exceed debits"},
		{"posted sides differ", func(m *Machine) { m.accounts[1].CreditsPosted = id(1000) }, "credits posted differ"},
		{"pending sides differ", func(m *Machine) { m.accounts[5].Ledger = 2 }, "credits pending differ"},
		{"transfer index", func(m *Machine) { m.transferIdx[id(10)], m.transferIdx[id(11)] = 1, 0 }, "transfer 10: not indexed"},
		{"transfer id", func(m *Machine) { rename(m, 10, u128.Uint128{}) }, "id out of range"},
		{"transfer flags", func(m *Machine) { tr(m, 10).Flags = 1 << 5 }, "flags"},
		{"same account twice", func(m *Machine) { tr(m, 10).cr = tr(m, 10).dr }, "accounts do not match"},
		{"other ledger", func(m *Machine) { tr(m, 10).Ledger = 2 }, "ledger 2"},
		{"amount 0", func(m *Machine) { tr(m, 10).Amount = u128.Uint128{} }, "amount is 0"},
		{"timeout without pending", func(m *Machine) { tr(m, 10).Timeout = 1 }, "timeout without pending"},
		{"pending id on a normal transfer", func(m *Machine) { tr(m, 10).PendingID = id(15) }, "pending id"},
		{"status on a normal transfer", func(m *Machine) { tr(m, 10).status = statusPosted }, "status 2"},
		{"expired too early", func(m *Machine) { tr(m, 17).Timeout = 100 }, "before its deadline"},
		{"missing from the expiry queue", func(m *Machine) { tr(m, 13).heap = -1 }, "expiry queue position"},
		{"post without pending", func(m *Machine) { tr(m, 20).PendingID = id(99) }, "missing"},
		{"pending resolved twice", func(m *Machine) { tr(m, 21).PendingID = id(15) }, "resolved twice"},
		{"pending still open", func(m *Machine) { tr(m, 16).status = statusPending }, "has status 1"},
		{"post with another code", func(m *Machine) { tr(m, 20).Code = 2 }, "accounts or code differ"},
		{"post exceeds pending", func(m *Machine) { tr(m, 20).Amount = id(5) }, "holds 4"},
		{"post before pending", func(m *Machine) { tr(m, 20).Timestamp = tr(m, 15).Timestamp }, "lifetime"},
		{"posted without post", func(m *Machine) { tr(m, 17).status = statusPosted }, "2 posts and voids for 3"},
		{"stray queue entry", func(m *Machine) { m.expiries = append(m.expiries, m.expiries[0]) }, "expiry queue holds 3"},
		{"queue out of order", func(m *Machine) {
			m.expiries[0], m.expiries[1] = m.expiries[1], m.expiries[0]
			m.transfer(m.expiries[0].tr).heap, m.transfer(m.expiries[1].tr).heap = 0, 1
		}, "out of order"},
		{"timestamp repeated", func(m *Machine) { tr(m, 11).Timestamp = tr(m, 10).Timestamp }, "repeated"},
		{"timestamp after last", func(m *Machine) { m.accounts[0].Timestamp = m.last + 1 }, "after last"},
		{"money without transfer", func(m *Machine) {
			m.accounts[0].DebitsPosted = m.accounts[0].DebitsPosted.AddWrap(id(1000))
			m.accounts[1].CreditsPosted = m.accounts[1].CreditsPosted.AddWrap(id(1000))
		}, "account 1: balances do not match"},
		{"lost transfer", func(m *Machine) {
			m.accounts[0].DebitsPosted = m.accounts[0].DebitsPosted.SubWrap(id(10))
			m.accounts[1].CreditsPosted = m.accounts[1].CreditsPosted.SubWrap(id(10))
		}, "account 1: balances do not match"},
		{"expiry list too short", func(m *Machine) { m.expired = m.expired[:1] }, "holds 1 entries for 2 expired"},
		{"open transfer in the expiry list", func(m *Machine) { m.expired[0] = m.transferIdx[id(13)] }, "with status 1"},
		{"expiry list out of order", func(m *Machine) { m.expired[0], m.expired[1] = m.expired[1], m.expired[0] }, "out of order at 1"},
		{"link time out of range", func(m *Machine) { link(m, 3).link.Time = tsLimit }, "link 3: time"},
		{"link before the end of the one before", func(m *Machine) { link(m, 2).link.Time = 2 }, "before the end"},
		{"link with an unknown op", func(m *Machine) { link(m, 1).link.Op = 3 }, "link 1: op 3"},
		{"link with another root", func(m *Machine) { link(m, 2).link.Root[0]++ }, "link 2: records give 11 leaves"},
		{"link with another leaf count", func(m *Machine) { link(m, 3).link.Leaves++ }, "the link holds 3"},
		{"expiry without a leaf", func(m *Machine) { link(m, 3).link.Leaves, link(m, 3).link.Root = 0, audit.Hash{} }, "link 3: records give 2 leaves"},
		{"head not folded", func(m *Machine) { link(m, 2).head[0]++ }, "link 2: head"},
		{"link with another body", func(m *Machine) { link(m, 2).link.Body[0]++ }, "link 2: head"},
		{"transfer outside its link", func(m *Machine) { link(m, 2).link.Time++ }, "transfer 10: timestamp 1000000000 belongs to no link"},
		{"transfers without a link", func(m *Machine) { m.nlinks = 1 }, "transfer 10: timestamp 1000000000 belongs to no link"},
		{"expiry without a link", func(m *Machine) { m.nlinks = 2 }, "expiry of transfer 17 belongs to no link"},
		{"last behind the links", func(m *Machine) { m.last++ }, "the links end at 3000000000"},
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
