package state

import (
	"testing"

	"github.com/Demonisreal/dmn-ledger/internal/u128"
)

func (m *Machine) ScanExpiries() (uint64, bool) {
	var at uint64
	waiting := false
	for i := range m.ntransfers {
		tr := m.transfer(i)
		if tr.status != statusPending || tr.Timeout == 0 {
			continue
		}
		if !waiting || tr.expiresAt() < at {
			at, waiting = tr.expiresAt(), true
		}
	}
	return at, waiting
}

func TestNextExpiry(t *testing.T) {
	t.Parallel()
	id := u128.From64
	hold := func(n uint64, timeout uint32) Transfer {
		return Transfer{ID: id(n), DebitAccountID: id(1), CreditAccountID: id(2), Amount: id(5),
			Ledger: 1, Code: 1, Flags: Pending, Timeout: timeout}
	}
	m := New(0, 0)
	if at, ok := m.NextExpiry(); at != 0 || ok {
		t.Fatalf("empty machine: next expiry %d %v", at, ok)
	}
	accts := AppendAccount(AppendAccount(nil, Account{ID: id(1), Ledger: 1, Code: 1}), Account{ID: id(2), Ledger: 1, Code: 1})
	if fails, err := m.Apply(CreateAccounts, 1, accts, nil); err != nil || len(fails) > 0 {
		t.Fatal(fails, err)
	}

	const sec = 1_000_000_000
	steps := []struct {
		name string
		ts   uint64
		trs  []Transfer
		want uint64
	}{
		{"hold without timeout", 1 * sec, []Transfer{hold(10, 0)}, 0},
		{"two deadlines", 2 * sec, []Transfer{hold(11, 9), hold(12, 3), hold(13, 5)}, 5*sec + 1},
		{"post of the first", 3 * sec, []Transfer{{ID: id(20), PendingID: id(12), Flags: PostPending}}, 7*sec + 2},
		{"void of the first", 4 * sec, []Transfer{{ID: id(21), PendingID: id(13), Flags: VoidPending}}, 11 * sec},
		{"shorter hold", 5 * sec, []Transfer{hold(14, 1)}, 6 * sec},
		{"just before the deadline", 6*sec - 1, nil, 6 * sec},
		{"expiry of the first", 6 * sec, nil, 11 * sec},
		{"failed chain", 7 * sec, []Transfer{hold(15, 1), {ID: id(22), PendingID: id(11), Flags: VoidPending | Linked}, {}}, 8 * sec},
		{"expiry after the rollback", 8 * sec, nil, 11 * sec},
		{"expiry of the last", 11 * sec, nil, 0},
	}
	for _, s := range steps {
		var body []byte
		for _, tr := range s.trs {
			body = AppendTransfer(body, tr)
		}
		if _, err := m.Apply(CreateTransfers, s.ts, body, nil); err != nil {
			t.Fatalf("%s: %v", s.name, err)
		}
		at, ok := m.NextExpiry()
		if at != s.want || ok != (s.want != 0) {
			t.Fatalf("%s: next expiry %d %v, want %d", s.name, at, ok, s.want)
		}
		if scanned, waiting := m.ScanExpiries(); at != scanned || ok != waiting {
			t.Fatalf("%s: next expiry %d %v, a scan of all transfers finds %d %v", s.name, at, ok, scanned, waiting)
		}
		if err := m.CheckInvariants(); err != nil {
			t.Fatalf("%s: %v", s.name, err)
		}
	}
}
