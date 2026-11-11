package state_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"slices"
	"strings"
	"testing"

	"github.com/Demonisreal/dmn-ledger/internal/state"
)

func snapshot(t testing.TB, m *state.Machine) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := m.WriteSnapshot(&buf); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func TestSnapshotRoundTrip(t *testing.T) {
	t.Parallel()
	m := newLedger(t)
	mustApply(t, m, 1e9,
		state.Transfer{ID: id128(300), DebitAccountID: id128(2), CreditAccountID: id128(1), Amount: id128(4),
			Ledger: 1, Code: 1, Flags: state.Pending, Timeout: 1},
		state.Transfer{ID: id128(301), DebitAccountID: id128(2), CreditAccountID: id128(1), Amount: id128(4),
			Ledger: 1, Code: 1, Flags: state.Pending, Timeout: 2},
	)
	snap := snapshot(t, m)
	c, err := state.ReadSnapshot(bytes.NewReader(snap))
	if err != nil {
		t.Fatal(err)
	}
	if c.Digest() != m.Digest() {
		t.Fatal("digest changed by the round trip")
	}
	if again := snapshot(t, c); !bytes.Equal(again, snap) {
		t.Fatal("restored machine writes other bytes")
	}

	later := []batchSpec{
		{state.CreateTransfers, 2e9, encodeTransfers(
			state.Transfer{ID: id128(400), PendingID: id128(300), Flags: state.PostPending},
			state.Transfer{ID: id128(401), PendingID: id128(301), Amount: id128(1), Flags: state.PostPending},
			state.Transfer{ID: id128(402), PendingID: id128(200), Flags: state.VoidPending | state.Linked},
			state.Transfer{ID: id128(403), DebitAccountID: id128(4), CreditAccountID: id128(1), Amount: id128(1), Ledger: 1, Code: 1},
		)},
		{state.CreateTransfers, 5, nil},
		{state.CreateAccounts, 6, encodeAccounts(state.Account{ID: id128(8), Ledger: 1, Code: 1})},
		{state.CreateTransfers, 40e9, encodeTransfers(state.Transfer{ID: id128(404), PendingID: id128(205), Flags: state.PostPending})},
	}
	for _, b := range later {
		got, err := m.Apply(b.op, b.ts, b.body, nil)
		again, cerr := c.Apply(b.op, b.ts, b.body, nil)
		if err != nil || cerr != nil || !slices.Equal(got, again) {
			t.Fatalf("batch at %d: original %v %v, copy %v %v", b.ts, got, err, again, cerr)
		}
		if c.Digest() != m.Digest() {
			t.Fatalf("batch at %d: digests differ", b.ts)
		}
	}
}

func TestReadSnapshotRejects(t *testing.T) {
	t.Parallel()
	valid := snapshot(t, newLedger(t))
	edit := func(f func(b []byte) []byte) []byte {
		return f(bytes.Clone(valid))
	}
	const accounts = 7
	transfers := 1 + 8 + 8 + accounts*state.AccountSize + 8
	rec := func(k int) int { return transfers + k*(state.TransferSize+1) }
	tests := []struct {
		name string
		snap []byte
		want string
	}{
		{"empty", nil, "EOF"},
		{"version", edit(func(b []byte) []byte { b[0] = 2; return b }), "version"},
		{"last at limit", edit(func(b []byte) []byte { binary.BigEndian.PutUint64(b[1:], 1<<63); return b }), "out of range"},
		{"truncated", valid[:len(valid)-1], "EOF"},
		{"trailing byte", append(bytes.Clone(valid), 0), "trailing"},
		{"huge account count", func() []byte {
			b := snapshot(t, state.New(0, 0))
			binary.BigEndian.PutUint64(b[9:], 1<<62)
			return b
		}(), "EOF"},
		{"huge transfer count", edit(func(b []byte) []byte {
			binary.BigEndian.PutUint64(b[transfers-8:], 1<<62)
			return b
		}), "EOF"},
		{"accounts out of order", edit(func(b []byte) []byte {
			first := b[17 : 17+state.AccountSize]
			second := b[17+state.AccountSize : 17+2*state.AccountSize]
			tmp := bytes.Clone(first)
			copy(first, second)
			copy(second, tmp)
			return b
		}), "out of order"},
		{"account from the future", edit(func(b []byte) []byte { b[17+state.AccountSize-8]++; return b }), "after last"},
		{"balance changed", edit(func(b []byte) []byte { b[17+state.AccountSize+70]++; return b }), "credits posted differ"},
		{"status unknown", edit(func(b []byte) []byte { b[rec(0)+state.TransferSize] = 9; return b }), "status"},
		{"pending without status", edit(func(b []byte) []byte { b[rec(2)+state.TransferSize] = 0; return b }), "status"},
		{"transfers out of order", edit(func(b []byte) []byte {
			first := b[rec(0):rec(1)]
			tmp := bytes.Clone(first)
			copy(first, b[rec(1):rec(2)])
			copy(b[rec(1):rec(2)], tmp)
			return b
		}), "transfer 100 out of order"},
		{"transfer from the future", edit(func(b []byte) []byte { b[rec(0)+92]++; return b }), "after last"},
		{"status on a normal transfer", edit(func(b []byte) []byte { b[rec(0)+state.TransferSize] = 2; return b }), "status 2"},
		{"unknown debit account", edit(func(b []byte) []byte { b[rec(0)+16]++; return b }), "unknown accounts"},
		{"same account twice", edit(func(b []byte) []byte { b[rec(0)+31]++; return b }), "accounts do not match"},
		{"post without pending", edit(func(b []byte) []byte { b[rec(4)+64]++; return b }), "missing"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := state.ReadSnapshot(bytes.NewReader(tt.snap))
			if !errors.Is(err, state.ErrBadSnapshot) || !strings.Contains(err.Error(), tt.want) {
				t.Errorf("err = %v, want %q", err, tt.want)
			}
		})
	}
}

func buildSnapshot(last uint64, accts []state.Account, trs []state.Transfer, statuses []byte) []byte {
	b := []byte{1}
	b = binary.BigEndian.AppendUint64(b, last)
	b = binary.BigEndian.AppendUint64(b, uint64(len(accts)))
	for _, a := range accts {
		b = state.AppendAccount(b, a)
	}
	b = binary.BigEndian.AppendUint64(b, uint64(len(trs)))
	for i, tr := range trs {
		b = append(state.AppendTransfer(b, tr), statuses[i])
	}
	return b
}

func TestReadSnapshotRejectsUnreachable(t *testing.T) {
	t.Parallel()
	acct := func(id uint64, ledger uint32) state.Account {
		return state.Account{ID: id128(id), Ledger: ledger, Code: 1, Timestamp: id}
	}
	held := func(a state.Account, debits, credits uint64) state.Account {
		a.DebitsPending, a.CreditsPending = id128(debits), id128(credits)
		return a
	}
	pending := state.Transfer{ID: id128(10), DebitAccountID: id128(1), CreditAccountID: id128(2), Amount: id128(5),
		Ledger: 1, Code: 1, Flags: state.Pending, Timestamp: 5}
	post := state.Transfer{ID: id128(11), DebitAccountID: id128(1), CreditAccountID: id128(2), Amount: id128(5),
		PendingID: id128(10), Ledger: 1, Code: 1, Flags: state.PostPending, Timestamp: 6}
	tests := []struct {
		name     string
		accts    []state.Account
		trs      []state.Transfer
		statuses []byte
		want     string
	}{
		{"account id 0", []state.Account{{Ledger: 1, Code: 1, Timestamp: 1}}, nil, nil, "id out of range"},
		{"account with every flag", []state.Account{{ID: id128(1), Ledger: 1, Code: 1, Flags: 0xffff, Timestamp: 1}}, nil, nil, "flags"},
		{"account timestamp 0", []state.Account{{ID: id128(1), Ledger: 1, Code: 1}}, nil, nil, "timestamp 0"},
		{"shared timestamp", []state.Account{acct(1, 1), {ID: id128(2), Ledger: 1, Code: 1, Timestamp: 1}}, nil, nil, "repeated"},
		{"money without transfer", []state.Account{
			{ID: id128(1), Ledger: 1, Code: 1, Timestamp: 1, DebitsPosted: id128(1000)},
			{ID: id128(2), Ledger: 1, Code: 1, Timestamp: 2, CreditsPosted: id128(1000)},
		}, nil, nil, "balances do not match"},
		{"transfer without effect", []state.Account{acct(1, 1), acct(2, 1)},
			[]state.Transfer{{ID: id128(10), DebitAccountID: id128(1), CreditAccountID: id128(2), Amount: id128(5), Ledger: 1, Code: 1,
				Timestamp: 5}}, []byte{0}, "balances do not match"},
		{"transfer between ledgers", []state.Account{
			{ID: id128(1), Ledger: 1, Code: 1, Timestamp: 1, DebitsPosted: id128(5), CreditsPosted: id128(5)},
			{ID: id128(2), Ledger: 2, Code: 1, Timestamp: 2, DebitsPosted: id128(5), CreditsPosted: id128(5)},
		}, []state.Transfer{
			{ID: id128(10), DebitAccountID: id128(1), CreditAccountID: id128(2), Amount: id128(5), Ledger: 1, Code: 1, Timestamp: 3},
			{ID: id128(11), DebitAccountID: id128(2), CreditAccountID: id128(1), Amount: id128(5), Ledger: 2, Code: 1, Timestamp: 4},
		}, []byte{0, 0}, "ledger"},
		{"posted without post", []state.Account{acct(1, 1), acct(2, 1)}, []state.Transfer{pending}, []byte{2}, "posts and voids"},
		{"expired without timeout", []state.Account{acct(1, 1), acct(2, 1)}, []state.Transfer{pending}, []byte{4}, "before its deadline"},
		{"posted but still open", []state.Account{
			{ID: id128(1), Ledger: 1, Code: 1, Timestamp: 1, DebitsPending: id128(5), DebitsPosted: id128(5)},
			{ID: id128(2), Ledger: 1, Code: 1, Timestamp: 2, CreditsPending: id128(5), CreditsPosted: id128(5)},
		}, []state.Transfer{pending, post}, []byte{1, 0}, "has status 1"},
		{"post larger than pending", []state.Account{
			{ID: id128(1), Ledger: 1, Code: 1, Timestamp: 1, DebitsPosted: id128(900)},
			{ID: id128(2), Ledger: 1, Code: 1, Timestamp: 2, CreditsPosted: id128(900)},
		}, []state.Transfer{pending, func() state.Transfer { p := post; p.Amount = id128(900); return p }()}, []byte{2, 0}, "holds 5"},
		{"open pending on another ledger", []state.Account{held(acct(1, 1), 5, 0), held(acct(2, 2), 0, 5)},
			[]state.Transfer{pending}, []byte{1}, "pending differ"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := state.ReadSnapshot(bytes.NewReader(buildSnapshot(10, tt.accts, tt.trs, tt.statuses)))
			if !errors.Is(err, state.ErrBadSnapshot) || !strings.Contains(err.Error(), tt.want) {
				t.Errorf("err = %v, want %q", err, tt.want)
			}
		})
	}
}

type failingDisk struct {
	left int
	err  error
}

func (d *failingDisk) Read(p []byte) (int, error) {
	if d.left == 0 {
		return 0, d.err
	}
	n := min(len(p), d.left)
	p[0] = 1
	clear(p[1:n])
	d.left -= n
	return n, nil
}

func TestReadSnapshotPassesReadErrors(t *testing.T) {
	t.Parallel()
	eio := errors.New("input/output error")
	_, err := state.ReadSnapshot(&failingDisk{left: 5, err: eio})
	if !errors.Is(err, eio) || errors.Is(err, state.ErrBadSnapshot) {
		t.Errorf("read error: %v", err)
	}
	_, err = state.ReadSnapshot(&failingDisk{left: 5, err: io.EOF})
	if !errors.Is(err, state.ErrBadSnapshot) || !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Errorf("cut off: %v", err)
	}
}

func FuzzReadSnapshot(f *testing.F) {
	f.Add(snapshot(f, state.New(0, 0)))
	m := state.New(0, 0)
	for _, b := range setup {
		if _, err := m.Apply(b.op, b.ts, b.body, nil); err != nil {
			f.Fatal(err)
		}
	}
	f.Add(snapshot(f, m))

	follow := encodeTransfers(
		state.Transfer{ID: id128(900), PendingID: id128(200), Flags: state.PostPending},
		state.Transfer{ID: id128(901), DebitAccountID: id128(1), CreditAccountID: id128(2), Amount: id128(1), Ledger: 1, Code: 1},
	)
	f.Fuzz(func(t *testing.T, snap []byte) {
		m, err := state.ReadSnapshot(bytes.NewReader(snap))
		if err != nil {
			return
		}
		if again := snapshot(t, m); !bytes.Equal(again, snap) {
			t.Fatal("accepted snapshot is not canonical")
		}
		if _, err := m.Apply(state.CreateTransfers, 1e15, follow, nil); err != nil && !errors.Is(err, state.ErrMalformedBatch) {
			t.Fatal(err)
		}
		if err := m.CheckInvariants(); err != nil {
			t.Fatal(err)
		}
	})
}
