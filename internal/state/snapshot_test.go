package state_test

import (
	"bytes"
	"encoding/binary"
	"errors"
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
		{"account from the future", edit(func(b []byte) []byte { b[17+state.AccountSize-8]++; return b }), "future"},
		{"balance changed", edit(func(b []byte) []byte { b[17+state.AccountSize+70]++; return b }), "credits posted differ"},
		{"status unknown", edit(func(b []byte) []byte { b[rec(0)+state.TransferSize] = 9; return b }), "status"},
		{"pending without status", edit(func(b []byte) []byte { b[rec(2)+state.TransferSize] = 0; return b }), "status"},
		{"unknown debit account", edit(func(b []byte) []byte { b[rec(0)+31]++; return b }), "unknown accounts"},
		{"post without pending", edit(func(b []byte) []byte { b[rec(4)+79]++; return b }), "missing"},
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
