package state_test

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/Demonisreal/dmn-ledger/internal/oracle"
	"github.com/Demonisreal/dmn-ledger/internal/state"
)

const eventsPerRun = 2000

func TestModel(t *testing.T) {
	t.Parallel()
	total := 1_000_000
	if testing.Short() {
		total = 40_000
	}
	var seen [state.ExceedsDebits + 1]atomic.Int64
	t.Run("seeds", func(t *testing.T) {
		for seed := range uint64(total / eventsPerRun) {
			t.Run(fmt.Sprint(seed), func(t *testing.T) {
				t.Parallel()
				r := rand.New(rand.NewPCG(seed, 0x1ed6e5))
				run(t, r, eventsPerRun, nil, func(c state.Reason) { seen[c].Add(1) })
			})
		}
	})
	if testing.Short() || t.Failed() {
		return
	}
	for c := state.LinkedEventFailed; c <= state.ExceedsDebits; c++ {
		if seen[c].Load() == 0 {
			t.Errorf("generator never produced %v", c)
		}
	}
}

func FuzzApply(f *testing.F) {
	r := rand.New(rand.NewPCG(7, 7))
	for range 8 {
		seed := make([]byte, 64+r.IntN(4096))
		for i := range seed {
			seed[i] = byte(r.Uint32())
		}
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, seed []byte) {
		src := &fuzzSource{b: seed}
		run(t, rand.New(src), eventsPerRun, src.exhausted, nil)
	})
}

func FuzzApplyRaw(f *testing.F) {
	valid := state.Transfer{ID: id128(1000), DebitAccountID: id128(1), CreditAccountID: id128(2), Amount: id128(5), Ledger: 1, Code: 1}
	f.Add(uint8(state.CreateTransfers), uint64(20), encodeTransfers(valid))
	f.Add(uint8(state.CreateTransfers), uint64(40e9), encodeTransfers(
		state.Transfer{ID: id128(1001), PendingID: id128(200), Amount: id128(3), Flags: state.PostPending | state.Linked},
		state.Transfer{ID: id128(1002), PendingID: id128(205), Flags: state.VoidPending},
	))
	f.Add(uint8(state.CreateTransfers), uint64(1<<63-1), encodeTransfers(valid))
	f.Add(uint8(state.CreateAccounts), uint64(20), encodeAccounts(state.Account{ID: id128(9), Ledger: 1, Code: 1}))
	f.Add(uint8(7), uint64(0), []byte{1})
	f.Fuzz(func(t *testing.T, op uint8, ts uint64, body []byte) {
		m, o := state.New(0, 0), oracle.New()
		for _, b := range setup {
			_, _ = m.Apply(b.op, b.ts, b.body, nil)
			_, _ = o.Apply(b.op, b.ts, b.body)
		}
		got, err := m.Apply(state.Op(op), ts, body, nil)
		want, oerr := o.Apply(state.Op(op), ts, body)
		if (err == nil) != (oerr == nil) || !slices.Equal(got, want) {
			t.Fatalf("got %v %v, oracle %v %v", got, err, want, oerr)
		}
		if m.Digest() != o.Digest() {
			t.Fatal("digests differ")
		}
		if err := m.CheckInvariants(); err != nil {
			t.Fatal(err)
		}
	})
}

func run(t *testing.T, r *rand.Rand, events int, stop func() bool, observe func(state.Reason)) {
	t.Helper()
	g := newGen(r)
	m, o := state.New(0, 0), oracle.New()
	var replica *state.Machine
	var got, again []state.Failure
	for batchNo := 0; events > 0 && (stop == nil || !stop()); batchNo++ {
		if batchNo%64 == 32 {
			replica = restore(t, m)
		}
		b := g.batch()
		var err error
		got, err = m.Apply(b.op, b.ts, b.body, got[:0])
		want, oerr := o.Apply(b.op, b.ts, b.body)
		if (err == nil) != (oerr == nil) || err != nil && !errors.Is(err, state.ErrMalformedBatch) {
			t.Fatalf("batch %d: errors differ: %v, oracle %v\n%s", batchNo, err, oerr, dump(b))
		}
		if !slices.Equal(got, want) {
			t.Fatalf("batch %d: failures differ\n got %v\nwant %v\n%s", batchNo, got, want, dump(b))
		}
		if got, want := m.LookupAccounts(b.ids), o.LookupAccounts(b.ids); !slices.Equal(got, want) {
			t.Fatalf("batch %d: accounts differ\n got %+v\nwant %+v\n%s", batchNo, got, want, dump(b))
		}
		if got, want := m.LookupTransfers(b.ids), o.LookupTransfers(b.ids); !slices.Equal(got, want) {
			t.Fatalf("batch %d: transfers differ\n got %+v\nwant %+v\n%s", batchNo, got, want, dump(b))
		}
		digest := m.Digest()
		if digest != o.Digest() {
			t.Fatalf("batch %d: digests differ\n%s", batchNo, dump(b))
		}
		if err := m.CheckInvariants(); err != nil {
			t.Fatalf("batch %d: %v", batchNo, err)
		}
		if err := o.CheckInvariants(); err != nil {
			t.Fatalf("batch %d: oracle: %v", batchNo, err)
		}
		if replica != nil {
			var rerr error
			again, rerr = replica.Apply(b.op, b.ts, b.body, again[:0])
			if (rerr == nil) != (err == nil) || !slices.Equal(again, got) || replica.Digest() != digest {
				t.Fatalf("batch %d: restored replica diverged: %v %v", batchNo, again, rerr)
			}
		}

		g.settle(b, got)
		if observe != nil {
			for _, fail := range got {
				observe(fail.Reason)
			}
		}
		if b.op == state.CreateAccounts {
			events -= len(b.body) / state.AccountSize
		} else {
			events -= len(b.body) / state.TransferSize
		}
	}
}

func restore(t *testing.T, m *state.Machine) *state.Machine {
	t.Helper()
	var snap bytes.Buffer
	if err := m.WriteSnapshot(&snap); err != nil {
		t.Fatal(err)
	}
	if sha256.Sum256(snap.Bytes()) != m.Digest() {
		t.Fatal("digest is not the hash of the snapshot")
	}
	c, err := state.ReadSnapshot(bytes.NewReader(snap.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	if c.Digest() != m.Digest() {
		t.Fatal("restored machine has another digest")
	}
	return c
}

func dump(b batch) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "op %d at %d:\n", b.op, b.ts)
	if b.op == state.CreateAccounts {
		for i := 0; i < len(b.body); i += state.AccountSize {
			fmt.Fprintf(&sb, "  %d %+v\n", i/state.AccountSize, state.DecodeAccount(b.body[i:]))
		}
		return sb.String()
	}
	for i := 0; i < len(b.body); i += state.TransferSize {
		fmt.Fprintf(&sb, "  %d %+v\n", i/state.TransferSize, state.DecodeTransfer(b.body[i:]))
	}
	return sb.String()
}
