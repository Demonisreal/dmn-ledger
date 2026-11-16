package state_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/Demonisreal/dmn-ledger/internal/audit"
	"github.com/Demonisreal/dmn-ledger/internal/oracle"
	"github.com/Demonisreal/dmn-ledger/internal/state"
	"github.com/Demonisreal/dmn-ledger/internal/u128"
	"github.com/Demonisreal/dmn-ledger/internal/workload"
)

const eventsPerRun = 2000

var proved [audit.KindExpiry + 1]atomic.Int64

func TestModel(t *testing.T) {
	t.Parallel()
	total := 1_000_000
	if testing.Short() {
		total = 40_000
	}
	var seen [state.LastReason + 1]atomic.Int64
	t.Run("seeds", func(t *testing.T) {
		for seed := range uint64(total / eventsPerRun) {
			t.Run(fmt.Sprint(seed), func(t *testing.T) {
				t.Parallel()
				r := rand.New(rand.NewPCG(seed, 0x1ed6e5))
				run(t, r, workload.New(r), eventsPerRun, nil, func(c state.Reason) { seen[c].Add(1) })
			})
		}
	})
	if testing.Short() || t.Failed() {
		return
	}
	for c := state.LinkedEventFailed; c <= state.LastReason; c++ {
		if seen[c].Load() == 0 {
			t.Errorf("generator never produced %v", c)
		}
	}
	for kind := range proved {
		if proved[kind].Load() == 0 {
			t.Errorf("no receipt of kind %d", kind)
		}
	}
}

func TestModelBeyondOneBlock(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("long runs")
	}
	for seed := range uint64(3) {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			t.Parallel()
			r := rand.New(rand.NewPCG(seed, 0xb10c))
			g := workload.New(r)
			g.MaxBatch, g.MaxAccounts, g.LimitPct, g.Huge = 2000, 64, 0, false
			if m := run(t, r, g, 60_000, nil, nil); m.Transfers() <= 1<<14 {
				t.Fatalf("only %d transfers stored", m.Transfers())
			}
		})
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
		r := rand.New(src)
		run(t, r, workload.New(r), eventsPerRun, src.exhausted, nil)
	})
}

func rawBatches(specs ...batchSpec) []byte {
	var raw []byte
	for _, b := range specs {
		raw = append(raw, byte(b.op))
		raw = binary.BigEndian.AppendUint64(raw, b.ts)
		raw = binary.BigEndian.AppendUint16(raw, uint16(len(b.body)))
		raw = append(raw, b.body...)
	}
	return raw
}

func FuzzApplyRaw(f *testing.F) {
	valid := state.Transfer{ID: id128(1000), DebitAccountID: id128(1), CreditAccountID: id128(2), Amount: id128(5), Ledger: 1, Code: 1}
	pending := state.Transfer{ID: id128(1001), DebitAccountID: id128(1), CreditAccountID: id128(2), Amount: id128(5), Ledger: 1, Code: 1,
		Flags: state.Pending, Timeout: 2}
	f.Add(rawBatches(
		batchSpec{state.CreateTransfers, 20, encodeTransfers(valid, pending)},
		batchSpec{state.CreateTransfers, 40e9, nil},
		batchSpec{state.CreateTransfers, 30, encodeTransfers(
			state.Transfer{ID: id128(1002), PendingID: id128(1001), Flags: state.PostPending | state.Linked},
			state.Transfer{ID: id128(1003), PendingID: id128(205), Flags: state.VoidPending},
		)},
	))
	f.Add(rawBatches(
		batchSpec{state.CreateTransfers, 1<<63 - 2, encodeTransfers(valid)},
		batchSpec{state.CreateTransfers, 5, nil},
		batchSpec{state.CreateTransfers, 5, nil},
	))
	f.Add(rawBatches(
		batchSpec{state.CreateAccounts, 20, encodeAccounts(state.Account{ID: id128(9), Ledger: 1, Code: 1})},
		batchSpec{7, 0, []byte{1}},
	))
	f.Fuzz(func(t *testing.T, raw []byte) {
		m, o := state.New(0, 0), oracle.New()
		for _, b := range setup {
			_, _ = m.Apply(b.op, b.ts, b.body, nil)
			_, _ = o.Apply(b.op, b.ts, b.body)
		}
		var got []state.Failure
		for batchNo := 0; len(raw) >= 11; batchNo++ {
			op, ts := state.Op(raw[0]), binary.BigEndian.Uint64(raw[1:])
			body := raw[11:][:min(int(binary.BigEndian.Uint16(raw[9:])), len(raw)-11)]
			raw = raw[11+len(body):]

			var err error
			got, err = m.Apply(op, ts, body, got[:0])
			want, oerr := o.Apply(op, ts, body)
			if (err == nil) != (oerr == nil) || !slices.Equal(got, want) {
				t.Fatalf("batch %d: got %v %v, oracle %v %v", batchNo, got, err, want, oerr)
			}
			if m.Digest() != o.Digest() || m.Head().Hash != o.Head() {
				t.Fatalf("batch %d: digests or heads differ", batchNo)
			}
			if err := m.CheckInvariants(); err != nil {
				t.Fatalf("batch %d: %v", batchNo, err)
			}
		}
	})
}

func run(t *testing.T, r *rand.Rand, g *workload.Gen, events int, stop func() bool, observe func(state.Reason)) *state.Machine {
	t.Helper()
	m, o := state.New(r.IntN(32), r.IntN(3000)), oracle.New()
	var replica *state.Machine
	var got, again []state.Failure
	for batchNo := 0; events > 0 && (stop == nil || !stop()); batchNo++ {
		if batchNo%64 == 32 {
			replica = restore(t, m)
		}
		b := g.Batch()
		start := state.StartTime(b.TS, m.Last())
		var err error
		got, err = m.Apply(b.Op, b.TS, b.Body, got[:0])
		want, oerr := o.Apply(b.Op, b.TS, b.Body)
		if (err == nil) != (oerr == nil) || err != nil && !errors.Is(err, state.ErrMalformedBatch) {
			t.Fatalf("batch %d: errors differ: %v, oracle %v\n%s", batchNo, err, oerr, dump(b))
		}
		if !slices.Equal(got, want) {
			t.Fatalf("batch %d: failures differ\n got %v\nwant %v\n%s", batchNo, got, want, dump(b))
		}
		if found, want := m.LookupAccounts(b.IDs, nil), o.LookupAccounts(b.IDs); !slices.Equal(found, want) {
			t.Fatalf("batch %d: accounts differ\n got %+v\nwant %+v\n%s", batchNo, found, want, dump(b))
		}
		if found, want := m.LookupTransfers(b.IDs, nil), o.LookupTransfers(b.IDs); !slices.Equal(found, want) {
			t.Fatalf("batch %d: transfers differ\n got %+v\nwant %+v\n%s", batchNo, found, want, dump(b))
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
		if h := m.Head(); h.Seq != uint64(o.Links()) || h.Hash != o.Head() {
			t.Fatalf("batch %d: head %d %x, oracle %d %x\n%s", batchNo, h.Seq, h.Hash, o.Links(), o.Head(), dump(b))
		}
		if n := o.Links(); n > 0 && !bytes.Equal(audit.AppendLink(nil, m.Links(uint64(n-1), uint64(n), nil)[0]), o.Link(n)) {
			t.Fatalf("batch %d: link differs from the oracle\n%s", batchNo, dump(b))
		}
		checkReceipts(t, m, o, b.IDs, start, err == nil)
		at, waiting := m.NextExpiry()
		if scanned, ok := m.ScanExpiries(); at != scanned || waiting != ok {
			t.Fatalf("batch %d: next expiry %d %v, a scan of all transfers finds %d %v\n%s", batchNo, at, waiting, scanned, ok, dump(b))
		}
		if replica != nil {
			var rerr error
			again, rerr = replica.Apply(b.Op, b.TS, b.Body, again[:0])
			if (rerr == nil) != (err == nil) || !slices.Equal(again, got) || replica.Digest() != digest {
				t.Fatalf("batch %d: restored replica diverged: %v %v", batchNo, again, rerr)
			}
		}

		g.Settle(b, got)
		if observe != nil {
			for _, fail := range got {
				observe(fail.Reason)
			}
		}
		if b.Op == state.CreateAccounts {
			events -= len(b.Body) / state.AccountSize
		} else {
			events -= len(b.Body) / state.TransferSize
		}
	}
	return m
}

func checkReceipts(t *testing.T, m *state.Machine, o *oracle.Ledger, ids []u128.Uint128, start uint64, accepted bool) {
	t.Helper()
	var batch []byte
	if accepted {
		batch = []byte{}
	}
	r, ok := m.ProveBatch(start)
	checkReceipt(t, o, r, ok, batch)
	for _, id := range ids {
		var account, transfer, expiry []byte
		if found := o.LookupAccounts([]u128.Uint128{id}); len(found) == 1 {
			a := found[0]
			a.DebitsPending, a.DebitsPosted, a.CreditsPending, a.CreditsPosted = u128.Uint128{}, u128.Uint128{}, u128.Uint128{}, u128.Uint128{}
			account = state.AppendAccount(nil, a)
		}
		if found := o.LookupTransfers([]u128.Uint128{id}); len(found) == 1 {
			transfer = state.AppendTransfer(nil, found[0])
			if o.Expired(id) {
				expiry = transfer
			}
		}
		r, ok := m.ProveAccount(id)
		checkReceipt(t, o, r, ok, account)
		r, ok = m.ProveTransfer(id)
		checkReceipt(t, o, r, ok, transfer)
		r, ok = m.ProveExpiry(id)
		checkReceipt(t, o, r, ok, expiry)
	}
}

func checkReceipt(t *testing.T, o *oracle.Ledger, r audit.Receipt, ok bool, record []byte) {
	t.Helper()
	if ok != (record != nil) {
		t.Fatalf("receipt of kind %d: found %v, the oracle holds %x", r.Kind, ok, record)
	}
	if !ok {
		return
	}
	proof := audit.AppendReceipt(nil, r)
	got, h, rest, err := audit.Decode(proof)
	if err != nil || len(rest) > 0 {
		t.Fatalf("receipt of kind %d does not decode: %v, %d bytes left", r.Kind, err, len(rest))
	}
	if again := audit.AppendReceipt(nil, got); !bytes.Equal(again, proof) {
		t.Fatalf("receipt of kind %d is not canonical", r.Kind)
	}
	if h.Seq > uint64(o.Links()) || h.Hash != o.HeadAt(int(h.Seq)) {
		t.Fatalf("receipt of kind %d leads to head %d %x, the oracle has another", r.Kind, h.Seq, h.Hash)
	}
	if !bytes.Equal(got.Record, record) {
		t.Fatalf("receipt of kind %d holds %x, the oracle %x", r.Kind, got.Record, record)
	}
	proved[r.Kind].Add(1)
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

func dump(b workload.Batch) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "op %d at %d:\n", b.Op, b.TS)
	if b.Op == state.CreateAccounts {
		for i := 0; i < len(b.Body); i += state.AccountSize {
			fmt.Fprintf(&sb, "  %d %+v\n", i/state.AccountSize, state.DecodeAccount(b.Body[i:]))
		}
		return sb.String()
	}
	for i := 0; i < len(b.Body); i += state.TransferSize {
		fmt.Fprintf(&sb, "  %d %+v\n", i/state.TransferSize, state.DecodeTransfer(b.Body[i:]))
	}
	return sb.String()
}

type fuzzSource struct {
	b    []byte
	weyl uint64
}

func (s *fuzzSource) Uint64() uint64 {
	if len(s.b) >= 8 {
		v := binary.LittleEndian.Uint64(s.b)
		s.b = s.b[8:]
		return v
	}
	s.b = nil
	s.weyl += 0x9e3779b97f4a7c15
	return s.weyl
}

func (s *fuzzSource) exhausted() bool {
	return s.b == nil
}
