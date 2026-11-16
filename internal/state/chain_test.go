package state_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"math"
	"os"
	"testing"

	"github.com/Demonisreal/dmn-ledger/internal/audit"
	"github.com/Demonisreal/dmn-ledger/internal/oracle"
	"github.com/Demonisreal/dmn-ledger/internal/state"
	"github.com/Demonisreal/dmn-ledger/internal/u128"
)

func proven(t *testing.T, m *state.Machine, r audit.Receipt, ok bool) audit.Receipt {
	t.Helper()
	if !ok {
		t.Fatal("no receipt")
	}
	got, h, _, err := audit.Decode(audit.AppendReceipt(nil, r))
	if err != nil {
		t.Fatal(err)
	}
	if want, _ := m.HeadAt(h.Seq); h != want {
		t.Fatalf("receipt leads to head %d %x, the machine has %x", h.Seq, h.Hash, want.Hash)
	}
	return got
}

func lastLink(t *testing.T, m *state.Machine) audit.Link {
	t.Helper()
	n := m.Head().Seq
	links := m.Links(n-1, n, nil)
	if len(links) != 1 {
		t.Fatalf("%d links at the head", len(links))
	}
	return links[0]
}

func failures(fails ...state.Failure) []byte {
	var b []byte
	for _, f := range fails {
		b = binary.BigEndian.AppendUint32(b, uint32(f.Index))
		b = binary.BigEndian.AppendUint32(b, uint32(f.Reason))
	}
	return b
}

func hold(id uint64, timeout uint32) state.Transfer {
	return state.Transfer{ID: id128(id), DebitAccountID: id128(2), CreditAccountID: id128(1), Amount: id128(1),
		Ledger: 1, Code: 1, Flags: state.Pending, Timeout: timeout}
}

func TestExpiryOrder(t *testing.T) {
	t.Parallel()
	m, o := newLedger(t), oracle.New()
	batches := append(setup[:len(setup):len(setup)],
		batchSpec{state.CreateTransfers, 1e9, encodeTransfers(hold(30, 3))},
		batchSpec{state.CreateTransfers, 2e9, encodeTransfers(hold(10, 2), hold(20, 1))},
		batchSpec{state.CreateTransfers, 5e9, nil},
	)
	for i, b := range batches {
		if _, err := o.Apply(b.op, b.ts, b.body); err != nil {
			t.Fatal(err)
		}
		if i >= len(setup) {
			mustApply(t, m, b.ts, decodeTransfers(b.body)...)
		}
	}

	var tree audit.Tree
	for i, id := range []uint64{20, 10, 30} {
		r, ok := m.ProveExpiry(id128(id))
		r = proven(t, m, r, ok)
		if r.Index != uint32(i) || r.Seq != m.Head().Seq {
			t.Errorf("expiry of %d: leaf %d of link %d, want leaf %d of link %d", id, r.Index, r.Seq, i, m.Head().Seq)
		}
		tree.Add(audit.Leaf(audit.KindExpiry, r.Record))
	}
	if l := lastLink(t, m); l.Leaves != 3 || l.Root != tree.Root() {
		t.Errorf("link holds %d leaves and root %x, want 3 and %x", l.Leaves, l.Root, tree.Root())
	}
	if m.Head().Hash != o.Head() {
		t.Error("head differs from the oracle")
	}
}

func decodeTransfers(body []byte) []state.Transfer {
	var trs []state.Transfer
	for ; len(body) > 0; body = body[state.TransferSize:] {
		trs = append(trs, state.DecodeTransfer(body))
	}
	return trs
}

func TestEmptyAndFailedBatchesAreLinked(t *testing.T) {
	t.Parallel()
	m := newLedger(t)
	before := m.Head()
	mustApply(t, m, 1e9)
	body := encodeTransfers(state.Transfer{}, state.Transfer{ID: id128(999)})
	fails, err := m.Apply(state.CreateTransfers, 2e9, body, nil)
	if err != nil || len(fails) != 2 {
		t.Fatal(fails, err)
	}

	empty := sha256.Sum256(nil)
	want := []audit.Link{
		{Time: 1e9, Op: 2, Body: audit.BodyHash(nil), Reply: audit.ReplyHash(nil), Root: empty},
		{Time: 2e9, Op: 2, Events: 2, Body: audit.BodyHash(body), Reply: audit.ReplyHash(failures(fails...)), Root: empty},
	}
	links := m.Links(before.Seq, m.Head().Seq, nil)
	if len(links) != 2 || links[0] != want[0] || links[1] != want[1] {
		t.Fatalf("links\n got %+v\nwant %+v", links, want)
	}
	if audit.Connect(before, links) != m.Head() {
		t.Error("links do not lead to the head")
	}
	r, ok := m.ProveBatch(2e9)
	if r = proven(t, m, r, ok); r.Link != want[1] {
		t.Errorf("batch receipt holds %+v", r.Link)
	}
}

func TestMalformedBatchLeavesNoLink(t *testing.T) {
	t.Parallel()
	m := newLedger(t)
	head := m.Head()
	for _, b := range []batchSpec{
		{0, 1e12, nil},
		{state.CreateTransfers, 1e12, make([]byte, state.TransferSize+1)},
		{state.CreateTransfers, 1 << 63, nil},
	} {
		if _, err := m.Apply(b.op, b.ts, b.body, nil); err == nil {
			t.Fatalf("batch at %d accepted", b.ts)
		}
	}
	if m.Head() != head {
		t.Error("a malformed batch left a link")
	}
	if _, ok := m.ProveBatch(1e12); ok {
		t.Error("receipt for a malformed batch")
	}
}

func TestRolledBackChainLeavesNoLeaf(t *testing.T) {
	t.Parallel()
	tr := func(id, dr, cr uint64, flags state.TransferFlags) state.Transfer {
		return state.Transfer{ID: id128(id), DebitAccountID: id128(dr), CreditAccountID: id128(cr), Amount: id128(1),
			Ledger: 1, Code: 1, Flags: flags}
	}
	m := newLedger(t)
	fails, err := m.Apply(state.CreateTransfers, 1e9, encodeTransfers(
		tr(1000, 1, 2, 0),
		tr(1001, 1, 2, state.Linked|state.Pending),
		tr(1002, 4, 2, state.Linked),
		tr(1003, 2, 1, 0),
		tr(1004, 1, 2, 0),
	), nil)
	if err != nil || len(fails) != 3 {
		t.Fatal(fails, err)
	}
	var tree audit.Tree
	for _, stored := range m.LookupTransfers([]u128.Uint128{id128(1000), id128(1004)}, nil) {
		tree.Add(audit.Leaf(audit.KindTransfer, state.AppendTransfer(nil, stored)))
	}
	if l := lastLink(t, m); l.Events != 5 || l.Leaves != 2 || l.Root != tree.Root() {
		t.Errorf("link holds %d events, %d leaves and root %x, want 5, 2 and %x", l.Events, l.Leaves, l.Root, tree.Root())
	}
	r, ok := m.ProveTransfer(id128(1004))
	if r = proven(t, m, r, ok); r.Index != 1 {
		t.Errorf("transfer after the rolled back chain is leaf %d", r.Index)
	}
}

func TestReplyHashCoversThisBatchOnly(t *testing.T) {
	t.Parallel()
	m := newLedger(t)
	earlier := []state.Failure{{Index: 7, Reason: state.Exists}}
	fails, err := m.Apply(state.CreateTransfers, 1e9, encodeTransfers(state.Transfer{}), earlier)
	if err != nil || len(fails) != 2 {
		t.Fatal(fails, err)
	}
	want := audit.ReplyHash(failures(state.Failure{Index: 0, Reason: state.IDMustNotBeZero}))
	if got := lastLink(t, m).Reply; got != want {
		t.Errorf("reply hash %x, want %x over the failures of this batch", got, want)
	}
}

func TestAccountReceiptAfterTransfers(t *testing.T) {
	t.Parallel()
	m := newLedger(t)
	now := account(t, m, 1)
	if now.DebitsPosted.IsZero() {
		t.Fatal("account 1 has no balance to leave out")
	}
	r, ok := m.ProveAccount(id128(1))
	r = proven(t, m, r, ok)
	want := state.Account{ID: now.ID, Ledger: now.Ledger, Code: now.Code, Flags: now.Flags, Timestamp: now.Timestamp}
	if got := state.DecodeAccount(r.Record); got != want || r.Seq != 1 || r.Index != 0 {
		t.Errorf("receipt holds %+v as leaf %d of link %d, want %+v as leaf 0 of link 1", got, r.Index, r.Seq, want)
	}
}

func TestLinksRange(t *testing.T) {
	t.Parallel()
	m := newLedger(t)
	for i := range uint64(3) {
		mustApply(t, m, 1e9+i)
	}
	tests := []struct {
		from, to uint64
		want     int
	}{
		{0, 5, 5},
		{2, 4, 2},
		{4, 9, 1},
		{5, 5, 0},
		{4, 2, 0},
		{9, 12, 0},
		{math.MaxUint64, math.MaxUint64, 0},
	}
	for _, tt := range tests {
		links := m.Links(tt.from, tt.to, nil)
		if len(links) != tt.want {
			t.Errorf("links %d to %d: got %d, want %d", tt.from, tt.to, len(links), tt.want)
			continue
		}
		if len(links) == 0 {
			continue
		}
		from, _ := m.HeadAt(tt.from)
		if to, _ := m.HeadAt(tt.from + uint64(len(links))); audit.Connect(from, links) != to {
			t.Errorf("links %d to %d do not connect the heads", tt.from, tt.to)
		}
	}
	if _, ok := m.HeadAt(6); ok {
		t.Error("head beyond the last link")
	}
}

func TestReceiptsMatchTheVectors(t *testing.T) {
	t.Parallel()
	text, err := os.ReadFile("../notary/testdata/vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	var vectors []struct {
		Name  string `json:"name"`
		Proof string `json:"proof"`
	}
	if err := json.Unmarshal(text, &vectors); err != nil {
		t.Fatal(err)
	}
	pinned := map[string][]byte{}
	for _, v := range vectors {
		proof, err := hex.DecodeString(v.Proof)
		if err != nil {
			t.Fatal(err)
		}
		pinned[v.Name] = proof[:max(len(proof)-104, 0)]
	}

	m := state.New(0, 0)
	var accounts []state.Account
	for id := range uint64(3) {
		accounts = append(accounts, state.Account{ID: id128(id + 1), Ledger: 1, Code: 1})
	}
	if _, err := m.Apply(state.CreateAccounts, 1e9, encodeAccounts(accounts...), nil); err != nil {
		t.Fatal(err)
	}
	var transfers []state.Transfer
	for i := range uint64(5) {
		tr := state.Transfer{ID: id128(100 + i), DebitAccountID: id128(1), CreditAccountID: id128(2), Amount: id128(10 + i), Ledger: 1, Code: 1}
		switch i {
		case 3:
			tr.Flags, tr.Timeout = state.Pending, 1
		case 4:
			tr.DebitAccountID = id128(99)
		}
		transfers = append(transfers, tr)
	}
	if _, err := m.Apply(state.CreateTransfers, 2e9, encodeTransfers(transfers...), nil); err != nil {
		t.Fatal(err)
	}
	mustApply(t, m, 9e9, state.Transfer{ID: id128(200), DebitAccountID: id128(2), CreditAccountID: id128(3), Amount: id128(1), Ledger: 1, Code: 1})

	receipt := func(r audit.Receipt, ok bool) []byte {
		if !ok {
			return nil
		}
		return audit.AppendReceipt(nil, r)
	}
	built := map[string][]byte{
		"transfer, third of four leaves":   receipt(m.ProveTransfer(id128(102))),
		"account, last of three leaves":    receipt(m.ProveAccount(id128(3))),
		"expiry of a pending transfer":     receipt(m.ProveExpiry(id128(103))),
		"batch with one failed event":      receipt(m.ProveBatch(2e9)),
		"single leaf after an expiry leaf": receipt(m.ProveTransfer(id128(200))),
		"head of the ledger":               audit.AppendHead(nil, m.Head()),
	}
	for name, proof := range built {
		if want, ok := pinned[name]; !ok || !bytes.Equal(proof, want) {
			t.Errorf("%s:\n got %x\nwant %x", name, proof, want)
		}
	}
}
