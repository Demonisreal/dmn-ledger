package audit_test

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/Demonisreal/dmn-ledger/internal/audit"
)

var update = flag.Bool("update", false, "rewrite testdata/layout.golden")

type batch struct {
	prev    audit.Head
	link    audit.Link
	kinds   []byte
	records [][]byte
	leaves  []audit.Hash
}

func record(size int, fill byte) []byte {
	rec := make([]byte, size)
	for i := range rec {
		rec[i] = fill + byte(i)
	}
	return rec
}

func seal(prev audit.Head, time uint64, op uint8, events uint32, failures []byte, kinds []byte, records [][]byte) batch {
	b := batch{prev: prev, kinds: kinds, records: records}
	var tree audit.Tree
	for i, rec := range records {
		b.leaves = append(b.leaves, audit.Leaf(kinds[i], rec))
		tree.Add(b.leaves[i])
	}
	body := bytes.Join(records, nil)
	b.link = audit.Link{
		Time:   time,
		Op:     op,
		Events: events,
		Leaves: tree.Len(),
		Body:   audit.BodyHash(body),
		Reply:  audit.ReplyHash(failures),
		Root:   tree.Root(),
	}
	return b
}

func (b batch) head() audit.Head {
	return b.prev.Next(b.link)
}

func (b batch) receipt(i int) audit.Receipt {
	return audit.Receipt{
		Kind:   b.kinds[i],
		Record: b.records[i],
		Index:  uint32(i),
		Path:   audit.Path(b.leaves, i),
		Seq:    b.prev.Seq + 1,
		Prev:   b.prev.Hash,
		Link:   b.link,
	}
}

func (b batch) whole() audit.Receipt {
	return audit.Receipt{Kind: audit.KindBatch, Seq: b.prev.Seq + 1, Prev: b.prev.Hash, Link: b.link}
}

func sampleChain() []batch {
	accounts := seal(audit.Head{}, 1e9, 1, 3, nil,
		[]byte{audit.KindAccount, audit.KindAccount, audit.KindAccount},
		[][]byte{record(96, 0x10), record(96, 0x20), record(96, 0x30)})
	transfers := seal(accounts.head(), 2e9, 2, 5, []byte{0, 0, 0, 4, 0, 0, 0, 0x2a},
		[]byte{audit.KindTransfer, audit.KindTransfer, audit.KindTransfer, audit.KindTransfer},
		[][]byte{record(100, 0x40), record(100, 0x50), record(100, 0x60), record(100, 0x70)})
	expiry := seal(transfers.head(), 9e9, 2, 1, nil,
		[]byte{audit.KindExpiry, audit.KindTransfer},
		[][]byte{record(100, 0x70), record(100, 0x80)})
	empty := seal(expiry.head(), 10e9, 2, 0, nil, nil, nil)
	return []batch{accounts, transfers, expiry, empty}
}

type proof struct {
	name  string
	bytes []byte
	head  audit.Head
}

func sampleProofs() []proof {
	chain := sampleChain()
	var proofs []proof
	for n, b := range chain {
		proofs = append(proofs, proof{fmt.Sprintf("batch %d", n+1), audit.AppendReceipt(nil, b.whole()), b.head()})
		for i := range b.records {
			proofs = append(proofs, proof{fmt.Sprintf("batch %d leaf %d", n+1, i), audit.AppendReceipt(nil, b.receipt(i)), b.head()})
		}
	}
	last := chain[len(chain)-1].head()
	proofs = append(proofs,
		proof{"head", audit.AppendHead(nil, last), last},
		proof{"start", audit.AppendHead(nil, audit.Head{}), audit.Head{}})
	return proofs
}

func recode(r audit.Receipt, h audit.Head) []byte {
	if r.Kind == audit.KindHead {
		return audit.AppendHead(nil, h)
	}
	return audit.AppendReceipt(nil, r)
}

func TestLayoutIsPinned(t *testing.T) {
	chain := sampleChain()
	leaf, body, reply := audit.Leaf(audit.KindTransfer, record(100, 1)), audit.BodyHash(record(100, 1)), audit.ReplyHash([]byte{0, 0, 0, 1, 0, 0, 0, 2})
	lines := []struct {
		name  string
		bytes []byte
	}{
		{"Leaf", leaf[:]},
		{"Body", body[:]},
		{"Reply", reply[:]},
		{"Link", audit.AppendLink(nil, chain[1].link)},
		{"Head", audit.AppendHead(nil, chain[1].head())},
		{"Batch", audit.AppendReceipt(nil, chain[1].whole())},
		{"Account", audit.AppendReceipt(nil, chain[0].receipt(2))},
		{"Transfer", audit.AppendReceipt(nil, chain[1].receipt(2))},
		{"Expiry", audit.AppendReceipt(nil, chain[2].receipt(0))},
	}
	var layout strings.Builder
	for _, l := range lines {
		fmt.Fprintf(&layout, "%s %x\n", l.name, l.bytes)
	}
	const path = "testdata/layout.golden"
	if *update {
		if err := os.WriteFile(path, []byte(layout.String()), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	got, pinned := strings.Split(layout.String(), "\n"), strings.Split(string(want), "\n")
	if len(got) != len(pinned) {
		t.Fatalf("%d lines, %d pinned", len(got), len(pinned))
	}
	for i := range got {
		if got[i] != pinned[i] {
			t.Errorf("line %q, pinned %q", got[i], pinned[i])
		}
	}
}

func TestDecode(t *testing.T) {
	stamp := record(104, 0xa0)
	for _, p := range sampleProofs() {
		for _, rest := range [][]byte{nil, stamp} {
			r, h, got, err := audit.Decode(slices.Concat(p.bytes, rest))
			if err != nil {
				t.Fatalf("%s: %v", p.name, err)
			}
			if h != p.head {
				t.Errorf("%s: head %d %x, want %d %x", p.name, h.Seq, h.Hash, p.head.Seq, p.head.Hash)
			}
			if !bytes.Equal(got, rest) {
				t.Errorf("%s: rest %x, want %x", p.name, got, rest)
			}
			if again := recode(r, h); !bytes.Equal(again, p.bytes) {
				t.Errorf("%s: encodes back to %x", p.name, again)
			}
		}
	}
	if n := len(audit.AppendReceipt(nil, sampleChain()[1].receipt(2))); n != 260+32*2 {
		t.Errorf("transfer receipt with two hops is %d bytes, want %d", n, 260+32*2)
	}
}

func TestDecodeRefuses(t *testing.T) {
	chain := sampleChain()
	transfer := audit.AppendReceipt(nil, chain[1].receipt(2))
	at := func(b []byte, i int, v byte) []byte {
		b = slices.Clone(b)
		b[i] = v
		return b
	}
	tooLong := chain[1].receipt(2)
	for len(tooLong.Path) <= audit.MaxHops {
		tooLong.Path = append(tooLong.Path, tooLong.Path[0])
	}
	unnumbered := chain[0].receipt(0)
	unnumbered.Seq = 0
	indexed := chain[1].whole()
	indexed.Index = 1
	withPath := chain[1].whole()
	withPath.Path = []audit.Hash{{}}
	beyond := chain[1].receipt(3)
	beyond.Index = 4
	cases := []struct {
		name  string
		proof []byte
	}{
		{"empty", nil},
		{"version only", []byte{1}},
		{"version 0", at(transfer, 0, 0)},
		{"version 2", at(transfer, 0, 2)},
		{"kind 5", at(transfer, 1, 5)},
		{"kind 255", at(transfer, 1, 255)},
		{"account kind on a transfer", at(transfer, 1, audit.KindAccount)},
		{"one byte short", transfer[:len(transfer)-1]},
		{"cut after the path length", transfer[:2+100+5]},
		{"one byte more", append(slices.Clone(transfer), 0)},
		{"stamp one byte short", slices.Concat(transfer, make([]byte, 103))},
		{"stamp one byte long", slices.Concat(transfer, make([]byte, 105))},
		{"two stamps", slices.Concat(transfer, make([]byte, 208))},
		{"path length one more", at(transfer, 2+100+4, 3)},
		{"path length one less", at(transfer, 2+100+4, 1)},
		{"33 hops", audit.AppendReceipt(nil, tooLong)},
		{"link number 0", audit.AppendReceipt(nil, unnumbered)},
		{"batch with a leaf index", audit.AppendReceipt(nil, indexed)},
		{"batch with a path", audit.AppendReceipt(nil, withPath)},
		{"leaf index beyond the leaves", audit.AppendReceipt(nil, beyond)},
		{"head one byte short", audit.AppendHead(nil, chain[0].head())[:41]},
		{"head one byte more", append(audit.AppendHead(nil, chain[0].head()), 0)},
		{"head 0 with a hash", audit.AppendHead(nil, audit.Head{Hash: chain[0].head().Hash})},
	}
	for _, c := range cases {
		r, h, rest, err := audit.Decode(c.proof)
		if !errors.Is(err, audit.ErrBadReceipt) {
			t.Errorf("%s: %v", c.name, err)
		}
		if r.Kind != 0 || r.Record != nil || r.Path != nil || h != (audit.Head{}) || rest != nil {
			t.Errorf("%s: refused, but returned %+v %+v %x", c.name, r, h, rest)
		}
	}
}

func TestDecodeRefusesBeforeAllocating(t *testing.T) {
	hops := func(n int) []byte {
		r := sampleChain()[1].receipt(2)
		for len(r.Path) < n {
			r.Path = append(r.Path, r.Path[0])
		}
		return audit.AppendReceipt(nil, r)
	}
	longest := hops(audit.MaxHops)
	cases := map[string][]byte{
		"32 hops, one byte short":  longest[:len(longest)-1],
		"32 hops, a byte more":     append(slices.Clone(longest), 0),
		"33 hops":                  hops(audit.MaxHops + 1),
		"255 hops":                 hops(255),
		"255 hops, cut after them": append(longest[:2+100+4:2+100+4], 255),
	}
	for name, proof := range cases {
		allocs := testing.AllocsPerRun(10, func() {
			if _, _, _, err := audit.Decode(proof); err == nil {
				t.Fatalf("%s: accepted", name)
			}
		})
		if allocs != 0 {
			t.Errorf("%s: %v allocs before refusing", name, allocs)
		}
	}
}

func TestEveryFlippedBitIsRefused(t *testing.T) {
	var flips, refused int
	for _, p := range sampleProofs() {
		for bit := range 8 * len(p.bytes) {
			flipped := slices.Clone(p.bytes)
			flipped[bit/8] ^= 1 << (bit % 8)
			flips++
			_, h, _, err := audit.Decode(flipped)
			if err != nil {
				refused++
				continue
			}
			if h == p.head {
				t.Errorf("%s: bit %d flipped, same head", p.name, bit)
			}
		}
	}
	t.Logf("%d flips, %d refused by the decoder, the rest give another head", flips, refused)
}

func FuzzDecode(f *testing.F) {
	for _, p := range sampleProofs() {
		f.Add(p.bytes)
		f.Add(slices.Concat(p.bytes, record(104, 7)))
	}
	f.Fuzz(func(t *testing.T, proof []byte) {
		r, h, rest, err := audit.Decode(proof)
		if err != nil {
			if !errors.Is(err, audit.ErrBadReceipt) {
				t.Fatalf("error %v is not ErrBadReceipt", err)
			}
			return
		}
		if cap(r.Path) > audit.MaxHops {
			t.Fatalf("path with room for %d hashes", cap(r.Path))
		}
		if len(rest) != 0 && len(rest) != 104 {
			t.Fatalf("rest of %d bytes", len(rest))
		}
		if again := append(recode(r, h), rest...); !bytes.Equal(again, proof) {
			t.Fatalf("accepted %x, encodes back to %x", proof, again)
		}
	})
}
