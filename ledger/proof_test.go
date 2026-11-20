package ledger

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/binary"
	"errors"
	"slices"
	"testing"
	"testing/synctest"

	"github.com/Demonisreal/dmn-ledger/internal/audit"
	"github.com/Demonisreal/dmn-ledger/internal/client"
	"github.com/Demonisreal/dmn-ledger/internal/notary"
	"github.com/Demonisreal/dmn-ledger/internal/state"
	"github.com/Demonisreal/dmn-ledger/internal/u128"
)

var (
	proofKey = ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x3c}, ed25519.SeedSize))
	otherKey = ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x3d}, ed25519.SeedSize))
)

func keysOf(keys ...ed25519.PrivateKey) [][]byte {
	var pub [][]byte
	for _, k := range keys {
		pub = append(pub, k.Public().(ed25519.PublicKey))
	}
	return pub
}

func bookOf(t *testing.T) *state.Machine {
	t.Helper()
	m := state.New(8, 64)
	at := uint64(1_000_000_000)
	apply := func(op state.Op, body []byte) {
		t.Helper()
		if _, err := m.Apply(op, at, body, nil); err != nil {
			t.Fatal(err)
		}
		at += 1_000_000_000
	}
	apply(state.CreateAccounts, state.AppendAccount(state.AppendAccount(nil,
		state.Account{ID: u128.From64(1), Ledger: 1, Code: 1}),
		state.Account{ID: u128.From64(2), Ledger: 1, Code: 1}))
	for id := uint64(10); id < 14; id++ {
		apply(state.CreateTransfers, state.AppendTransfer(nil, state.Transfer{
			ID: u128.From64(id), DebitAccountID: u128.From64(1), CreditAccountID: u128.From64(2), Amount: u128.From64(id), Ledger: 1, Code: 1,
		}))
	}
	apply(state.CreateTransfers, state.AppendTransfer(nil, state.Transfer{
		ID: u128.From64(20), DebitAccountID: u128.From64(1), CreditAccountID: u128.From64(2), Amount: u128.From64(5), Ledger: 1, Code: 1,
		Flags: state.Pending, Timeout: 1,
	}))
	apply(state.CreateTransfers, nil)
	apply(state.CreateTransfers, nil)
	return m
}

func stamped(key ed25519.PrivateKey, signedFor uint64, proof []byte) []byte {
	_, h, _, err := audit.Decode(proof)
	if err != nil {
		panic(err)
	}
	return append(slices.Clip(proof), notary.Sign(key, signedFor, h)...)
}

func receiptOf(r audit.Receipt, ok bool) []byte {
	if !ok {
		panic("nothing to prove")
	}
	return audit.AppendReceipt(nil, r)
}

func TestVerifyProof(t *testing.T) {
	m := bookOf(t)
	head := m.Head()
	for _, tc := range []struct {
		name  string
		proof []byte
		want  Proof
	}{
		{"account", receiptOf(m.ProveAccount(u128.From64(2))), Proof{Kind: ProofAccount, Time: 1_000_000_000, HeadSeq: 1,
			Account: Account{ID: From64(2), Ledger: 1, Code: 1, Timestamp: 1_000_000_001}}},
		{"transfer", receiptOf(m.ProveTransfer(u128.From64(11))), Proof{Kind: ProofTransfer, Time: 3_000_000_000, HeadSeq: 3,
			Transfer: Transfer{ID: From64(11), DebitAccountID: From64(1), CreditAccountID: From64(2), Amount: From64(11), Ledger: 1, Code: 1, Timestamp: 3_000_000_000}}},
		{"expiry", receiptOf(m.ProveExpiry(u128.From64(20))), Proof{Kind: ProofExpiry, Time: 7_000_000_000, HeadSeq: 7,
			Transfer: Transfer{ID: From64(20), DebitAccountID: From64(1), CreditAccountID: From64(2), Amount: From64(5), Timeout: 1, Ledger: 1, Code: 1, Flags: Pending, Timestamp: 6_000_000_000}}},
		{"batch", receiptOf(m.ProveBatch(3_000_000_000)), Proof{Kind: ProofBatch, Time: 3_000_000_000, HeadSeq: 3}},
		{"head", audit.AppendHead(nil, head), Proof{Kind: ProofHead, HeadSeq: head.Seq, Head: head.Hash}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.want.Kind != ProofHead {
				h, _ := m.HeadAt(tc.want.HeadSeq)
				tc.want.Head = h.Hash
			}
			tc.want.Key = keysOf(proofKey)[0]
			got, err := VerifyProof(stamped(proofKey, cluster, tc.proof), cluster, keysOf(otherKey, proofKey))
			if err != nil || !equalProofs(got, tc.want) {
				t.Fatalf("got %+v, %v\nwant %+v", got, err, tc.want)
			}
			unsigned, err := VerifyProof(tc.proof, cluster, keysOf(proofKey))
			tc.want.Key = nil
			if !errors.Is(err, ErrUnsigned) || !equalProofs(unsigned, tc.want) {
				t.Fatalf("without a stamp: %+v, %v, want ErrUnsigned and %+v", unsigned, err, tc.want)
			}
		})
	}
}

func equalProofs(a, b Proof) bool {
	return a.Kind == b.Kind && a.Account == b.Account && a.Transfer == b.Transfer && a.Time == b.Time &&
		a.HeadSeq == b.HeadSeq && a.Head == b.Head && bytes.Equal(a.Key, b.Key) && (a.Key == nil) == (b.Key == nil)
}

func TestVerifyProofRefuses(t *testing.T) {
	m := bookOf(t)
	proof := stamped(proofKey, cluster, receiptOf(m.ProveTransfer(u128.From64(12))))
	if _, err := VerifyProof(proof, cluster, keysOf(proofKey)); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name    string
		proof   []byte
		cluster uint64
		keys    [][]byte
	}{
		{"checked for another cluster", proof, cluster + 1, keysOf(proofKey)},
		{"signed for another cluster", stamped(proofKey, cluster+1, receiptOf(m.ProveTransfer(u128.From64(12)))), cluster, keysOf(proofKey)},
		{"signed by an untrusted key", stamped(otherKey, cluster, receiptOf(m.ProveTransfer(u128.From64(12)))), cluster, keysOf(proofKey)},
		{"no trusted keys", proof, cluster, nil},
		{"a key of the wrong length", proof, cluster, [][]byte{keysOf(proofKey)[0][:31]}},
		{"stamp cut short", proof[:len(proof)-1], cluster, keysOf(proofKey)},
		{"one byte more", append(slices.Clone(proof), 0), cluster, keysOf(proofKey)},
		{"empty", nil, cluster, keysOf(proofKey)},
	} {
		if p, err := VerifyProof(tc.proof, tc.cluster, tc.keys); !errors.Is(err, ErrBadProof) {
			t.Errorf("%s: %+v, %v, want ErrBadProof", tc.name, p, err)
		}
	}
	flipped := 0
	for i := range len(proof) * 8 {
		bad := slices.Clone(proof)
		bad[i/8] ^= 1 << (i % 8)
		if _, err := VerifyProof(bad, cluster, keysOf(proofKey)); err == nil {
			t.Fatalf("bit %d flipped and the proof still holds", i)
		}
		flipped++
	}
	if flipped != len(proof)*8 {
		t.Fatalf("%d bits flipped", flipped)
	}
}

func linksOf(m *state.Machine, from, to uint64) []byte {
	var b []byte
	for _, l := range m.Links(from, to, nil) {
		b = audit.AppendLink(b, l)
	}
	return b
}

func verified(t *testing.T, proof []byte) Proof {
	t.Helper()
	p, err := VerifyProof(stamped(proofKey, cluster, proof), cluster, keysOf(proofKey))
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestConnectProofs(t *testing.T) {
	m := bookOf(t)
	older := verified(t, receiptOf(m.ProveTransfer(u128.From64(11))))
	newer := verified(t, audit.AppendHead(nil, m.Head()))
	links := linksOf(m, older.HeadSeq, newer.HeadSeq)
	if err := ConnectProofs(older, newer, links); err != nil {
		t.Fatalf("transfer at %d to the head at %d: %v", older.HeadSeq, newer.HeadSeq, err)
	}
	if err := ConnectProofs(newer, newer, nil); err != nil {
		t.Fatalf("a head to itself: %v", err)
	}
	if err := ConnectProofs(older, verified(t, receiptOf(m.ProveBatch(3_000_000_000))), nil); err != nil {
		t.Fatalf("two receipts under the same head: %v", err)
	}

	changed := slices.Clone(links)
	changed[audit.LinkSize+20] ^= 1
	swapped := slices.Concat(links[audit.LinkSize:2*audit.LinkSize], links[:audit.LinkSize], links[2*audit.LinkSize:])
	other := newer
	other.Head[0] ^= 1
	for _, tc := range []struct {
		name   string
		older  Proof
		newer  Proof
		links  []byte
		denied error
	}{
		{"a changed link", older, newer, changed, ErrBadLinks},
		{"two links swapped", older, newer, swapped, ErrBadLinks},
		{"another newer head", older, other, links, ErrBadLinks},
		{"a link missing", older, newer, links[audit.LinkSize:], ErrBadLinks},
		{"a link too many", older, newer, slices.Concat(links, links[:audit.LinkSize]), ErrBadLinks},
		{"a link cut short", older, newer, links[:len(links)-1], ErrBadLinks},
		{"no links", older, newer, nil, ErrBadLinks},
		{"a link between a head and itself", newer, newer, links[:audit.LinkSize], ErrBadLinks},
		{"another head at the same number", newer, other, nil, ErrConflict},
		{"another head at the same number, with a link", newer, other, links[:audit.LinkSize], ErrConflict},
		{"older and newer swapped", newer, older, links, nil},
		{"older and newer swapped, without links", newer, older, nil, nil},
	} {
		err := ConnectProofs(tc.older, tc.newer, tc.links)
		switch {
		case err == nil:
			t.Errorf("%s: connected", tc.name)
		case tc.denied == nil && (errors.Is(err, ErrConflict) || errors.Is(err, ErrBadLinks)):
			t.Errorf("%s: %v, want an error that is neither a conflict nor bad links", tc.name, err)
		case tc.denied != nil && (errors.Is(err, ErrConflict) != (tc.denied == ErrConflict) || errors.Is(err, ErrBadLinks) != (tc.denied == ErrBadLinks)):
			t.Errorf("%s: %v, want only %v", tc.name, err, tc.denied)
		}
	}
}

func TestProofsOverTheWire(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFake(t)
		c := dialed(t, f)
		defer c.Close()
		ctx := context.Background()

		type fetched struct {
			proof []byte
			err   error
		}
		fetch := func(do func() ([]byte, error)) <-chan fetched {
			done := make(chan fetched, 1)
			go func() {
				proof, err := do()
				done <- fetched{proof, err}
			}()
			synctest.Wait()
			return done
		}
		for _, tc := range []struct {
			name string
			do   func() ([]byte, error)
			op   client.Op
			body []byte
		}{
			{"transfer", func() ([]byte, error) { return c.Prove(ctx, ProofTransfer, Uint128{Hi: 1, Lo: 2}) }, client.Prove,
				append([]byte{2}, u128.Uint128{Hi: 1, Lo: 2}.AppendBytes(nil)...)},
			{"account", func() ([]byte, error) { return c.Prove(ctx, ProofAccount, From64(3)) }, client.Prove, append([]byte{1}, u128.From64(3).AppendBytes(nil)...)},
			{"expiry", func() ([]byte, error) { return c.Prove(ctx, ProofExpiry, From64(4)) }, client.Prove, append([]byte{3}, u128.From64(4).AppendBytes(nil)...)},
			{"batch", func() ([]byte, error) { return c.ProveBatch(ctx, 0x0102030405060708) }, client.Prove, []byte{0, 1, 2, 3, 4, 5, 6, 7, 8}},
			{"head", func() ([]byte, error) { return c.Head(ctx) }, client.Head, nil},
		} {
			done := fetch(tc.do)
			g := f.next(t)
			if g.req.Op != tc.op || !bytes.Equal(g.req.Body, tc.body) {
				t.Fatalf("%s: sent %s %x, want %s %x", tc.name, g.req.Op, g.req.Body, tc.op, tc.body)
			}
			g.reply(t, client.Reply{Status: client.OK, Leader: 1, Body: []byte(tc.name)})
			if got := await(t, done); got.err != nil || string(got.proof) != tc.name {
				t.Fatalf("%s: %q, %v", tc.name, got.proof, got.err)
			}
		}

		done := fetch(func() ([]byte, error) { return c.Prove(ctx, ProofTransfer, From64(99)) })
		f.next(t).ok(t)
		if got := await(t, done); got.err != nil || got.proof != nil {
			t.Fatalf("nothing to prove: %x, %v, want nil and no error", got.proof, got.err)
		}
		for _, kind := range []ProofKind{ProofBatch, ProofHead, ProofHead + 1} {
			if _, err := c.Prove(ctx, kind, From64(1)); err == nil {
				t.Errorf("Prove of a %s went through", kind)
			}
		}
		if _, err := c.Links(ctx, 5, 4); err == nil {
			t.Error("links from 5 to 4 went through")
		}
		f.quiet(t, patience/4)
	})
}

func TestLinksComeInPieces(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newFake(t)
		c := dialed(t, f)
		defer c.Close()
		ctx := context.Background()
		piece := func(first, count uint64) []byte {
			var b []byte
			for seq := first; seq < first+count; seq++ {
				b = audit.AppendLink(b, audit.Link{Time: seq})
			}
			return b
		}
		asked := func(g request) (from, to uint64) {
			if g.req.Op != client.Links || len(g.req.Body) != 16 {
				t.Fatalf("sent %s %x", g.req.Op, g.req.Body)
			}
			return binary.BigEndian.Uint64(g.req.Body), binary.BigEndian.Uint64(g.req.Body[8:])
		}

		type fetched struct {
			links []byte
			err   error
		}
		fetch := func(ctx context.Context, from, to uint64) <-chan fetched {
			done := make(chan fetched, 1)
			go func() {
				links, err := c.Links(ctx, from, to)
				done <- fetched{links, err}
			}()
			synctest.Wait()
			return done
		}

		done := fetch(ctx, 10, 3000)
		for _, want := range []struct{ from, count uint64 }{{10, client.MaxLinks}, {10 + client.MaxLinks, client.MaxLinks}, {10 + 2*client.MaxLinks, 7}} {
			g := f.next(t)
			if from, to := asked(g); from != want.from || to != 3000 {
				t.Fatalf("asked for links from %d to %d, want from %d to 3000", from, to, want.from)
			}
			g.reply(t, client.Reply{Status: client.OK, Leader: 1, Body: piece(want.from+1, want.count)})
		}
		got := await(t, done)
		if want := piece(11, 2*client.MaxLinks+7); got.err != nil || !bytes.Equal(got.links, want) {
			t.Fatalf("%d bytes, %v; want the %d links of the three pieces", len(got.links), got.err, len(want)/audit.LinkSize)
		}
		f.quiet(t, patience/4)

		short, cancel := context.WithCancel(ctx)
		done = fetch(short, 0, 5000)
		f.next(t).reply(t, client.Reply{Status: client.OK, Leader: 1, Body: piece(1, client.MaxLinks)})
		synctest.Wait()
		f.next(t)
		cancel()
		got = await(t, done)
		if !errors.Is(got.err, context.Canceled) || !bytes.Equal(got.links, piece(1, client.MaxLinks)) {
			t.Fatalf("cancelled after one piece: %d bytes, %v; want the first piece and the cause", len(got.links), got.err)
		}
	})
}
