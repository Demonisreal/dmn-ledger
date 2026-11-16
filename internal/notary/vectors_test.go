package notary_test

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"math/big"
	"os"
	"reflect"
	"slices"
	"strconv"
	"testing"

	"github.com/Demonisreal/dmn-ledger/internal/audit"
	"github.com/Demonisreal/dmn-ledger/internal/client"
	"github.com/Demonisreal/dmn-ledger/internal/notary"
	"github.com/Demonisreal/dmn-ledger/internal/state"
	"github.com/Demonisreal/dmn-ledger/internal/u128"
)

var update = flag.Bool("update", false, "rewrite testdata/vectors.json")

const (
	cluster      = 0xc1057e1200000001
	otherCluster = cluster ^ 1
)

var (
	key      = keyFromSeed("dmn-ledger test vectors")
	otherKey = keyFromSeed("another key")
)

func keyFromSeed(s string) ed25519.PrivateKey {
	seed := sha256.Sum256([]byte(s))
	return ed25519.NewKeyFromSeed(seed[:])
}

func public(k ed25519.PrivateKey) ed25519.PublicKey {
	return k.Public().(ed25519.PublicKey)
}

type sealed struct {
	prev    audit.Head
	link    audit.Link
	kinds   []byte
	records [][]byte
}

func (s sealed) receipt(i int) audit.Receipt {
	leaves := make([]audit.Hash, len(s.records))
	for k, rec := range s.records {
		leaves[k] = audit.Leaf(s.kinds[k], rec)
	}
	return audit.Receipt{
		Kind:   s.kinds[i],
		Record: s.records[i],
		Index:  uint32(i),
		Path:   audit.Path(leaves, i),
		Seq:    s.prev.Seq + 1,
		Prev:   s.prev.Hash,
		Link:   s.link,
	}
}

func (s sealed) whole() audit.Receipt {
	return audit.Receipt{Kind: audit.KindBatch, Seq: s.prev.Seq + 1, Prev: s.prev.Hash, Link: s.link}
}

type book struct {
	m       *state.Machine
	head    audit.Head
	batches []sealed
}

func (b *book) apply(t testing.TB, op state.Op, ts uint64, body []byte, expired ...uint64) sealed {
	t.Helper()
	start := state.StartTime(ts, b.m.Last())
	fails, err := b.m.Apply(op, ts, body, nil)
	if err != nil {
		t.Fatal(err)
	}
	s := sealed{prev: b.head}
	for _, id := range expired {
		s.kinds = append(s.kinds, audit.KindExpiry)
		s.records = append(s.records, b.transfer(t, u128.From64(id)))
	}
	size := state.TransferSize
	if op == state.CreateAccounts {
		size = state.AccountSize
	}
	for i := range len(body) / size {
		if slices.ContainsFunc(fails, func(f state.Failure) bool { return f.Index == i }) {
			continue
		}
		id := u128.FromBytes(body[i*size:])
		if op == state.CreateAccounts {
			found := b.m.LookupAccounts([]u128.Uint128{id}, nil)
			s.kinds = append(s.kinds, audit.KindAccount)
			s.records = append(s.records, state.AppendAccount(nil, found[0]))
		} else {
			s.kinds = append(s.kinds, audit.KindTransfer)
			s.records = append(s.records, b.transfer(t, id))
		}
	}
	var tree audit.Tree
	for i, rec := range s.records {
		tree.Add(audit.Leaf(s.kinds[i], rec))
	}
	s.link = audit.Link{
		Time:   start,
		Op:     uint8(op),
		Events: uint32(len(body) / size),
		Leaves: tree.Len(),
		Body:   audit.BodyHash(body),
		Reply:  audit.ReplyHash(client.AppendFailures(nil, fails)),
		Root:   tree.Root(),
	}
	b.head = b.head.Next(s.link)
	b.batches = append(b.batches, s)
	return s
}

func (b *book) transfer(t testing.TB, id u128.Uint128) []byte {
	t.Helper()
	found := b.m.LookupTransfers([]u128.Uint128{id}, nil)
	if len(found) != 1 {
		t.Fatalf("transfer %v not stored", id)
	}
	return state.AppendTransfer(nil, found[0])
}

type vector struct {
	Name    string `json:"name"`
	Proof   string `json:"proof"`
	Cluster string `json:"cluster"`
	Key     string `json:"key"`
	Valid   bool   `json:"valid"`
	Seq     uint64 `json:"seq,omitempty"`
	Head    string `json:"head,omitempty"`
}

func clusterHex(c uint64) string {
	return hex.EncodeToString(binary.BigEndian.AppendUint64(nil, c))
}

func check(v vector) (bool, error) {
	proof, err := hex.DecodeString(v.Proof)
	if err != nil {
		return false, err
	}
	pub, err := hex.DecodeString(v.Key)
	if err != nil {
		return false, err
	}
	c, err := strconv.ParseUint(v.Cluster, 16, 64)
	if err != nil {
		return false, err
	}
	_, h, signer, err := notary.Verify(proof, c, []ed25519.PublicKey{pub})
	if err != nil {
		return false, err
	}
	if !signer.Equal(ed25519.PublicKey(pub)) || h.Seq != v.Seq || hex.EncodeToString(h.Hash[:]) != v.Head {
		return true, errors.New("verified, but with another signer, number or head")
	}
	return true, nil
}

func decodeHex(tb testing.TB, s string) []byte {
	b, err := hex.DecodeString(s)
	if err != nil {
		tb.Fatal(err)
	}
	return b
}

func ledger(t testing.TB) *book {
	b := &book{m: state.New(0, 0)}
	var accounts []byte
	for id := range uint64(3) {
		accounts = state.AppendAccount(accounts, state.Account{ID: u128.From64(id + 1), Ledger: 1, Code: 1})
	}
	b.apply(t, state.CreateAccounts, 1e9, accounts)

	var transfers []byte
	for i := range uint64(5) {
		tr := state.Transfer{ID: u128.From64(100 + i), DebitAccountID: u128.From64(1), CreditAccountID: u128.From64(2),
			Amount: u128.From64(10 + i), Ledger: 1, Code: 1}
		switch i {
		case 3:
			tr.Flags, tr.Timeout = state.Pending, 1
		case 4:
			tr.DebitAccountID = u128.From64(99)
		}
		transfers = state.AppendTransfer(transfers, tr)
	}
	b.apply(t, state.CreateTransfers, 2e9, transfers)

	b.apply(t, state.CreateTransfers, 9e9, state.AppendTransfer(nil, state.Transfer{ID: u128.From64(200),
		DebitAccountID: u128.From64(2), CreditAccountID: u128.From64(3), Amount: u128.From64(1), Ledger: 1, Code: 1}), 103)
	return b
}

func stamped(r audit.Receipt, c uint64, k ed25519.PrivateKey) []byte {
	proof := audit.AppendReceipt(nil, r)
	_, h, _, err := audit.Decode(proof)
	if err != nil {
		panic(err)
	}
	return append(proof, notary.Sign(k, c, h)...)
}

func flip(b []byte, at int) []byte {
	b = slices.Clone(b)
	b[at] ^= 1
	return b
}

func plusOrder(proof []byte) []byte {
	order, _ := new(big.Int).SetString("7237005577332262213973186563042994240857116359379907606001950938285454250989", 10)
	s := slices.Clone(proof[len(proof)-32:])
	slices.Reverse(s)
	sum := new(big.Int).Add(new(big.Int).SetBytes(s), order).FillBytes(make([]byte, 32))
	slices.Reverse(sum)
	return slices.Concat(proof[:len(proof)-32], sum)
}

func vectors(t testing.TB) []vector {
	b := ledger(t)
	accounts, transfers, expiry := b.batches[0], b.batches[1], b.batches[2]
	if transfers.link.Leaves != 4 || transfers.link.Events != 5 || expiry.link.Leaves != 2 {
		t.Fatalf("book has %d leaves of %d events and %d after the expiry", transfers.link.Leaves, transfers.link.Events, expiry.link.Leaves)
	}
	pub := public(key)
	good := stamped(transfers.receipt(2), cluster, key)
	head := append(audit.AppendHead(nil, b.head), notary.Sign(key, cluster, b.head)...)
	relabelled := stamped(transfers.receipt(2), otherCluster, key)
	binary.BigEndian.PutUint64(relabelled[len(relabelled)-notary.StampSize:], cluster)
	early := accounts.receipt(2)
	early.Prev[31] = 1
	before := append(audit.AppendReceipt(nil, early), notary.Sign(key, cluster, audit.Head{Hash: early.Prev}.Next(early.Link))...)

	cases := []struct {
		name  string
		proof []byte
		c     uint64
		k     ed25519.PublicKey
		want  error
	}{
		{"transfer, third of four leaves", good, cluster, pub, nil},
		{"account, last of three leaves", stamped(accounts.receipt(2), cluster, key), cluster, pub, nil},
		{"expiry of a pending transfer", stamped(expiry.receipt(0), cluster, key), cluster, pub, nil},
		{"batch with one failed event", stamped(transfers.whole(), cluster, key), cluster, pub, nil},
		{"single leaf after an expiry leaf", stamped(expiry.receipt(1), cluster, key), cluster, pub, nil},
		{"head of the ledger", head, cluster, pub, nil},
		{"amount changed", flip(good, 2+63), cluster, pub, audit.ErrBadReceipt},
		{"leaf index changed", flip(good, 2+100+3), cluster, pub, audit.ErrBadReceipt},
		{"path hash changed", flip(good, 2+100+5), cluster, pub, audit.ErrBadReceipt},
		{"previous head changed", flip(good, len(good)-notary.StampSize-audit.LinkSize-1), cluster, pub, notary.ErrSignature},
		{"link time changed", flip(good, len(good)-notary.StampSize-audit.LinkSize+7), cluster, pub, notary.ErrSignature},
		{"signature changed", flip(good, len(good)-64), cluster, pub, notary.ErrSignature},
		{"one byte short", good[:len(good)-1], cluster, pub, audit.ErrBadReceipt},
		{"one byte more", append(slices.Clone(good), 0), cluster, pub, audit.ErrBadReceipt},
		{"unknown version", flip(good, 0), cluster, pub, audit.ErrBadReceipt},
		{"checked against another cluster", good, otherCluster, pub, notary.ErrCluster},
		{"signed for another cluster", stamped(transfers.receipt(2), otherCluster, key), cluster, pub, notary.ErrCluster},
		{"signed for another cluster, stamp names this one", relabelled, cluster, pub, notary.ErrSignature},
		{"checked against another key", good, cluster, public(otherKey), notary.ErrUntrusted},
		{"signature with S plus the group order", plusOrder(good), cluster, pub, notary.ErrSignature},
		{"head with a changed hash", flip(head, 2+8+31), cluster, pub, notary.ErrSignature},
		{"receipt without a stamp", good[:len(good)-notary.StampSize], cluster, pub, notary.ErrUnsigned},
		{"first link with a head before it", before, cluster, pub, audit.ErrBadReceipt},
	}
	var vs []vector
	for _, c := range cases {
		v := vector{Name: c.name, Proof: hex.EncodeToString(c.proof), Cluster: clusterHex(c.c), Key: hex.EncodeToString(c.k), Valid: c.want == nil}
		if v.Valid {
			_, h, _, err := audit.Decode(c.proof)
			if err != nil {
				t.Fatalf("%s: %v", c.name, err)
			}
			v.Seq, v.Head = h.Seq, hex.EncodeToString(h.Hash[:])
		}
		if _, err := check(v); !errors.Is(err, c.want) {
			t.Fatalf("%s: %v, want %v", c.name, err, c.want)
		}
		vs = append(vs, v)
	}
	return vs
}

func TestVectors(t *testing.T) {
	built := vectors(t)
	const path = "testdata/vectors.json"
	if *update {
		out, err := json.MarshalIndent(built, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, append(out, '\n'), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	text, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var pinned []vector
	if err := json.Unmarshal(text, &pinned); err != nil {
		t.Fatal(err)
	}
	var valid int
	for _, v := range pinned {
		ok, err := check(v)
		if ok != v.Valid || (ok && err != nil) {
			t.Errorf("%s: valid %v, file says %v (%v)", v.Name, ok, v.Valid, err)
		}
		if ok {
			valid++
		}
	}
	if valid < 6 || len(pinned)-valid < 17 {
		t.Errorf("%d valid and %d invalid vectors", valid, len(pinned)-valid)
	}
	if !reflect.DeepEqual(pinned, built) {
		t.Errorf("%s differs from the vectors of the fixed book", path)
	}
}
