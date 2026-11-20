package ledger

import (
	"context"
	"crypto/ed25519"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"

	"github.com/Demonisreal/dmn-ledger/internal/audit"
	"github.com/Demonisreal/dmn-ledger/internal/client"
	"github.com/Demonisreal/dmn-ledger/internal/notary"
	"github.com/Demonisreal/dmn-ledger/internal/u128"
)

var (
	ErrBadProof = errors.New("ledger: proof does not hold")
	ErrUnsigned = errors.New("ledger: proof carries no stamp")
	ErrConflict = errors.New("ledger: the heads contradict each other")
	ErrBadLinks = errors.New("ledger: the links do not connect the heads")
)

type ProofKind uint8

const (
	ProofBatch ProofKind = iota
	ProofAccount
	ProofTransfer
	ProofExpiry
	ProofHead
)

var proofNames = [...]string{
	ProofBatch:    "batch",
	ProofAccount:  "account",
	ProofTransfer: "transfer",
	ProofExpiry:   "expiry",
	ProofHead:     "head",
}

func (k ProofKind) String() string {
	if int(k) >= len(proofNames) {
		return fmt.Sprintf("ProofKind(%d)", uint8(k))
	}
	return proofNames[k]
}

type Proof struct {
	Kind     ProofKind
	Account  Account
	Transfer Transfer
	Time     uint64
	HeadSeq  uint64
	Head     [32]byte
	Key      []byte
}

func (c *Client) Prove(ctx context.Context, kind ProofKind, id Uint128) ([]byte, error) {
	if kind != ProofAccount && kind != ProofTransfer && kind != ProofExpiry {
		return nil, fmt.Errorf("ledger: Prove takes an account, a transfer or an expiry, not a %s", kind)
	}
	return c.receipt(ctx, client.Prove, u128.Uint128(id).AppendBytes([]byte{byte(kind)}))
}

func (c *Client) ProveBatch(ctx context.Context, timestamp uint64) ([]byte, error) {
	return c.receipt(ctx, client.Prove, binary.BigEndian.AppendUint64([]byte{byte(ProofBatch)}, timestamp))
}

func (c *Client) Head(ctx context.Context) ([]byte, error) {
	return c.receipt(ctx, client.Head, nil)
}

func (c *Client) receipt(ctx context.Context, op client.Op, body []byte) ([]byte, error) {
	o := c.do(ctx, &call{op: op, body: body})
	if o.err != nil || len(o.body) == 0 {
		return nil, o.err
	}
	return o.body, nil
}

func (c *Client) Links(ctx context.Context, from, to uint64) ([]byte, error) {
	if from > to {
		return nil, fmt.Errorf("ledger: links from %d to %d", from, to)
	}
	var links []byte
	for from < to {
		o := c.do(ctx, &call{op: client.Links, body: binary.BigEndian.AppendUint64(binary.BigEndian.AppendUint64(nil, from), to)})
		if o.err != nil {
			return links, o.err
		}
		links = append(links, o.body...)
		got := uint64(len(o.body) / audit.LinkSize)
		if got < client.MaxLinks {
			break
		}
		from += got
	}
	return links, nil
}

func VerifyProof(proof []byte, cluster uint64, keys [][]byte) (Proof, error) {
	trusted := make([]ed25519.PublicKey, len(keys))
	for i, k := range keys {
		trusted[i] = k
	}
	r, h, key, err := notary.Verify(proof, cluster, trusted)
	if errors.Is(err, notary.ErrUnsigned) {
		r, h, _, _ = audit.Decode(proof)
		return proofOf(r, h, nil), ErrUnsigned
	}
	if err != nil {
		return Proof{}, fmt.Errorf("%w: %w", ErrBadProof, err)
	}
	return proofOf(r, h, slices.Clone(key)), nil
}

func proofOf(r audit.Receipt, h audit.Head, key []byte) Proof {
	p := Proof{Kind: ProofKind(r.Kind), HeadSeq: h.Seq, Head: h.Hash, Key: key}
	if r.Kind != audit.KindHead {
		p.Time = r.Link.Time
	}
	switch r.Kind {
	case audit.KindAccount:
		p.Account = decodeAccounts(r.Record)[0]
	case audit.KindTransfer, audit.KindExpiry:
		p.Transfer = decodeTransfers(r.Record)[0]
	}
	return p
}

func ConnectProofs(older, newer Proof, links []byte) error {
	switch {
	case older.HeadSeq > newer.HeadSeq:
		return fmt.Errorf("ledger: head %d comes after head %d", older.HeadSeq, newer.HeadSeq)
	case older.HeadSeq == newer.HeadSeq && older.Head != newer.Head:
		return fmt.Errorf("%w: head %d is %x in one and %x in the other", ErrConflict, older.HeadSeq, older.Head[:8], newer.Head[:8])
	case len(links)%audit.LinkSize != 0 || uint64(len(links)/audit.LinkSize) != newer.HeadSeq-older.HeadSeq:
		return fmt.Errorf("%w: %d bytes of links between heads %d and %d, want %d links", ErrBadLinks, len(links), older.HeadSeq, newer.HeadSeq, newer.HeadSeq-older.HeadSeq)
	}
	h := audit.Head{Seq: older.HeadSeq, Hash: older.Head}
	for ; len(links) > 0; links = links[audit.LinkSize:] {
		h = h.Next(audit.DecodeLink(links))
	}
	if h.Hash != newer.Head {
		return fmt.Errorf("%w: head %d %x does not lead to head %d %x over these links", ErrBadLinks, older.HeadSeq, older.Head[:8], newer.HeadSeq, newer.Head[:8])
	}
	return nil
}
