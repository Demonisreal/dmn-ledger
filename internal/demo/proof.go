//go:build linux

package demo

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strconv"
	"time"

	"github.com/Demonisreal/dmn-ledger/ledger"
)

const proveWithin = 2 * time.Second

func (s *Stage) Proof(ctx context.Context) (Proof, error) {
	s.mu.Lock()
	r := s.cur
	if r == nil || r.found != nil {
		s.mu.Unlock()
		if r != nil {
			return Proof{}, ErrFinding
		}
		return Proof{}, ErrEmpty
	}
	id := r.provable
	if id == 0 {
		id = r.latest
	}
	if id == 0 || !r.ready {
		s.mu.Unlock()
		return Proof{}, ErrEmpty
	}
	tr := r.sent[id-1]
	s.mu.Unlock()
	ctx, cancel := context.WithTimeout(ctx, proveWithin)
	defer cancel()
	raw, err := r.teller.Prove(ctx, ledger.ProofTransfer, ledger.From64(id))
	if err != nil {
		return Proof{}, fmt.Errorf("%w: %v", ErrNoLeader, err)
	}
	keys := r.c.Keys()
	p, err := ledger.VerifyProof(raw, r.id, keys)
	if err != nil {
		s.fail(r, "proof", fmt.Sprintf(failProof, id, err))
		return Proof{}, ErrFinding
	}
	got := p.Transfer
	if p.Kind != ledger.ProofTransfer || got.ID != ledger.From64(id) || got.DebitAccountID != ledger.From64(uint64(tr.debit)) ||
		got.CreditAccountID != ledger.From64(uint64(tr.credit)) || got.Amount != ledger.From64(uint64(tr.amount)) {
		s.fail(r, "proof", fmt.Sprintf(failProofOther, id, got.ID, got.DebitAccountID, got.CreditAccountID, got.Amount, id, tr.debit, tr.credit, tr.amount))
		return Proof{}, ErrFinding
	}
	pf := Proof{
		At:       time.Now().UnixMilli(),
		Cluster:  fmt.Sprintf("%016x", r.id),
		Transfer: Booking{ID: strconv.FormatUint(id, 10), Debit: int(tr.debit), Credit: int(tr.credit), Amount: strconv.FormatUint(uint64(tr.amount), 10), At: int64(got.Timestamp / uint64(time.Millisecond))}, //nolint:gosec // G115: ledger time is nanoseconds of the wall clock
		Receipt:  hex.EncodeToString(raw),
		Base64:   base64.StdEncoding.EncodeToString(raw),
		Head:     Head{Seq: p.HeadSeq, Hash: hex.EncodeToString(p.Head[:])},
	}
	for i, k := range keys {
		pf.Keys[i] = hex.EncodeToString(k)
		if bytes.Equal(k, p.Key) {
			pf.Head.Signer = i + 1
		}
	}
	return pf, nil
}
