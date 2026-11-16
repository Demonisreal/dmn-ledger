//go:build chaos && linux

package chaos

import (
	"bytes"
	"context"
	"maps"
	"slices"

	"github.com/Demonisreal/dmn-ledger/internal/u128"
	"github.com/Demonisreal/dmn-ledger/ledger"
)

const provedTransfers, provedAccounts, tries = 16, 4, 256

func spread(ids map[u128.Uint128]bool, most int) []u128.Uint128 {
	sorted := slices.SortedFunc(maps.Keys(ids), u128.Uint128.Cmp)
	step := max(1, len(sorted)/most)
	var picked []u128.Uint128
	for i := 0; i < len(sorted) && len(picked) < most; i += step {
		picked = append(picked, sorted[i])
	}
	return picked
}

func (h *harness) prove(cl *ledger.Client, sts []*ledger.NodeStatus) bool {
	top := sts[0]
	for i, st := range sts {
		if st.HeadSeq != top.HeadSeq || st.Head != top.Head {
			h.fail("head", "n%d and n%d applied up to %d with heads %d %x and %d %x", top.ID, st.ID, st.Applied, top.HeadSeq, top.Head[:8], st.HeadSeq, st.Head[:8])
			return false
		}
		if !bytes.Equal(st.Key, h.keys[i]) {
			h.fail("key", "n%d shows key %x, it was started with %x", st.ID, st.Key, h.keys[i])
			return false
		}
	}
	ctx, cancel := context.WithTimeout(h.ctx, answerWithin)
	defer cancel()
	raw, err := cl.Head(ctx)
	if err != nil {
		h.fail("proof", "head: %v", err)
		return false
	}
	head, err := ledger.VerifyProof(raw, clusterID, h.keys)
	if err != nil || head.HeadSeq != top.HeadSeq || head.Head != top.Head {
		h.fail("proof", "head %d %x, %v; the nodes show %d %x", head.HeadSeq, head.Head[:8], err, top.HeadSeq, top.Head[:8])
		return false
	}
	h.mu.Lock()
	transfers, accounts := spread(h.transferIDs, tries), spread(h.accountIDs, tries)
	h.mu.Unlock()
	check := func(kind ledger.ProofKind, id u128.Uint128) (bool, bool) {
		raw, err := cl.Prove(ctx, kind, ledger.Uint128(id))
		if err != nil {
			h.fail("proof", "%s %v: %v", kind, id, err)
			return false, false
		}
		if raw == nil {
			return false, true
		}
		p, err := ledger.VerifyProof(raw, clusterID, h.keys)
		if err != nil {
			h.fail("proof", "%s %v: %v", kind, id, err)
			return false, false
		}
		proved := p.Transfer.ID
		if kind == ledger.ProofAccount {
			proved = p.Account.ID
		}
		if proved != ledger.Uint128(id) || p.Kind != kind {
			h.fail("proof", "asked for %s %v, the receipt is for %s %v", kind, id, p.Kind, proved)
			return false, false
		}
		links, err := cl.Links(ctx, p.HeadSeq, head.HeadSeq)
		if err != nil {
			h.fail("proof", "links %d to %d: %v", p.HeadSeq, head.HeadSeq, err)
			return false, false
		}
		if err := ledger.ConnectProofs(p, head, links); err != nil {
			h.fail("proof", "%s %v at link %d: %v", kind, id, p.HeadSeq, err)
			return false, false
		}
		h.receipts++
		return true, true
	}
	for _, try := range []struct {
		kind ledger.ProofKind
		ids  []u128.Uint128
		want int
	}{{ledger.ProofTransfer, transfers, provedTransfers}, {ledger.ProofAccount, accounts, provedAccounts}} {
		found := 0
		for _, id := range try.ids {
			proved, ok := check(try.kind, id)
			if !ok {
				return false
			}
			if proved {
				found++
			}
			if found == try.want {
				break
			}
		}
	}
	return true
}
