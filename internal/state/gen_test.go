package state_test

import (
	"encoding/binary"
	"math"
	"math/rand/v2"
	"slices"

	"github.com/Demonisreal/dmn-ledger/internal/state"
	"github.com/Demonisreal/dmn-ledger/internal/u128"
)

type batch struct {
	op       state.Op
	ts       uint64
	body     []byte
	ids      []u128.Uint128
	pendings []int
}

type gen struct {
	r      *rand.Rand
	ts     uint64
	nextID uint64

	accts    []state.Account
	sent     []state.Transfer
	pendings []state.Transfer

	maxAccounts int
	ledgers     uint32
	maxBatch    int
	maxStep     uint64
	linkedPct   int
	limitPct    int
	pendingPct  int
	huge        bool
	coarse      bool
}

func newGen(r *rand.Rand) *gen {
	g := &gen{
		r:           r,
		ts:          r.Uint64N(1e9),
		maxAccounts: 2 + r.IntN(24),
		ledgers:     1 + r.Uint32N(3),
		maxBatch:    1 + r.IntN(64),
		maxStep:     1 + r.Uint64N(3e9),
		linkedPct:   r.IntN(25),
		limitPct:    r.IntN(60),
		pendingPct:  10 + r.IntN(40),
		huge:        r.IntN(4) == 0,
		coarse:      r.IntN(3) == 0,
	}
	if r.IntN(16) == 0 {
		g.ts = 1<<63 - r.Uint64N(600e9)
	}
	return g
}

func (g *gen) batch() batch {
	r := g.r
	g.ts += r.Uint64N(g.maxStep)
	if g.coarse {
		g.ts = g.ts/1e9*1e9 + r.Uint64N(16)
	}
	if r.IntN(20) == 0 {
		g.ts -= min(g.ts, r.Uint64N(2e9))
	}
	b := batch{op: state.CreateTransfers, ts: g.ts}
	if len(g.accts) < 2 || (len(g.accts) < g.maxAccounts && r.IntN(6) == 0) || r.IntN(40) == 0 {
		b.op = state.CreateAccounts
	}
	for i := range r.IntN(g.maxBatch + 1) {
		if b.op == state.CreateAccounts {
			a := g.account()
			b.body = state.AppendAccount(b.body, a)
			b.ids = append(b.ids, a.ID)
			continue
		}
		before := len(g.pendings)
		tr := g.transfer()
		if len(g.pendings) > before {
			b.pendings = append(b.pendings, i)
		}
		b.body = state.AppendTransfer(b.body, tr)
		b.ids = append(b.ids, tr.ID, tr.DebitAccountID, tr.CreditAccountID, tr.PendingID)
	}
	return b
}

func (g *gen) settle(b batch, fails []state.Failure) {
	base := len(g.pendings) - len(b.pendings)
	kept := g.pendings[:base]
	for k, i := range b.pendings {
		if !slices.ContainsFunc(fails, func(f state.Failure) bool { return f.Index == i }) {
			kept = append(kept, g.pendings[base+k])
		}
	}
	g.pendings = kept
}

func (g *gen) id() u128.Uint128 {
	g.nextID++
	return u128.Uint128{Hi: g.r.Uint64N(3) << 62, Lo: g.nextID}
}

func (g *gen) account() state.Account {
	r := g.r
	var a state.Account
	if len(g.accts) >= g.maxAccounts || len(g.accts) > 0 && r.IntN(5) == 0 {
		a = g.accts[r.IntN(len(g.accts))]
		if r.IntN(3) == 0 {
			a.Code++
		}
	} else {
		a = state.Account{ID: g.id(), Ledger: 1 + r.Uint32N(g.ledgers), Code: uint16(1 + r.IntN(3))}
		switch n := r.IntN(100); {
		case n < g.limitPct/2:
			a.Flags = state.DebitsMustNotExceedCredits
		case n < g.limitPct:
			a.Flags = state.CreditsMustNotExceedDebits
		}
		g.accts = append(g.accts, a)
	}

	switch r.IntN(150) {
	case 0:
		a.ID = u128.Uint128{}
	case 1:
		a.ID = u128.Max()
	case 2:
		a.Flags |= state.AccountFlags(1) << (2 + r.IntN(14))
	case 3:
		a.Flags = state.DebitsMustNotExceedCredits | state.CreditsMustNotExceedDebits
	case 4:
		a.Ledger = 0
	case 5:
		a.Code = 0
	case 6:
		a.DebitsPending = u128.From64(1)
	case 7:
		a.CreditsPosted = u128.Max()
	case 8:
		a.Timestamp = 1 + r.Uint64N(1e12)
	}
	return a
}

func (g *gen) amount() u128.Uint128 {
	r := g.r
	if g.huge {
		switch r.IntN(6) {
		case 0:
			return u128.Max().SubWrap(u128.From64(r.Uint64N(1000)))
		case 1:
			return u128.Uint128{Hi: 1<<63 - r.Uint64N(4), Lo: r.Uint64()}
		}
	}
	return u128.From64(1 + r.Uint64N(100))
}

func (g *gen) pickAccount() state.Account {
	if g.r.IntN(30) == 0 {
		return state.Account{ID: g.id(), Ledger: 1}
	}
	return g.accts[g.r.IntN(len(g.accts))]
}

func (g *gen) pair() (dr, cr state.Account) {
	dr = g.pickAccount()
	for range 8 {
		cr = g.pickAccount()
		if cr.ID != dr.ID && (cr.Ledger == dr.Ledger || g.r.IntN(40) == 0) {
			break
		}
	}
	return dr, cr
}

func (g *gen) transfer() state.Transfer {
	r := g.r
	var tr state.Transfer
	switch {
	case len(g.sent) > 0 && r.IntN(12) == 0:
		tr = g.sent[r.IntN(len(g.sent))]
		tr.Flags &^= state.Linked
		if r.IntN(3) == 0 {
			tr.Amount = tr.Amount.AddWrap(u128.From64(1))
		}
	case len(g.pendings) > 0 && r.IntN(100) < g.pendingPct:
		p := g.pendings[len(g.pendings)-1-r.IntN(min(len(g.pendings), 16))]
		tr = state.Transfer{ID: g.id(), PendingID: p.ID, Flags: state.PostPending}
		if r.IntN(3) == 0 {
			tr.Flags = state.VoidPending
		}
		if r.IntN(3) == 0 {
			tr.DebitAccountID, tr.CreditAccountID = p.DebitAccountID, p.CreditAccountID
			tr.Ledger, tr.Code = p.Ledger, p.Code
		}
		switch r.IntN(5) {
		case 0:
			tr.Amount = p.Amount
		case 1:
			if p.Amount.Cmp(u128.From64(1)) > 0 {
				tr.Amount = p.Amount.SubWrap(u128.From64(1 + r.Uint64N(2)))
			}
		case 2:
			tr.Amount = p.Amount.AddWrap(u128.From64(1))
		}
	default:
		dr, cr := g.pair()
		tr = state.Transfer{
			ID:              g.id(),
			DebitAccountID:  dr.ID,
			CreditAccountID: cr.ID,
			Amount:          g.amount(),
			Ledger:          dr.Ledger,
			Code:            uint16(1 + r.IntN(3)),
		}
		if r.IntN(100) < g.pendingPct {
			tr.Flags = state.Pending
			switch k := r.IntN(30); {
			case k == 0:
				tr.Timeout = math.MaxUint32 - r.Uint32N(3)
			case k > 10:
				tr.Timeout = 1 + r.Uint32N(5)
			}
			g.pendings = append(g.pendings, tr)
		}
	}

	if r.IntN(100) < g.linkedPct {
		tr.Flags |= state.Linked
	}
	switch r.IntN(300) {
	case 0:
		tr.ID = u128.Uint128{}
	case 1:
		tr.ID = u128.Max()
	case 2:
		tr.Flags |= state.TransferFlags(1) << (4 + r.IntN(12))
	case 3:
		tr.Flags |= state.Pending << r.IntN(3)
	case 4:
		tr.DebitAccountID = u128.Uint128{}
	case 5:
		tr.CreditAccountID = u128.Uint128{}
	case 6:
		tr.CreditAccountID = tr.DebitAccountID
	case 7:
		tr.PendingID = u128.Uint128{}
	case 8:
		if len(g.sent) > 0 {
			tr.PendingID = g.sent[r.IntN(len(g.sent))].ID
		}
	case 9:
		tr.Timeout = 1
	case 10:
		tr.Ledger = 0
	case 11:
		tr.Ledger++
	case 12:
		tr.Code = 0
	case 13:
		tr.Code++
	case 14:
		tr.Amount = u128.Uint128{}
	case 15:
		tr.Timestamp = 1 + r.Uint64N(1e12)
	case 16:
		tr.DebitAccountID = g.pickAccount().ID
	}

	if len(g.sent) < 256 {
		g.sent = append(g.sent, tr)
	} else {
		g.sent[r.IntN(len(g.sent))] = tr
	}
	return tr
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
