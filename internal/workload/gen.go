package workload

import (
	"math"
	"math/rand/v2"
	"slices"

	"github.com/Demonisreal/dmn-ledger/internal/state"
	"github.com/Demonisreal/dmn-ledger/internal/u128"
)

type Batch struct {
	Op   state.Op
	TS   uint64
	Body []byte
	IDs  []u128.Uint128

	pendings   []int
	pendingIDs []u128.Uint128
}

type Gen struct {
	r      *rand.Rand
	ts     uint64
	nextID uint64

	accts    []state.Account
	sent     []state.Transfer
	pendings []state.Transfer

	MaxAccounts int
	Ledgers     uint32
	MaxBatch    int
	MaxStep     uint64
	LinkedPct   int
	LimitPct    int
	PendingPct  int
	Huge        bool
	Coarse      bool
}

func New(r *rand.Rand) *Gen {
	g := &Gen{
		r:           r,
		ts:          r.Uint64N(1e9),
		MaxAccounts: 2 + r.IntN(24),
		Ledgers:     1 + r.Uint32N(3),
		MaxBatch:    1 + r.IntN(64),
		MaxStep:     1 + r.Uint64N(3e9),
		LinkedPct:   r.IntN(25),
		LimitPct:    r.IntN(60),
		PendingPct:  10 + r.IntN(40),
		Huge:        r.IntN(4) == 0,
		Coarse:      r.IntN(3) == 0,
	}
	if r.IntN(16) == 0 {
		g.ts = 1<<63 - r.Uint64N(600e9)
	}
	if r.IntN(8) == 0 {
		g.LinkedPct = 50 + r.IntN(41)
	}
	return g
}

func (g *Gen) Batch() Batch {
	r := g.r
	g.ts += r.Uint64N(g.MaxStep)
	if g.Coarse {
		g.ts = g.ts/1e9*1e9 + r.Uint64N(16)
	}
	if r.IntN(20) == 0 {
		g.ts -= min(g.ts, r.Uint64N(2e9))
	}
	b := Batch{Op: state.CreateTransfers, TS: g.ts}
	if len(g.accts) < 2 || (len(g.accts) < g.MaxAccounts && r.IntN(6) == 0) || r.IntN(40) == 0 {
		b.Op = state.CreateAccounts
	}
	for i := range r.IntN(g.MaxBatch + 1) {
		if b.Op == state.CreateAccounts {
			a := g.account()
			b.Body = state.AppendAccount(b.Body, a)
			b.IDs = append(b.IDs, a.ID)
			continue
		}
		before := len(g.pendings)
		tr := g.transfer()
		if len(g.pendings) > before {
			b.pendings = append(b.pendings, i)
			b.pendingIDs = append(b.pendingIDs, g.pendings[before].ID)
		}
		b.Body = state.AppendTransfer(b.Body, tr)
		b.IDs = append(b.IDs, tr.ID, tr.DebitAccountID, tr.CreditAccountID, tr.PendingID)
	}
	return b
}

func (g *Gen) Settle(b Batch, fails []state.Failure) {
	for k, i := range b.pendings {
		if slices.ContainsFunc(fails, func(f state.Failure) bool { return f.Index == i }) {
			id := b.pendingIDs[k]
			g.pendings = slices.DeleteFunc(g.pendings, func(p state.Transfer) bool { return p.ID == id })
		}
	}
}

func (g *Gen) Lookup() (accounts bool, ids []u128.Uint128) {
	r := g.r
	accounts = len(g.sent) == 0 || r.IntN(2) == 0
	for range 1 + r.IntN(8) {
		switch {
		case r.IntN(10) == 0:
			ids = append(ids, g.id())
		case accounts && len(g.accts) > 0:
			ids = append(ids, g.accts[r.IntN(len(g.accts))].ID)
		case !accounts:
			ids = append(ids, g.sent[r.IntN(len(g.sent))].ID)
		}
	}
	return accounts, ids
}

func (g *Gen) id() u128.Uint128 {
	g.nextID++
	return u128.Uint128{Hi: g.r.Uint64N(3) << 62, Lo: g.nextID}
}

func (g *Gen) account() state.Account {
	r := g.r
	var a state.Account
	if len(g.accts) >= g.MaxAccounts || len(g.accts) > 0 && r.IntN(5) == 0 {
		a = g.accts[r.IntN(len(g.accts))]
		if r.IntN(3) == 0 {
			a.Code++
		}
	} else {
		a = state.Account{ID: g.id(), Ledger: 1 + r.Uint32N(g.Ledgers), Code: uint16(1 + r.IntN(3))} //nolint:gosec
		switch n := r.IntN(100); {
		case n < g.LimitPct/2:
			a.Flags = state.DebitsMustNotExceedCredits
		case n < g.LimitPct:
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

func (g *Gen) amount() u128.Uint128 {
	r := g.r
	if g.Huge {
		switch r.IntN(6) {
		case 0:
			return u128.Max().SubWrap(u128.From64(r.Uint64N(1000)))
		case 1:
			return u128.Uint128{Hi: 1<<63 - r.Uint64N(4), Lo: r.Uint64()}
		}
	}
	return u128.From64(1 + r.Uint64N(100))
}

func (g *Gen) pickAccount() state.Account {
	if g.r.IntN(30) == 0 {
		return state.Account{ID: g.id(), Ledger: 1}
	}
	return g.accts[g.r.IntN(len(g.accts))]
}

func (g *Gen) pair() (dr, cr state.Account) {
	dr = g.pickAccount()
	for range 8 {
		cr = g.pickAccount()
		if cr.ID != dr.ID && (cr.Ledger == dr.Ledger || g.r.IntN(40) == 0) {
			break
		}
	}
	return dr, cr
}

func (g *Gen) transfer() state.Transfer {
	r := g.r
	var tr state.Transfer
	switch {
	case len(g.sent) > 0 && r.IntN(12) == 0:
		tr = g.sent[r.IntN(len(g.sent))]
		tr.Flags &^= state.Linked
		if r.IntN(3) == 0 {
			tr.Amount = tr.Amount.AddWrap(u128.From64(1))
		}
	case len(g.pendings) > 0 && r.IntN(100) < g.PendingPct:
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
			Code:            uint16(1 + r.IntN(3)), //nolint:gosec
		}
		if r.IntN(100) < g.PendingPct {
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

	if r.IntN(100) < g.LinkedPct {
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
