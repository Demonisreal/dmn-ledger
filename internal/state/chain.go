package state

import (
	"cmp"
	"sort"

	"github.com/Demonisreal/dmn-ledger/internal/audit"
	"github.com/Demonisreal/dmn-ledger/internal/u128"
)

const (
	linkBits  = 8
	linkBlock = 1 << linkBits
)

var (
	bodyPrefix  = []byte{0x03}
	replyPrefix = []byte{0x04}
)

type sealed struct {
	link audit.Link
	head audit.Hash
}

func (m *Machine) sealedAt(k uint64) *sealed {
	return &m.links[k>>linkBits][k&(linkBlock-1)]
}

func (m *Machine) seal(l audit.Link, body []byte, fails []Failure) {
	m.hash.Reset()
	_, _ = m.hash.Write(bodyPrefix)
	_, _ = m.hash.Write(body)
	l.Body = audit.Hash(m.hash.Sum(m.sum[:0]))
	l.Reply = m.answered(fails)
	l.Leaves, l.Root = m.tree.Len(), m.tree.Root()
	m.link(l)
}

func (m *Machine) link(l audit.Link) {
	if m.nlinks>>linkBits == uint64(len(m.links)) {
		m.links = append(m.links, new([linkBlock]sealed))
	}
	*m.sealedAt(m.nlinks) = sealed{link: l, head: chained(m.Head(), l).Hash}
	m.nlinks++
}

func opened(a Account) Account {
	a.DebitsPending, a.DebitsPosted, a.CreditsPending, a.CreditsPosted = u128.Uint128{}, u128.Uint128{}, u128.Uint128{}, u128.Uint128{}
	return a
}

func (m *Machine) lapseOrder(i, j int) int {
	a, b := m.transfer(i), m.transfer(j)
	return cmp.Or(cmp.Compare(a.expiresAt(), b.expiresAt()), a.ID.Cmp(b.ID))
}

func (m *Machine) Head() audit.Head {
	h, _ := m.HeadAt(m.nlinks)
	return h
}

func (m *Machine) HeadAt(seq uint64) (audit.Head, bool) {
	switch {
	case seq > m.nlinks:
		return audit.Head{}, false
	case seq == 0:
		return audit.Head{}, true
	}
	return audit.Head{Seq: seq, Hash: m.sealedAt(seq - 1).head}, true
}

func (m *Machine) Links(from, to uint64, dst []audit.Link) []audit.Link {
	for k := from; k < min(to, m.nlinks); k++ {
		dst = append(dst, m.sealedAt(k).link)
	}
	return dst
}

type batchLeaves struct {
	hashes          []audit.Hash
	lapsed, nlapsed int
	first           int
}

func (m *Machine) leaves(k uint64) batchLeaves {
	var rec [TransferSize]byte
	l := m.sealedAt(k).link
	var after uint64
	if k > 0 {
		after = m.sealedAt(k - 1).link.Time
	}
	deadline := func(x int) uint64 { return m.transfer(m.expired[x]).expiresAt() }

	var b batchLeaves
	b.lapsed = sort.Search(len(m.expired), func(x int) bool { return deadline(x) > after })
	for x := b.lapsed; x < len(m.expired) && deadline(x) <= l.Time; x++ {
		b.hashes = append(b.hashes, audit.Leaf(audit.KindExpiry, AppendTransfer(rec[:0], m.transfer(m.expired[x]).Transfer)))
	}
	b.nlapsed = len(b.hashes)

	end := l.Time + max(uint64(l.Events), 1)
	if l.Op == uint8(CreateAccounts) {
		b.first = sort.Search(len(m.accounts), func(i int) bool { return m.accounts[i].Timestamp >= l.Time })
		for i := b.first; i < len(m.accounts) && m.accounts[i].Timestamp < end; i++ {
			b.hashes = append(b.hashes, audit.Leaf(audit.KindAccount, AppendAccount(rec[:0], opened(m.accounts[i]))))
		}
		return b
	}
	b.first = sort.Search(m.ntransfers, func(i int) bool { return m.transfer(i).Timestamp >= l.Time })
	for i := b.first; i < m.ntransfers && m.transfer(i).Timestamp < end; i++ {
		b.hashes = append(b.hashes, audit.Leaf(audit.KindTransfer, AppendTransfer(rec[:0], m.transfer(i).Transfer)))
	}
	return b
}

func (m *Machine) linkFrom(ts uint64) uint64 {
	lo, hi := uint64(0), m.nlinks
	for lo < hi {
		mid := lo + (hi-lo)/2
		if m.sealedAt(mid).link.Time >= ts {
			hi = mid
		} else {
			lo = mid + 1
		}
	}
	return lo
}

func (m *Machine) receipt(k uint64, kind byte, record []byte, index int, hashes []audit.Hash) audit.Receipt {
	prev, _ := m.HeadAt(k)
	return audit.Receipt{
		Kind:   kind,
		Record: record,
		Index:  uint32(index), //nolint:gosec // G115: a leaf index, below Link.Leaves
		Path:   audit.Path(hashes, index),
		Seq:    k + 1,
		Prev:   prev.Hash,
		Link:   m.sealedAt(k).link,
	}
}

func (m *Machine) ProveAccount(id u128.Uint128) (audit.Receipt, bool) {
	i, ok := m.accountIdx[id]
	if !ok {
		return audit.Receipt{}, false
	}
	k := m.linkFrom(m.accounts[i].Timestamp+1) - 1
	b := m.leaves(k)
	return m.receipt(k, audit.KindAccount, AppendAccount(nil, opened(m.accounts[i])), b.nlapsed+i-b.first, b.hashes), true
}

func (m *Machine) ProveTransfer(id u128.Uint128) (audit.Receipt, bool) {
	i, ok := m.transferIdx[id]
	if !ok {
		return audit.Receipt{}, false
	}
	tr := m.transfer(i)
	k := m.linkFrom(tr.Timestamp+1) - 1
	b := m.leaves(k)
	return m.receipt(k, audit.KindTransfer, AppendTransfer(nil, tr.Transfer), b.nlapsed+i-b.first, b.hashes), true
}

func (m *Machine) ProveExpiry(id u128.Uint128) (audit.Receipt, bool) {
	i, ok := m.transferIdx[id]
	if !ok || m.transfer(i).status != statusExpired {
		return audit.Receipt{}, false
	}
	tr := m.transfer(i)
	k := m.linkFrom(tr.expiresAt())
	b := m.leaves(k)
	x := b.lapsed
	for m.expired[x] != i {
		x++
	}
	return m.receipt(k, audit.KindExpiry, AppendTransfer(nil, tr.Transfer), x-b.lapsed, b.hashes), true
}

func (m *Machine) ProveBatch(time uint64) (audit.Receipt, bool) {
	k := m.linkFrom(time)
	if k == m.nlinks || m.sealedAt(k).link.Time != time {
		return audit.Receipt{}, false
	}
	prev, _ := m.HeadAt(k)
	return audit.Receipt{Kind: audit.KindBatch, Seq: k + 1, Prev: prev.Hash, Link: m.sealedAt(k).link}, true
}
