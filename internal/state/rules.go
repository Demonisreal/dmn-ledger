//go:build !simbug

package state

import (
	"encoding/binary"

	"github.com/Demonisreal/dmn-ledger/internal/audit"
)

func leafOf(kind byte, record []byte) audit.Hash {
	return audit.Leaf(kind, record)
}

func (m *Machine) answered(fails []Failure) audit.Hash {
	m.hash.Reset()
	m.hashw.Reset(m.hash)
	_, _ = m.hashw.Write(replyPrefix)
	for _, f := range fails {
		b := binary.BigEndian.AppendUint32(m.rec[:0], uint32(f.Index)) //nolint:gosec // G115: an index into the batch
		_, _ = m.hashw.Write(binary.BigEndian.AppendUint32(b, uint32(f.Reason)))
	}
	_ = m.hashw.Flush()
	return audit.Hash(m.hash.Sum(m.sum[:0]))
}

func chained(prev audit.Head, l audit.Link) audit.Head {
	return prev.Next(l)
}

func (m *Machine) lapse(i int) {
	m.tree.Add(leafOf(audit.KindExpiry, AppendTransfer(m.rec[:0], m.transfer(i).Transfer)))
	m.expired = append(m.expired, i)
}
