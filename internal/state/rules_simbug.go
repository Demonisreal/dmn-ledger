//go:build simbug

package state

import (
	"crypto/sha256"
	"encoding/binary"

	"github.com/Demonisreal/dmn-ledger/internal/audit"
	"github.com/Demonisreal/dmn-ledger/internal/simbug"
)

func leafOf(kind byte, record []byte) audit.Hash {
	if simbug.On("B19") {
		var buf [1 + TransferSize]byte
		return sha256.Sum256(append(append(buf[:0], kind), record...))
	}
	return audit.Leaf(kind, record)
}

func (m *Machine) answered(fails []Failure) audit.Hash {
	if simbug.On("B20") {
		return audit.Hash{}
	}
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
	if simbug.On("B21") {
		prev.Hash = audit.Hash{}
	}
	return prev.Next(l)
}

func (m *Machine) lapse(i int) {
	if !simbug.On("B22") {
		m.tree.Add(leafOf(audit.KindExpiry, AppendTransfer(m.rec[:0], m.transfer(i).Transfer)))
	}
	m.expired = append(m.expired, i)
}
