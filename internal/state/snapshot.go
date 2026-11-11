package state

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

const snapshotVersion = 1

var ErrBadSnapshot = errors.New("bad snapshot")

func (m *Machine) encode(w *bufio.Writer) error {
	var buf [TransferSize + 1]byte
	accts, trs := m.sortedAccounts(), m.sortedTransfers()

	b := append(buf[:0], snapshotVersion)
	b = binary.BigEndian.AppendUint64(b, m.last)
	if _, err := w.Write(binary.BigEndian.AppendUint64(b, uint64(len(accts)))); err != nil {
		return err
	}
	for _, i := range accts {
		if _, err := w.Write(AppendAccount(buf[:0], m.accounts[i])); err != nil {
			return err
		}
	}
	if _, err := w.Write(binary.BigEndian.AppendUint64(buf[:0], uint64(len(trs)))); err != nil {
		return err
	}
	for _, i := range trs {
		t := m.transfer(i)
		if _, err := w.Write(append(AppendTransfer(buf[:0], t.Transfer), byte(t.status))); err != nil {
			return err
		}
	}
	return w.Flush()
}

func (m *Machine) WriteSnapshot(w io.Writer) error {
	return m.encode(bufio.NewWriter(w))
}

func ReadSnapshot(r io.Reader) (*Machine, error) {
	m, err := readSnapshot(bufio.NewReader(r))
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrBadSnapshot, err)
	}
	return m, nil
}

func readSnapshot(r *bufio.Reader) (*Machine, error) {
	var buf [TransferSize + 1]byte
	head := buf[:1+8+8]
	if _, err := io.ReadFull(r, head); err != nil {
		return nil, err
	}
	if head[0] != snapshotVersion {
		return nil, fmt.Errorf("version %d", head[0])
	}
	m := New(0, 0)
	m.last = binary.BigEndian.Uint64(head[1:])
	if m.last >= tsLimit {
		return nil, fmt.Errorf("last timestamp %d out of range", m.last)
	}

	for k := binary.BigEndian.Uint64(head[9:]); k > 0; k-- {
		if _, err := io.ReadFull(r, buf[:AccountSize]); err != nil {
			return nil, err
		}
		a := DecodeAccount(buf[:])
		switch {
		case len(m.accounts) > 0 && a.ID.Cmp(m.accounts[len(m.accounts)-1].ID) <= 0:
			return nil, fmt.Errorf("account %v out of order", a.ID)
		case a.Timestamp > m.last:
			return nil, fmt.Errorf("account %v from the future", a.ID)
		}
		m.accountIdx[a.ID] = len(m.accounts)
		m.accounts = append(m.accounts, a)
	}

	if _, err := io.ReadFull(r, buf[:8]); err != nil {
		return nil, err
	}
	var prev *Transfer
	for k := binary.BigEndian.Uint64(buf[:]); k > 0; k-- {
		if _, err := io.ReadFull(r, buf[:TransferSize+1]); err != nil {
			return nil, err
		}
		tr, st := DecodeTransfer(buf[:]), status(buf[TransferSize])
		dr, drOK := m.accountIdx[tr.DebitAccountID]
		cr, crOK := m.accountIdx[tr.CreditAccountID]
		switch {
		case prev != nil && tr.ID.Cmp(prev.ID) <= 0:
			return nil, fmt.Errorf("transfer %v out of order", tr.ID)
		case tr.Timestamp > m.last:
			return nil, fmt.Errorf("transfer %v from the future", tr.ID)
		case !drOK || !crOK || dr == cr:
			return nil, fmt.Errorf("transfer %v: unknown accounts", tr.ID)
		case st > statusExpired || (st != statusNone) != (tr.Flags&Pending != 0):
			return nil, fmt.Errorf("transfer %v: status %d", tr.ID, st)
		}
		i := m.store(stored{Transfer: tr, dr: dr, cr: cr, heap: -1, status: st})
		if st == statusPending && tr.Timeout > 0 {
			m.queue(i)
		}
		prev = &m.transfer(i).Transfer
	}
	switch _, err := r.ReadByte(); {
	case err == nil:
		return nil, errors.New("trailing data")
	case err != io.EOF:
		return nil, err
	}

	for i := range m.ntransfers {
		tr := m.transfer(i)
		if tr.Flags&(PostPending|VoidPending) == 0 {
			continue
		}
		if p, ok := m.transferIdx[tr.PendingID]; !ok || m.transfer(p).Flags&Pending == 0 {
			return nil, fmt.Errorf("transfer %v: pending transfer %v missing", tr.ID, tr.PendingID)
		}
	}
	if err := m.CheckInvariants(); err != nil {
		return nil, err
	}
	return m, nil
}
