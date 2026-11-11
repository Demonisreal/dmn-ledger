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
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return nil, fmt.Errorf("%w: %w", ErrBadSnapshot, err)
	}
	return m, err
}

func readSnapshot(r *bufio.Reader) (*Machine, error) {
	var buf [TransferSize + 1]byte
	head := buf[:1+8+8]
	if _, err := io.ReadFull(r, head); err != nil {
		return nil, err
	}
	if head[0] != snapshotVersion {
		return nil, fmt.Errorf("%w: version %d", ErrBadSnapshot, head[0])
	}
	m := New(0, 0)
	m.last = binary.BigEndian.Uint64(head[1:])

	for k := binary.BigEndian.Uint64(head[9:]); k > 0; k-- {
		if _, err := io.ReadFull(r, buf[:AccountSize]); err != nil {
			return nil, err
		}
		a := DecodeAccount(buf[:])
		if len(m.accounts) > 0 && a.ID.Cmp(m.accounts[len(m.accounts)-1].ID) <= 0 {
			return nil, fmt.Errorf("%w: account %v out of order", ErrBadSnapshot, a.ID)
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
			return nil, fmt.Errorf("%w: transfer %v out of order", ErrBadSnapshot, tr.ID)
		case !drOK || !crOK:
			return nil, fmt.Errorf("%w: transfer %v: unknown accounts", ErrBadSnapshot, tr.ID)
		}
		i := m.store(stored{Transfer: tr, dr: dr, cr: cr, heap: -1, status: st})
		if st == statusPending && tr.Timeout > 0 {
			m.scheduleExpiry(i)
		}
		prev = &m.transfer(i).Transfer
	}
	switch _, err := r.ReadByte(); {
	case err == nil:
		return nil, fmt.Errorf("%w: trailing data", ErrBadSnapshot)
	case err != io.EOF:
		return nil, err
	}

	if err := m.CheckInvariants(); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrBadSnapshot, err)
	}
	return m, nil
}
