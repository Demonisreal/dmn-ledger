package wire

import (
	"encoding/binary"
	"fmt"

	"github.com/Demonisreal/dmn-ledger/internal/raft"
)

const (
	KindMessages uint8 = 1
	KindSnapshot uint8 = 2
)

const (
	helloSize   = 17
	messageHead = 24
	appendHead  = messageHead + 36
	entryHead   = 13
	endSize     = 12
)

var messageSize = [...]int{
	raft.MsgPreVote:     messageHead + 16,
	raft.MsgPreVoteResp: messageHead + 1,
	raft.MsgVote:        messageHead + 16,
	raft.MsgVoteResp:    messageHead + 1,
	raft.MsgApp:         appendHead,
	raft.MsgAppResp:     messageHead + 17,
	raft.MsgSnap:        messageHead + 32,
	raft.MsgRepairProbe: messageHead + 8,
	raft.MsgRepairInfo:  messageHead + 16,
}

type Hello struct {
	From uint64
	To   uint64
	Kind uint8
}

func AppendHello(dst []byte, cluster uint64, h Hello) []byte {
	start := len(dst)
	dst = appendHeader(dst, cluster, TypeHello)
	dst = binary.BigEndian.AppendUint64(dst, h.From)
	dst = binary.BigEndian.AppendUint64(dst, h.To)
	dst = append(dst, h.Kind)
	return finish(dst, start)
}

func (f Frame) Hello() (Hello, error) {
	b := f.Body
	if f.Type != TypeHello || len(b) != helloSize {
		return Hello{}, f.mismatch("hello")
	}
	return Hello{From: binary.BigEndian.Uint64(b), To: binary.BigEndian.Uint64(b[8:]), Kind: b[16]}, nil
}

func AppendMessage(dst []byte, cluster uint64, m raft.Message) ([]byte, error) {
	if m.Type < raft.MsgPreVote || m.Type > raft.MsgRepairInfo {
		return dst, fmt.Errorf("%w: message type %d", ErrFormat, m.Type)
	}
	start := len(dst)
	dst = appendHeader(dst, cluster, TypeMessage+Type(m.Type))
	dst = binary.BigEndian.AppendUint64(dst, m.From)
	dst = binary.BigEndian.AppendUint64(dst, m.To)
	dst = binary.BigEndian.AppendUint64(dst, m.Term)
	switch m.Type {
	case raft.MsgPreVote, raft.MsgVote:
		dst = binary.BigEndian.AppendUint64(dst, m.Index)
		dst = binary.BigEndian.AppendUint64(dst, m.LogTerm)
	case raft.MsgPreVoteResp, raft.MsgVoteResp:
		dst = append(dst, bit(m.Reject))
	case raft.MsgApp:
		dst = binary.BigEndian.AppendUint64(dst, m.Index)
		dst = binary.BigEndian.AppendUint64(dst, m.LogTerm)
		dst = binary.BigEndian.AppendUint64(dst, m.Commit)
		dst = binary.BigEndian.AppendUint64(dst, m.Seq)
		dst = binary.BigEndian.AppendUint32(dst, uint32(len(m.Entries))) //nolint:gosec // G115: that many entries are over MaxPeerBody, refused below
		for k, e := range m.Entries {
			switch {
			case e.Index != m.Index+1+uint64(k) || e.Index <= m.Index:
				return dst[:start], fmt.Errorf("%w: entry %d at position %d after index %d", ErrFormat, e.Index, k, m.Index)
			case len(e.Body) > MaxEntryBody:
				return dst[:start], fmt.Errorf("%w: entry %d of %d bytes", ErrTooLarge, e.Index, len(e.Body))
			}
			dst = binary.BigEndian.AppendUint64(dst, e.Term)
			dst = append(dst, e.Kind)
			dst = binary.BigEndian.AppendUint32(dst, uint32(len(e.Body))) //nolint:gosec // G115: at most MaxEntryBody
			dst = append(dst, e.Body...)
		}
	case raft.MsgAppResp:
		dst = binary.BigEndian.AppendUint64(dst, m.Index)
		dst = binary.BigEndian.AppendUint64(dst, m.Seq)
		dst = append(dst, bit(m.Reject))
	case raft.MsgSnap:
		dst = binary.BigEndian.AppendUint64(dst, m.Index)
		dst = binary.BigEndian.AppendUint64(dst, m.LogTerm)
		dst = binary.BigEndian.AppendUint64(dst, m.Commit)
		dst = binary.BigEndian.AppendUint64(dst, m.Seq)
	case raft.MsgRepairProbe:
		dst = binary.BigEndian.AppendUint64(dst, m.Seq)
	case raft.MsgRepairInfo:
		dst = binary.BigEndian.AppendUint64(dst, m.Index)
		dst = binary.BigEndian.AppendUint64(dst, m.Seq)
	}
	if size := len(dst) - start - HeaderSize; size > MaxPeerBody {
		return dst[:start], fmt.Errorf("%w: %s of %d bytes", ErrTooLarge, m.Type, size)
	}
	return finish(dst, start), nil
}

func (f Frame) Message() (raft.Message, error) {
	b := f.Body
	t := raft.MsgType(f.Type - TypeMessage)
	switch {
	case f.Type <= TypeMessage || f.Type >= TypeSnapshotChunk:
		return raft.Message{}, f.mismatch("raft message")
	case len(b) < messageSize[t] || len(b) > MaxPeerBody || t != raft.MsgApp && len(b) != messageSize[t]:
		return raft.Message{}, f.mismatch(t.String())
	}
	m := raft.Message{
		Type: t,
		From: binary.BigEndian.Uint64(b),
		To:   binary.BigEndian.Uint64(b[8:]),
		Term: binary.BigEndian.Uint64(b[16:]),
	}
	b = b[messageHead:]
	var err error
	switch t {
	case raft.MsgPreVote, raft.MsgVote:
		m.Index = binary.BigEndian.Uint64(b)
		m.LogTerm = binary.BigEndian.Uint64(b[8:])
	case raft.MsgPreVoteResp, raft.MsgVoteResp:
		m.Reject, err = rejected(b[0])
	case raft.MsgApp:
		m.Index = binary.BigEndian.Uint64(b)
		m.LogTerm = binary.BigEndian.Uint64(b[8:])
		m.Commit = binary.BigEndian.Uint64(b[16:])
		m.Seq = binary.BigEndian.Uint64(b[24:])
		m.Entries, err = decodeEntries(b[32:], m.Index)
	case raft.MsgAppResp:
		m.Index = binary.BigEndian.Uint64(b)
		m.Seq = binary.BigEndian.Uint64(b[8:])
		m.Reject, err = rejected(b[16])
	case raft.MsgSnap:
		m.Index = binary.BigEndian.Uint64(b)
		m.LogTerm = binary.BigEndian.Uint64(b[8:])
		m.Commit = binary.BigEndian.Uint64(b[16:])
		m.Seq = binary.BigEndian.Uint64(b[24:])
	case raft.MsgRepairProbe:
		m.Seq = binary.BigEndian.Uint64(b)
	case raft.MsgRepairInfo:
		m.Index = binary.BigEndian.Uint64(b)
		m.Seq = binary.BigEndian.Uint64(b[8:])
	}
	if err != nil {
		return raft.Message{}, err
	}
	return m, nil
}

func rejected(b byte) (bool, error) {
	if b > 1 {
		return false, fmt.Errorf("%w: reject flag %d", ErrFormat, b)
	}
	return b == 1, nil
}

func decodeEntries(b []byte, prev uint64) ([]raft.Entry, error) {
	count := binary.BigEndian.Uint32(b)
	b = b[4:]
	if int64(count)*entryHead > int64(len(b)) || prev+uint64(count) < prev {
		return nil, fmt.Errorf("%w: %d entries after index %d in %d bytes", ErrFormat, count, prev, len(b))
	}
	var ents []raft.Entry
	if count > 0 {
		ents = make([]raft.Entry, count)
	}
	for k := range ents {
		if len(b) < entryHead {
			return nil, fmt.Errorf("%w: entry %d cut off", ErrFormat, k)
		}
		size := binary.BigEndian.Uint32(b[9:])
		if size > MaxEntryBody || int64(size) > int64(len(b)-entryHead) {
			return nil, fmt.Errorf("%w: entry %d of %d bytes", ErrFormat, k, size)
		}
		end := entryHead + int(size)
		ents[k] = raft.Entry{
			Index: prev + 1 + uint64(k),
			Term:  binary.BigEndian.Uint64(b),
			Kind:  b[8],
			Body:  trailing(b[entryHead:end:end]),
		}
		b = b[end:]
	}
	if len(b) != 0 {
		return nil, fmt.Errorf("%w: %d bytes after the last entry", ErrFormat, len(b))
	}
	return ents, nil
}

func AppendChunk(dst []byte, cluster uint64, chunk []byte) []byte {
	start := len(dst)
	dst = appendHeader(dst, cluster, TypeSnapshotChunk)
	return finish(append(dst, chunk...), start)
}

func (f Frame) Chunk() ([]byte, error) {
	if f.Type != TypeSnapshotChunk || len(f.Body) == 0 || len(f.Body) > ChunkSize {
		return nil, f.mismatch("snapshot chunk")
	}
	return f.Body, nil
}

func AppendSnapshotEnd(dst []byte, cluster uint64, size uint64, crc uint32) []byte {
	start := len(dst)
	dst = appendHeader(dst, cluster, TypeSnapshotEnd)
	dst = binary.BigEndian.AppendUint64(dst, size)
	dst = binary.BigEndian.AppendUint32(dst, crc)
	return finish(dst, start)
}

func (f Frame) SnapshotEnd() (size uint64, crc uint32, err error) {
	b := f.Body
	if f.Type != TypeSnapshotEnd || len(b) != endSize {
		return 0, 0, f.mismatch("snapshot end")
	}
	return binary.BigEndian.Uint64(b), binary.BigEndian.Uint32(b[8:]), nil
}
