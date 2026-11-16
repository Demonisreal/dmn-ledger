package wire

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"

	"github.com/Demonisreal/dmn-ledger/internal/client"
)

const (
	Version    = 1
	HeaderSize = 24

	MaxClientBody = replyHead + client.MaxBody
	MaxPeerBody   = 4 << 20
	MaxEntryBody  = requestHead + 8 + client.MaxBody
	ChunkSize     = 256 << 10
	HelloSize     = helloSize
)

const (
	offVersion  = 4
	offType     = 5
	offReserved = 6
	offCluster  = 8
	offSize     = 16
	offCRC      = 20
)

const magic = "DMNL"

var (
	ErrMagic    = errors.New("wire: bad magic")
	ErrVersion  = errors.New("wire: unknown version")
	ErrCluster  = errors.New("wire: other cluster")
	ErrTooLarge = errors.New("wire: frame too large")
	ErrChecksum = errors.New("wire: checksum mismatch")
	ErrFormat   = errors.New("wire: malformed frame")
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

type Type uint8

const (
	TypeRequest       Type = 0x01
	TypeReply         Type = 0x02
	TypeStatusRequest Type = 0x03
	TypeStatus        Type = 0x04
	TypeHello         Type = 0x10
	TypeMessage       Type = 0x20
	TypeSnapshotChunk Type = 0x2a
	TypeSnapshotEnd   Type = 0x2b
)

type Frame struct {
	Type Type
	Body []byte
}

func appendHeader(dst []byte, cluster uint64, t Type) []byte {
	dst = append(dst, magic...)
	dst = append(dst, Version, byte(t), 0, 0)
	dst = binary.BigEndian.AppendUint64(dst, cluster)
	return append(dst, make([]byte, 8)...)
}

func finish(dst []byte, start int) []byte {
	frame := dst[start:]
	binary.BigEndian.PutUint32(frame[offSize:], uint32(len(frame)-HeaderSize)) //nolint:gosec // G115: AppendMessage refuses bodies over MaxPeerBody, the other callers bound theirs
	binary.BigEndian.PutUint32(frame[offCRC:], checksum(frame[:offCRC], frame[HeaderSize:]))
	return dst
}

func checksum(head, body []byte) uint32 {
	return crc32.Update(crc32.Checksum(head, castagnoli), castagnoli, body)
}

func parseHeader(head []byte, cluster uint64, limit uint32) (Type, uint32, error) {
	switch {
	case string(head[:offVersion]) != magic:
		return 0, 0, ErrMagic
	case head[offVersion] != Version:
		return 0, 0, fmt.Errorf("%w: %d", ErrVersion, head[offVersion])
	case head[offReserved] != 0 || head[offReserved+1] != 0:
		return 0, 0, fmt.Errorf("%w: reserved bytes set", ErrFormat)
	}
	if id := binary.BigEndian.Uint64(head[offCluster:]); id == 0 || id != cluster {
		return 0, 0, fmt.Errorf("%w: %d", ErrCluster, id)
	}
	size := binary.BigEndian.Uint32(head[offSize:])
	if size > limit {
		return 0, 0, fmt.Errorf("%w: %d bytes, limit %d", ErrTooLarge, size, limit)
	}
	return Type(head[offType]), size, nil
}

func ReadFrame(r io.Reader, buf []byte, cluster uint64, limit uint32) (Frame, []byte, error) {
	var head [HeaderSize]byte
	if _, err := io.ReadFull(r, head[:]); err != nil {
		return Frame{}, buf, err
	}
	t, size, err := parseHeader(head[:], cluster, limit)
	if err != nil {
		return Frame{}, buf, err
	}
	if int64(cap(buf)) < int64(size) {
		buf = make([]byte, size)
	}
	body := buf[:size:size]
	if _, err := io.ReadFull(r, body); err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return Frame{}, buf, err
	}
	if checksum(head[:offCRC], body) != binary.BigEndian.Uint32(head[offCRC:]) {
		return Frame{}, buf, ErrChecksum
	}
	return Frame{Type: t, Body: body}, buf, nil
}

func Decode(b []byte, cluster uint64, limit uint32) (Frame, []byte, error) {
	switch {
	case len(b) == 0:
		return Frame{}, nil, io.EOF
	case len(b) < HeaderSize:
		return Frame{}, nil, io.ErrUnexpectedEOF
	}
	t, size, err := parseHeader(b, cluster, limit)
	if err != nil {
		return Frame{}, nil, err
	}
	if int64(len(b)-HeaderSize) < int64(size) {
		return Frame{}, nil, io.ErrUnexpectedEOF
	}
	end := HeaderSize + int(size)
	body := b[HeaderSize:end:end]
	if checksum(b[:offCRC], body) != binary.BigEndian.Uint32(b[offCRC:]) {
		return Frame{}, nil, ErrChecksum
	}
	return Frame{Type: t, Body: body}, b[end:], nil
}

func (f Frame) mismatch(kind string) error {
	return fmt.Errorf("%w: type %#x with %d bytes is not a %s", ErrFormat, uint8(f.Type), len(f.Body), kind)
}

func trailing(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}
	return b
}
