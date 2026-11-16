package audit

import (
	"crypto/sha256"
	"encoding/binary"
)

const LinkSize = 8 + 1 + 4 + 4 + 3*HashSize

type Link struct {
	Time   uint64
	Op     uint8
	Events uint32
	Leaves uint32
	Body   Hash
	Reply  Hash
	Root   Hash
}

func AppendLink(dst []byte, l Link) []byte {
	dst = binary.BigEndian.AppendUint64(dst, l.Time)
	dst = append(dst, l.Op)
	dst = binary.BigEndian.AppendUint32(dst, l.Events)
	dst = binary.BigEndian.AppendUint32(dst, l.Leaves)
	dst = append(dst, l.Body[:]...)
	dst = append(dst, l.Reply[:]...)
	return append(dst, l.Root[:]...)
}

func DecodeLink(b []byte) Link {
	_ = b[LinkSize-1]
	return Link{
		Time:   binary.BigEndian.Uint64(b),
		Op:     b[8],
		Events: binary.BigEndian.Uint32(b[9:]),
		Leaves: binary.BigEndian.Uint32(b[13:]),
		Body:   Hash(b[17:]),
		Reply:  Hash(b[17+HashSize:]),
		Root:   Hash(b[17+2*HashSize:]),
	}
}

type Head struct {
	Seq  uint64
	Hash Hash
}

func (h Head) Next(l Link) Head {
	var buf [1 + HashSize + 8 + LinkSize]byte
	buf[0] = prefixHead
	copy(buf[1:], h.Hash[:])
	binary.BigEndian.PutUint64(buf[1+HashSize:], h.Seq+1)
	AppendLink(buf[:1+HashSize+8], l)
	return Head{Seq: h.Seq + 1, Hash: sha256.Sum256(buf[:])}
}

func Connect(from Head, links []Link) Head {
	for _, l := range links {
		from = from.Next(l)
	}
	return from
}

func BodyHash(body []byte) Hash {
	return prefixed(prefixBody, body)
}

func ReplyHash(failures []byte) Hash {
	return prefixed(prefixReply, failures)
}

func prefixed(prefix byte, b []byte) Hash {
	h := sha256.New()
	h.Write([]byte{prefix})
	h.Write(b)
	return Hash(h.Sum(nil))
}
