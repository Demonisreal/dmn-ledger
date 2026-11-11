package wal

import (
	"encoding/binary"
	"hash/crc32"
)

const (
	hdrSize     = 33
	MaxBodySize = 16 << 20
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

type Entry struct {
	Index uint64
	Term  uint64
	Kind  uint8
	Body  []byte
}

type frame struct {
	Entry
	synced uint64
	size   int
}

func frameSize(e Entry) int {
	return hdrSize + len(e.Body)
}

func appendFrame(dst []byte, e Entry, synced uint64) []byte {
	start := len(dst)
	dst = binary.LittleEndian.AppendUint32(dst, uint32(len(e.Body))) //nolint:gosec
	dst = append(dst, 0, 0, 0, 0)
	dst = binary.LittleEndian.AppendUint64(dst, e.Index)
	dst = binary.LittleEndian.AppendUint64(dst, e.Term)
	dst = append(dst, e.Kind)
	dst = binary.LittleEndian.AppendUint64(dst, synced)
	dst = append(dst, e.Body...)
	binary.LittleEndian.PutUint32(dst[start+4:], checksum(dst[start:]))
	return dst
}

func decodeFrame(b []byte) (frame, bool) {
	if len(b) < hdrSize {
		return frame{}, false
	}
	n := binary.LittleEndian.Uint32(b)
	if n > MaxBodySize || int(n) > len(b)-hdrSize {
		return frame{}, false
	}
	b = b[:hdrSize+int(n)]
	if binary.LittleEndian.Uint32(b[4:]) != checksum(b) {
		return frame{}, false
	}
	return frame{
		Entry: Entry{
			Index: binary.LittleEndian.Uint64(b[8:]),
			Term:  binary.LittleEndian.Uint64(b[16:]),
			Kind:  b[24],
			Body:  b[hdrSize:],
		},
		synced: binary.LittleEndian.Uint64(b[25:]),
		size:   len(b),
	}, true
}

func checksum(fr []byte) uint32 {
	crc := crc32.Update(0, castagnoli, fr[:4])
	return crc32.Update(crc, castagnoli, fr[8:])
}

func provenSynced(tail []byte, want uint64) bool {
	limit := want + uint64(len(tail)/hdrSize)
	for p := 1; p+hdrSize <= len(tail); p++ {
		idx := binary.LittleEndian.Uint64(tail[p+8:])
		synced := binary.LittleEndian.Uint64(tail[p+25:])
		if synced < want || idx <= synced || idx > limit {
			continue
		}
		if fr, ok := decodeFrame(tail[p:]); ok && fr.synced >= want {
			return true
		}
	}
	return false
}
