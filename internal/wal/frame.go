package wal

import (
	"encoding/binary"
	"hash/crc32"
)

const MaxBodySize = 16 << 20

const (
	offLen      = 0
	offHdrCRC   = 4
	offBodyCRC  = 8
	offIndex    = 12
	offTerm     = 20
	offKind     = 28
	offSynced   = 29
	frameHdrLen = 37
)

const (
	segMagic   uint32 = 0x6c6e6d64
	segVersion uint16 = 1

	segOffVersion = 4
	segOffNonce   = 6
	segOffCRC     = 14
	segHdrLen     = 18
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

type Kind uint8

type Entry struct {
	Index uint64
	Term  uint64
	Kind  Kind
	Body  []byte
}

type frame struct {
	Entry
	synced uint64
	size   int
}

func frameSize(e Entry) int {
	return frameHdrLen + len(e.Body)
}

func appendSegmentHeader(dst []byte, nonce uint64) []byte {
	start := len(dst)
	dst = binary.LittleEndian.AppendUint32(dst, segMagic)
	dst = binary.LittleEndian.AppendUint16(dst, segVersion)
	dst = binary.LittleEndian.AppendUint64(dst, nonce)
	return binary.LittleEndian.AppendUint32(dst, crc32.Checksum(dst[start:], castagnoli))
}

func seedOf(nonce uint64) uint32 {
	return crc32.Checksum(binary.LittleEndian.AppendUint64(nil, nonce), castagnoli)
}

func appendFrame(dst []byte, e Entry, synced uint64, seed uint32) []byte {
	start := len(dst)
	dst = binary.LittleEndian.AppendUint32(dst, uint32(len(e.Body))) //nolint:gosec // G115: Append rejects bodies over MaxBodySize
	dst = append(dst, make([]byte, 8)...)
	dst = binary.LittleEndian.AppendUint64(dst, e.Index)
	dst = binary.LittleEndian.AppendUint64(dst, e.Term)
	dst = append(dst, byte(e.Kind))
	dst = binary.LittleEndian.AppendUint64(dst, synced)
	dst = append(dst, e.Body...)
	hdr := dst[start : start+frameHdrLen]
	binary.LittleEndian.PutUint32(hdr[offBodyCRC:], crc32.Update(seed, castagnoli, e.Body))
	binary.LittleEndian.PutUint32(hdr[offHdrCRC:], headerCRC(hdr, seed))
	return dst
}

func headerCRC(hdr []byte, seed uint32) uint32 {
	crc := crc32.Update(seed, castagnoli, hdr[:offHdrCRC])
	return crc32.Update(crc, castagnoli, hdr[offBodyCRC:frameHdrLen])
}

func decodeFrame(b []byte, seed uint32) (frame, bool) {
	if len(b) < frameHdrLen || binary.LittleEndian.Uint32(b[offHdrCRC:]) != headerCRC(b, seed) {
		return frame{}, false
	}
	n := binary.LittleEndian.Uint32(b[offLen:])
	if n > MaxBodySize || int(n) > len(b)-frameHdrLen {
		return frame{}, false
	}
	body := b[frameHdrLen : frameHdrLen+int(n)]
	if binary.LittleEndian.Uint32(b[offBodyCRC:]) != crc32.Update(seed, castagnoli, body) {
		return frame{}, false
	}
	fr := frame{
		Entry: Entry{
			Index: binary.LittleEndian.Uint64(b[offIndex:]),
			Term:  binary.LittleEndian.Uint64(b[offTerm:]),
			Kind:  Kind(b[offKind]),
			Body:  body,
		},
		synced: binary.LittleEndian.Uint64(b[offSynced:]),
		size:   frameHdrLen + int(n),
	}
	return fr, fr.synced < fr.Index
}

func provenSynced(tail []byte, want uint64, seed uint32) bool {
	limit := want + uint64(len(tail)/frameHdrLen)
	for p := 1; p+frameHdrLen <= len(tail); p++ {
		idx := binary.LittleEndian.Uint64(tail[p+offIndex:])
		synced := binary.LittleEndian.Uint64(tail[p+offSynced:])
		if synced < want || idx <= synced || idx > limit {
			continue
		}
		if _, ok := decodeFrame(tail[p:], seed); ok {
			return true
		}
	}
	return false
}
