package wal

import (
	"bytes"
	"encoding/binary"
	"slices"
	"testing"
)

func TestFrameRoundTrip(t *testing.T) {
	cases := []Entry{
		{Index: 1, Term: 1},
		{Index: 1 << 63, Term: 7, Kind: 3, Body: []byte("body")},
		{Index: 9, Term: 2, Kind: 255, Body: bytes.Repeat([]byte{0xff}, 4096)},
	}
	for _, e := range cases {
		fr := appendFrame(nil, e, 5, 77)
		got, ok := decodeFrame(append(slices.Clip(fr), 0xaa, 0xbb), 77)
		if !ok {
			t.Fatalf("index %d: frame rejected", e.Index)
		}
		if got.size != len(fr) || got.synced != 5 || got.Index != e.Index || got.Term != e.Term ||
			got.Kind != e.Kind || !bytes.Equal(got.Body, e.Body) {
			t.Fatalf("index %d: decoded %+v", e.Index, got)
		}
	}
}

func flipped(fr []byte, off int) []byte {
	out := slices.Clone(fr)
	out[off] ^= 0x10
	return out
}

func TestDecodeFrameRejects(t *testing.T) {
	good := appendFrame(nil, Entry{Index: 3, Term: 1, Body: []byte("abc")}, 0, 77)
	huge := slices.Clone(good)
	binary.LittleEndian.PutUint32(huge, MaxBodySize+1)
	huge = append(huge, make([]byte, MaxBodySize+1)...)

	cases := map[string][]byte{
		"empty":            nil,
		"short header":     good[:frameHdrLen-1],
		"short body":       good[:len(good)-1],
		"length too large": huge,
		"length bit":       flipped(good, 0),
		"header crc bit":   flipped(good, offHdrCRC+1),
		"body crc bit":     flipped(good, offBodyCRC),
		"index bit":        flipped(good, offIndex),
		"kind bit":         flipped(good, offKind),
		"synced bit":       flipped(good, offSynced+2),
		"body bit":         flipped(good, len(good)-1),
		"zeros":            make([]byte, 64),
	}
	if _, ok := decodeFrame(good, 78); ok {
		t.Error("other nonce: accepted")
	}
	for name, b := range cases {
		if _, ok := decodeFrame(b, 77); ok {
			t.Errorf("%s: accepted", name)
		}
	}
}

func FuzzDecodeFrame(f *testing.F) {
	f.Add(uint32(0), appendFrame(nil, Entry{Index: 1, Term: 1, Body: []byte("x")}, 0, 0))
	f.Add(uint32(9), appendFrame(nil, Entry{Index: 1 << 40, Term: 3, Kind: 2}, 7, 9))
	f.Add(uint32(1), make([]byte, frameHdrLen))
	f.Fuzz(func(t *testing.T, seed uint32, b []byte) {
		fr, ok := decodeFrame(b, seed)
		if !ok {
			return
		}
		if again := appendFrame(nil, fr.Entry, fr.synced, seed); !bytes.Equal(again, b[:fr.size]) {
			t.Fatalf("re-encoded frame differs:\n%x\n%x", again, b[:fr.size])
		}
	})
}
