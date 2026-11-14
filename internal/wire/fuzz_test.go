package wire

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"reflect"
	"slices"
	"testing"

	"github.com/Demonisreal/dmn-ledger/internal/raft"
)

var refusals = []error{io.EOF, io.ErrUnexpectedEOF, ErrMagic, ErrVersion, ErrCluster, ErrTooLarge, ErrChecksum, ErrFormat}

type pieces struct {
	stream []byte
	sizes  []byte
}

func (p *pieces) Read(b []byte) (int, error) {
	if len(p.stream) == 0 {
		return 0, io.EOF
	}
	n := len(b)
	if len(p.sizes) > 0 {
		n = min(n, 1+int(p.sizes[0]))
		p.sizes = p.sizes[1:]
	}
	n = copy(b[:n], p.stream)
	p.stream = p.stream[n:]
	return n, nil
}

func FuzzReadFrame(f *testing.F) {
	var all []byte
	for _, s := range samples() {
		all = append(all, s.frame...)
		f.Add(s.frame, []byte{0}, uint16(1024))
	}
	f.Add(all, []byte{0, 3, 200, 24}, uint16(110))
	f.Add(all, []byte{23, 0, 0, 255}, uint16(109))
	f.Add(all[:len(all)-1], []byte{}, uint16(4096))
	f.Fuzz(func(t *testing.T, stream, sizes []byte, limit uint16) {
		r := &pieces{stream: stream, sizes: sizes}
		rest := stream
		var buf []byte
		for {
			fr, grown, err := ReadFrame(r, buf, cluster, uint32(limit))
			if cap(grown) > int(limit) {
				t.Fatalf("buffer of %d bytes under a limit of %d", cap(grown), limit)
			}
			want, tail, wantErr := Decode(rest, cluster, uint32(limit))
			for _, refusal := range refusals {
				if errors.Is(err, refusal) != errors.Is(wantErr, refusal) {
					t.Fatalf("ReadFrame says %v, Decode says %v", err, wantErr)
				}
			}
			if err != nil || wantErr != nil {
				if err == nil || wantErr == nil {
					t.Fatalf("ReadFrame says %v, Decode says %v", err, wantErr)
				}
				return
			}
			if fr.Type != want.Type || !bytes.Equal(fr.Body, want.Body) {
				t.Fatalf("ReadFrame took type %#x with %x, Decode type %#x with %x", uint8(fr.Type), fr.Body, uint8(want.Type), want.Body)
			}
			if len(r.stream) != len(tail) {
				t.Fatalf("ReadFrame left %d bytes unread, Decode %d", len(r.stream), len(tail))
			}
			rest, buf = tail, grown
		}
	})
}

func FuzzBody(f *testing.F) {
	for _, s := range samples() {
		f.Add(s.frame[offType], s.frame[HeaderSize:])
	}
	f.Add(uint8(TypeMessage)+uint8(raft.MsgApp), make([]byte, appendHead))
	f.Add(uint8(TypeStatus), make([]byte, statusSize))
	f.Fuzz(func(t *testing.T, typ uint8, body []byte) {
		fr := Frame{Type: Type(typ), Body: body}
		for _, recode := range recoders {
			again, err := recode(nil, fr)
			if err != nil {
				if !errors.Is(err, ErrFormat) {
					t.Fatalf("type %#x with %x: %v", typ, body, err)
				}
				continue
			}
			if again[offType] != typ || !bytes.Equal(again[HeaderSize:], body) {
				t.Fatalf("type %#x with %x encodes back as type %#x with %x", typ, body, again[offType], again[HeaderSize:])
			}
		}
	})
}

func entriesFrom(b []byte, prev uint64) []raft.Entry {
	var ents []raft.Entry
	for len(b) >= 10 {
		e := raft.Entry{Index: prev + 1 + uint64(len(ents)), Term: binary.BigEndian.Uint64(b), Kind: b[8]}
		size := min(int(b[9]), len(b)-10)
		if size > 0 {
			e.Body = b[10 : 10+size]
		}
		ents = append(ents, e)
		b = b[10+size:]
	}
	return ents
}

func FuzzMessage(f *testing.F) {
	ents := []byte{0, 0, 0, 0, 0, 0, 0, 5, 1, 0, 0, 0, 0, 0, 0, 0, 0, 5, 2, 3, 0xb1, 0xb2, 0xb3}
	for typ := uint8(0); typ <= uint8(raft.MsgRepairInfo)+1; typ++ {
		f.Add(typ, uint64(1), uint64(2), uint64(5), uint64(9), uint64(4), uint64(8), uint64(3), typ%2 == 0, ents, uint8(0))
	}
	f.Add(uint8(raft.MsgApp), uint64(1), uint64(2), uint64(5), uint64(9), uint64(4), uint64(8), uint64(3), false, ents, uint8(2))
	f.Add(uint8(raft.MsgApp), uint64(1), uint64(1), uint64(0), uint64(math.MaxUint64-1), uint64(9), uint64(0), uint64(0), true, ents, uint8(0))
	f.Add(uint8(raft.MsgApp), uint64(0), uint64(0), uint64(math.MaxUint64), uint64(0), uint64(0), uint64(0), uint64(0), false, []byte{}, uint8(0))
	f.Fuzz(func(t *testing.T, typ uint8, from, to, term, index, logTerm, commit, seq uint64, reject bool, ents []byte, gap uint8) {
		m := raft.Message{
			Type: raft.MsgType(typ), From: from, To: to, Term: term,
			Index: index, LogTerm: logTerm, Commit: commit, Seq: seq, Reject: reject,
			Entries: entriesFrom(ents, index),
		}
		count := uint64(len(m.Entries))
		if gap > 0 && count > 0 {
			m.Entries[uint64(gap)%count].Index++
		}
		refused := m.Type < raft.MsgPreVote || m.Type > raft.MsgRepairInfo ||
			m.Type == raft.MsgApp && (gap > 0 && count > 0 || index+count < index)
		frame, err := AppendMessage(nil, cluster, m)
		if refused {
			if !errors.Is(err, ErrFormat) || len(frame) != 0 {
				t.Fatalf("%+v encodes to %d bytes, %v", m, len(frame), err)
			}
			return
		}
		if err != nil {
			t.Fatalf("%+v: %v", m, err)
		}
		fr, rest, err := Decode(frame, cluster, MaxPeerBody)
		if err != nil || len(rest) != 0 {
			t.Fatalf("%+v: %v, %d bytes left", m, err, len(rest))
		}
		got, err := fr.Message()
		if want := carried(m); err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("%+v came back as %+v, %v", want, got, err)
		}
		if again, err := AppendMessage(nil, cluster, got); err != nil || !bytes.Equal(again, frame) {
			t.Fatalf("%+v encodes back as %x, %v", got, again, err)
		}
	})
}

func FuzzSnapshotStream(f *testing.F) {
	f.Add([]byte{}, []byte{}, uint16(0), uint32(0))
	f.Add([]byte("one chunk"), []byte{}, uint16(0), uint32(77))
	f.Add(bytes.Repeat([]byte("snapshot"), 64), []byte{0, 0, 7, 255, 100}, uint16(3), uint32(1000))
	f.Add(make([]byte, ChunkSize+1), []byte{}, uint16(1), uint32(ChunkSize*8))
	f.Fuzz(func(t *testing.T, snap, sizes []byte, drop uint16, flip uint32) {
		var cuts []int
		left := len(snap)
		for _, s := range sizes {
			size := 1 + int(s)
			if size > left {
				break
			}
			cuts = append(cuts, size)
			left -= size
		}
		frames := chunked(snap, cuts...)
		if got, err := assemble(frames); err != nil || !bytes.Equal(got, snap) {
			t.Fatalf("%d bytes in %d frames came back as %d, %v", len(snap), len(frames), len(got), err)
		}
		if chunks := len(frames) - 1; chunks > 0 {
			i := int(drop) % chunks
			if _, err := assemble(slices.Delete(slices.Clone(frames), i, i+1)); !errors.Is(err, errStream) {
				t.Fatalf("chunk %d of %d missing: %v", i, chunks, err)
			}
		}
		all := slices.Concat(frames...)
		bit := int(flip) % (len(all) * 8)
		all[bit/8] ^= 1 << (bit % 8)
		if _, err := assemble([][]byte{all}); err == nil {
			t.Fatalf("bit %d flipped, stream of %d bytes taken", bit, len(snap))
		}
	})
}
