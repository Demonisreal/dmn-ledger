package wire

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"reflect"
	"runtime"
	"slices"
	"testing"
	"testing/iotest"

	"github.com/Demonisreal/dmn-ledger/internal/audit"
	"github.com/Demonisreal/dmn-ledger/internal/client"
	"github.com/Demonisreal/dmn-ledger/internal/raft"
	"github.com/Demonisreal/dmn-ledger/internal/u128"
)

var errStream = errors.New("snapshot stream does not match its end")

var readers = map[string]func(b []byte, limit uint32) (Frame, error){
	"ReadFrame": func(b []byte, limit uint32) (Frame, error) {
		f, _, err := ReadFrame(bytes.NewReader(b), nil, cluster, limit)
		return f, err
	},
	"Decode": func(b []byte, limit uint32) (Frame, error) {
		f, _, err := Decode(b, cluster, limit)
		return f, err
	},
}

var recoders = []func(dst []byte, f Frame) ([]byte, error){
	func(dst []byte, f Frame) ([]byte, error) {
		req, err := f.Request()
		return AppendRequest(dst, cluster, req), err
	},
	func(dst []byte, f Frame) ([]byte, error) {
		r, err := f.Reply()
		return AppendReply(dst, cluster, r), err
	},
	func(dst []byte, f Frame) ([]byte, error) {
		return AppendStatusRequest(dst, cluster), f.StatusRequest()
	},
	func(dst []byte, f Frame) ([]byte, error) {
		st, err := f.Status()
		return AppendStatus(dst, cluster, st), err
	},
	func(dst []byte, f Frame) ([]byte, error) {
		h, err := f.Hello()
		return AppendHello(dst, cluster, h), err
	},
	func(dst []byte, f Frame) ([]byte, error) {
		m, err := f.Message()
		if err != nil {
			return nil, err
		}
		if dst, err = AppendMessage(dst, cluster, m); err != nil {
			return nil, fmt.Errorf("decoded message does not encode: %v", err)
		}
		return dst, nil
	},
	func(dst []byte, f Frame) ([]byte, error) {
		chunk, err := f.Chunk()
		return AppendChunk(dst, cluster, chunk), err
	},
	func(dst []byte, f Frame) ([]byte, error) {
		size, crc, err := f.SnapshotEnd()
		return AppendSnapshotEnd(dst, cluster, size, crc), err
	},
}

func raw(t Type, body []byte) []byte {
	return finish(append(appendHeader(nil, cluster, t), body...), 0)
}

func parse(t *testing.T, frame []byte) Frame {
	t.Helper()
	f, rest, err := Decode(frame, cluster, MaxPeerBody)
	if err != nil || len(rest) != 0 {
		t.Fatalf("frame %x: %v, %d bytes left", frame[:min(len(frame), HeaderSize)], err, len(rest))
	}
	return f
}

func accepted(f Frame) bool {
	for _, recode := range recoders {
		if _, err := recode(nil, f); err == nil {
			return true
		}
	}
	return false
}

func carried(m raft.Message) raft.Message {
	c := raft.Message{Type: m.Type, From: m.From, To: m.To, Term: m.Term}
	switch m.Type {
	case raft.MsgPreVote, raft.MsgVote:
		c.Index, c.LogTerm = m.Index, m.LogTerm
	case raft.MsgPreVoteResp, raft.MsgVoteResp:
		c.Reject = m.Reject
	case raft.MsgApp:
		c.Index, c.LogTerm, c.Commit, c.Seq, c.Entries = m.Index, m.LogTerm, m.Commit, m.Seq, m.Entries
	case raft.MsgAppResp:
		c.Index, c.Seq, c.Reject = m.Index, m.Seq, m.Reject
	case raft.MsgSnap:
		c.Index, c.LogTerm, c.Commit, c.Seq = m.Index, m.LogTerm, m.Commit, m.Seq
	case raft.MsgRepairProbe:
		c.Seq = m.Seq
	case raft.MsgRepairInfo:
		c.Index, c.Seq = m.Index, m.Seq
	}
	return c
}

func allocated(run func()) uint64 {
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	run()
	runtime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc
}

func TestRoundTrip(t *testing.T) {
	const top = math.MaxUint64
	full := bytes.Repeat([]byte{0xab}, client.MaxBody)

	for _, req := range []client.Request{
		{},
		{Client: u128.Max(), Session: top, Number: top, Op: 255, Body: full},
		{Client: u128.Uint128{Hi: 1, Lo: 2}, Session: 3, Number: 4, Op: client.LookupAccounts, Body: []byte{5}},
	} {
		got, err := parse(t, AppendRequest(nil, cluster, req)).Request()
		if err != nil || !reflect.DeepEqual(got, req) {
			t.Errorf("request %+v came back as %+v, %v", req, got, err)
		}
	}
	for _, r := range []client.Reply{
		{},
		{Client: u128.Max(), Number: top, Op: 255, Status: 255, Leader: top, Time: top, Body: full},
		{Client: u128.Uint128{Hi: 1, Lo: 2}, Number: 3, Op: client.Register, Status: client.OK, Leader: 4, Time: 5, Body: []byte{6}},
	} {
		got, err := parse(t, AppendReply(nil, cluster, r)).Reply()
		if err != nil || !reflect.DeepEqual(got, r) {
			t.Errorf("reply %+v came back as %+v, %v", r, got, err)
		}
	}
	if err := parse(t, AppendStatusRequest(nil, cluster)).StatusRequest(); err != nil {
		t.Errorf("status request: %v", err)
	}
	busy := Status{
		ID: top, Role: raft.Leader, Repair: true, Term: top, Lead: top, Commit: top, Applied: top, Last: top,
		Sessions: math.MaxUint32, Time: top, Clock: top, NextExpiry: top, Head: audit.Head{Seq: top},
	}
	for i := range busy.ApplyHash {
		busy.ApplyHash[i], busy.Head.Hash[i], busy.Key[i] = 0xff, 0xff, 0xff
	}
	distinct := Status{
		ID: 1, Role: raft.Candidate, Term: 2, Lead: 3, Commit: 4, Applied: 5, Last: 6,
		ApplyHash: [32]byte{7, 31: 8}, Sessions: 9, Time: 10, Clock: 11, NextExpiry: 12,
		Head: audit.Head{Seq: 13, Hash: audit.Hash{14, 31: 15}}, Key: [32]byte{16, 31: 17},
	}
	for _, st := range []Status{{}, busy, distinct} {
		got, err := parse(t, AppendStatus(nil, cluster, st)).Status()
		if err != nil || got != st {
			t.Errorf("status %+v came back as %+v, %v", st, got, err)
		}
	}
	for _, h := range []Hello{{}, {From: top, To: top, MaxSessions: top, Kind: 255}, {From: 1, To: 2, MaxSessions: 3, Kind: KindMessages}} {
		got, err := parse(t, AppendHello(nil, cluster, h)).Hello()
		if err != nil || got != h {
			t.Errorf("hello %+v came back as %+v, %v", h, got, err)
		}
	}
	for typ := raft.MsgPreVote; typ <= raft.MsgRepairInfo; typ++ {
		for _, m := range []raft.Message{
			{Type: typ},
			{Type: typ, From: top, To: top, Term: top, Index: top - 2, LogTerm: top, Commit: top, Seq: top, Reject: true, Entries: []raft.Entry{
				{Index: top - 1, Term: top, Kind: 255, Body: full},
				{Index: top, Term: top},
			}},
			{Type: typ, From: 1, To: 2, Term: 3, Index: 4, LogTerm: 5, Commit: 6, Seq: 7, Entries: []raft.Entry{
				{Index: 5, Term: 8, Kind: 9, Body: []byte{10}},
			}},
		} {
			got, err := parse(t, message(m)).Message()
			if want := carried(m); err != nil || !reflect.DeepEqual(got, want) {
				t.Errorf("%s came back as %+v, %v, want %+v", typ, got, err, want)
			}
		}
	}
	for _, snap := range [][]byte{{1}, bytes.Repeat([]byte{0xcd}, ChunkSize)} {
		got, err := parse(t, AppendChunk(nil, cluster, snap)).Chunk()
		if err != nil || !bytes.Equal(got, snap) {
			t.Errorf("chunk of %d bytes came back with %d, %v", len(snap), len(got), err)
		}
	}
	for _, end := range []struct {
		size uint64
		crc  uint32
	}{{}, {top, math.MaxUint32}, {1, 2}} {
		size, crc, err := parse(t, AppendSnapshotEnd(nil, cluster, end.size, end.crc)).SnapshotEnd()
		if err != nil || size != end.size || crc != end.crc {
			t.Errorf("snapshot end %+v came back as %d %d, %v", end, size, crc, err)
		}
	}
}

func TestUnknownOpPassesThrough(t *testing.T) {
	for _, op := range []client.Op{0, client.LookupTransfers + 1, 7, 8, 0x80, 0xff} {
		req := client.Request{Client: u128.From64(9), Session: 3, Number: 4, Op: op, Body: []byte{1, 2, 3}}
		frame := AppendRequest(nil, cluster, req)
		got, err := parse(t, frame).Request()
		if err != nil || !reflect.DeepEqual(got, req) {
			t.Errorf("request with op %d came back as %+v, %v", op, got, err)
		}
		if again := AppendRequest(nil, cluster, got); !bytes.Equal(again, frame) || frame[HeaderSize+32] != byte(op) {
			t.Errorf("request with op %d encodes back as %x", op, again)
		}
		r := client.Reply{Client: u128.From64(9), Number: 4, Op: op, Status: client.Malformed, Leader: 2, Time: 5}
		frame = AppendReply(nil, cluster, r)
		back, err := parse(t, frame).Reply()
		if err != nil || !reflect.DeepEqual(back, r) {
			t.Errorf("reply with op %d came back as %+v, %v", op, back, err)
		}
		if again := AppendReply(nil, cluster, back); !bytes.Equal(again, frame) || frame[HeaderSize+24] != byte(op) {
			t.Errorf("reply with op %d encodes back as %x", op, again)
		}
	}
}

func TestSamplesEncodeBack(t *testing.T) {
	var all []byte
	for _, s := range samples() {
		f := parse(t, s.frame)
		var took int
		for _, recode := range recoders {
			again, err := recode(slices.Clone(all), f)
			if err != nil {
				continue
			}
			took++
			if !bytes.Equal(again[:len(all)], all) || !bytes.Equal(again[len(all):], s.frame) {
				t.Errorf("%s encodes back as %x", s.name, again[len(all):])
			}
		}
		if took != 1 {
			t.Errorf("%s: %d methods take the frame", s.name, took)
		}
		all = append(all, s.frame...)
	}
	for _, s := range samples() {
		f, rest, err := Decode(all, cluster, MaxPeerBody)
		if want := parse(t, s.frame); err != nil || f.Type != want.Type || !bytes.Equal(f.Body, want.Body) {
			t.Fatalf("%s out of a longer slice: type %#x, %v", s.name, uint8(f.Type), err)
		}
		all = rest
	}
	if len(all) != 0 {
		t.Errorf("%d bytes left after the last frame", len(all))
	}
}

func TestEveryFlippedBitIsRefused(t *testing.T) {
	for _, s := range samples() {
		for name, read := range readers {
			for i := range len(s.frame) * 8 {
				s.frame[i/8] ^= 1 << (i % 8)
				if _, err := read(s.frame, MaxPeerBody); err == nil {
					t.Errorf("%s: %s takes the frame with bit %d flipped", s.name, name, i)
				}
				s.frame[i/8] ^= 1 << (i % 8)
			}
			if _, err := read(s.frame, MaxPeerBody); err != nil {
				t.Errorf("%s: %s: %v", s.name, name, err)
			}
		}
	}
}

type zeros struct{}

func (zeros) Read(p []byte) (int, error) {
	clear(p)
	return len(p), nil
}

func TestLengthIsCheckedBeforeAllocation(t *testing.T) {
	head := appendHeader(nil, cluster, TypeRequest)
	binary.BigEndian.PutUint32(head[offSize:], 2<<30)
	for _, limit := range []uint32{MaxClientBody, MaxPeerBody} {
		stream := io.MultiReader(bytes.NewReader(head), zeros{})
		var err error
		grown := allocated(func() {
			_, _, err = ReadFrame(stream, nil, cluster, limit)
		})
		if !errors.Is(err, ErrTooLarge) || grown > 64<<10 {
			t.Errorf("ReadFrame, limit %d: %v after allocating %d bytes", limit, err, grown)
		}
		grown = allocated(func() {
			_, _, err = Decode(head, cluster, limit)
		})
		if !errors.Is(err, ErrTooLarge) || grown > 64<<10 {
			t.Errorf("Decode, limit %d: %v after allocating %d bytes", limit, err, grown)
		}
	}
}

func TestLimitsAreExact(t *testing.T) {
	if MaxClientBody != 42+1<<20 || MaxPeerBody != 4<<20 || MaxEntryBody != 41+1<<20 || ChunkSize != 256<<10 || HelloSize != 25 {
		t.Fatalf("limits %d %d %d %d %d", MaxClientBody, MaxPeerBody, MaxEntryBody, ChunkSize, HelloSize)
	}
	for _, limit := range []uint32{MaxClientBody, MaxPeerBody} {
		fits, over := raw(TypeSnapshotChunk, make([]byte, limit)), raw(TypeSnapshotChunk, make([]byte, limit+1))
		for name, read := range readers {
			if f, err := read(fits, limit); err != nil || len(f.Body) != int(limit) {
				t.Errorf("%s, limit %d: body at the limit: %v", name, limit, err)
			}
			if _, err := read(over, limit); !errors.Is(err, ErrTooLarge) {
				t.Errorf("%s, limit %d: body over the limit: %v", name, limit, err)
			}
		}
	}

	sizes := []struct {
		typ      Type
		fit, off []int
	}{
		{TypeRequest, []int{33, 33 + 1<<20}, []int{32, 33 + 1<<20 + 1}},
		{TypeReply, []int{42, 42 + 1<<20}, []int{41, 42 + 1<<20 + 1}},
		{TypeStatusRequest, []int{0}, []int{1}},
		{TypeStatus, []int{182}, []int{181, 183}},
		{TypeHello, []int{25}, []int{24, 26}},
		{TypeMessage + Type(raft.MsgPreVote), []int{40}, []int{39, 41}},
		{TypeMessage + Type(raft.MsgPreVoteResp), []int{25}, []int{24, 26}},
		{TypeMessage + Type(raft.MsgVote), []int{40}, []int{39, 41}},
		{TypeMessage + Type(raft.MsgVoteResp), []int{25}, []int{24, 26}},
		{TypeMessage + Type(raft.MsgApp), []int{60}, []int{59}},
		{TypeMessage + Type(raft.MsgAppResp), []int{41}, []int{40, 42}},
		{TypeMessage + Type(raft.MsgSnap), []int{56}, []int{55, 57}},
		{TypeMessage + Type(raft.MsgRepairProbe), []int{32}, []int{31, 33}},
		{TypeMessage + Type(raft.MsgRepairInfo), []int{40}, []int{39, 41}},
		{TypeSnapshotChunk, []int{1, 256 << 10}, []int{0, 256<<10 + 1}},
		{TypeSnapshotEnd, []int{12}, []int{11, 13}},
	}
	for _, s := range sizes {
		for _, size := range s.fit {
			if !accepted(Frame{Type: s.typ, Body: make([]byte, size)}) {
				t.Errorf("type %#x: body of %d bytes refused", uint8(s.typ), size)
			}
		}
		for _, size := range s.off {
			f := Frame{Type: s.typ, Body: make([]byte, size)}
			for _, recode := range recoders {
				if _, err := recode(nil, f); !errors.Is(err, ErrFormat) {
					t.Errorf("type %#x: body of %d bytes: %v", uint8(s.typ), size, err)
				}
			}
		}
	}
	for _, typ := range []Type{0, 0x05, 0x0f, 0x11, TypeMessage, 0x2c, 0xff} {
		if accepted(Frame{Type: typ}) || accepted(Frame{Type: typ, Body: make([]byte, 40)}) {
			t.Errorf("type %#x accepted", uint8(typ))
		}
	}

	entry := raft.Message{Type: raft.MsgApp, Entries: []raft.Entry{{Index: 1, Body: make([]byte, MaxEntryBody)}}}
	frame, err := AppendMessage(nil, cluster, entry)
	if err != nil {
		t.Fatalf("entry at the limit: %v", err)
	}
	if _, err := parse(t, frame).Message(); err != nil {
		t.Errorf("entry at the limit: %v", err)
	}
	body := slices.Concat(frame[HeaderSize:], []byte{0})
	binary.BigEndian.PutUint32(body[appendHead+9:], MaxEntryBody+1)
	if _, err := (Frame{Type: TypeMessage + Type(raft.MsgApp), Body: body}).Message(); !errors.Is(err, ErrFormat) {
		t.Errorf("entry over the limit decodes: %v", err)
	}
	entry.Entries[0].Body = make([]byte, MaxEntryBody+1)
	if _, err := AppendMessage(nil, cluster, entry); !errors.Is(err, ErrTooLarge) {
		t.Errorf("entry over the limit encodes: %v", err)
	}

	const count = 4
	share := (MaxPeerBody - appendHead - count*entryHead) / count
	widest := raft.Message{Type: raft.MsgApp}
	for i := range count {
		widest.Entries = append(widest.Entries, raft.Entry{Index: uint64(i) + 1, Body: make([]byte, share)})
	}
	if frame, err = AppendMessage(nil, cluster, widest); err != nil || len(frame) != HeaderSize+MaxPeerBody {
		t.Fatalf("message at the limit: %d bytes, %v", len(frame), err)
	}
	if _, err := parse(t, frame).Message(); err != nil {
		t.Errorf("message at the limit: %v", err)
	}
	if _, err := (Frame{Type: TypeMessage + Type(raft.MsgApp), Body: make([]byte, MaxPeerBody+1)}).Message(); !errors.Is(err, ErrFormat) {
		t.Errorf("message over the limit decodes: %v", err)
	}
	widest.Entries[0].Body = make([]byte, share+1)
	if _, err := AppendMessage(nil, cluster, widest); !errors.Is(err, ErrTooLarge) {
		t.Errorf("message over the limit encodes: %v", err)
	}
}

func TestTrailingBytesAreRefused(t *testing.T) {
	for _, s := range samples() {
		f := parse(t, s.frame)
		switch f.Type {
		case TypeRequest, TypeReply, TypeSnapshotChunk:
			continue
		}
		for _, extra := range []int{1, entryHead} {
			longer := Frame{Type: f.Type, Body: slices.Concat(f.Body, make([]byte, extra))}
			for _, recode := range recoders {
				if _, err := recode(nil, longer); !errors.Is(err, ErrFormat) {
					t.Errorf("%s with %d more bytes: %v", s.name, extra, err)
				}
			}
		}
	}
	empty := Frame{Type: TypeMessage + Type(raft.MsgApp), Body: make([]byte, appendHead+entryHead)}
	if _, err := empty.Message(); !errors.Is(err, ErrFormat) {
		t.Errorf("an entry after a count of 0: %v", err)
	}
}

func TestRejectIsZeroOrOne(t *testing.T) {
	for _, typ := range []raft.MsgType{raft.MsgPreVoteResp, raft.MsgVoteResp, raft.MsgAppResp} {
		f := parse(t, message(raft.Message{Type: typ}))
		last := len(f.Body) - 1
		for _, b := range []byte{0, 1} {
			f.Body[last] = b
			if m, err := f.Message(); err != nil || m.Reject != (b == 1) {
				t.Errorf("%s with reject byte %d: reject %v, %v", typ, b, m.Reject, err)
			}
		}
		for _, b := range []byte{2, 3, 0x80, 0x81, 0xff} {
			f.Body[last] = b
			if _, err := f.Message(); !errors.Is(err, ErrFormat) {
				t.Errorf("%s with reject byte %d: %v", typ, b, err)
			}
		}
	}
}

func TestStatusRoleAndFlagsAreKnown(t *testing.T) {
	f := parse(t, AppendStatus(nil, cluster, Status{}))
	for role := range 256 {
		f.Body[8] = byte(role)
		if _, err := f.Status(); (err == nil) != (role <= int(raft.Leader)) {
			t.Errorf("role %d: %v", role, err)
		}
	}
	f.Body[8] = 0
	for flags := range 256 {
		f.Body[9] = byte(flags)
		if st, err := f.Status(); (err == nil) != (flags <= 1) || st.Repair != (flags == 1) {
			t.Errorf("flags %#x: repair %v, %v", flags, st.Repair, err)
		}
	}
}

func TestEntryCountIsCheckedBeforeMake(t *testing.T) {
	app := raft.Message{Type: raft.MsgApp, Index: 7, Entries: []raft.Entry{{Index: 8}, {Index: 9}}}
	f := parse(t, message(app))
	for _, count := range []uint32{3, 1 << 20, math.MaxUint32} {
		binary.BigEndian.PutUint32(f.Body[appendHead-4:], count)
		var err error
		grown := allocated(func() {
			_, err = f.Message()
		})
		if !errors.Is(err, ErrFormat) || grown > 64<<10 {
			t.Errorf("count %d over two entries: %v after allocating %d bytes", count, err, grown)
		}
	}
	binary.BigEndian.PutUint32(f.Body[appendHead-4:], 1)
	if _, err := f.Message(); !errors.Is(err, ErrFormat) {
		t.Errorf("count 1 over two entries: %v", err)
	}

	binary.BigEndian.PutUint32(f.Body[appendHead-4:], 2)
	binary.BigEndian.PutUint32(f.Body[appendHead+9:], 5)
	if _, err := f.Message(); !errors.Is(err, ErrFormat) {
		t.Errorf("first entry reaching into the second: %v", err)
	}
	binary.BigEndian.PutUint32(f.Body[appendHead+9:], 0)
	for prev, fits := range map[uint64]bool{math.MaxUint64 - 2: true, math.MaxUint64 - 1: false, math.MaxUint64: false} {
		binary.BigEndian.PutUint64(f.Body[messageHead:], prev)
		if _, err := f.Message(); (err == nil) != fits || !fits && !errors.Is(err, ErrFormat) {
			t.Errorf("two entries after index %d: %v", prev, err)
		}
		app.Index, app.Entries[0].Index, app.Entries[1].Index = prev, prev+1, prev+2
		if _, err := AppendMessage(nil, cluster, app); (err == nil) != fits || !fits && !errors.Is(err, ErrFormat) {
			t.Errorf("two entries after index %d encode: %v", prev, err)
		}
	}
}

func TestMessagesTheEncoderRefuses(t *testing.T) {
	for name, m := range map[string]raft.Message{
		"type 0":       {},
		"type 10":      {Type: raft.MsgRepairInfo + 1},
		"gap":          {Type: raft.MsgApp, Index: 3, Entries: []raft.Entry{{Index: 4}, {Index: 6}}},
		"index 0":      {Type: raft.MsgApp, Index: 3, Entries: []raft.Entry{{Index: 0}}},
		"same index":   {Type: raft.MsgApp, Index: 3, Entries: []raft.Entry{{Index: 3}}},
		"out of order": {Type: raft.MsgApp, Index: 3, Entries: []raft.Entry{{Index: 5}, {Index: 4}}},
	} {
		if dst, err := AppendMessage([]byte("kept"), cluster, m); !errors.Is(err, ErrFormat) || string(dst) != "kept" {
			t.Errorf("%s: left %q, %v", name, dst, err)
		}
	}
}

func TestOtherClusterIsRefused(t *testing.T) {
	req := client.Request{Number: 1, Op: client.CreateAccounts, Body: make([]byte, 128)}
	for _, other := range []uint64{0, 1, cluster + 1, cluster ^ 1<<63} {
		frame := AppendRequest(nil, other, req)
		for name, read := range readers {
			if _, err := read(frame, MaxClientBody); !errors.Is(err, ErrCluster) {
				t.Errorf("%s: frame of cluster %#x: %v", name, other, err)
			}
			if _, err := read(frame[:HeaderSize], MaxClientBody); !errors.Is(err, ErrCluster) {
				t.Errorf("%s: header of cluster %#x: %v", name, other, err)
			}
		}
	}
	unset := AppendRequest(nil, 0, req)
	if _, _, err := Decode(unset, 0, MaxClientBody); !errors.Is(err, ErrCluster) {
		t.Errorf("Decode for cluster 0: %v", err)
	}
	if _, _, err := ReadFrame(bytes.NewReader(unset), nil, 0, MaxClientBody); !errors.Is(err, ErrCluster) {
		t.Errorf("ReadFrame for cluster 0: %v", err)
	}
}

func TestOtherVersionIsRefused(t *testing.T) {
	for _, version := range []byte{0, Version - 1, Version + 1, 0xff} {
		head := appendHeader(nil, cluster, TypeRequest)
		head[offVersion] = version
		frame := finish(append(head, make([]byte, 128)...), 0)
		for name, read := range readers {
			if _, err := read(frame, MaxClientBody); !errors.Is(err, ErrVersion) {
				t.Errorf("%s: frame of version %d: %v", name, version, err)
			}
			if _, err := read(frame[:HeaderSize], MaxClientBody); !errors.Is(err, ErrVersion) {
				t.Errorf("%s: header of version %d: %v", name, version, err)
			}
		}
	}
}

func TestHeaderIsCheckedInOrder(t *testing.T) {
	head := appendHeader(nil, cluster+1, TypeRequest)
	copy(head, "dmnl")
	head[offVersion], head[offReserved+1] = Version+1, 1
	binary.BigEndian.PutUint32(head[offSize:], MaxPeerBody+1)
	steps := []struct {
		want error
		mend func()
	}{
		{ErrMagic, func() { copy(head, magic) }},
		{ErrVersion, func() { head[offVersion] = Version }},
		{ErrFormat, func() { head[offReserved+1] = 0 }},
		{ErrCluster, func() { binary.BigEndian.PutUint64(head[offCluster:], cluster) }},
		{ErrTooLarge, func() { binary.BigEndian.PutUint32(head[offSize:], 3) }},
		{io.ErrUnexpectedEOF, func() { head = append(head, 1, 2, 3) }},
		{ErrChecksum, func() { head = finish(head, 0) }},
	}
	for _, step := range steps {
		for name, read := range readers {
			if _, err := read(head, MaxPeerBody); !errors.Is(err, step.want) {
				t.Errorf("%s: %v, want %v", name, err, step.want)
			}
		}
		step.mend()
	}
	for name, read := range readers {
		if f, err := read(head, MaxPeerBody); err != nil || f.Type != TypeRequest || !bytes.Equal(f.Body, []byte{1, 2, 3}) {
			t.Errorf("%s: mended frame %+v, %v", name, f, err)
		}
	}
	for _, off := range []int{offReserved, offReserved + 1} {
		reserved := AppendStatusRequest(nil, cluster)
		reserved[off] = 0x80
		for name, read := range readers {
			if _, err := read(finish(reserved, 0), MaxPeerBody); !errors.Is(err, ErrFormat) {
				t.Errorf("%s: reserved byte %d set: %v", name, off, err)
			}
		}
	}
}

func TestReadFrameFromOneByteReader(t *testing.T) {
	var stream []byte
	for _, s := range samples() {
		stream = append(stream, s.frame...)
	}
	for name, slow := range map[string]func(io.Reader) io.Reader{
		"one byte": iotest.OneByteReader,
		"half":     iotest.HalfReader,
		"late eof": iotest.DataErrReader,
	} {
		r := slow(bytes.NewReader(stream))
		var buf []byte
		for _, s := range samples() {
			f, grown, err := ReadFrame(r, buf, cluster, MaxPeerBody)
			if err != nil {
				t.Fatalf("%s: %s: %v", name, s.name, err)
			}
			if want := parse(t, s.frame); f.Type != want.Type || !bytes.Equal(f.Body, want.Body) {
				t.Errorf("%s: %s read as type %#x with %x", name, s.name, uint8(f.Type), f.Body)
			}
			if len(buf) > 0 && len(f.Body) > 0 && cap(buf) >= len(f.Body) && &grown[0] != &buf[0] {
				t.Errorf("%s: %s: buffer of %d bytes replaced for a body of %d", name, s.name, cap(buf), len(f.Body))
			}
			buf = grown[:cap(grown)]
		}
		if _, _, err := ReadFrame(r, buf, cluster, MaxPeerBody); err != io.EOF {
			t.Errorf("%s: after the last frame: %v", name, err)
		}
	}
}

type broken struct{}

func (broken) Read([]byte) (int, error) {
	return 0, errors.ErrUnsupported
}

func TestCutStreamIsUnexpectedEOF(t *testing.T) {
	frame := AppendReply(nil, cluster, client.Reply{Number: 1, Op: client.LookupAccounts, Status: client.OK, Body: make([]byte, 128)})
	for name, read := range readers {
		if _, err := read(nil, MaxClientBody); err != io.EOF {
			t.Errorf("%s: empty stream: %v", name, err)
		}
		for cut := 1; cut < len(frame); cut++ {
			if _, err := read(frame[:cut], MaxClientBody); err != io.ErrUnexpectedEOF {
				t.Errorf("%s: stream cut at %d: %v", name, cut, err)
			}
		}
	}
	for _, cut := range []int{0, 5, HeaderSize, HeaderSize + 9} {
		stream := io.MultiReader(bytes.NewReader(frame[:cut]), broken{})
		if _, _, err := ReadFrame(stream, nil, cluster, MaxClientBody); !errors.Is(err, errors.ErrUnsupported) {
			t.Errorf("read error after %d bytes: %v", cut, err)
		}
	}
}

func TestLargestAppendFitsTheFrame(t *testing.T) {
	const (
		maxAppendBytes = 1 << 20
		maxUncommitted = 8192
		walFrameHead   = 37
	)
	stable := maxAppendBytes / walFrameHead
	count := stable + maxUncommitted + 1
	if worst := appendHead + count*entryHead + MaxEntryBody; worst > MaxPeerBody {
		t.Fatalf("%d entries with %d bytes of bodies need %d bytes, MaxPeerBody is %d", count, MaxEntryBody, worst, MaxPeerBody)
	}
	app := raft.Message{Type: raft.MsgApp, From: 1, To: 2, Term: 3, Index: 4, LogTerm: 3, Commit: 4, Seq: 5}
	for i := range count {
		app.Entries = append(app.Entries, raft.Entry{Index: app.Index + 1 + uint64(i), Term: 3, Kind: 2})
	}
	app.Entries[stable].Body = make([]byte, MaxEntryBody)
	frame, err := AppendMessage(nil, cluster, app)
	if err != nil {
		t.Fatal(err)
	}
	f, _, err := ReadFrame(bytes.NewReader(frame), nil, cluster, MaxPeerBody)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := f.Message(); err != nil || !reflect.DeepEqual(got, app) {
		t.Fatalf("largest append came back with %d entries, %v", len(got.Entries), err)
	}
}

func chunked(snap []byte, sizes ...int) [][]byte {
	var frames [][]byte
	rest := snap
	for _, size := range sizes {
		frames = append(frames, AppendChunk(nil, cluster, rest[:size]))
		rest = rest[size:]
	}
	for chunk := range slices.Chunk(rest, ChunkSize) {
		frames = append(frames, AppendChunk(nil, cluster, chunk))
	}
	return append(frames, AppendSnapshotEnd(nil, cluster, uint64(len(snap)), crc32.Checksum(snap, castagnoli)))
}

func assemble(frames [][]byte) ([]byte, error) {
	rest := slices.Concat(frames...)
	var snap []byte
	for {
		f, tail, err := Decode(rest, cluster, ChunkSize)
		if err != nil {
			return nil, err
		}
		rest = tail
		if f.Type != TypeSnapshotChunk {
			size, crc, err := f.SnapshotEnd()
			switch {
			case err != nil:
				return nil, err
			case size != uint64(len(snap)) || crc != crc32.Checksum(snap, castagnoli) || len(rest) != 0:
				return nil, errStream
			}
			return snap, nil
		}
		chunk, err := f.Chunk()
		if err != nil {
			return nil, err
		}
		snap = append(snap, chunk...)
	}
}

func TestSnapshotEndCatchesAMissingChunk(t *testing.T) {
	snap := make([]byte, 2*ChunkSize+100)
	for i := range snap {
		snap[i] = byte(i * 7)
	}
	frames := chunked(snap, 1, 100, 100)
	if got, err := assemble(frames); err != nil || !bytes.Equal(got, snap) {
		t.Fatalf("whole stream: %d bytes, %v", len(got), err)
	}
	last := len(frames) - 1
	size, crc, err := parse(t, frames[last]).SnapshotEnd()
	if err != nil || size != uint64(len(snap)) || crc != crc32.Checksum(snap, castagnoli) {
		t.Fatalf("end says %d bytes with checksum %#x, %v", size, crc, err)
	}
	for i := range last {
		if _, err := assemble(slices.Delete(slices.Clone(frames), i, i+1)); !errors.Is(err, errStream) {
			t.Errorf("chunk %d missing: %v", i, err)
		}
		if _, err := assemble(slices.Insert(slices.Clone(frames), i, frames[i])); !errors.Is(err, errStream) {
			t.Errorf("chunk %d twice: %v", i, err)
		}
	}
	swapped := slices.Clone(frames)
	swapped[1], swapped[2] = swapped[2], swapped[1]
	if _, err := assemble(swapped); !errors.Is(err, errStream) {
		t.Errorf("two chunks of one length swapped: %v", err)
	}
	if _, err := assemble(frames[:last]); err != io.EOF {
		t.Errorf("end missing: %v", err)
	}
	if _, err := assemble(append(slices.Clone(frames), frames[0])); !errors.Is(err, errStream) {
		t.Errorf("chunk after the end: %v", err)
	}
	if got, err := assemble(chunked(nil)); err != nil || len(got) != 0 {
		t.Errorf("empty snapshot: %d bytes, %v", len(got), err)
	}
}
