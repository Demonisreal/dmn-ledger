package wire

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/Demonisreal/dmn-ledger/internal/client"
	"github.com/Demonisreal/dmn-ledger/internal/raft"
	"github.com/Demonisreal/dmn-ledger/internal/u128"
)

var update = flag.Bool("update", false, "rewrite testdata/frames.golden")

const cluster = 0xc1c2c3c4c5c6c7c8

type sample struct {
	name  string
	frame []byte
}

func message(m raft.Message) []byte {
	frame, err := AppendMessage(nil, cluster, m)
	if err != nil {
		panic(err)
	}
	return frame
}

func samples() []sample {
	alice := u128.Uint128{Hi: 0x0102030405060708, Lo: 0x090a0b0c0d0e0f10}
	st := Status{
		ID:         0x1112131415161718,
		Role:       raft.Leader,
		Repair:     true,
		Term:       0x2122232425262728,
		Lead:       0x3132333435363738,
		Commit:     0x4142434445464748,
		Applied:    0x5152535455565758,
		Last:       0x6162636465666768,
		Sessions:   0x71727374,
		Time:       0x8182838485868788,
		Clock:      0x9192939495969798,
		NextExpiry: 0xa1a2a3a4a5a6a7a8,
	}
	for i := range st.ApplyHash {
		st.ApplyHash[i] = 0xe0 + byte(i)
	}
	m := raft.Message{
		From:    0x1112131415161718,
		To:      0x2122232425262728,
		Term:    0x3132333435363738,
		Index:   0x4142434445464748,
		LogTerm: 0x5152535455565758,
		Commit:  0x6162636465666768,
		Seq:     0x7172737475767778,
		Reject:  true,
		Entries: []raft.Entry{
			{Index: 0x4142434445464749, Term: 0x8182838485868788, Kind: 1},
			{Index: 0x414243444546474a, Term: 0x9192939495969798, Kind: 2, Body: []byte{0xb1, 0xb2, 0xb3}},
		},
	}
	kind := func(t raft.MsgType) []byte {
		m.Type = t
		return message(m)
	}
	return []sample{
		{"Request", AppendRequest(nil, cluster, client.Request{
			Client:  alice,
			Session: 0x1112131415161718,
			Number:  0x2122232425262728,
			Op:      client.CreateTransfers,
			Body:    []byte{0xb1, 0xb2, 0xb3},
		})},
		{"Reply", AppendReply(nil, cluster, client.Reply{
			Client: alice,
			Number: 0x2122232425262728,
			Op:     client.CreateTransfers,
			Status: client.Busy,
			Leader: 0x3132333435363738,
			Time:   0x4142434445464748,
			Body:   []byte{0xb1, 0xb2, 0xb3},
		})},
		{"StatusRequest", AppendStatusRequest(nil, cluster)},
		{"Status", AppendStatus(nil, cluster, st)},
		{"Hello", AppendHello(nil, cluster, Hello{From: 0x1112131415161718, To: 0x2122232425262728, Kind: KindSnapshot})},
		{"PreVote", kind(raft.MsgPreVote)},
		{"PreVoteResp", kind(raft.MsgPreVoteResp)},
		{"Vote", kind(raft.MsgVote)},
		{"VoteResp", kind(raft.MsgVoteResp)},
		{"App", kind(raft.MsgApp)},
		{"AppResp", kind(raft.MsgAppResp)},
		{"Snap", kind(raft.MsgSnap)},
		{"RepairProbe", kind(raft.MsgRepairProbe)},
		{"RepairInfo", kind(raft.MsgRepairInfo)},
		{"SnapshotChunk", AppendChunk(nil, cluster, []byte{0xb1, 0xb2, 0xb3})},
		{"SnapshotEnd", AppendSnapshotEnd(nil, cluster, 0x1112131415161718, 0x21222324)},
	}
}

func TestLayoutIsPinned(t *testing.T) {
	var frames strings.Builder
	for _, s := range samples() {
		fmt.Fprintf(&frames, "%s %x\n", s.name, s.frame)
	}
	const path = "testdata/frames.golden"
	if *update {
		if err := os.WriteFile(path, []byte(frames.String()), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	got, pinned := strings.Split(frames.String(), "\n"), strings.Split(string(want), "\n")
	if len(got) != len(pinned) {
		t.Fatalf("%d frames, %d pinned", len(got), len(pinned))
	}
	for i := range got {
		if got[i] != pinned[i] {
			t.Errorf("frame %q, pinned %q", got[i], pinned[i])
		}
	}
}
