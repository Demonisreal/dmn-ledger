package raft

import (
	"math/rand/v2"
	"testing"
)

type benchNode struct {
	r   *Raft
	log *memLog
}

func newBenchNode(b *testing.B, id uint64, peers []uint64, hs HardState, terms ...uint64) *benchNode {
	log := newMemLog(terms...)
	r, err := New(Config{
		ID: id, Peers: peers, ElectionTicks: 10, HeartbeatTicks: 2, SnapshotTicks: 50,
		MaxAppendBytes: 4 << 20, MaxUncommitted: 1 << 20, Rand: rand.New(rand.NewPCG(id, 1)),
		Log: log, HardState: hs,
	})
	if err != nil {
		b.Fatal(err)
	}
	return &benchNode{r: r, log: log}
}

func (n *benchNode) flush() []Message {
	rd := n.r.Ready()
	if rd.Snapshot != nil {
		n.log.install(*rd.Snapshot)
	}
	if len(rd.Entries) > 0 {
		n.log.append(rd.Entries)
	}
	n.r.Advance()
	return rd.Messages
}

func (n *benchNode) trim() {
	if n.r.commit > n.log.first+4096 {
		n.log.compact(n.r.commit, n.r.commit-1024)
	}
}

func BenchmarkStepHeartbeat(b *testing.B) {
	n := newBenchNode(b, 2, []uint64{1, 2, 3}, HardState{Term: 2}, 1, 2)
	beat := Message{Type: MsgApp, From: 1, To: 2, Term: 2, Index: 2, LogTerm: 2, Commit: 2}
	b.ReportAllocs()
	for b.Loop() {
		if err := n.r.Step(beat); err != nil {
			b.Fatal(err)
		}
		n.flush()
	}
}

func BenchmarkFollowerAppend(b *testing.B) {
	n := newBenchNode(b, 2, []uint64{1, 2, 3}, HardState{Term: 2}, 1)
	ents := make([]Entry, 16)
	body := make([]byte, 64)
	b.ReportAllocs()
	b.SetBytes(int64(len(ents) * len(body)))
	for b.Loop() {
		last := n.r.lastIndex()
		for k := range ents {
			ents[k] = Entry{Index: last + 1 + uint64(k), Term: 2, Kind: 1, Body: body}
		}
		lt, _, err := n.r.termAt(last)
		if err != nil {
			b.Fatal(err)
		}
		m := Message{Type: MsgApp, From: 1, To: 2, Term: 2, Index: last, LogTerm: lt, Commit: last, Entries: ents}
		if err := n.r.Step(m); err != nil {
			b.Fatal(err)
		}
		n.flush()
		n.trim()
	}
}

func BenchmarkProposeCommit(b *testing.B) {
	peers := []uint64{1, 2, 3}
	nodes := []*benchNode{
		newBenchNode(b, 1, peers, HardState{Term: 1}),
		newBenchNode(b, 2, peers, HardState{Term: 1}),
		newBenchNode(b, 3, peers, HardState{Term: 1}),
	}
	route := func(msgs []Message) {
		for len(msgs) > 0 {
			var next []Message
			for _, m := range msgs {
				to := nodes[m.To-1]
				if err := to.r.Step(m); err != nil {
					b.Fatal(err)
				}
				next = append(next, to.flush()...)
			}
			msgs = next
		}
	}
	lead := nodes[0]
	lead.r.elapsed = lead.r.timeout - 1
	if err := lead.r.Tick(); err != nil {
		b.Fatal(err)
	}
	route(lead.flush())
	if lead.r.role != Leader {
		b.Fatal("no leader")
	}
	body := make([]byte, 64)
	b.ReportAllocs()
	for b.Loop() {
		i, err := lead.r.Propose(1, body)
		if err != nil {
			b.Fatal(err)
		}
		route(lead.flush())
		for lead.r.HasReady() {
			route(lead.flush())
		}
		if lead.r.commit != i {
			b.Fatalf("commit %d, want %d", lead.r.commit, i)
		}
		for _, n := range nodes {
			n.trim()
		}
	}
}
