package raft

import (
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"testing"
)

func TestNewRejectsConfig(t *testing.T) {
	valid := func() Config {
		return Config{
			ID: 2, Peers: []uint64{1, 2, 3}, ElectionTicks: 10, HeartbeatTicks: 2, SnapshotTicks: 50,
			MaxAppendBytes: 1 << 20, MaxUncommitted: 16, Rand: rand.New(rand.NewPCG(1, 1)),
			Log: newMemLog(1, 2), HardState: HardState{Term: 2},
		}
	}
	if _, err := New(valid()); err != nil {
		t.Fatalf("valid config: %v", err)
	}
	cases := []struct {
		name string
		edit func(*Config)
	}{
		{"two peers", func(c *Config) { c.Peers = []uint64{1, 2} }},
		{"peer 0", func(c *Config) { c.Peers = []uint64{0, 1, 2} }},
		{"unsorted peers", func(c *Config) { c.Peers = []uint64{1, 3, 2} }},
		{"duplicate peers", func(c *Config) { c.Peers = []uint64{1, 2, 2} }},
		{"id not a peer", func(c *Config) { c.ID = 4 }},
		{"heartbeat not below election", func(c *Config) { c.HeartbeatTicks = 10 }},
		{"no heartbeat", func(c *Config) { c.HeartbeatTicks = 0 }},
		{"no snapshot ticks", func(c *Config) { c.SnapshotTicks = 0 }},
		{"negative append bytes", func(c *Config) { c.MaxAppendBytes = -1 }},
		{"no uncommitted budget", func(c *Config) { c.MaxUncommitted = 0 }},
		{"no rand", func(c *Config) { c.Rand = nil }},
		{"no log", func(c *Config) { c.Log = nil }},
		{"term below last entry", func(c *Config) { c.HardState.Term = 1 }},
		{"log unreadable", func(c *Config) { c.Log.(*memLog).err = errors.New("eio") }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := valid()
			tc.edit(&cfg)
			if _, err := New(cfg); err == nil {
				t.Fatal("accepted")
			}
		})
	}
}

func TestLogErrorStopsNode(t *testing.T) {
	eio := errors.New("eio")
	t.Run("step", func(t *testing.T) {
		c := newCluster(t, 3, defaultTuning, 1)
		n := c.load(1, HardState{Term: 2}, 1, 1, 2)
		n.log.err = eio
		err := n.r.Step(Message{Type: MsgApp, From: 2, To: 1, Term: 2, Index: 2, LogTerm: 1})
		if !errors.Is(err, eio) || errors.Is(err, ErrSafety) {
			t.Fatalf("err %v", err)
		}
	})
	t.Run("vote", func(t *testing.T) {
		c := newCluster(t, 3, defaultTuning, 1)
		n := c.load(1, HardState{Term: 2}, 1, 1, 2)
		n.log.err = eio
		for _, typ := range []MsgType{MsgVote, MsgPreVote} {
			if err := n.r.Step(Message{Type: typ, From: 2, To: 1, Term: 3, Index: 3, LogTerm: 2}); !errors.Is(err, eio) {
				t.Fatalf("%v: err %v", typ, err)
			}
		}
	})
	t.Run("campaign", func(t *testing.T) {
		c := newCluster(t, 3, defaultTuning, 1)
		n := c.load(1, HardState{Term: 2}, 1, 1, 2)
		n.log.err = eio
		n.r.elapsed = n.r.timeout - 1
		if err := n.r.Tick(); !errors.Is(err, eio) {
			t.Fatalf("err %v", err)
		}
	})
	t.Run("heartbeat to lagging follower", func(t *testing.T) {
		c := newCluster(t, 3, defaultTuning, 1)
		n := c.node(1)
		c.elect(n)
		c.isolate(3)
		c.propose(n, "a")
		c.settle()
		n.log.err = eio
		n.r.prs[2].inflight = false
		var err error
		for range defaultTuning.heartbeat {
			if err = n.r.Tick(); err != nil {
				break
			}
		}
		if !errors.Is(err, eio) {
			t.Fatalf("err %v", err)
		}
	})
}

func TestMalformedMessagesIgnored(t *testing.T) {
	cases := []struct {
		name string
		edit func(*Message)
	}{
		{"wrong recipient", func(m *Message) { m.To = 3 }},
		{"from self", func(m *Message) { m.From = 1 }},
		{"unknown sender", func(m *Message) { m.From = 7 }},
		{"term 0", func(m *Message) { m.Term = 0 }},
		{"type 0", func(m *Message) { m.Type = 0 }},
		{"unknown type", func(m *Message) { m.Type = MsgRepairInfo + 1 }},
		{"log term above term", func(m *Message) { m.LogTerm = 4 }},
		{"term at the limit", func(m *Message) { m.Term = math.MaxUint64 }},
		{"log term without index", func(m *Message) { m.Index, m.LogTerm = 0, 1 }},
		{"gap in entries", func(m *Message) { m.Entries = []Entry{ent(3, 3), ent(5, 3)} }},
		{"entry from a later term", func(m *Message) { m.Entries = []Entry{ent(3, 4)} }},
		{"entry terms falling", func(m *Message) { m.Entries = []Entry{ent(3, 3), ent(4, 2)} }},
		{"entry term below log term", func(m *Message) { m.Entries = []Entry{ent(3, 1)} }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := newCluster(t, 3, defaultTuning, 1)
			n := c.load(1, HardState{Term: 2}, 1, 2)
			m := Message{Type: MsgApp, From: 2, To: 1, Term: 3, Index: 2, LogTerm: 2}
			tc.edit(&m)
			before := n.r.Status()
			c.must(n, n.r.Step(m))
			if n.r.HasReady() || n.r.Status() != before {
				t.Fatalf("malformed message changed state: %+v", n.r.Status())
			}
		})
	}
}

func TestStrings(t *testing.T) {
	cases := []struct {
		v    fmt.Stringer
		want string
	}{
		{MsgPreVote, "PreVote"},
		{MsgPreVoteResp, "PreVoteResp"},
		{MsgVote, "Vote"},
		{MsgVoteResp, "VoteResp"},
		{MsgApp, "App"},
		{MsgAppResp, "AppResp"},
		{MsgSnap, "Snap"},
		{MsgRepairProbe, "RepairProbe"},
		{MsgRepairInfo, "RepairInfo"},
		{MsgType(0), "MsgType(0)"},
		{Follower, "follower"},
		{PreCandidate, "precandidate"},
		{Candidate, "candidate"},
		{Leader, "leader"},
		{Role(9), "Role(9)"},
	}
	for _, tc := range cases {
		if got := tc.v.String(); got != tc.want {
			t.Errorf("%q, want %q", got, tc.want)
		}
	}
}
