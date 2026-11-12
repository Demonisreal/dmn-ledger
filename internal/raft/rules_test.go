package raft

import (
	"errors"
	"slices"
	"testing"
)

func TestHigherTermBecomesFollower(t *testing.T) {
	cases := []struct {
		name string
		m    Message
		role Role
		term uint64
		vote uint64
		lead uint64
	}{
		{"app", Message{Type: MsgApp, Term: 5, Index: 1, LogTerm: 1}, Follower, 5, 0, 2},
		{"snap", Message{Type: MsgSnap, Term: 5, Index: 1, LogTerm: 1}, Follower, 5, 0, 2},
		{"vote", Message{Type: MsgVote, Term: 5, Index: 1, LogTerm: 1}, Follower, 5, 2, 0},
		{"vote response", Message{Type: MsgVoteResp, Term: 5, Reject: true}, Follower, 5, 0, 0},
		{"append response", Message{Type: MsgAppResp, Term: 5, Reject: true}, Follower, 5, 0, 0},
		{"rejected prevote response", Message{Type: MsgPreVoteResp, Term: 5, Reject: true}, Follower, 5, 0, 0},
		{"repair probe", Message{Type: MsgRepairProbe, Term: 5, Seq: 7}, Follower, 5, 0, 0},
		{"repair info", Message{Type: MsgRepairInfo, Term: 5, Index: 1, Seq: 7}, Follower, 5, 0, 0},
		{"prevote", Message{Type: MsgPreVote, Term: 5, Index: 1, LogTerm: 1}, Leader, 1, 1, 1},
		{"granted prevote response", Message{Type: MsgPreVoteResp, Term: 5}, Leader, 1, 1, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := newCluster(t, 3, defaultTuning, 1)
			n := c.node(1)
			c.elect(n)
			tc.m.From = 2
			c.step(n, tc.m)
			st := n.r.Status()
			if st.Role != tc.role || st.Term != tc.term || st.Vote != tc.vote || st.Lead != tc.lead {
				t.Fatalf("status %+v, want %v term %d vote %d lead %d", st, tc.role, tc.term, tc.vote, tc.lead)
			}
		})
	}
}

func TestLowerTermRejected(t *testing.T) {
	cases := []struct {
		name  string
		m     Message
		reply MsgType
	}{
		{"app", Message{Type: MsgApp, Term: 2}, MsgAppResp},
		{"snap", Message{Type: MsgSnap, Term: 2, Index: 1, LogTerm: 1}, MsgAppResp},
		{"vote", Message{Type: MsgVote, Term: 2, Index: 9, LogTerm: 2}, MsgVoteResp},
		{"prevote", Message{Type: MsgPreVote, Term: 2, Index: 9, LogTerm: 2}, MsgPreVoteResp},
		{"append response", Message{Type: MsgAppResp, Term: 2, Index: 1}, 0},
		{"vote response", Message{Type: MsgVoteResp, Term: 2}, 0},
		{"prevote response", Message{Type: MsgPreVoteResp, Term: 2}, 0},
		{"repair probe", Message{Type: MsgRepairProbe, Term: 2, Seq: 7}, 0},
		{"repair info", Message{Type: MsgRepairInfo, Term: 2, Index: 1, Seq: 7}, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := newCluster(t, 3, defaultTuning, 1)
			n := c.load(1, HardState{Term: 3, Vote: 3}, 1, 1)
			tc.m.From = 2
			c.step(n, tc.m)
			msgs := c.sent(1)
			switch {
			case tc.reply == 0 && len(msgs) > 0:
				t.Fatalf("stale response answered: %+v", msgs)
			case tc.reply != 0 && (len(msgs) != 1 || msgs[0].Type != tc.reply || !msgs[0].Reject || msgs[0].Term != 3):
				t.Fatalf("reply %+v, want rejecting %v in term 3", msgs, tc.reply)
			}
			if st := n.r.Status(); st.Role != Follower || st.Term != 3 || st.Vote != 3 || st.Last != 2 || st.Commit != 0 {
				t.Fatalf("state changed: %+v", st)
			}
		})
	}
}

func ticksUntil(c *cluster, n *node, t MsgType, limit int) int {
	for k := 1; k <= limit; k++ {
		c.tick(n)
		if len(c.take(func(m Message) bool { return m.From == n.id && m.Type == t })) > 0 {
			return k
		}
	}
	return -1
}

func TestElectionTimeoutRange(t *testing.T) {
	seen := map[int]bool{}
	for seed := range uint64(100) {
		c := newCluster(t, 3, defaultTuning, seed)
		n := c.node(1)
		for round := range 3 {
			k := ticksUntil(c, n, MsgPreVote, 100)
			if k < 10 || k >= 20 {
				t.Fatalf("seed %d round %d: pre-vote after %d ticks, want [10, 20)", seed, round, k)
			}
			seen[k] = true
		}
	}
	if len(seen) < 8 {
		t.Fatalf("only %d distinct timeouts", len(seen))
	}
}

func TestElectionTimeoutReset(t *testing.T) {
	cases := []struct {
		name  string
		vote  uint64
		m     Message
		reset bool
	}{
		{"app from leader", 0, Message{Type: MsgApp, From: 2, Term: 1}, true},
		{"snap from leader", 0, Message{Type: MsgSnap, From: 2, Term: 1}, true},
		{"granted vote", 0, Message{Type: MsgVote, From: 2, Term: 1}, true},
		{"rejected vote", 2, Message{Type: MsgVote, From: 3, Term: 1}, false},
		{"granted prevote", 0, Message{Type: MsgPreVote, From: 2, Term: 2}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := newCluster(t, 3, defaultTuning, 1)
			n := c.load(1, HardState{Term: 1, Vote: tc.vote})
			for range n.r.timeout - 1 {
				c.tick(n)
			}
			c.step(n, tc.m)
			c.sent(1)
			c.tick(n)
			campaigned := len(c.take(is(MsgPreVote))) > 0
			if campaigned == tc.reset {
				t.Fatalf("campaigned %v, want reset %v", campaigned, tc.reset)
			}
		})
	}
}

func TestTimeoutStartsPreVote(t *testing.T) {
	c := newCluster(t, 3, defaultTuning, 1)
	n := c.load(1, HardState{Term: 3, Vote: 2}, 1, 2)
	n.r.elapsed = n.r.timeout - 1
	c.must(n, n.r.Tick())
	rd := n.r.Ready()
	if rd.HardState != nil || len(rd.Entries) > 0 {
		t.Fatalf("pre-vote persists: %+v", rd)
	}
	var dst []uint64
	for _, m := range rd.Messages {
		if m.Type != MsgPreVote || m.Term != 4 || m.Index != 2 || m.LogTerm != 2 {
			t.Fatalf("message %+v, want PreVote term 4 index 2 log term 2", m)
		}
		dst = append(dst, m.To)
	}
	if !slices.Equal(dst, []uint64{2, 3}) {
		t.Fatalf("pre-vote to %v", dst)
	}
	c.process(n)
	if st := n.r.Status(); st.Role != PreCandidate || st.Term != 3 || st.Vote != 2 {
		t.Fatalf("status %+v", st)
	}
}

func TestPreVoteGrant(t *testing.T) {
	cases := []struct {
		name  string
		m     Message
		heard bool
		grant bool
	}{
		{"same log", Message{Term: 3, Index: 3, LogTerm: 2}, false, true},
		{"longer log", Message{Term: 3, Index: 4, LogTerm: 2}, false, true},
		{"newer last term", Message{Term: 4, Index: 2, LogTerm: 3}, false, true},
		{"shorter log", Message{Term: 3, Index: 2, LogTerm: 2}, false, false},
		{"older last term", Message{Term: 3, Index: 5, LogTerm: 1}, false, false},
		{"term not higher", Message{Term: 2, Index: 3, LogTerm: 2}, false, false},
		{"leader heard recently", Message{Term: 3, Index: 3, LogTerm: 2}, true, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := newCluster(t, 3, defaultTuning, 1)
			n := c.load(1, HardState{Term: 2, Vote: 3}, 1, 1, 2)
			if tc.heard {
				c.step(n, Message{Type: MsgApp, From: 3, Term: 2, Index: 3, LogTerm: 2})
				c.sent(1)
			}
			tc.m.Type, tc.m.From = MsgPreVote, 2
			c.step(n, tc.m)
			resp := c.only(1)
			if resp.Type != MsgPreVoteResp || resp.Reject == tc.grant {
				t.Fatalf("response %+v, want grant %v", resp, tc.grant)
			}
			if tc.grant && resp.Term != tc.m.Term {
				t.Fatalf("granted in term %d, want proposed term %d", resp.Term, tc.m.Term)
			}
			if !tc.grant && resp.Term != 2 {
				t.Fatalf("rejected in term %d, want own term 2", resp.Term)
			}
			if st := n.r.Status(); st.Term != 2 || st.Vote != 3 || st.Role != Follower {
				t.Fatalf("pre-vote changed state: %+v", st)
			}
		})
	}
}

func TestLeaderRejectsPreVote(t *testing.T) {
	c := newCluster(t, 3, defaultTuning, 1)
	n := c.node(1)
	c.elect(n)
	n.r.sinceLead = n.r.electionTicks
	c.step(n, Message{Type: MsgPreVote, From: 2, Term: 2, Index: 1, LogTerm: 1})
	if resp := c.only(1); !resp.Reject {
		t.Fatalf("leader granted pre-vote: %+v", resp)
	}
	if n.r.role != Leader || n.r.term != 1 {
		t.Fatalf("status %+v", n.r.Status())
	}
}

func TestPreVoteMajorityStartsElection(t *testing.T) {
	for _, size := range []int{3, 5} {
		c := newCluster(t, size, defaultTuning, 1)
		n := c.node(1)
		c.timeout(n)
		for k, id := range c.ids[1 : size/2+1] {
			if n.r.role != PreCandidate || n.r.term != 0 || n.hs.Term != 0 {
				t.Fatalf("size %d: campaign after %d grants: %+v", size, k, n.r.Status())
			}
			c.deliverAll(func(m Message) bool { return m.Type == MsgPreVote && m.To == id })
			c.deliverAll(func(m Message) bool { return m.Type == MsgPreVoteResp && m.From == id })
		}
		if st := n.r.Status(); st.Role != Candidate || st.Term != 1 || st.Vote != 1 {
			t.Fatalf("size %d: status %+v", size, st)
		}
		if n.hs != (HardState{Term: 1, Vote: 1}) {
			t.Fatalf("size %d: hard state %+v not persisted", size, n.hs)
		}
		votes := c.take(is(MsgVote))
		if len(votes) != size-1 {
			t.Fatalf("size %d: %d vote requests", size, len(votes))
		}
		for _, env := range votes {
			if env.m.Term != 1 || env.m.From != 1 {
				t.Fatalf("vote request %+v", env.m)
			}
		}
	}
}

func TestVoteGrant(t *testing.T) {
	cases := []struct {
		name           string
		vote           uint64
		index, logTerm uint64
		grant          bool
	}{
		{"no vote, same log", 0, 3, 2, true},
		{"voted for candidate", 2, 3, 2, true},
		{"voted for other", 3, 3, 2, false},
		{"shorter log", 0, 2, 2, false},
		{"older last term", 0, 5, 1, false},
		{"newer last term", 0, 1, 3, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := newCluster(t, 3, defaultTuning, 1)
			n := c.load(1, HardState{Term: 3, Vote: tc.vote}, 1, 1, 2)
			c.must(n, n.r.Step(Message{Type: MsgVote, From: 2, To: 1, Term: 3, Index: tc.index, LogTerm: tc.logTerm}))
			rd := n.r.Ready()
			if len(rd.Messages) != 1 || rd.Messages[0].Type != MsgVoteResp || rd.Messages[0].Reject == tc.grant {
				t.Fatalf("messages %+v, want grant %v", rd.Messages, tc.grant)
			}
			switch {
			case tc.grant && tc.vote == 0 && (rd.HardState == nil || *rd.HardState != HardState{Term: 3, Vote: 2}):
				t.Fatalf("vote not in the same Ready: %+v", rd.HardState)
			case (!tc.grant || tc.vote != 0) && rd.HardState != nil:
				t.Fatalf("unexpected hard state %+v", rd.HardState)
			}
			c.process(n)
			want := tc.vote
			if tc.grant {
				want = 2
			}
			if n.hs.Vote != want {
				t.Fatalf("vote %d, want %d", n.hs.Vote, want)
			}
		})
	}
}

func TestBecomeLeaderAppendsEmptyEntry(t *testing.T) {
	c := newCluster(t, 3, defaultTuning, 1)
	n := c.load(1, HardState{Term: 1}, 1, 1)
	c.load(2, HardState{Term: 1}, 1)
	c.load(3, HardState{Term: 1}, 1)
	c.timeout(n)
	c.exchange(func(m Message) bool { return m.Type == MsgPreVote || m.Type == MsgPreVoteResp })
	c.deliverAll(is(MsgVote))
	resp := c.take(func(m Message) bool { return m.Type == MsgVoteResp && m.From == 2 })
	c.must(n, n.r.Step(resp[0].m))
	rd := n.r.Ready()
	if rd.Commit != 0 {
		t.Fatalf("new leader reports commit %d before anything of its term is stored", rd.Commit)
	}
	noop := Entry{Index: 3, Term: 2}
	if n.r.role != Leader || len(rd.Entries) != 1 || rd.Entries[0].Index != noop.Index || rd.Entries[0].Term != noop.Term || rd.Entries[0].Kind != 0 {
		t.Fatalf("role %v entries %+v, want empty entry %+v", n.r.role, rd.Entries, noop)
	}
	var dst []uint64
	for _, m := range rd.Messages {
		if m.Type != MsgApp || m.Index != 2 || m.LogTerm != 1 || m.Commit != 0 || len(m.Entries) != 1 || m.Entries[0].Index != 3 {
			t.Fatalf("message %+v", m)
		}
		dst = append(dst, m.To)
	}
	if !slices.Equal(dst, []uint64{2, 3}) {
		t.Fatalf("sent to %v", dst)
	}
	for i, p := range n.r.prs {
		if p.next != 3 || p.match != 0 {
			t.Fatalf("progress %d: %+v", i, p)
		}
	}
	c.process(n)
}

func TestCheckQuorum(t *testing.T) {
	t.Run("steps down without majority", func(t *testing.T) {
		c := newCluster(t, 3, defaultTuning, 1)
		n := c.node(1)
		c.elect(n)
		for k := 1; k < 20; k++ {
			c.tick(n)
			c.drop(func(Message) bool { return true })
			if n.r.role != Leader {
				t.Fatalf("stepped down after %d ticks", k)
			}
		}
		c.tick(n)
		if st := n.r.Status(); st.Role != Follower || st.Term != 1 || st.Vote != 1 || st.Lead != 0 {
			t.Fatalf("status %+v, want follower in term 1", st)
		}
	})
	t.Run("stays with majority", func(t *testing.T) {
		c := newCluster(t, 3, defaultTuning, 1)
		n := c.node(1)
		c.elect(n)
		for range 50 {
			c.tick(n)
			c.drop(to(3))
			c.deliverAll(to(2))
			c.deliverAll(from(2))
		}
		if n.r.role != Leader {
			t.Fatalf("leader with one follower stepped down: %+v", n.r.Status())
		}
	})
}

func TestOneAppendInFlight(t *testing.T) {
	c := newCluster(t, 3, defaultTuning, 1)
	n := c.node(1)
	c.elect(n)
	app2 := func(m Message) bool { return m.Type == MsgApp && m.To == 2 }
	c.propose(n, "a")
	if got := c.take(app2); len(got) != 1 || len(got[0].m.Entries) != 1 {
		t.Fatalf("first append %+v", got)
	}
	c.propose(n, "b")
	if got := c.take(app2); len(got) != 0 {
		t.Fatalf("second append while first unacknowledged: %+v", got)
	}
	c.must(n, c.read(n, 1))
	if got := c.take(app2); len(got) != 1 || len(got[0].m.Entries) != 0 {
		t.Fatalf("read must send an empty append: %+v", got)
	}
	c.tick(n)
	c.tick(n)
	got := c.take(app2)
	if len(got) != 1 || got[0].m.Index != 1 || len(got[0].m.Entries) != 2 {
		t.Fatalf("heartbeat resend %+v, want both entries after index 1", got)
	}
}

func TestAppendRespectsMaxBytes(t *testing.T) {
	tune := defaultTuning
	tune.maxBytes = 3
	c := newCluster(t, 3, tune, 1)
	n := c.node(1)
	c.elect(n)
	for _, body := range []string{"aa", "bb", "cc"} {
		c.propose(n, body)
	}
	c.drop(func(Message) bool { return true })
	c.tick(n)
	c.tick(n)
	for _, env := range c.take(is(MsgApp)) {
		if len(env.m.Entries) != 1 || string(env.m.Entries[0].Body) != "aa" {
			t.Fatalf("append %+v exceeds 3 bytes", env.m)
		}
	}
}

func TestSnapshotForCompactedFollower(t *testing.T) {
	tune := defaultTuning
	tune.snapshot = 6
	c := newCluster(t, 3, tune, 1)
	n := c.node(1)
	c.elect(n)
	c.isolate(3)
	for _, body := range []string{"a", "b", "c", "d", "e"} {
		c.propose(n, body)
		c.settle()
	}
	if n.r.commit != 6 {
		t.Fatalf("commit %d", n.r.commit)
	}
	c.snapshot(n, false)
	c.heal()
	to3 := func() []envelope { return c.take(to(3)) }
	keep := func() {
		c.deliverAll(to(2))
		c.deliverAll(from(2))
	}
	c.tick(n)
	c.tick(n)
	snaps := to3()
	if len(snaps) != 1 || snaps[0].m.Type != MsgSnap || snaps[0].m.Index != 6 || snaps[0].m.LogTerm != 1 {
		t.Fatalf("messages to 3: %+v", snaps)
	}
	keep()
	k, beats := 0, 0
	for ; k < 20; k++ {
		c.tick(n)
		keep()
		resent := false
		for _, env := range to3() {
			switch {
			case env.m.Type == MsgSnap:
				resent = true
			case env.m.Type == MsgApp && env.m.Index == 0 && len(env.m.Entries) == 0:
				beats++
			default:
				t.Fatalf("append while snapshot pending: %+v", env.m)
			}
		}
		if resent {
			break
		}
	}
	if beats == 0 {
		t.Fatal("no heartbeat to the follower while its snapshot is pending")
	}
	if k+1 < tune.snapshot || k+1 > tune.snapshot+tune.heartbeat {
		t.Fatalf("snapshot resent after %d ticks, want [%d, %d]", k+1, tune.snapshot, tune.snapshot+tune.heartbeat)
	}
	c.deliver(snaps[0])
	c.settle()
	f := c.node(3)
	if f.log.snap.Index != 6 || f.applied != 6 || f.hash != n.hash {
		t.Fatalf("follower snapshot %+v applied %d", f.log.snap, f.applied)
	}
	if p := n.r.prs[2]; p.match != 6 || p.snapping {
		t.Fatalf("progress after snapshot %+v", p)
	}
	c.checkOK()
}

func TestFollowerAppend(t *testing.T) {
	cases := []struct {
		name   string
		log    []uint64
		commit uint64
		m      Message
		reject bool
		index  uint64
		want   []uint64
		after  uint64
	}{
		{"below commit", []uint64{1, 1, 2, 2}, 3, Message{Index: 2, LogTerm: 1, Entries: []Entry{ent(3, 3)}}, false, 3, []uint64{1, 1, 2, 2}, 3},
		{"log too short", []uint64{1, 1}, 0, Message{Index: 4, LogTerm: 2}, true, 3, []uint64{1, 1}, 0},
		{"conflicting term", []uint64{1, 1, 2, 2, 2}, 1, Message{Index: 5, LogTerm: 3}, true, 3, []uint64{1, 1, 2, 2, 2}, 1},
		{"hint above commit", []uint64{1, 2, 2, 2}, 3, Message{Index: 4, LogTerm: 3}, true, 4, []uint64{1, 2, 2, 2}, 3},
		{"matching entries kept", []uint64{1, 1, 2}, 0, Message{Index: 1, LogTerm: 1, Entries: []Entry{ent(2, 1), ent(3, 2)}}, false, 3, []uint64{1, 1, 2}, 0},
		{"stale append keeps tail", []uint64{1, 1, 2, 2}, 0, Message{Index: 1, LogTerm: 1, Entries: []Entry{ent(2, 1)}}, false, 2, []uint64{1, 1, 2, 2}, 0},
		{"conflict replaces tail", []uint64{1, 1, 2, 2}, 0, Message{Index: 2, LogTerm: 1, Entries: []Entry{ent(3, 3)}}, false, 3, []uint64{1, 1, 3}, 0},
		{"new entries appended", []uint64{1, 1}, 0, Message{Index: 2, LogTerm: 1, Entries: []Entry{ent(3, 3), ent(4, 3)}}, false, 4, []uint64{1, 1, 3, 3}, 0},
		{"commit bounded by message", []uint64{1, 1, 1, 1}, 0, Message{Index: 1, LogTerm: 1, Commit: 4}, false, 1, []uint64{1, 1, 1, 1}, 1},
		{"commit follows message", []uint64{1, 1}, 0, Message{Index: 2, LogTerm: 1, Entries: []Entry{ent(3, 3)}, Commit: 5}, false, 3, []uint64{1, 1, 3}, 3},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := newCluster(t, 3, defaultTuning, 1)
			n := c.load(1, HardState{Term: 3}, tc.log...)
			if tc.commit > 0 {
				c.step(n, Message{Type: MsgApp, From: 2, Term: 3, Index: tc.commit, LogTerm: tc.log[tc.commit-1], Commit: tc.commit})
				c.sent(1)
			}
			tc.m.Type, tc.m.From, tc.m.Term, tc.m.Seq = MsgApp, 2, 3, 7
			c.step(n, tc.m)
			resp := c.only(1)
			if resp.Type != MsgAppResp || resp.Reject != tc.reject || resp.Index != tc.index || resp.Seq != 7 {
				t.Fatalf("response %+v, want reject %v index %d seq 7", resp, tc.reject, tc.index)
			}
			if got := n.log.terms(); !slices.Equal(got, tc.want) {
				t.Fatalf("log %v, want %v", got, tc.want)
			}
			if n.r.commit != tc.after {
				t.Fatalf("commit %d, want %d", n.r.commit, tc.after)
			}
		})
	}
}

func TestFollowerAppendCommittedConflict(t *testing.T) {
	c := newCluster(t, 3, defaultTuning, 1)
	n := c.load(1, HardState{Term: 3}, 1, 1, 2)
	c.step(n, Message{Type: MsgApp, From: 2, Term: 3, Index: 2, LogTerm: 1, Commit: 2})
	err := n.r.Step(Message{Type: MsgApp, From: 2, To: 1, Term: 3, Index: 2, LogTerm: 2})
	if !errors.Is(err, ErrSafety) {
		t.Fatalf("err %v, want ErrSafety", err)
	}
}

func TestSecondLeaderInTerm(t *testing.T) {
	c := newCluster(t, 3, defaultTuning, 1)
	n := c.node(1)
	c.elect(n)
	err := n.r.Step(Message{Type: MsgApp, From: 2, To: 1, Term: 1, Index: 1, LogTerm: 1})
	if !errors.Is(err, ErrSafety) {
		t.Fatalf("err %v, want ErrSafety", err)
	}
}

func TestLeaderHandlesAppResp(t *testing.T) {
	cases := []struct {
		name        string
		match, next uint64
		resp        Message
		wantMatch   uint64
		wantNext    uint64
		sent        bool
		prev        uint64
		ents        int
	}{
		{"success", 1, 2, Message{Index: 3}, 3, 4, true, 3, 1},
		{"stale success", 3, 4, Message{Index: 1}, 3, 4, false, 0, 0},
		{"reject lowers next", 1, 4, Message{Index: 2, Reject: true}, 1, 2, true, 1, 3},
		{"reject below match rewinds", 2, 4, Message{Index: 2, Reject: true}, 1, 2, true, 1, 3},
		{"stale-term rejection ignored", 1, 4, Message{Index: 0, Reject: true}, 1, 4, false, 0, 0},
		{"success beyond log ignored", 1, 2, Message{Index: 9}, 1, 2, false, 0, 0},
		{"reject at next dropped", 1, 3, Message{Index: 3, Reject: true}, 1, 3, false, 0, 0},
		{"reject above next dropped", 1, 3, Message{Index: 9, Reject: true}, 1, 3, false, 0, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := newCluster(t, 3, defaultTuning, 1)
			n := c.node(1)
			c.elect(n)
			for _, body := range []string{"a", "b", "c"} {
				c.propose(n, body)
			}
			c.drop(func(Message) bool { return true })
			p := &n.r.prs[1]
			p.match, p.next, p.inflight = tc.match, tc.next, true
			tc.resp.Type, tc.resp.From, tc.resp.Term = MsgAppResp, 2, 1
			c.step(n, tc.resp)
			if p.match != tc.wantMatch || p.next != tc.wantNext {
				t.Fatalf("match %d next %d, want %d %d", p.match, p.next, tc.wantMatch, tc.wantNext)
			}
			got := c.take(to(2))
			switch {
			case !tc.sent && len(got) > 0:
				t.Fatalf("unexpected send %+v", got)
			case tc.sent && (len(got) != 1 || got[0].m.Index != tc.prev || len(got[0].m.Entries) != tc.ents):
				t.Fatalf("sent %+v, want append after %d with %d entries", got, tc.prev, tc.ents)
			}
		})
	}
}

func TestCommitCountsLeaderAfterAdvance(t *testing.T) {
	c := newCluster(t, 5, defaultTuning, 1)
	n := c.node(1)
	c.elect(n)
	i, err := n.r.Propose(1, []byte("x"))
	c.must(n, err)
	for _, id := range []uint64{2, 3} {
		c.must(n, n.r.Step(Message{Type: MsgAppResp, From: id, To: 1, Term: 1, Index: i}))
	}
	if n.r.commit != 1 {
		t.Fatalf("commit %d before the leader stored entry %d", n.r.commit, i)
	}
	c.process(n)
	if n.r.commit != i {
		t.Fatalf("commit %d after Advance, want %d", n.r.commit, i)
	}
}

func TestCommitOnlyOwnTerm(t *testing.T) {
	cases := []struct {
		name   string
		ack    uint64
		commit uint64
	}{
		{"old term entry on majority", 2, 0},
		{"own term entry on majority", 3, 3},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := newCluster(t, 3, defaultTuning, 1)
			n := c.load(1, HardState{Term: 2}, 1, 2)
			c.load(2, HardState{Term: 2}, 1, 2)
			c.load(3, HardState{Term: 2}, 1)
			c.timeout(n)
			c.exchange(voting)
			if n.r.role != Leader || n.r.term != 3 {
				t.Fatalf("status %+v", n.r.Status())
			}
			c.drop(func(Message) bool { return true })
			c.step(n, Message{Type: MsgAppResp, From: 2, Term: 3, Index: tc.ack})
			if n.r.commit != tc.commit {
				t.Fatalf("commit %d, want %d", n.r.commit, tc.commit)
			}
		})
	}
}

func TestPropose(t *testing.T) {
	t.Run("follower", func(t *testing.T) {
		c := newCluster(t, 3, defaultTuning, 1)
		if _, err := c.node(1).r.Propose(1, nil); !errors.Is(err, ErrNotLeader) {
			t.Fatalf("err %v", err)
		}
	})
	t.Run("kind 0", func(t *testing.T) {
		c := newCluster(t, 3, defaultTuning, 1)
		c.elect(c.node(1))
		if _, err := c.node(1).r.Propose(0, nil); !errors.Is(err, ErrKind) {
			t.Fatalf("err %v", err)
		}
	})
	t.Run("busy", func(t *testing.T) {
		tune := defaultTuning
		tune.maxUncommitted = 2
		c := newCluster(t, 3, tune, 1)
		n := c.node(1)
		c.elect(n)
		c.isolate(1)
		for want := uint64(2); want <= 3; want++ {
			if i := c.propose(n, "x"); i != want {
				t.Fatalf("index %d, want %d", i, want)
			}
		}
		if _, err := n.r.Propose(1, []byte("x")); !errors.Is(err, ErrBusy) {
			t.Fatalf("err %v, want ErrBusy", err)
		}
		c.heal()
		c.drop(func(Message) bool { return true })
		c.tick(n)
		c.tick(n)
		c.settle()
		if _, err := n.r.Propose(1, []byte("x")); err != nil {
			t.Fatalf("after commit: %v", err)
		}
	})
}

func TestFollowerSnapshot(t *testing.T) {
	cases := []struct {
		name    string
		m       Message
		install bool
		index   uint64
		commit  uint64
		log     []uint64
	}{
		{"below commit", Message{Index: 1, LogTerm: 1}, false, 2, 2, []uint64{1, 1, 2, 2}},
		{"at commit", Message{Index: 2, LogTerm: 1}, false, 2, 2, []uint64{1, 1, 2, 2}},
		{"entry present", Message{Index: 3, LogTerm: 2}, false, 3, 3, []uint64{1, 1, 2, 2}},
		{"term differs", Message{Index: 3, LogTerm: 3}, true, 3, 3, nil},
		{"beyond log", Message{Index: 9, LogTerm: 3}, true, 9, 9, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := newCluster(t, 3, defaultTuning, 1)
			n := c.load(1, HardState{Term: 3}, 1, 1, 2, 2)
			c.step(n, Message{Type: MsgApp, From: 2, Term: 3, Index: 2, LogTerm: 1, Commit: 2})
			c.sent(1)
			tc.m.Type, tc.m.From, tc.m.To, tc.m.Term = MsgSnap, 2, 1, 3
			c.must(n, n.r.Step(tc.m))
			rd := n.r.Ready()
			if (rd.Snapshot != nil) != tc.install || tc.install && *rd.Snapshot != (Snapshot{Index: tc.m.Index, Term: tc.m.LogTerm}) {
				t.Fatalf("snapshot %+v, want install %v", rd.Snapshot, tc.install)
			}
			if rd.Commit != tc.commit {
				t.Fatalf("commit %d, want %d", rd.Commit, tc.commit)
			}
			n.offered[tc.m.Index] = 0
			c.process(n)
			resp := c.only(1)
			if resp.Type != MsgAppResp || resp.Reject || resp.Index != tc.index {
				t.Fatalf("response %+v, want success at %d", resp, tc.index)
			}
			if got := n.log.terms(); !slices.Equal(got, tc.log) {
				t.Fatalf("log %v, want %v", got, tc.log)
			}
		})
	}
}

func TestSnapshotDropsUnstableEntries(t *testing.T) {
	c := newCluster(t, 3, defaultTuning, 1)
	n := c.load(1, HardState{Term: 3}, 1, 1, 2, 2)
	c.must(n, n.r.Step(Message{Type: MsgApp, From: 2, To: 1, Term: 3, Index: 4, LogTerm: 2, Entries: []Entry{ent(5, 3)}}))
	c.must(n, n.r.Step(Message{Type: MsgSnap, From: 2, To: 1, Term: 3, Index: 7, LogTerm: 3}))
	rd := n.r.Ready()
	if len(rd.Entries) != 0 || rd.Snapshot == nil || *rd.Snapshot != (Snapshot{Index: 7, Term: 3}) || rd.Commit != 7 {
		t.Fatalf("ready %+v", rd)
	}
	n.offered[7] = 0
	c.process(n)
	if st := n.r.Status(); st.Last != 7 || st.Commit != 7 {
		t.Fatalf("status %+v", st)
	}
}

func TestReadIndex(t *testing.T) {
	t.Run("follower", func(t *testing.T) {
		c := newCluster(t, 3, defaultTuning, 1)
		if err := c.node(1).r.ReadIndex(1); !errors.Is(err, ErrNotLeader) {
			t.Fatalf("err %v", err)
		}
	})
	t.Run("nothing committed in term", func(t *testing.T) {
		c := newCluster(t, 3, defaultTuning, 1)
		n := c.node(1)
		c.timeout(n)
		c.exchange(voting)
		if n.r.role != Leader {
			t.Fatal("not leader")
		}
		if err := n.r.ReadIndex(1); !errors.Is(err, ErrNotReady) {
			t.Fatalf("err %v, want ErrNotReady", err)
		}
	})
	t.Run("heartbeat with seq", func(t *testing.T) {
		c := newCluster(t, 3, defaultTuning, 1)
		n := c.node(1)
		c.elect(n)
		c.must(n, c.read(n, 7))
		msgs := c.sent(1)
		if len(msgs) != 2 {
			t.Fatalf("sent %+v", msgs)
		}
		for _, m := range msgs {
			if m.Type != MsgApp || m.Seq != 1 || len(m.Entries) != 0 {
				t.Fatalf("message %+v, want empty append with seq 1", m)
			}
		}
		if len(n.reads) != 0 {
			t.Fatalf("read released without confirmation: %+v", n.reads)
		}
	})
}

func TestReadConfirmedByQuorum(t *testing.T) {
	cases := []struct {
		name string
		size int
		acks []Message
		done bool
	}{
		{"one ack of three", 3, []Message{{From: 2, Seq: 1, Index: 1}}, true},
		{"stale seq", 3, []Message{{From: 2, Seq: 0, Index: 1}}, false},
		{"rejection counts", 3, []Message{{From: 2, Seq: 1, Index: 2, Reject: true}}, true},
		{"one ack of five", 5, []Message{{From: 2, Seq: 1, Index: 1}}, false},
		{"two acks of five", 5, []Message{{From: 2, Seq: 1, Index: 1}, {From: 3, Seq: 1, Index: 1}}, true},
		{"same follower twice", 5, []Message{{From: 2, Seq: 1, Index: 1}, {From: 2, Seq: 1, Index: 1}}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := newCluster(t, tc.size, defaultTuning, 1)
			n := c.node(1)
			c.elect(n)
			c.must(n, c.read(n, 7))
			c.drop(func(Message) bool { return true })
			for _, ack := range tc.acks {
				ack.Type, ack.Term = MsgAppResp, 1
				c.step(n, ack)
			}
			want := []Read{{Tag: 7, Index: 1}}
			if !tc.done {
				want = nil
			}
			if !slices.Equal(n.reads, want) {
				t.Fatalf("reads %+v, want %+v", n.reads, want)
			}
		})
	}
}

func TestDeposedLeaderGetsNoRead(t *testing.T) {
	c := newCluster(t, 3, defaultTuning, 1)
	n := c.node(1)
	c.elect(n)
	c.must(n, c.read(n, 7))
	for range 20 {
		c.tick(n)
		c.drop(func(Message) bool { return true })
	}
	if n.r.role == Leader {
		t.Fatal("still leader")
	}
	c.step(n, Message{Type: MsgAppResp, From: 2, Term: 1, Index: 1, Seq: 1})
	c.step(n, Message{Type: MsgAppResp, From: 3, Term: 1, Index: 1, Seq: 1})
	if len(n.reads) != 0 || len(n.r.pending) != 0 {
		t.Fatalf("deposed leader answered read: %+v", n.reads)
	}
}

func TestLeaderIgnoresStaleTermAck(t *testing.T) {
	c := newCluster(t, 3, defaultTuning, 1)
	n := c.node(1)
	c.elect(n)
	c.crash(n)
	c.elect(n)
	if n.r.term != 2 || n.r.commit != 2 {
		t.Fatalf("status %+v", n.r.Status())
	}
	c.propose(n, "a")
	c.propose(n, "b")
	c.drop(func(Message) bool { return true })
	c.step(n, Message{Type: MsgAppResp, From: 2, Term: 1, Index: 4})
	if p := n.r.prs[1]; p.match != 2 || n.r.commit != 2 {
		t.Fatalf("ack from term 1 counted in term 2: match %d commit %d", p.match, n.r.commit)
	}
}

func TestCandidateIgnoresStaleTermVote(t *testing.T) {
	c := newCluster(t, 3, defaultTuning, 1)
	n := c.load(1, HardState{Term: 1})
	c.timeout(n)
	c.exchange(func(m Message) bool { return m.Type == MsgPreVote || m.Type == MsgPreVoteResp })
	c.drop(func(Message) bool { return true })
	if n.r.role != Candidate || n.r.term != 2 {
		t.Fatalf("status %+v", n.r.Status())
	}
	c.step(n, Message{Type: MsgVoteResp, From: 2, Term: 1})
	if n.r.role != Candidate {
		t.Fatalf("vote from term 1 counted in term 2: %+v", n.r.Status())
	}
}

func TestRejectionsAreNotVotes(t *testing.T) {
	c := newCluster(t, 3, defaultTuning, 1)
	n := c.node(1)
	c.timeout(n)
	c.exchange(func(m Message) bool { return m.Type == MsgPreVote || m.Type == MsgPreVoteResp })
	c.drop(func(Message) bool { return true })
	for _, id := range []uint64{2, 3} {
		c.step(n, Message{Type: MsgVoteResp, From: id, Term: 1, Reject: true})
	}
	if n.r.role != Candidate {
		t.Fatalf("rejections elected a leader: %+v", n.r.Status())
	}
}

func TestReelectedLeaderResetsProgress(t *testing.T) {
	c := newCluster(t, 3, defaultTuning, 1)
	n := c.node(1)
	c.elect(n)
	c.propose(n, "a")
	c.settle()
	c.must(n, c.read(n, 1))
	c.settle()
	if p := n.r.prs[1]; p.match != 2 || p.seq != 1 {
		t.Fatalf("progress %+v", p)
	}
	c.step(n, Message{Type: MsgVoteResp, From: 2, Term: 5, Reject: true})
	c.timeout(n)
	c.exchange(voting)
	if n.r.role != Leader || n.r.term != 6 {
		t.Fatalf("status %+v", n.r.Status())
	}
	for i, p := range n.r.prs {
		if i != n.r.self && (p.match != 0 || p.seq != 0 || p.active || p.snapping) {
			t.Fatalf("progress %d carried over from term 1: %+v", i, p)
		}
	}
}

func TestFollowerRejectsSecondLeader(t *testing.T) {
	c := newCluster(t, 3, defaultTuning, 1)
	n := c.load(1, HardState{Term: 3}, 1)
	c.step(n, Message{Type: MsgApp, From: 2, Term: 3, Index: 1, LogTerm: 1})
	for _, typ := range []MsgType{MsgApp, MsgSnap} {
		err := n.r.Step(Message{Type: typ, From: 3, To: 1, Term: 3, Index: 1, LogTerm: 1})
		if !errors.Is(err, ErrSafety) {
			t.Fatalf("%v from a second leader: err %v", typ, err)
		}
	}
}

func TestAppendRespectsMaxBytesUnstable(t *testing.T) {
	tune := defaultTuning
	tune.maxBytes = 3
	c := newCluster(t, 3, tune, 1)
	n := c.node(1)
	c.elect(n)
	for _, body := range []string{"aa", "bb", "cc"} {
		_, err := n.r.Propose(1, []byte(body))
		c.must(n, err)
	}
	c.must(n, n.r.Tick())
	c.must(n, n.r.Tick())
	for _, m := range n.r.Ready().Messages {
		if m.Type == MsgApp && len(m.Entries) > 1 {
			t.Fatalf("append %+v exceeds 3 bytes", m)
		}
	}
	c.process(n)
}

func TestTermChangeDropsOlderMessages(t *testing.T) {
	t.Run("acknowledgement", func(t *testing.T) {
		c := newCluster(t, 5, defaultTuning, 1)
		n := c.load(1, HardState{Term: 3}, 1, 1, 2)
		c.must(n, n.r.Step(Message{Type: MsgApp, From: 2, To: 1, Term: 3, Index: 3, LogTerm: 2, Entries: []Entry{ent(4, 3), ent(5, 3)}}))
		c.must(n, n.r.Step(Message{Type: MsgApp, From: 3, To: 1, Term: 4, Index: 3, LogTerm: 2, Entries: []Entry{ent(4, 4)}}))
		c.process(n)
		for _, m := range c.sent(1) {
			if m.Term != 4 || m.To != 3 {
				t.Fatalf("sent %+v after moving to term 4", m)
			}
		}
	})
	t.Run("vote", func(t *testing.T) {
		c := newCluster(t, 5, defaultTuning, 1)
		n := c.load(1, HardState{Term: 3}, 1, 1, 2)
		c.must(n, n.r.Step(Message{Type: MsgVote, From: 2, To: 1, Term: 4, Index: 3, LogTerm: 2}))
		c.must(n, n.r.Step(Message{Type: MsgVote, From: 3, To: 1, Term: 5, Index: 3, LogTerm: 2}))
		c.process(n)
		msgs := c.sent(1)
		if len(msgs) != 1 || msgs[0].Term != 5 || msgs[0].To != 3 || msgs[0].Reject {
			t.Fatalf("sent %+v, want only the vote for 3 in term 5", msgs)
		}
	})
	t.Run("campaign", func(t *testing.T) {
		c := newCluster(t, 3, defaultTuning, 1)
		n := c.load(1, HardState{Term: 3}, 1, 1, 2)
		c.must(n, n.r.Step(Message{Type: MsgVote, From: 2, To: 1, Term: 3, Index: 1, LogTerm: 1}))
		n.r.elapsed = n.r.timeout - 1
		c.must(n, n.r.Tick())
		c.must(n, n.r.Step(Message{Type: MsgPreVoteResp, From: 3, To: 1, Term: 4}))
		for _, m := range n.r.Ready().Messages {
			if m.Term < 4 {
				t.Fatalf("candidate of term 4 still sends %+v", m)
			}
		}
		c.process(n)
	})
}

func TestLeaderSendsPendingSnapshot(t *testing.T) {
	c := newCluster(t, 3, defaultTuning, 1)
	n := c.load(1, HardState{Term: 2}, 1, 1)
	c.must(n, n.r.Step(Message{Type: MsgSnap, From: 2, To: 1, Term: 2, Index: 9, LogTerm: 2}))
	n.r.elapsed = n.r.timeout - 1
	c.must(n, n.r.Tick())
	c.must(n, n.r.Step(Message{Type: MsgPreVoteResp, From: 3, To: 1, Term: 3}))
	c.must(n, n.r.Step(Message{Type: MsgVoteResp, From: 3, To: 1, Term: 3}))
	if n.r.role != Leader || n.r.snap == nil {
		t.Fatalf("want leader with pending snapshot: %+v", n.r.Status())
	}
	c.must(n, n.r.Step(Message{Type: MsgAppResp, From: 2, To: 1, Term: 3, Index: 3, Reject: true}))
	var snaps []Message
	for _, m := range n.r.Ready().Messages {
		if m.Type == MsgSnap {
			snaps = append(snaps, m)
		}
	}
	if len(snaps) != 1 || snaps[0].Index != 9 || snaps[0].LogTerm != 2 {
		t.Fatalf("snapshots %+v, want the pending one at 9", snaps)
	}
	n.offered[9] = 0
	c.process(n)
}

func TestReadSurvivesStepDown(t *testing.T) {
	c := newCluster(t, 3, defaultTuning, 1)
	n := c.node(1)
	c.elect(n)
	c.propose(n, "a")
	c.settle()
	c.must(n, c.read(n, 7))
	c.deliverAll(to(2))
	ack := c.take(from(2))
	c.drop(func(Message) bool { return true })
	c.must(n, n.r.Step(ack[0].m))
	c.must(n, n.r.Step(Message{Type: MsgApp, From: 3, To: 1, Term: 2, Index: 2, LogTerm: 1, Entries: []Entry{{Index: 3, Term: 2}}, Commit: 2}))
	rd := n.r.Ready()
	if n.r.role == Leader || !slices.Equal(rd.Reads, []Read{{Tag: 7, Index: 2}}) {
		t.Fatalf("role %v reads %+v, want the confirmed read after stepping down", n.r.role, rd.Reads)
	}
	c.process(n)
	c.checkOK()
}

func TestStaleRejectDoesNotTriggerSnapshot(t *testing.T) {
	c := newCluster(t, 3, defaultTuning, 1)
	n := c.node(1)
	c.elect(n)
	for range 3 {
		c.propose(n, "x")
		c.settle()
	}
	c.tick(n)
	c.tick(n)
	stale := c.take(func(m Message) bool { return m.Type == MsgApp && m.To == 2 })
	c.settle()
	for _, o := range c.nodes {
		c.snapshot(o, false)
	}
	c.crash(n)
	c.timeout(n)
	c.exchange(voting)
	if n.r.role != Leader || n.r.term != 2 || len(stale) == 0 {
		t.Fatalf("status %+v stale %d", n.r.Status(), len(stale))
	}
	c.drop(to(2))
	c.deliver(stale[0])
	rej := c.take(from(2))
	if len(rej) != 1 || !rej[0].m.Reject || rej[0].m.Index != 0 {
		t.Fatalf("want one stale-term rejection: %+v", rej)
	}
	c.deliver(rej[0])
	for _, env := range c.take(to(2)) {
		if env.m.Type == MsgSnap {
			t.Fatalf("stale rejection made the leader send %+v", env.m)
		}
	}
}

func TestSlowSnapshotKeepsLeader(t *testing.T) {
	tune := defaultTuning
	tune.snapshot = 200
	c := newCluster(t, 3, tune, 1)
	n1, n2 := c.node(1), c.node(2)
	c.elect(n1)
	c.isolate(2)
	for range 5 {
		c.propose(n1, "x")
		c.settle()
	}
	c.snapshot(n1, false)
	c.heal()
	c.isolate(3)
	var held []envelope
	for tick := range 300 {
		c.tick(n1)
		c.tick(n2)
		if tick == 100 {
			for _, env := range held {
				c.deliver(env)
			}
		}
		for range 20 {
			for _, env := range c.take(func(Message) bool { return true }) {
				if env.m.Type == MsgSnap && tick < 100 {
					held = append(held, env)
					continue
				}
				c.deliver(env)
			}
		}
		c.checkOK()
		if n1.r.role != Leader {
			t.Fatalf("leader stepped down at tick %d while its only follower waits for a snapshot", tick)
		}
	}
	if len(held) == 0 || n2.applied != 6 || n2.r.commit != n1.r.commit {
		t.Fatalf("follower applied %d commit %d, leader commit %d, %d snapshots held", n2.applied, n2.r.commit, n1.r.commit, len(held))
	}
}

func TestRejectBelowMatchRewinds(t *testing.T) {
	c := newCluster(t, 3, defaultTuning, 1)
	n, f := c.node(1), c.node(2)
	c.elect(n)
	for _, body := range []string{"a", "b", "c"} {
		c.propose(n, body)
		c.settle()
	}
	if p := n.r.prs[1]; p.match != 4 || n.r.commit != 4 {
		t.Fatalf("progress %+v commit %d", p, n.r.commit)
	}
	c.damage(f, 1)
	c.tick(n)
	c.tick(n)
	c.deliverAll(to(2))
	rej := c.take(from(2))
	if len(rej) != 1 || !rej[0].m.Reject || rej[0].m.Index != 2 {
		t.Fatalf("want one refusal with hint 2: %+v", rej)
	}
	c.deliver(rej[0])
	if p := n.r.prs[1]; p.match != 1 || p.next != 2 || n.r.commit != 4 {
		t.Fatalf("progress %+v commit %d, want match 1 next 2 commit 4", p, n.r.commit)
	}
	c.settle()
	if p := n.r.prs[1]; p.match != 4 || !slices.Equal(f.log.terms(), n.log.terms()) {
		t.Fatalf("progress %+v, follower log %v, leader log %v", p, f.log.terms(), n.log.terms())
	}
}

func TestRejectAtOrAboveNextIsDropped(t *testing.T) {
	c := newCluster(t, 3, defaultTuning, 1)
	n := c.node(1)
	c.elect(n)
	for _, body := range []string{"a", "b", "c"} {
		c.propose(n, body)
	}
	c.drop(func(Message) bool { return true })
	p := &n.r.prs[1]
	p.match, p.next, p.inflight = 1, 5, true
	for range 10 {
		c.step(n, Message{Type: MsgAppResp, From: 2, Term: 1, Index: 3, Reject: true})
	}
	got := c.take(to(2))
	if len(got) != 1 || got[0].m.Index != 2 || len(got[0].m.Entries) != 2 {
		t.Fatalf("ten copies of one refusal sent %+v, want one append after 2", got)
	}
	for _, hint := range []uint64{3, 4, 9} {
		c.step(n, Message{Type: MsgAppResp, From: 2, Term: 1, Index: hint, Reject: true})
	}
	if got := c.take(to(2)); len(got) > 0 || p.match != 1 || p.next != 3 {
		t.Fatalf("refusals at or above next sent %+v, progress %+v", got, *p)
	}
}

func TestRepairComesFromHardState(t *testing.T) {
	c := newCluster(t, 3, defaultTuning, 1)
	n := c.load(1, HardState{Term: 2, Vote: 3, Repair: true}, 1, 2)
	if st := n.r.Status(); !st.Repair || n.r.HasReady() {
		t.Fatalf("status %+v, ready %v", st, n.r.HasReady())
	}
	c.must(n, n.r.Step(Message{Type: MsgApp, From: 2, To: 1, Term: 3, Index: 2, LogTerm: 2}))
	if rd := n.r.Ready(); rd.HardState == nil || *rd.HardState != (HardState{Term: 3, Repair: true}) {
		t.Fatalf("hard state %+v, want term 3 still in repair", rd.HardState)
	}
	c.process(n)
	nonce := n.r.nonce
	c.crash(n)
	if !n.r.Status().Repair || n.r.nonce == nonce {
		t.Fatalf("after restart: status %+v, nonce %d, was %d", n.r.Status(), n.r.nonce, nonce)
	}
}

func TestRepairingNodeDoesNotVote(t *testing.T) {
	cases := []struct {
		name string
		m    Message
		term uint64
	}{
		{"pre-vote", Message{Type: MsgPreVote, Term: 3, Index: 2, LogTerm: 2}, 2},
		{"vote", Message{Type: MsgVote, Term: 2, Index: 2, LogTerm: 2}, 2},
		{"vote in a higher term", Message{Type: MsgVote, Term: 3, Index: 2, LogTerm: 2}, 3},
	}
	for _, tc := range cases {
		for _, repair := range []bool{false, true} {
			name := tc.name
			if repair {
				name += " in repair"
			}
			t.Run(name, func(t *testing.T) {
				c := newCluster(t, 3, defaultTuning, 1)
				n := c.load(1, HardState{Term: 2, Repair: repair}, 1, 2)
				tc.m.From = 2
				c.step(n, tc.m)
				resp := c.only(1)
				if resp.Reject != repair {
					t.Fatalf("response %+v, want refusal %v", resp, repair)
				}
				if repair && (resp.Term != tc.term || n.hs != (HardState{Term: tc.term, Repair: true})) {
					t.Fatalf("refused in term %d, disk %+v, want term %d without a vote", resp.Term, n.hs, tc.term)
				}
			})
		}
	}
	t.Run("timeout", func(t *testing.T) {
		c := newCluster(t, 3, defaultTuning, 1)
		n := c.load(1, HardState{Term: 2, Repair: true}, 1, 2)
		if k := ticksUntil(c, n, MsgPreVote, 100); k != -1 {
			t.Fatalf("pre-vote after %d ticks in repair", k)
		}
		if st := n.r.Status(); st.Role != Follower || st.Term != 2 {
			t.Fatalf("status %+v", st)
		}
	})
}

func TestRepairingNodeCountsForCommitAndReads(t *testing.T) {
	c := newCluster(t, 3, defaultTuning, 1)
	n, f := c.node(1), c.node(3)
	c.elect(n)
	c.propose(n, "a")
	c.settle()
	c.damage(f, 0)
	c.isolate(2)
	i := c.propose(n, "b")
	c.settle()
	if n.r.commit != i || f.r.commit != 2 || f.applied != 2 {
		t.Fatalf("leader commit %d, follower commit %d applied %d, want %d with the follower in repair", n.r.commit, f.r.commit, f.applied, i)
	}
	c.must(n, c.read(n, 7))
	c.settle()
	if !slices.Equal(n.reads, []Read{{Tag: 7, Index: i}}) {
		t.Fatalf("reads %+v", n.reads)
	}
	for range 40 {
		c.tick(n)
		c.settle()
	}
	if n.r.role != Leader || !f.r.Status().Repair || f.r.commit != i {
		t.Fatalf("leader %+v, follower %+v", n.r.Status(), f.r.Status())
	}
}

func TestRepairProbe(t *testing.T) {
	probes := func(c *cluster, id uint64) []Message {
		var got []Message
		for _, env := range c.take(func(m Message) bool { return m.From == id && m.Type == MsgRepairProbe }) {
			got = append(got, env.m)
		}
		return got
	}
	t.Run("every heartbeat to the leader", func(t *testing.T) {
		c := newCluster(t, 3, defaultTuning, 1)
		n := c.load(1, HardState{Term: 2, Repair: true}, 1, 2)
		for range 10 {
			c.tick(n)
		}
		if got := probes(c, 1); len(got) > 0 {
			t.Fatalf("probe without a known leader: %+v", got)
		}
		c.step(n, Message{Type: MsgApp, From: 2, Term: 2, Index: 2, LogTerm: 2})
		c.sent(1)
		var at []int
		for k := 1; k <= 6; k++ {
			c.tick(n)
			for _, m := range probes(c, 1) {
				if m.To != 2 || m.Term != 2 || m.Seq != n.r.nonce {
					t.Fatalf("probe %+v, want to 2 in term 2 with nonce %d", m, n.r.nonce)
				}
				at = append(at, k)
			}
		}
		if !slices.Equal(at, []int{1, 3, 5}) {
			t.Fatalf("probes at ticks %v, want every %d ticks", at, defaultTuning.heartbeat)
		}
	})
	t.Run("leader answers with its last index", func(t *testing.T) {
		c := newCluster(t, 3, defaultTuning, 1)
		l := c.node(1)
		c.elect(l)
		c.propose(l, "a")
		c.settle()
		c.propose(l, "b")
		c.drop(func(Message) bool { return true })
		c.step(l, Message{Type: MsgRepairProbe, From: 2, Term: 1, Seq: 77})
		if m := c.only(1); m.Type != MsgRepairInfo || m.To != 2 || m.Term != 1 || m.Index != 3 || m.Seq != 77 {
			t.Fatalf("answer %+v, want RepairInfo to 2 with index 3 and seq 77, commit is %d", m, l.r.commit)
		}
	})
	t.Run("follower drops it", func(t *testing.T) {
		c := newCluster(t, 3, defaultTuning, 1)
		c.elect(c.node(1))
		c.step(c.node(3), Message{Type: MsgRepairProbe, From: 2, Term: 1, Seq: 77})
		if got := c.sent(3); len(got) > 0 {
			t.Fatalf("follower answered %+v", got)
		}
	})
	t.Run("lower term dropped", func(t *testing.T) {
		c := newCluster(t, 3, defaultTuning, 1)
		l := c.load(1, HardState{Term: 2}, 1)
		c.elect(l)
		c.step(l, Message{Type: MsgRepairProbe, From: 2, Term: 2, Seq: 77})
		if got := c.sent(1); len(got) > 0 {
			t.Fatalf("leader of term 3 answered a probe of term 2: %+v", got)
		}
	})
}

func TestProbeGoesOnlyToTheKnownLeader(t *testing.T) {
	c := newCluster(t, 5, defaultTuning, 1)
	n := c.load(1, HardState{Term: 2, Repair: true}, 1, 2)
	ticks := func() []uint64 {
		var dst []uint64
		for range 10 {
			c.tick(n)
		}
		for _, m := range c.sent(1) {
			if m.Type == MsgRepairProbe {
				dst = append(dst, m.To)
			}
		}
		return dst
	}
	c.step(n, Message{Type: MsgApp, From: 3, Term: 2, Index: 2, LogTerm: 2})
	if dst := ticks(); len(dst) == 0 || slices.ContainsFunc(dst, func(id uint64) bool { return id != 3 }) {
		t.Fatalf("probes to %v, want only to leader 3", dst)
	}
	c.step(n, Message{Type: MsgVote, From: 4, Term: 3, Index: 2, LogTerm: 2})
	if dst := ticks(); len(dst) > 0 {
		t.Fatalf("probes to %v in term 3 without a known leader", dst)
	}
	c.step(n, Message{Type: MsgApp, From: 4, Term: 3, Index: 2, LogTerm: 2})
	if dst := ticks(); len(dst) == 0 || slices.ContainsFunc(dst, func(id uint64) bool { return id != 4 }) {
		t.Fatalf("probes to %v, want only to leader 4", dst)
	}
}

func TestRepairGoal(t *testing.T) {
	c := newCluster(t, 3, defaultTuning, 1)
	n := c.load(1, HardState{Term: 2, Repair: true}, 1, 2)
	c.step(n, Message{Type: MsgApp, From: 2, Term: 2, Index: 2, LogTerm: 2})
	c.step(n, Message{Type: MsgRepairInfo, From: 2, Term: 2, Index: 4, Seq: n.r.nonce})
	c.step(n, Message{Type: MsgRepairInfo, From: 2, Term: 2, Index: 3, Seq: n.r.nonce})
	c.sent(1)
	for range 10 {
		c.tick(n)
	}
	for _, m := range c.sent(1) {
		if m.Type == MsgRepairProbe {
			t.Fatalf("probe %+v with a goal for the term", m)
		}
	}
	c.step(n, Message{Type: MsgApp, From: 2, Term: 2, Index: 2, LogTerm: 2, Entries: []Entry{ent(3, 2)}})
	if !n.hs.Repair || !n.r.Status().Repair {
		t.Fatal("left repair at 3, the first answer in the term named 4")
	}
	c.step(n, Message{Type: MsgApp, From: 2, Term: 2, Index: 3, LogTerm: 2, Entries: []Entry{ent(4, 2)}})
	if n.hs.Repair || n.r.Status().Repair {
		t.Fatalf("still in repair after acknowledging the goal: %+v", n.hs)
	}
}

func TestRepairIgnoresOtherNonce(t *testing.T) {
	c := newCluster(t, 3, defaultTuning, 1)
	n := c.load(1, HardState{Term: 2, Repair: true}, 1, 2, 2)
	probe := func() Message {
		t.Helper()
		for range defaultTuning.heartbeat {
			c.tick(n)
		}
		got := c.take(is(MsgRepairProbe))
		if len(got) != 1 {
			t.Fatalf("%d probes in one heartbeat interval, want 1", len(got))
		}
		return got[0].m
	}
	c.step(n, Message{Type: MsgApp, From: 2, Term: 2, Index: 3, LogTerm: 2})
	first := probe()
	c.step(n, Message{Type: MsgApp, From: 2, Term: 2, Index: 3, LogTerm: 2, Entries: []Entry{ent(4, 2)}})
	c.damage(n, 1)
	c.step(n, Message{Type: MsgApp, From: 2, Term: 2, Index: 1, LogTerm: 1, Entries: []Entry{ent(2, 2), ent(3, 2)}})
	c.step(n, Message{Type: MsgRepairInfo, From: 2, Term: 2, Index: 3, Seq: first.Seq})
	c.step(n, Message{Type: MsgApp, From: 2, Term: 2, Index: 3, LogTerm: 2})
	if !n.hs.Repair || !n.r.Status().Repair {
		t.Fatalf("the answer to an earlier process ended repair at 3, that process had acknowledged 4: disk %+v", n.hs)
	}
	second := probe()
	if second.Seq == first.Seq {
		t.Fatalf("nonce %d again after a restart", second.Seq)
	}
	c.step(n, Message{Type: MsgRepairInfo, From: 2, Term: 2, Index: 4, Seq: second.Seq})
	c.step(n, Message{Type: MsgApp, From: 2, Term: 2, Index: 3, LogTerm: 2, Entries: []Entry{ent(4, 2)}})
	if n.hs.Repair {
		t.Fatalf("still in repair after the answer to its own probe: %+v", n.hs)
	}
}

func TestRepairGoalIsBoundToTheTerm(t *testing.T) {
	c := newCluster(t, 3, defaultTuning, 1)
	n := c.load(1, HardState{Term: 2, Repair: true}, 1, 2)
	c.step(n, Message{Type: MsgApp, From: 2, Term: 2, Index: 2, LogTerm: 2})
	c.step(n, Message{Type: MsgRepairInfo, From: 2, Term: 2, Index: 5, Seq: n.r.nonce})
	c.step(n, Message{Type: MsgApp, From: 3, Term: 3, Index: 2, LogTerm: 2, Entries: []Entry{ent(3, 3), ent(4, 3), ent(5, 3)}})
	if !n.hs.Repair || n.hs.Term != 3 {
		t.Fatalf("goal of term 2 healed in term 3: disk %+v", n.hs)
	}
	c.step(n, Message{Type: MsgRepairInfo, From: 2, Term: 2, Index: 5, Seq: n.r.nonce})
	c.sent(1)
	if !n.hs.Repair {
		t.Fatal("answer of term 2 healed in term 3")
	}
	for range defaultTuning.heartbeat {
		c.tick(n)
	}
	if got := c.sent(1); len(got) != 1 || got[0].Type != MsgRepairProbe || got[0].To != 3 || got[0].Term != 3 {
		t.Fatalf("sent %+v, want a probe to the leader of term 3", got)
	}
	c.step(n, Message{Type: MsgRepairInfo, From: 3, Term: 3, Index: 5, Seq: n.r.nonce})
	c.step(n, Message{Type: MsgApp, From: 3, Term: 3, Index: 5, LogTerm: 3})
	if n.hs.Repair {
		t.Fatalf("not healed by the goal of term 3: %+v", n.hs)
	}
}

func TestRepairCountsOnlyAcksOfTheTerm(t *testing.T) {
	c := newCluster(t, 3, defaultTuning, 1)
	n := c.load(1, HardState{Term: 2, Repair: true}, 1, 2, 2)
	c.step(n, Message{Type: MsgApp, From: 2, Term: 2, Index: 3, LogTerm: 2})
	other := Message{Type: MsgApp, From: 3, Term: 3, Index: 3, LogTerm: 3}
	c.step(n, other)
	c.step(n, Message{Type: MsgRepairInfo, From: 3, Term: 3, Index: 3, Seq: n.r.nonce})
	c.step(n, other)
	if resp := c.sent(1); len(resp) != 3 || resp[0].Reject || !resp[1].Reject || !resp[2].Reject {
		t.Fatalf("sent %+v, want an acknowledgement in term 2 and two refusals in term 3", resp)
	}
	if !n.hs.Repair || !n.r.Status().Repair {
		t.Fatalf("the acknowledgement of 3 in term 2 ended repair in term 3: disk %+v", n.hs)
	}
	c.step(n, Message{Type: MsgApp, From: 3, Term: 3, Index: 1, LogTerm: 1, Entries: []Entry{ent(2, 3), ent(3, 3)}})
	if n.hs.Repair || !slices.Equal(n.log.terms(), []uint64{1, 3, 3}) {
		t.Fatalf("disk %+v log %v, want out of repair with the log of term 3", n.hs, n.log.terms())
	}
}

func TestRepairCountsEveryAcknowledgement(t *testing.T) {
	cases := []struct {
		name string
		goal uint64
		m    Message
	}{
		{"append", 5, Message{Type: MsgApp, Index: 5, LogTerm: 2}},
		{"append below commit", 5, Message{Type: MsgApp, Index: 3, LogTerm: 2}},
		{"snapshot below commit", 5, Message{Type: MsgSnap, Index: 4, LogTerm: 2}},
		{"snapshot", 7, Message{Type: MsgSnap, Index: 7, LogTerm: 2}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := newCluster(t, 3, defaultTuning, 1)
			n := c.node(1)
			n.log.install(Snapshot{Index: 5, Term: 2})
			n.hs = HardState{Term: 2, Repair: true}
			c.boot(n)
			c.step(n, Message{Type: MsgRepairInfo, From: 2, Term: 2, Index: tc.goal, Seq: n.r.nonce})
			if !n.r.Status().Repair {
				t.Fatal("left repair without an acknowledgement in this process")
			}
			tc.m.From, tc.m.Term = 2, 2
			c.step(n, tc.m)
			if resp := c.only(1); resp.Reject || resp.Index != tc.goal {
				t.Fatalf("answer %+v, want the acknowledgement of %d", resp, tc.goal)
			}
			if n.hs.Repair {
				t.Fatalf("still in repair after acknowledging %d: disk %+v", tc.goal, n.hs)
			}
		})
	}
}

func TestRepairEndsOneReadyLater(t *testing.T) {
	c := newCluster(t, 3, defaultTuning, 1)
	n := c.load(1, HardState{Term: 2, Repair: true}, 1, 2)
	c.step(n, Message{Type: MsgApp, From: 2, Term: 2, Index: 2, LogTerm: 2})
	c.step(n, Message{Type: MsgRepairInfo, From: 2, Term: 2, Index: 4, Seq: n.r.nonce})
	c.sent(1)
	c.must(n, n.r.Step(Message{Type: MsgApp, From: 2, To: 1, Term: 2, Index: 2, LogTerm: 2, Entries: []Entry{ent(3, 2), ent(4, 2)}}))
	rd := n.r.Ready()
	if rd.HardState != nil || len(rd.Entries) != 2 || len(rd.Messages) != 1 || rd.Messages[0].Index != 4 {
		t.Fatalf("ready %+v, want the entries and their acknowledgement without a hard state", rd)
	}
	n.log.append(rd.Entries)
	n.r.Advance()
	if n.r.Status().Repair {
		t.Fatal("still in repair after the acknowledgement of the goal is stored")
	}
	rd = n.r.Ready()
	if rd.HardState == nil || *rd.HardState != (HardState{Term: 2}) || len(rd.Entries) > 0 || len(rd.Messages) > 0 {
		t.Fatalf("next ready %+v, want only the hard state without repair", rd)
	}
	c.process(n)
}

func TestRepairedNodeVotesAgain(t *testing.T) {
	c := newCluster(t, 3, defaultTuning, 1)
	n := c.load(1, HardState{Term: 2, Repair: true}, 1, 2)
	c.step(n, Message{Type: MsgApp, From: 2, Term: 2, Index: 2, LogTerm: 2})
	for range 30 {
		c.tick(n)
	}
	for _, m := range c.sent(1) {
		if m.Type != MsgAppResp && m.Type != MsgRepairProbe {
			t.Fatalf("sent %+v in repair", m)
		}
	}
	c.step(n, Message{Type: MsgRepairInfo, From: 2, Term: 2, Index: 2, Seq: n.r.nonce})
	c.step(n, Message{Type: MsgPreVote, From: 3, Term: 3, Index: 2, LogTerm: 2})
	if resp := c.only(1); !resp.Reject || n.hs.Repair {
		t.Fatalf("pre-vote %+v, disk %+v, want refused and healed in the same round", resp, n.hs)
	}
	if k := ticksUntil(c, n, MsgPreVote, 100); k < 10 || k >= 20 {
		t.Fatalf("pre-vote %d ticks after healing, want a fresh timeout in [10, 20)", k)
	}
	c.sent(1)
	c.step(n, Message{Type: MsgVote, From: 3, Term: 3, Index: 2, LogTerm: 2})
	if resp := c.only(1); resp.Reject || n.hs != (HardState{Term: 3, Vote: 3}) {
		t.Fatalf("vote %+v, disk %+v, want granted", resp, n.hs)
	}
}
