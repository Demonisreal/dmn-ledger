package raft

import (
	"slices"
	"testing"
)

func TestFigure7(t *testing.T) {
	leaderLog := []uint64{1, 1, 1, 4, 4, 5, 5, 6, 6, 6}
	cases := []struct {
		name string
		log  []uint64
	}{
		{"a", []uint64{1, 1, 1, 4, 4, 5, 5, 6, 6}},
		{"b", []uint64{1, 1, 1, 4}},
		{"c", []uint64{1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 6}},
		{"d", []uint64{1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 7, 7}},
		{"e", []uint64{1, 1, 1, 4, 4, 4, 4}},
		{"f", []uint64{1, 1, 1, 2, 2, 2, 3, 3, 3, 3, 3}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := newCluster(t, 3, defaultTuning, 1)
			n := c.load(1, HardState{Term: 7}, leaderLog...)
			c.load(2, HardState{Term: 7}, tc.log...)
			c.load(3, HardState{Term: 7}, leaderLog...)
			c.timeout(n)
			c.exchange(voting)
			if n.r.role != Leader || n.r.term != 8 {
				t.Fatalf("leader %+v", n.r.Status())
			}
			rejects := 0
			for round := 0; len(c.net) > 0; round++ {
				if round == 100 {
					t.Fatal("network did not settle")
				}
				for _, env := range c.take(func(Message) bool { return true }) {
					if env.m.Type == MsgAppResp && env.m.From == 2 && env.m.Reject {
						rejects++
					}
					c.deliver(env)
				}
				c.checkOK()
			}
			want := append(slices.Clone(leaderLog), 8)
			if got := c.node(2).log.terms(); !slices.Equal(got, want) {
				t.Fatalf("follower log %v, want %v", got, want)
			}
			if rejects > 2 {
				t.Fatalf("%d rejections, conflict hint should need at most 2", rejects)
			}
		})
	}
}

func TestFigure8(t *testing.T) {
	tune := defaultTuning
	tune.maxBytes = 1
	c := newCluster(t, 5, tune, 1)
	s1 := c.load(1, HardState{Term: 3}, 1, 2)
	c.load(2, HardState{Term: 3}, 1, 2)
	c.load(3, HardState{Term: 3}, 1)
	c.load(4, HardState{Term: 3}, 1)
	s5 := c.load(5, HardState{Term: 3}, 1, 3)

	c.isolate(5)
	c.timeout(s1)
	c.exchange(voting)
	if s1.r.role != Leader || s1.r.term != 4 {
		t.Fatalf("s1 %+v", s1.r.Status())
	}
	noopOrS2 := func(m Message) bool {
		return m.To == 2 || m.Type == MsgApp && m.Index == 2 && c.node(m.To).log.LastIndex() == 2
	}
	for round := 0; len(c.net) > 0; round++ {
		if round == 100 {
			t.Fatal("network did not settle")
		}
		c.drop(noopOrS2)
		c.deliverAll(func(Message) bool { return true })
	}
	if s1.r.prs[2].match != 2 || s1.r.prs[3].match != 2 {
		t.Fatalf("index 2 not on s3 and s4: %+v", s1.r.prs)
	}
	if s1.r.commit != 0 {
		t.Fatalf("leader of term 4 committed %d by counting entries of term 2", s1.r.commit)
	}

	c.crash(s1)
	c.heal()
	c.isolate(1)
	c.timeout(s5)
	c.settle()
	if s5.r.term != 4 || s5.r.role != Follower {
		t.Fatalf("s5 should learn term 4 from the rejections: %+v", s5.r.Status())
	}
	c.elect(s5)
	if s5.r.term != 5 {
		t.Fatalf("s5 %+v", s5.r.Status())
	}
	c.settle()
	for _, id := range []uint64{2, 3, 4, 5} {
		if got := c.node(id).log.terms(); !slices.Equal(got, []uint64{1, 3, 5}) {
			t.Fatalf("s%d log %v", id, got)
		}
	}
	c.heal()
	for range 4 {
		c.tick(s5)
		c.settle()
	}
	if got := s1.log.terms(); !slices.Equal(got, []uint64{1, 3, 5}) {
		t.Fatalf("s1 log %v", got)
	}
	c.checkOK()
}

func TestSplitVote(t *testing.T) {
	c := newCluster(t, 3, defaultTuning, 3)
	n1, n2 := c.node(1), c.node(2)
	c.isolate(3)
	c.timeout(n1)
	c.timeout(n2)
	c.exchange(func(m Message) bool { return m.Type == MsgPreVote || m.Type == MsgPreVoteResp })
	if n1.r.role != Candidate || n2.r.role != Candidate || n1.r.term != 1 || n2.r.term != 1 {
		t.Fatalf("want two candidates in term 1: %+v %+v", n1.r.Status(), n2.r.Status())
	}
	c.settle()
	if n1.r.role == Leader || n2.r.role == Leader {
		t.Fatal("leader elected with a split vote")
	}
	c.heal()
	var lead *node
	for range 200 {
		for _, n := range c.nodes {
			c.tick(n)
		}
		c.settle()
		if lead = c.leader(); lead != nil {
			break
		}
	}
	if lead == nil || lead.r.term < 2 {
		t.Fatalf("no leader after split vote: %+v", lead)
	}
	c.checkOK()
}

func TestPartitionedNodeKeepsTerm(t *testing.T) {
	c := newCluster(t, 3, defaultTuning, 1)
	n := c.node(1)
	c.elect(n)
	c.isolate(3)
	f := c.node(3)
	for range 100 {
		c.tick(n)
		c.tick(f)
		c.settle()
	}
	if st := f.r.Status(); st.Term != 1 || st.Role != PreCandidate {
		t.Fatalf("partitioned node %+v, want term 1", st)
	}
	c.heal()
	for range 30 {
		c.tick(n)
		c.tick(f)
		c.settle()
	}
	if st := n.r.Status(); st.Role != Leader || st.Term != 1 {
		t.Fatalf("rejoining node disrupted the leader: %+v", st)
	}
	if st := f.r.Status(); st.Role != Follower || st.Lead != 1 || st.Commit != n.r.commit {
		t.Fatalf("rejoined node %+v", st)
	}
}

func TestMinorityLeaderStepsDown(t *testing.T) {
	c := newCluster(t, 5, defaultTuning, 2)
	old := c.node(1)
	c.elect(old)
	for _, a := range []uint64{1, 2} {
		for _, b := range []uint64{3, 4, 5} {
			c.cut[a][b], c.cut[b][a] = true, true
		}
	}
	var lead *node
	for range 100 {
		for _, n := range c.nodes {
			c.tick(n)
		}
		c.settle()
		for _, n := range c.nodes[2:] {
			if n.r.role == Leader {
				lead = n
			}
		}
	}
	if lead == nil || lead.r.term != 2 && lead.r.term != 3 {
		t.Fatalf("majority side elected %+v", lead)
	}
	if st := old.r.Status(); st.Role == Leader || st.Term != 1 {
		t.Fatalf("old leader %+v, want stepped down in term 1", st)
	}
	if _, err := old.r.Propose(1, nil); err == nil {
		t.Fatal("old leader accepts proposals")
	}
}

func TestDeposedLeaderAfterPartitionGetsNoRead(t *testing.T) {
	c := newCluster(t, 3, defaultTuning, 1)
	old := c.node(1)
	c.elect(old)
	c.isolate(1)
	c.elect(c.node(2))
	c.propose(c.node(2), "x")
	c.settle()
	c.must(old, c.read(old, 9))
	c.heal()
	for range 5 {
		for _, n := range c.nodes {
			c.tick(n)
		}
		c.settle()
	}
	if old.r.role == Leader || len(old.reads) != 0 {
		t.Fatalf("deposed leader %v reads %+v", old.r.role, old.reads)
	}
	c.checkOK()
}

func TestDuplicateAndReorderedAppends(t *testing.T) {
	c := newCluster(t, 3, defaultTuning, 1)
	n := c.node(1)
	c.elect(n)
	app2 := func(m Message) bool { return m.Type == MsgApp && m.To == 2 }
	c.propose(n, "a")
	short := c.take(app2)
	c.propose(n, "b")
	c.propose(n, "c")
	c.tick(n)
	c.tick(n)
	long := c.take(app2)
	if len(short) != 1 || len(long) != 1 || len(long[0].m.Entries) != 3 {
		t.Fatalf("short %+v long %+v", short, long)
	}
	c.drop(func(Message) bool { return true })
	for _, env := range []envelope{long[0], short[0], long[0], short[0]} {
		c.deliver(env)
	}
	if got := c.node(2).log.terms(); !slices.Equal(got, []uint64{1, 1, 1, 1}) {
		t.Fatalf("follower log %v", got)
	}
	resps := c.take(is(MsgAppResp))
	slices.Reverse(resps)
	for _, env := range resps {
		c.deliver(env)
	}
	c.settle()
	if p := n.r.prs[1]; p.match != 4 || p.next != 5 {
		t.Fatalf("progress %+v", p)
	}
	if n.r.commit != 4 {
		t.Fatalf("commit %d", n.r.commit)
	}
	c.checkOK()
}

func TestSingleNode(t *testing.T) {
	c := newCluster(t, 1, defaultTuning, 1)
	n := c.node(1)
	for range 20 {
		c.tick(n)
	}
	if n.r.role != Leader || n.r.term != 1 || n.applied != 1 {
		t.Fatalf("status %+v applied %d", n.r.Status(), n.applied)
	}
	i := c.propose(n, "x")
	if n.applied != i {
		t.Fatalf("applied %d, want %d", n.applied, i)
	}
	c.must(n, c.read(n, 5))
	if !slices.Equal(n.reads, []Read{{Tag: 5, Index: i}}) {
		t.Fatalf("reads %+v", n.reads)
	}
	if len(c.net) != 0 {
		t.Fatalf("single node sent %+v", c.net)
	}
	c.crash(n)
	for range 20 {
		c.tick(n)
	}
	if n.r.role != Leader || n.r.term != 2 || n.applied != i+1 {
		t.Fatalf("after restart %+v applied %d", n.r.Status(), n.applied)
	}
	c.checkOK()
}

func TestStaleAckAfterReelection(t *testing.T) {
	c := newCluster(t, 5, defaultTuning, 1)
	a, b := c.node(1), c.node(3)
	c.elect(a)
	for _, body := range []string{"a", "b", "c", "d"} {
		c.propose(a, body)
		c.drop(func(Message) bool { return true })
	}
	c.tick(a)
	c.tick(a)
	c.drop(func(m Message) bool { return m.To != 2 })
	c.deliverAll(to(2))
	stale := c.take(from(2))
	if len(stale) != 1 || stale[0].m.Type != MsgAppResp || stale[0].m.Term != 1 || stale[0].m.Index != 5 {
		t.Fatalf("stale ack %+v", stale)
	}

	c.isolate(1)
	c.isolate(2)
	c.timeout(b)
	c.settle()
	if b.r.role != Leader || b.r.term != 2 {
		t.Fatalf("b %+v", b.r.Status())
	}
	c.heal()
	for range 4 {
		c.tick(b)
		c.settle()
	}

	c.timeout(a)
	c.settle()
	if a.r.role != Leader || a.r.term != 3 || a.r.commit != 3 {
		t.Fatalf("a %+v", a.r.Status())
	}
	c.isolate(2)
	c.isolate(3)
	c.isolate(5)
	c.propose(a, "x")
	c.settle()
	c.propose(a, "y")
	c.settle()
	c.cut[2][1] = false
	c.deliver(stale[0])
	if a.r.commit != 3 || a.r.prs[1].match > c.node(2).log.LastIndex() {
		t.Fatalf("ack from term 1 counted: commit %d, match of node 2 %d, its log ends at %d", a.r.commit, a.r.prs[1].match, c.node(2).log.LastIndex())
	}
	c.heal()
	c.isolate(1)
	c.isolate(4)
	c.timeout(b)
	c.settle()
	c.checkOK()
}

func TestLostEntriesComeBackWithoutStorm(t *testing.T) {
	c := newCluster(t, 3, defaultTuning, 1)
	l, f := c.node(1), c.node(3)
	c.elect(l)
	for _, body := range []string{"a", "b", "c", "d", "e"} {
		c.propose(l, body)
		c.settle()
	}
	c.damage(f, 2)
	refusals, appends := 0, 0
	for round := range 20 {
		for _, n := range c.nodes {
			c.tick(n)
		}
		if round == 0 {
			c.propose(l, "f")
		}
		for k := 0; len(c.net) > 0; k++ {
			if k == 50 {
				t.Fatalf("round %d: network did not settle, %d refusals and %d appends to the damaged node", round, refusals, appends)
			}
			for _, env := range c.take(func(Message) bool { return true }) {
				switch m := env.m; {
				case m.Type == MsgAppResp && m.From == 3 && m.Reject:
					refusals++
				case m.Type == MsgApp && m.To == 3 && len(m.Entries) > 0:
					appends++
				}
				c.deliver(env)
			}
		}
		c.checkOK()
	}
	if f.hs.Repair || !slices.Equal(f.log.terms(), l.log.terms()) || f.applied != l.applied {
		t.Fatalf("damaged node disk %+v log %v applied %d, leader log %v applied %d", f.hs, f.log.terms(), f.applied, l.log.terms(), l.applied)
	}
	if l.r.role != Leader || l.r.term != 1 || l.applied != 7 {
		t.Fatalf("leader %+v applied %d, want the same leader to apply 7", l.r.Status(), l.applied)
	}
	if refusals > 1 || appends > 2 {
		t.Fatalf("%d refusals and %d appends to bring back 5 entries", refusals, appends)
	}
}

func TestRepairingNodeDoesNotHelpElect(t *testing.T) {
	for _, size := range []int{3, 5} {
		c := newCluster(t, size, defaultTuning, 1)
		old := c.node(1)
		c.elect(old)
		c.propose(old, "a")
		c.settle()
		c.isolate(1)
		c.damage(c.node(2), 1)
		var lead *node
		for range 200 {
			for _, n := range c.nodes[1:] {
				c.tick(n)
			}
			c.settle()
			for _, n := range c.nodes[1:] {
				if n.r.role == Leader {
					lead = n
				}
			}
		}
		switch {
		case size == 3 && lead != nil:
			t.Fatalf("size 3: node %d elected while the other is in repair", lead.id)
		case size == 3 && c.node(3).r.term != 1:
			t.Fatalf("size 3: term moved to %d without a leader", c.node(3).r.term)
		case size == 5 && (lead == nil || lead.id == 2):
			t.Fatalf("size 5: leader %+v, want one of the three healthy nodes", lead)
		}
		c.heal()
		for range 100 {
			for _, n := range c.nodes {
				c.tick(n)
			}
			c.settle()
		}
		if lead = c.leader(); lead == nil || c.node(2).hs.Repair || c.node(2).applied != lead.applied {
			t.Fatalf("size %d: after the partition leader %v, damaged node %+v", size, lead, c.node(2).r.Status())
		}
		c.checkOK()
	}
}

func TestTwoRepairingNodes(t *testing.T) {
	t.Run("leader stays and heals both", func(t *testing.T) {
		c := newCluster(t, 3, defaultTuning, 1)
		l := c.node(1)
		c.elect(l)
		for _, body := range []string{"a", "b", "c"} {
			c.propose(l, body)
			c.settle()
		}
		c.damage(c.node(2), 1)
		c.damage(c.node(3), 2)
		for range 50 {
			for _, n := range c.nodes {
				c.tick(n)
			}
			c.settle()
		}
		if l.r.role != Leader || l.r.term != 1 {
			t.Fatalf("leader %+v", l.r.Status())
		}
		for _, n := range c.nodes[1:] {
			if n.hs.Repair || !slices.Equal(n.log.terms(), l.log.terms()) {
				t.Fatalf("node %d disk %+v log %v, leader log %v", n.id, n.hs, n.log.terms(), l.log.terms())
			}
		}
		c.checkOK()
	})
	t.Run("no leader, no election", func(t *testing.T) {
		c := newCluster(t, 3, defaultTuning, 1)
		l := c.node(1)
		c.elect(l)
		c.propose(l, "a")
		c.settle()
		c.damage(c.node(2), 1)
		c.damage(c.node(3), 1)
		c.crash(l)
		for range 200 {
			for _, n := range c.nodes {
				c.tick(n)
			}
			c.settle()
		}
		for _, n := range c.nodes {
			if st := n.r.Status(); st.Role == Leader || st.Term != 1 || n.id != 1 && !n.hs.Repair {
				t.Fatalf("node %d %+v, disk %+v, want the cluster to stand", n.id, st, n.hs)
			}
		}
		c.checkOK()
	})
}
