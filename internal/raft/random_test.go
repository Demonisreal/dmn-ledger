package raft

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"
)

type runStats struct {
	commit     uint64
	leaders    int
	snaps      int
	installs   int
	reads      int
	staleReads int
	late       int
	damages    int
	heals      int
	crowd      int
}

type damage uint8

const (
	undamaged damage = iota
	oneDamaged
	anyDamaged
)

func runRandom(t testing.TB, seed uint64, size int, early bool, d damage) (string, runStats) {
	rng := rand.New(rand.NewPCG(seed, uint64(size)))
	tune := tuning{election: 10, heartbeat: 2, snapshot: 20, maxBytes: 8, maxUncommitted: 32}
	c := newCluster(t, size, tune, seed)
	c.early = early
	crashRate := 1
	if early {
		crashRate = 10
	}
	c.crashAt = func(*node) int {
		if rng.IntN(100) < crashRate {
			return rng.IntN(4)
		}
		return -1
	}
	c.hold = func() bool { return rng.IntN(10) < 3 }
	pick := func() *node { return c.nodes[rng.IntN(size)] }
	var st runStats
	reader := func() *node {
		var oldest *node
		for _, n := range c.nodes {
			if n.r.role == Leader && (oldest == nil || n.r.term < oldest.r.term) {
				oldest = n
			}
		}
		if oldest == nil || rng.IntN(4) == 0 {
			return pick()
		}
		return oldest
	}
	deposed := func(n *node) bool {
		return n.r.role == Leader && slices.ContainsFunc(c.nodes, func(o *node) bool { return o.r.term > n.r.term })
	}
	var parked []envelope
	tag := uint64(0)
	for step := range 5000 {
		switch x := rng.IntN(100); {
		case x < 25:
			c.tick(pick())
		case x < 75:
			switch {
			case len(parked) > 0 && rng.IntN(60) == 0:
				k := rng.IntN(len(parked))
				env := parked[k]
				parked = append(parked[:k], parked[k+1:]...)
				st.late++
				c.deliver(env)
			case len(c.net) > 0:
				k := rng.IntN(len(c.net))
				env := c.net[k]
				c.net = append(c.net[:k], c.net[k+1:]...)
				if rng.IntN(25) == 0 {
					parked = append(parked, env)
				} else {
					c.deliver(env)
				}
			}
		case x < 78:
			if len(c.net) > 0 {
				k := rng.IntN(len(c.net))
				c.net = append(c.net[:k], c.net[k+1:]...)
			}
		case x < 80:
			if len(c.net) > 0 {
				c.net = append(c.net, c.net[rng.IntN(len(c.net))])
			}
		case x < 90:
			n := pick()
			if l := c.leader(); l != nil && rng.IntN(4) > 0 {
				n = l
			}
			_, err := n.r.Propose(1, []byte(fmt.Sprintf("%d-%d", seed, step)))
			if err != nil && !errors.Is(err, ErrNotLeader) && !errors.Is(err, ErrBusy) {
				c.chk.fail("node %d: Propose: %v", n.id, err)
			}
			c.after(n)
		case x < 93:
			n := reader()
			if deposed(n) {
				st.staleReads++
			}
			tag++
			err := c.read(n, tag)
			if err != nil && !errors.Is(err, ErrNotLeader) && !errors.Is(err, ErrNotReady) {
				c.chk.fail("node %d: ReadIndex: %v", n.id, err)
			}
		case x < 94:
			n := pick()
			sick := slices.ContainsFunc(c.nodes, func(o *node) bool { return o != n && o.hs.Repair })
			if d != undamaged && (d == anyDamaged || !sick) && rng.IntN(2) == 0 && len(n.log.ents) > 0 {
				c.damage(n, rng.IntN(len(n.log.ents)))
				st.damages++
			} else {
				c.crash(n)
			}
		case x < 95:
			c.process(pick())
		case x < 97:
			n := pick()
			before := n.log.snap.Index
			c.snapshot(n, rng.IntN(2) == 0)
			if n.log.snap.Index > before {
				st.snaps++
			}
		case x < 98:
			c.isolate(pick().id)
		case x < 99:
			a, b := pick(), pick()
			if a != b {
				c.cut[a.id][b.id] = true
			}
		default:
			c.heal()
		}
		if c.chk.failed() {
			return fmt.Sprintf("step %d: %s", step, c.chk.violation), st
		}
		st.crowd = max(st.crowd, len(c.net))
	}
	st.heals = c.heals
	st.commit = c.chk.maxCommit
	st.leaders = len(c.chk.leaders)
	st.installs = c.installs
	for _, n := range c.nodes {
		st.reads += len(n.reads)
	}
	if early || d == anyDamaged {
		return "", st
	}
	return converge(c), st
}

func converge(c *cluster) string {
	c.heal()
	c.hold, c.crashAt = nil, nil
	round := func() {
		for _, n := range c.nodes {
			c.tick(n)
		}
		for range 50 {
			if len(c.net) == 0 {
				break
			}
			for _, env := range c.take(func(Message) bool { return true }) {
				c.deliver(env)
			}
		}
	}
	for _, n := range c.nodes {
		c.process(n)
	}
	for range 100 {
		round()
	}
	if c.chk.failed() {
		return c.chk.violation
	}
	var lead *node
	for _, n := range c.nodes {
		if n.r.role == Leader {
			if lead != nil {
				return fmt.Sprintf("liveness: leaders %d and %d after the quiet phase", lead.id, n.id)
			}
			lead = n
		}
	}
	if lead == nil {
		return "liveness: no leader after the quiet phase"
	}
	i, err := lead.r.Propose(1, []byte("final"))
	if err != nil {
		return fmt.Sprintf("liveness: final propose: %v", err)
	}
	c.process(lead)
	for range 20 {
		round()
	}
	if c.chk.failed() {
		return c.chk.violation
	}
	for _, n := range c.nodes {
		if n.hs.Repair {
			return fmt.Sprintf("liveness: node %d still in repair after the quiet phase", n.id)
		}
		if n.applied != i || n.r.commit != i || n.hash != lead.hash {
			return fmt.Sprintf("liveness: node %d applied %d commit %d, leader %d applied %d", n.id, n.applied, n.r.commit, lead.id, i)
		}
	}
	return ""
}

func TestRandomSchedules(t *testing.T) {
	seeds := uint64(500)
	if testing.Short() {
		seeds = 50
	}
	kinds := []struct {
		name    string
		d       damage
		commits uint64
		leaders int
		stale   int
	}{
		{"undamaged", undamaged, 20, 2, int(seeds / 100)},
		{"one damaged", oneDamaged, 10, 2, 0},
		{"any damaged", anyDamaged, 5, 1, 0},
	}
	for _, k := range kinds {
		for _, size := range []int{3, 5} {
			var total runStats
			for seed := range seeds {
				violation, st := runRandom(t, seed, size, false, k.d)
				if violation != "" {
					t.Fatalf("%s, size %d seed %d: %s", k.name, size, seed, violation)
				}
				total.commit += st.commit
				total.leaders += st.leaders
				total.snaps += st.snaps
				total.installs += st.installs
				total.reads += st.reads
				total.staleReads += st.staleReads
				total.late += st.late
				total.damages += st.damages
				total.heals += st.heals
				total.crowd = max(total.crowd, st.crowd)
			}
			t.Logf("%s, size %d: %d seeds, %d entries committed, %d terms with a leader, %d snapshots taken, %d installed, "+
				"%d reads, %d asked of a deposed leader, %d late messages, %d damaged, %d healed before the quiet phase, "+
				"at most %d messages in flight",
				k.name, size, seeds, total.commit, total.leaders, total.snaps, total.installs, total.reads, total.staleReads, total.late,
				total.damages, total.heals, total.crowd)
			switch {
			case total.commit < k.commits*seeds || total.leaders < k.leaders*int(seeds) || total.installs == 0 || total.staleReads < k.stale:
				t.Fatalf("%s, size %d: schedules make too little progress: %+v", k.name, size, total)
			case k.d != undamaged && (total.damages < int(seeds) || total.heals == 0):
				t.Fatalf("%s, size %d: %d damaged, %d healed", k.name, size, total.damages, total.heals)
			}
		}
	}
}

func TestRandomSchedulesSendBeforePersist(t *testing.T) {
	for seed := range uint64(500) {
		for _, size := range []int{3, 5} {
			violation, _ := runRandom(t, seed, size, true, undamaged)
			if violation == "" {
				continue
			}
			for _, kind := range []string{"election safety", "log matching", "state machine safety", "leader completeness", "linearizable read"} {
				if strings.Contains(violation, kind) {
					t.Logf("size %d seed %d: %s", size, seed, violation)
					return
				}
			}
			t.Fatalf("size %d seed %d: unexpected violation: %s", size, seed, violation)
		}
	}
	t.Fatal("sending before persisting went unnoticed in 500 seeds")
}

func FuzzSchedules(f *testing.F) {
	f.Add(uint64(0), false, uint8(undamaged))
	f.Add(uint64(1818), true, uint8(undamaged))
	f.Add(uint64(7), false, uint8(oneDamaged))
	f.Add(uint64(7), true, uint8(anyDamaged))
	f.Add(uint64(16719), false, uint8(anyDamaged))
	f.Add(uint64(16011), true, uint8(oneDamaged))
	f.Add(uint64(6504), true, uint8(anyDamaged))
	f.Fuzz(func(t *testing.T, seed uint64, five bool, kind uint8) {
		size := 3
		if five {
			size = 5
		}
		if violation, _ := runRandom(t, seed, size, false, damage(kind%3)); violation != "" {
			t.Fatalf("size %d seed %d damage %d: %s", size, seed, kind%3, violation)
		}
	})
}
