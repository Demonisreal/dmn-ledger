//go:build chaos && linux

package chaos

import (
	"cmp"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Demonisreal/dmn-ledger/internal/faultnet"
)

const (
	round    = 30 * time.Second
	storm    = 20 * time.Second
	downGap  = 500 * time.Millisecond
	awayOdds = 4
	backIn   = 10 * time.Second
)

type kind int

const (
	crash kind = iota
	freeze
	cut
	blackhole
	stall
	noise
	away
)

var kindNames = [...]string{"crash", "freeze", "cut", "blackhole", "stall", "noise", "away"}

func (k kind) String() string {
	return kindNames[k]
}

type side int

const (
	bothWays side = iota
	outgoing
	incoming
	fromClients
)

var sideNames = [...]string{"both ways around", "out of", "into", "clients to"}

type route struct{ from, to int }

func (r route) String() string {
	if r.from == 0 {
		return fmt.Sprintf("c->n%d", r.to)
	}
	return fmt.Sprintf("n%d->n%d", r.from, r.to)
}

func routes() []route {
	var all []route
	for to := 1; to <= nodes; to++ {
		all = append(all, route{0, to})
	}
	for from := 1; from <= nodes; from++ {
		for to := 1; to <= nodes; to++ {
			if from != to {
				all = append(all, route{from, to})
			}
		}
	}
	return all
}

func (s side) routes(node int) []route {
	if s == fromClients {
		return []route{{0, node}}
	}
	var rs []route
	for _, r := range routes() {
		if r.from != 0 && (s != incoming && r.from == node || s != outgoing && r.to == node) {
			rs = append(rs, r)
		}
	}
	return rs
}

type window struct {
	kind       kind
	via        kind
	start, end time.Duration
	node       int
	leader     bool
	side       side
	routes     []route
	fault      faultnet.Fault
	hit        int

	on, off time.Time
	behind  uint64
	waited  time.Duration
	absent  time.Duration
	passed  bool
}

func (w *window) act() kind {
	if w.kind == away {
		return w.via
	}
	return w.kind
}

func (w *window) down() bool {
	return w.kind == crash || w.kind == freeze || w.kind == away
}

func (w *window) near(o *window) bool {
	return w.start < o.end+downGap && o.start < w.end+downGap
}

func (w *window) String() string {
	var target string
	switch w.kind {
	case crash, freeze, cut, away:
		target = fmt.Sprintf("n%d", w.node)
		if w.leader {
			target = fmt.Sprintf("the leader, else n%d", w.node)
		}
		if w.act() == cut {
			target = sideNames[w.side] + " " + target
		}
		if w.kind == away {
			target += " by " + w.via.String()
		}
		if w.hit != 0 {
			target += fmt.Sprintf(", hit n%d", w.hit)
		}
		if w.waited != 0 {
			target += fmt.Sprintf(", %v before the storm and gone for %v of it, the leader compacted past index %d: %t",
				w.waited.Round(time.Millisecond), w.absent.Round(time.Millisecond), w.behind, w.passed)
		}
	case noise:
		target = fmt.Sprintf("all routes, delay %v loss %.1f%% flip %.2f%%", w.fault.Delay, 100*w.fault.Loss, 100*w.fault.Flip)
	default:
		var names []string
		for _, r := range w.routes {
			names = append(names, r.String())
		}
		target = strings.Join(names, " ")
	}
	return fmt.Sprintf("%9.3fs %9.3fs  %-9s %s", w.start.Seconds(), w.end.Seconds(), w.kind, target)
}

func plan(seed uint64, rounds int) []*window {
	rng := rand.New(rand.NewPCG(seed, 1))
	all := routes()
	var ws []*window
	for r := range rounds {
		base := time.Duration(r) * round
		var down []*window
		windows := 1 + rng.IntN(6)
		if rng.IntN(awayOdds) == 0 {
			w := &window{kind: away, via: kind(rng.IntN(int(cut) + 1)), node: 1 + rng.IntN(nodes), leader: rng.IntN(2) == 0}
			w.start, w.end = base, base+span(rng, backIn)
			if w.via == cut {
				w.fault = faultnet.Fault{Cut: true}
			}
			down = append(down, w)
			ws = append(ws, w)
			if rng.IntN(2) == 0 {
				windows--
				back := &window{kind: noise, routes: all, fault: noisy(rng)}
				back.start = max(base, w.end-span(rng, time.Second))
				back.end = back.start + 2*time.Second + span(rng, 6*time.Second)
				ws = append(ws, back)
			}
		}
		for range windows {
			w := &window{kind: kind(rng.IntN(int(away))), node: 1 + rng.IntN(nodes), leader: rng.IntN(2) == 0}
			length := 500*time.Millisecond + span(rng, 7500*time.Millisecond)
			if w.kind == crash {
				length = 200*time.Millisecond + span(rng, 2800*time.Millisecond)
			}
			w.start = base + span(rng, storm-length)
			w.end = w.start + length
			switch w.kind {
			case crash, freeze:
				if slices.ContainsFunc(down, w.near) {
					continue
				}
				down = append(down, w)
			case cut:
				w.side = side(rng.IntN(len(sideNames)))
				w.fault = faultnet.Fault{Cut: true}
			case blackhole:
				for _, k := range rng.Perm(len(all))[:1+rng.IntN(3)] {
					w.routes = append(w.routes, all[k])
				}
				w.fault = faultnet.Fault{Blackhole: true}
			case stall:
				w.routes = []route{all[rng.IntN(len(all))]}
				w.fault = faultnet.Fault{Stall: true}
			case noise:
				w.routes, w.fault = all, noisy(rng)
			}
			ws = append(ws, w)
		}
	}
	slices.SortStableFunc(ws, func(a, b *window) int { return cmp.Compare(a.start, b.start) })
	return ws
}

func noisy(rng *rand.Rand) faultnet.Fault {
	return faultnet.Fault{
		Delay: 5*time.Millisecond + span(rng, 295*time.Millisecond),
		Loss:  0.3 * rng.Float64(),
		Flip:  0.01 * rng.Float64(),
	}
}

func span(rng *rand.Rand, d time.Duration) time.Duration {
	return time.Duration(rng.Int64N(int64(d)))
}

func planText(ws []*window) string {
	var b strings.Builder
	for _, w := range ws {
		b.WriteString(w.String())
		b.WriteByte('\n')
	}
	return b.String()
}

func TestPlanFollowsTheSeed(t *testing.T) {
	const rounds = 60
	first := planText(plan(7, rounds))
	if again := planText(plan(7, rounds)); again != first {
		t.Fatalf("seed 7 planned twice differently:\n%s\n%s", first, again)
	}
	if other := planText(plan(8, rounds)); other == first {
		t.Fatal("seeds 7 and 8 plan the same storms")
	}

	var ways [len(kindNames)]int
	for seed := range uint64(200) {
		ws := plan(seed, rounds)
		perRound, outages := map[int]int{}, map[int]int{}
		var down []*window
		for _, w := range ws {
			r := int(w.start / round)
			length := w.end - w.start
			base := time.Duration(r) * round
			switch {
			case w.start < base || w.end > base+storm:
				t.Fatalf("seed %d: %v leaves the storm of round %d", seed, w, r)
			case w.kind == away && (w.start != base || length >= backIn || w.via > cut):
				t.Fatalf("seed %d: %v is not a long outage before the storm", seed, w)
			case w.kind == crash && (length < 200*time.Millisecond || length >= 3*time.Second),
				w.kind != crash && w.kind != away && (length < 500*time.Millisecond || length >= 8*time.Second):
				t.Fatalf("seed %d: %v lasts %v", seed, w, length)
			case w.kind == noise && (w.fault.Delay < 5*time.Millisecond || w.fault.Delay > 300*time.Millisecond || w.fault.Loss > 0.3 || w.fault.Flip > 0.01):
				t.Fatalf("seed %d: %v is out of bounds", seed, w)
			}
			if w.kind == away {
				outages[r]++
				ways[w.via]++
			} else {
				perRound[r]++
			}
			if w.down() {
				if k := slices.IndexFunc(down, w.near); k >= 0 {
					t.Fatalf("seed %d: %v and %v hold two nodes down at once", seed, down[k], w)
				}
				down = append(down, w)
			}
		}
		for r, n := range perRound {
			if n > 6 {
				t.Fatalf("seed %d: round %d has %d windows", seed, r, n)
			}
		}
		for r, n := range outages {
			if n > 1 {
				t.Fatalf("seed %d: round %d has %d long outages", seed, r, n)
			}
		}
	}
	want := 200 * rounds / awayOdds / 3
	for _, k := range []kind{crash, freeze, cut} {
		if ways[k] < want*3/4 {
			t.Errorf("%d long outages by %s in %d rounds, want about %d", ways[k], k, 200*rounds, want)
		}
	}
}

func TestSidesCutTheRightRoutes(t *testing.T) {
	cases := []struct {
		side side
		want string
	}{
		{bothWays, "[n1->n2 n2->n1 n2->n3 n3->n2]"},
		{outgoing, "[n2->n1 n2->n3]"},
		{incoming, "[n1->n2 n3->n2]"},
		{fromClients, "[c->n2]"},
	}
	for _, tc := range cases {
		if got := fmt.Sprint(tc.side.routes(2)); got != tc.want {
			t.Errorf("%s n2 cuts %s, want %s", sideNames[tc.side], got, tc.want)
		}
	}
}
