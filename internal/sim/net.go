package sim

import (
	"fmt"
	"math/rand/v2"
)

type Msg struct {
	From, To uint64
	Body     Body
}

type Body interface {
	fmt.Stringer
	Clone() Body
}

type link struct {
	rng          *rand.Rand
	cut          int
	base, jitter uint64
	loss, dup    float64
}

type network struct {
	links [][]link
}

func newNetwork(rng *rand.Rand, hosts uint64, loss, dup float64) network {
	n := network{links: make([][]link, hosts)}
	for from := range n.links {
		n.links[from] = make([]link, hosts)
		for to := range n.links[from] {
			r := fork(rng)
			base := linkBaseMin + r.Uint64N(linkBaseSpan)
			n.links[from][to] = link{
				rng:    r,
				base:   base,
				jitter: r.Uint64N(base + 1),
				loss:   loss * r.Float64(),
				dup:    dup * r.Float64(),
			}
		}
	}
	return n
}

func (n *network) at(from, to uint64) *link {
	return &n.links[from-1][to-1]
}

func (l *link) delay(storm bool) uint64 {
	d := l.base + l.rng.Uint64N(l.jitter+1)
	switch {
	case l.rng.IntN(lagOdds) == 0:
		d += lagMin + l.rng.Uint64N(lagSpan)
	case storm && l.rng.IntN(stallOdds) == 0:
		d += stallMin + l.rng.Uint64N(stallSpan)
	}
	return d
}

func (l *link) lost() bool {
	return l.loss > 0 && l.rng.Float64() < l.loss
}

func (l *link) doubled() bool {
	return l.dup > 0 && l.rng.Float64() < l.dup
}

func (n *network) partition(side []bool, oneway bool, by int) {
	for from := range n.links {
		for to := range n.links[from] {
			if side[from] && !side[to] || !oneway && side[to] && !side[from] {
				n.links[from][to].cut += by
			}
		}
	}
}

func (n *network) sever(a, b uint64, by int) {
	n.at(a, b).cut += by
	n.at(b, a).cut += by
}

func (n *network) calm() {
	for from := range n.links {
		for to := range n.links[from] {
			n.links[from][to].loss = 0
			n.links[from][to].dup = 0
		}
	}
}
