//go:build linux

package local

import "fmt"

type Route struct{ From, To int }

func (r Route) String() string {
	if r.From == 0 {
		return fmt.Sprintf("c->n%d", r.To)
	}
	return fmt.Sprintf("n%d->n%d", r.From, r.To)
}

func routes(clients bool) []Route {
	var all []Route
	if clients {
		for to := 1; to <= nodes; to++ {
			all = append(all, Route{0, to})
		}
	}
	for from := 1; from <= nodes; from++ {
		for to := 1; to <= nodes; to++ {
			if from != to {
				all = append(all, Route{from, to})
			}
		}
	}
	return all
}

func (c *Cluster) Routes() []Route {
	return c.routes
}

func (c *Cluster) Around(node int) []Route {
	var rs []Route
	for _, r := range c.routes {
		if r.From != 0 && (r.From == node || r.To == node) {
			rs = append(rs, r)
		}
	}
	return rs
}
