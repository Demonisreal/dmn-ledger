package server

import (
	"sync/atomic"
	"time"
)

type Stats struct {
	Rounds  uint64
	Wait    time.Duration
	Process time.Duration
	Slowest time.Duration
}

type counters struct {
	rounds  atomic.Uint64
	wait    atomic.Int64
	process atomic.Int64
	slowest atomic.Int64
}

func (s *Server) Stats() Stats {
	return Stats{
		Rounds:  s.counters.rounds.Load(),
		Wait:    time.Duration(s.counters.wait.Load()),
		Process: time.Duration(s.counters.process.Load()),
		Slowest: time.Duration(s.counters.slowest.Swap(0)),
	}
}

func (c *counters) waited(d time.Duration) {
	c.wait.Add(int64(d))
}

func (c *counters) processed(d time.Duration) {
	c.rounds.Add(1)
	c.process.Add(int64(d))
	if int64(d) > c.slowest.Load() {
		c.slowest.Store(int64(d))
	}
}
