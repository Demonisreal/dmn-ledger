//go:build linux

package demo

import (
	"net/netip"
	"slices"
	"time"

	"github.com/Demonisreal/dmn-ledger/ledger"
)

const (
	freshSight = 300 * time.Millisecond
	youngLog   = 8 << 10
)

type wish struct {
	kind   Kind
	target int
	leader int
}

type visit struct {
	from netip.Prefix
	at   time.Time
}

func (s *Stage) Act(kind Kind, target int, from netip.Prefix) (time.Time, error) {
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.st.Phase != "steady" && s.st.Phase != "action" {
		return time.Time{}, ErrRecovering
	}
	if s.acting {
		return time.Time{}, ErrBusy
	}
	if until := s.cooled(); now.Before(until) {
		return until, ErrCooling
	}
	for _, v := range s.visits {
		if until := v.at.Add(s.pace.perAddr); v.from == from && now.Before(until) {
			return until, ErrLimited
		}
	}
	if now.Sub(s.sight.at) > freshSight || !s.sight.whole() {
		return time.Time{}, ErrRecovering
	}
	if node := target; kind == Flip {
		if node == 0 {
			node = s.sight.leader
		}
		if s.sight.sizes[node-1] < youngLog {
			return time.Time{}, ErrYoung
		}
	}
	s.forget(now)
	s.visits = append(s.visits, visit{from, now})
	s.began, s.acting = now, true
	s.wishes <- wish{kind, target, s.sight.leader}
	return now.Add(s.pace.perAddr), nil
}

func (s *Stage) forget(now time.Time) {
	s.visits = slices.DeleteFunc(s.visits, func(v visit) bool { return !now.Before(v.at.Add(s.pace.perAddr)) })
}

func (s *Stage) cooled() time.Time {
	if s.began.IsZero() {
		return time.Time{}
	}
	until := s.began.Add(s.pace.gap)
	if calm := s.ended.Add(s.pace.calm); !s.ended.IsZero() && calm.After(until) {
		until = calm
	}
	return until
}

type sight struct {
	at     time.Time
	sts    [3]*ledger.NodeStatus
	sizes  [3]int64
	leader int
}

func (sg *sight) whole() bool {
	leaders := 0
	for _, st := range sg.sts {
		if st == nil || st.Repair {
			return false
		}
		if st.Role == ledger.Leader {
			leaders++
		}
	}
	return leaders == 1
}
