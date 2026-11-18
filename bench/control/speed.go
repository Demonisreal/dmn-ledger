package control

import (
	"fmt"
	"slices"
	"time"
)

type Speed uint8

const (
	None Speed = iota
	Fast
	Transition
	Slow
	Restless
	Unpaced
)

var speedNames = [...]string{"none", "fast", "transition", "slow", "restless", "unpaced"}

func (s Speed) String() string {
	if int(s) < len(speedNames) {
		return speedNames[s]
	}
	return fmt.Sprintf("speed(%d)", s)
}

func (s Speed) Rank() int {
	switch s {
	case Slow:
		return 3
	case Fast:
		return 2
	case Unpaced:
		return 1
	}
	return 0
}

func (s Speed) MarshalText() ([]byte, error) {
	return []byte(s.String()), nil
}

func (s *Speed) UnmarshalText(b []byte) error {
	for i, name := range speedNames {
		if name == string(b) {
			*s = Speed(i) //nolint:gosec // G115: an index into speedNames
			return nil
		}
	}
	return fmt.Errorf("control: unknown speed %q", b)
}

const (
	ForeignLimit = 1.0
	IdleCPU      = 0.5
)

type Thresholds struct {
	Fast       time.Duration `json:"fast_ns"`
	Slow       time.Duration `json:"slow_ns"`
	MinFlushes uint64        `json:"min_flushes"`
}

func Defaults() Thresholds {
	return Thresholds{Fast: 500 * time.Microsecond, Slow: time.Millisecond, MinFlushes: 20}
}

func (th Thresholds) Speed(flushes uint64, took time.Duration) Speed {
	if flushes < th.MinFlushes || flushes == 0 {
		return None
	}
	mean := took / time.Duration(flushes) //nolint:gosec // G115: flushes in one second
	switch {
	case mean < th.Fast:
		return Fast
	case mean > th.Slow:
		return Slow
	}
	return Transition
}

const (
	WhyFewFlushes = "too few flushes"
	WhyTransition = "transition"
	WhyChange     = "first second after a change"
	WhyForeign    = "foreign cpu"
	WhySwapped    = "swapped out"
	WhyUnread     = "unread"
	WhyIdle       = "target idle"
)

func (th Thresholds) Unpace(marks []Mark, secs []Second, window func(i int) bool) bool {
	var few, all int
	for i, m := range marks {
		if window(i) {
			all++
			if m.Speed == None {
				few++
			}
		}
	}
	if 2*few <= all {
		return false
	}
	for i := range marks {
		m := &marks[i]
		m.Speed = Unpaced
		m.Why = slices.DeleteFunc(m.Why, func(why string) bool {
			return why == WhyFewFlushes || why == WhyTransition || why == WhyChange
		})
		if window(i) && secs[i].Flushes < th.MinFlushes && secs[i].Target < IdleCPU {
			m.Why = append(m.Why, WhyIdle)
		}
		m.Valid = len(m.Why) == 0
	}
	return true
}

type Mark struct {
	Speed Speed
	Valid bool
	Why   []string
}

func (th Thresholds) Judge(secs []Second) []Mark {
	marks := make([]Mark, len(secs))
	for i, s := range secs {
		m := Mark{Speed: th.Speed(s.Flushes, s.FlushTime)}
		switch m.Speed {
		case None:
			m.Why = append(m.Why, WhyFewFlushes)
		case Transition:
			m.Why = append(m.Why, WhyTransition)
		default:
			if i > 0 && marks[i-1].Speed != m.Speed {
				m.Why = append(m.Why, WhyChange)
			}
		}
		if s.Foreign >= ForeignLimit {
			m.Why = append(m.Why, WhyForeign)
		}
		if s.SwappedOut > 0 {
			m.Why = append(m.Why, WhySwapped)
		}
		if len(s.Unread) > 0 {
			m.Why = append(m.Why, WhyUnread)
		}
		m.Valid = len(m.Why) == 0
		marks[i] = m
	}
	return marks
}

func Settle(window []Mark, need int) Speed {
	var slow, fast, unpaced int
	for _, m := range window {
		switch {
		case !m.Valid:
		case m.Speed == Slow:
			slow++
		case m.Speed == Fast:
			fast++
		case m.Speed == Unpaced:
			unpaced++
		}
	}
	switch {
	case slow >= need:
		return Slow
	case fast >= need:
		return Fast
	case unpaced >= need:
		return Unpaced
	}
	return Restless
}
