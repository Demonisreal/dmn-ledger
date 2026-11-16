package control

import (
	"fmt"
	"time"
)

type Speed uint8

const (
	None Speed = iota
	Fast
	Transition
	Slow
	Restless
)

var speedNames = [...]string{"none", "fast", "transition", "slow", "restless"}

func (s Speed) String() string {
	if int(s) < len(speedNames) {
		return speedNames[s]
	}
	return fmt.Sprintf("speed(%d)", s)
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

const ForeignLimit = 1.0

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
	WhyThrottled  = "throttled"
	WhySwapped    = "swapped out"
	WhyUnread     = "unread"
)

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
		if s.Throttled > 0 {
			m.Why = append(m.Why, WhyThrottled)
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
	var slow, fast int
	for _, m := range window {
		switch {
		case !m.Valid:
		case m.Speed == Slow:
			slow++
		case m.Speed == Fast:
			fast++
		}
	}
	switch {
	case slow >= need:
		return Slow
	case fast >= need:
		return Fast
	}
	return Restless
}
