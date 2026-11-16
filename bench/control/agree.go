package control

import (
	"fmt"
	"time"
)

const (
	Tolerance   = 0.10
	runsForUnit = 4
)

type Figures struct {
	Valid      bool
	Speed      Speed
	Flush      time.Duration
	Throughput float64
	P50        time.Duration
	Syncs      float64
}

type Accord uint8

const (
	Undecided Accord = iota
	Agreed
	AgreedInFlushes
	Missed
)

func (a Accord) String() string {
	switch a {
	case Agreed:
		return "agreed"
	case AgreedInFlushes:
		return "agreed in flush units"
	case Missed:
		return "missed"
	}
	return "undecided"
}

type Agreement struct {
	Accord Accord
	Speed  Speed
	Pair   [2]int
	Why    string
}

func within(a, b float64) bool {
	lo, hi := min(a, b), max(a, b)
	return hi-lo <= Tolerance*lo
}

func Agree(runs []Figures) Agreement {
	speed := None
	for _, r := range runs {
		if r.Valid && (r.Speed == Slow || r.Speed == Fast && speed != Slow) {
			speed = r.Speed
		}
	}
	if speed == None {
		return Agreement{Why: "no valid run"}
	}
	var same []int
	for i, r := range runs {
		if r.Valid && r.Speed == speed {
			same = append(same, i)
		}
	}
	for j := 1; j < len(same); j++ {
		for i := range j {
			a, b := runs[same[i]], runs[same[j]]
			if !within(float64(a.Flush), float64(b.Flush)) {
				continue
			}
			ag := Agreement{Accord: Agreed, Speed: speed, Pair: [2]int{same[i], same[j]}}
			switch {
			case !within(a.Throughput, b.Throughput):
				ag.Accord, ag.Why = Missed, fmt.Sprintf("throughput %.0f against %.0f", a.Throughput, b.Throughput)
			case !within(float64(a.P50), float64(b.P50)):
				ag.Accord, ag.Why = Missed, fmt.Sprintf("p50 %v against %v", a.P50, b.P50)
			case !within(a.Syncs, b.Syncs):
				ag.Accord, ag.Why = Missed, fmt.Sprintf("fsync per transfer %.3f against %.3f", a.Syncs, b.Syncs)
			}
			return ag
		}
	}
	if len(same) < runsForUnit {
		return Agreement{Speed: speed, Why: fmt.Sprintf("no pair on an equally fast disk in %d runs", len(same))}
	}
	pair := [2]int{same[0], same[1]}
	gap := func(p [2]int) float64 {
		a, b := float64(runs[p[0]].Flush), float64(runs[p[1]].Flush)
		return max(a, b) / min(a, b)
	}
	for j := 1; j < len(same); j++ {
		for i := range j {
			if p := [2]int{same[i], same[j]}; gap(p) < gap(pair) {
				pair = p
			}
		}
	}
	a, b := runs[pair[0]], runs[pair[1]]
	ag := Agreement{Accord: AgreedInFlushes, Speed: speed, Pair: pair}
	ua, ub := float64(a.P50)/float64(a.Flush), float64(b.P50)/float64(b.Flush)
	switch {
	case !within(ua, ub):
		ag.Accord, ag.Why = Missed, fmt.Sprintf("p50 %.2f against %.2f flushes", ua, ub)
	case !within(a.Syncs, b.Syncs):
		ag.Accord, ag.Why = Missed, fmt.Sprintf("fsync per transfer %.3f against %.3f", a.Syncs, b.Syncs)
	}
	return ag
}
