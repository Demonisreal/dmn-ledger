package sim

import (
	"fmt"
	"math/rand/v2"
	"slices"
)

type Profile string

const (
	ProfileCalm    Profile = "calm"
	ProfileNet     Profile = "net"
	ProfileCrash   Profile = "crash"
	ProfileClock   Profile = "clock"
	ProfileAll     Profile = "all"
	ProfileEvict   Profile = "evict"
	ProfileLeaders Profile = "leaders"
	ProfileSplit   Profile = "split"
	ProfileRot     Profile = "rot"
	ProfileBeyond  Profile = "beyond"
)

func Profiles() []Profile {
	return []Profile{ProfileCalm, ProfileNet, ProfileCrash, ProfileClock, ProfileAll, ProfileEvict, ProfileLeaders, ProfileSplit, ProfileRot, ProfileBeyond}
}

const (
	microsecond uint64 = 1000
	millisecond        = 1000 * microsecond
	second             = 1000 * millisecond

	tick       = 10 * millisecond
	clockBase  = 1000 * second
	clockSkew  = 2 * second
	jumpMax    = 30 * second
	stormMin   = 2 * second
	stormSpan  = 4 * second
	quietLen   = 5 * second
	maxWindows = 12
	windowMin  = 50 * millisecond
	windowSpan = 1500 * millisecond

	armSpan        = 16
	gateSpan       = 4
	restartArmOdds = 3
	restartMin     = 10 * millisecond
	restartSpan    = second
	blackoutSpread = 500 * millisecond

	fsyncMin     = 100 * microsecond
	fsyncSpan    = 1900 * microsecond
	slowSyncMin  = 20 * millisecond
	slowSyncSpan = 280 * millisecond
	bytesPerNs   = 2

	linkBaseMin  = 200 * microsecond
	linkBaseSpan = 1800 * microsecond
	lagOdds      = 100
	lagMin       = 20 * millisecond
	lagSpan      = 180 * millisecond
	stallOdds    = 1000
	stallMin     = second
	stallSpan    = 4 * second
	holdMin      = 100 * millisecond
	holdSpan     = 900 * millisecond

	crawlMin  = 20 * millisecond
	crawlSpan = 180 * millisecond

	rotTail   = 200
	shredRate = 0.3
)

type fault uint8

const (
	split fault = iota + 1
	oneway
	isolate
	bridge
	crash
	midway
	blackout
	slow
	eio
	fsyncgate
	jump
	pause
	leadercrash
	rot
)

var faultNames = [...]string{
	split: "split", oneway: "oneway", isolate: "isolate", bridge: "bridge", crash: "crash", midway: "midway",
	blackout: "blackout", slow: "slow", eio: "eio", fsyncgate: "fsyncgate", jump: "jump", pause: "pause",
	leadercrash: "leadercrash", rot: "rot",
}

func (f fault) String() string {
	return faultNames[f]
}

type moment uint8

const (
	onCommit moment = iota
	onElection
	atOnce
)

type window struct {
	kind       fault
	start, end uint64
	node, peer uint64
	side       []bool
	rate       float64
	shift      uint64
	back, keep bool
	step       uint64
	lead       bool
	when       moment
	hit        uint64
	down       bool
	sector     bool
	meta       bool
}

type plan struct {
	windows   []window
	quiet     uint64
	end       uint64
	loss, dup float64
	journal   []float64
	rearm     bool
	beyond    bool
}

func profileFaults(p Profile) ([]fault, error) {
	netFaults := []fault{split, oneway, isolate, bridge}
	diskFaults := []fault{crash, midway, midway, midway, blackout, slow, eio, fsyncgate, leadercrash}
	all := append(append(netFaults, diskFaults...), jump, pause)
	switch p {
	case ProfileCalm, "":
		return nil, nil
	case ProfileNet:
		return netFaults, nil
	case ProfileCrash:
		return diskFaults, nil
	case ProfileClock:
		return []fault{jump, pause}, nil
	case ProfileAll:
		return all, nil
	case ProfileRot, ProfileBeyond:
		return append(all, rot, rot, rot, rot, rot), nil
	case ProfileLeaders:
		return []fault{leadercrash, leadercrash, leadercrash, crash, isolate, bridge}, nil
	case ProfileSplit:
		return []fault{split}, nil
	}
	return nil, fmt.Errorf("sim: unknown profile %q", p)
}

func newPlan(rng *rand.Rand, p Profile, nodes, hosts uint64) (plan, error) {
	faults, err := profileFaults(p)
	if err != nil {
		return plan{}, err
	}
	if slices.Contains(faults, rot) && nodes < 3 {
		return plan{}, fmt.Errorf("sim: profile %s needs at least three nodes", p)
	}
	pl := plan{quiet: stormMin + rng.Uint64N(stormSpan), journal: make([]float64, nodes)}
	pl.end = pl.quiet + quietLen
	for i := range pl.journal {
		if rng.IntN(2) == 0 {
			pl.journal[i] = float64(1+rng.IntN(50)) / 100
		}
	}
	switch p {
	case ProfileNet, ProfileAll, ProfileRot, ProfileBeyond:
		pl.loss = float64(rng.IntN(201)) / 1000
		pl.dup = float64(rng.IntN(101)) / 1000
	case ProfileLeaders:
		pl.loss = float64(rng.IntN(201)) / 1000
	}
	pl.rearm = p == ProfileCrash || p == ProfileAll || p == ProfileRot || p == ProfileBeyond
	pl.beyond = p == ProfileBeyond
	if len(faults) == 0 {
		return pl, nil
	}
	for range 1 + rng.IntN(maxWindows) {
		win := window{kind: faults[rng.IntN(len(faults))], node: 1 + rng.Uint64N(nodes)}
		dur := windowMin + rng.Uint64N(windowSpan)
		win.start = rng.Uint64N(pl.quiet - dur)
		win.end = win.start + dur
		if win.kind == bridge && nodes == 1 {
			win.kind = isolate
		}
		switch win.kind {
		case split, oneway:
			win.side = make([]bool, hosts)
			for i := range win.side {
				win.side[i] = rng.IntN(2) == 0
			}
			win.side[win.node-1] = true
			if nodes > 1 {
				win.side[(win.node+rng.Uint64N(nodes-1))%nodes] = false
			}
		case isolate:
			win.side = make([]bool, hosts)
			win.side[win.node-1] = true
		case bridge:
			win.peer = 1 + (win.node+rng.Uint64N(nodes-1))%nodes
		case eio:
			win.rate = float64(10+rng.IntN(201)) / 10000
		case jump:
			win.shift = millisecond + rng.Uint64N(jumpMax)
			win.back = rng.IntN(2) == 0
			win.keep = rng.IntN(2) == 0
		case pause:
			win.lead = rng.IntN(2) == 0
			if rng.IntN(2) == 0 {
				win.step = crawlMin + rng.Uint64N(crawlSpan)
			}
		case leadercrash:
			win.when = moment(rng.IntN(3)) //nolint:gosec // G115: 0 to 2
		case rot:
			win.down = rng.IntN(2) == 0
			win.sector = rng.IntN(4) == 0
			win.meta = win.down && rng.IntN(3) == 0
		}
		pl.windows = append(pl.windows, win)
	}
	return pl, nil
}
