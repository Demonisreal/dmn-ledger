package report

import (
	"bufio"
	"bytes"
	"cmp"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/Demonisreal/dmn-ledger/bench/raw"
)

var ErrTraces = errors.New("report: not the output of pprof -traces")

const module = "github.com/Demonisreal/dmn-ledger/"

const (
	postChain   = "SHA-256 of the chain"
	postHash    = "SHA-256 of the apply hash"
	postApply   = "applying"
	postLog     = "log"
	postWire    = "wire and network"
	postRaft    = "raft"
	postNode    = "rest of the node"
	postRuntime = "runtime"
	postKernel  = "kernel"
	postRest    = "rest"
)

var posts = []string{postChain, postHash, postApply, postLog, postWire, postRaft, postNode, postRuntime, postKernel, postRest}

var (
	sha256Frames = []string{"crypto/sha256.", "crypto/internal/fips140/sha256."}
	chainFrames  = []string{module + "internal/state.(*Machine).seal", module + "internal/audit."}
	hashFrames   = []string{module + "internal/replica.(*Node).apply"}
	applyFrames  = []string{module + "internal/state."}
	logFrames    = []string{module + "internal/wal.", "hash/crc32."}
	wireFrames   = []string{
		module + "internal/wire.", module + "internal/client.", "net.",
		module + "internal/server.(*frameReader).", module + "internal/server.(*clientConn).write",
		module + "internal/server.(*Server).acceptClients", module + "internal/server.(*Server).serveClient",
		module + "internal/server.(*Server).readClient", module + "internal/server.(*Server).acceptPeers",
		module + "internal/server.(*Server).servePeer", module + "internal/server.(*Server).readPeer",
		module + "internal/server.(*Server).writePeer", module + "internal/server.(*Server).dial",
		module + "internal/server.(*Server).greet", module + "internal/server.(*Server).hello",
		module + "internal/server.(*Server).welcome", module + "internal/server.(*link).",
		module + "internal/server.closeOnStop",
	}
	raftFrames    = []string{module + "internal/raft."}
	nodeFrames    = []string{module + "internal/replica.", module + "internal/server.", module + "internal/notary."}
	syscallFrames = []string{"syscall.", "internal/runtime/syscall/", "runtime/internal/syscall.", "runtime.futex"}
	runtimeFrames = []string{"runtime.", "internal/runtime/"}
)

func is(frame string, names []string) bool {
	for _, n := range names {
		if strings.HasSuffix(n, ".") || strings.HasSuffix(n, "/") {
			if strings.HasPrefix(frame, n) {
				return true
			}
		} else if frame == n || strings.HasPrefix(frame, n+".") {
			return true
		}
	}
	return false
}

func above(stack []string, i int, names []string) bool {
	return slices.ContainsFunc(stack[i+1:], func(f string) bool { return is(f, names) })
}

func postOf(stack []string) string {
	for i, f := range stack {
		switch {
		case is(f, sha256Frames) && above(stack, i, chainFrames):
			return postChain
		case is(f, sha256Frames) && above(stack, i, hashFrames):
			return postHash
		case is(f, applyFrames):
			return postApply
		case is(f, logFrames):
			return postLog
		case is(f, wireFrames):
			return postWire
		case is(f, raftFrames):
			return postRaft
		case is(f, nodeFrames):
			return postNode
		}
	}
	switch {
	case above(stack, -1, syscallFrames):
		return postKernel
	case above(stack, -1, runtimeFrames):
		return postRuntime
	}
	return postRest
}

type shares struct {
	total time.Duration
	by    map[string]time.Duration
}

func countTraces(text []byte) (shares, error) {
	s := shares{by: make(map[string]time.Duration)}
	var stack []string
	var weight time.Duration
	flush := func() {
		if len(stack) > 0 {
			s.by[postOf(stack)] += weight
			s.total += weight
		}
		stack = stack[:0]
	}
	started := false
	sc := bufio.NewScanner(bytes.NewReader(text))
	sc.Buffer(nil, 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if strings.HasPrefix(line, "-----------+") {
			flush()
			started = true
			continue
		}
		if !started || strings.TrimSpace(line) == "" {
			continue
		}
		trimmed := strings.TrimLeft(line, " ")
		if len(line)-len(trimmed) >= 13 {
			stack = append(stack, strings.TrimSuffix(trimmed, " (inline)"))
			continue
		}
		v, frame, ok := strings.Cut(trimmed, "   ")
		if !ok && strings.Contains(v, ":") {
			continue
		}
		d, err := time.ParseDuration(v)
		if !ok || err != nil {
			return shares{}, fmt.Errorf("%w: %q", ErrTraces, line)
		}
		flush()
		weight = d
		stack = append(stack, strings.TrimSuffix(frame, " (inline)"))
	}
	if err := sc.Err(); err != nil {
		return shares{}, err
	}
	flush()
	if !started {
		return shares{}, ErrTraces
	}
	return s, nil
}

func (s shares) of(post string) float64 {
	if s.total == 0 {
		return 0
	}
	return float64(s.by[post]) / float64(s.total)
}

func (s shares) top(n int) []string {
	ranked := slices.Clone(posts)
	slices.SortStableFunc(ranked, func(a, b string) int { return cmp.Compare(s.by[b], s.by[a]) })
	var out []string
	for _, p := range ranked[:n] {
		if s.by[p] > 0 {
			out = append(out, p)
		}
	}
	return out
}

const (
	loopLog     = "log fsync"
	loopMeta    = "meta fsync"
	loopOther   = "other fsync"
	loopCompute = "computing in Process"
	loopIO      = "reading, sending, answering"
	loopWait    = "waiting for work"
)

var loopPosts = []string{loopLog, loopMeta, loopOther, loopCompute, loopIO, loopWait}

var loopCounters = []string{"up", "wait", "process", "logsync_ns", "metasync_ns", "othersync_ns"}

type loop map[string]uint64

func (l loop) shares() map[string]float64 {
	up := float64(l["up"])
	if up == 0 {
		return nil
	}
	syncs := l["logsync_ns"] + l["metasync_ns"] + l["othersync_ns"]
	busy := l["process"] + l["wait"]
	return map[string]float64{
		loopLog:     float64(l["logsync_ns"]) / up,
		loopMeta:    float64(l["metasync_ns"]) / up,
		loopOther:   float64(l["othersync_ns"]) / up,
		loopCompute: (float64(l["process"]) - float64(syncs)) / up,
		loopIO:      (up - float64(busy)) / up,
		loopWait:    float64(l["wait"]) / up,
	}
}

func loopsOf(f *raw.File, speed func(int) bool) map[string]loop {
	roles := make(map[string]loop)
	prev := f.Before
	for i, s := range f.Seconds {
		if speed(i) {
			for _, p := range s.Parts {
				j := slices.IndexFunc(prev, func(q raw.Part) bool { return q.Name == p.Name })
				if j < 0 || !hasLoop(p) || !hasLoop(prev[j]) {
					continue
				}
				l := roles[p.Role]
				if l == nil {
					l = make(loop)
					roles[p.Role] = l
				}
				for _, k := range loopCounters {
					l[k] += p.Counters[k] - prev[j].Counters[k]
				}
			}
		}
		prev = s.Parts
	}
	return roles
}

func hasLoop(p raw.Part) bool {
	for _, k := range loopCounters {
		if _, ok := p.Counters[k]; !ok {
			return false
		}
	}
	return true
}
