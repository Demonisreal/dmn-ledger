//go:build chaos && linux

package chaos

import (
	"bytes"
	"cmp"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/Demonisreal/dmn-ledger/internal/check"
	"github.com/Demonisreal/dmn-ledger/internal/faultnet"
	"github.com/Demonisreal/dmn-ledger/internal/sim"
	"github.com/Demonisreal/dmn-ledger/ledger"
)

const (
	answerWithin = 15 * time.Second
	agreeWithin  = 10 * time.Second
	probeFor     = 200 * time.Millisecond
	fillFor      = 60 * time.Second
	awayFor      = 90 * time.Second
	stampLayout  = "2006/01/02 15:04:05.000000"
)

func TestChaos(t *testing.T) {
	seed := pickSeed()
	h := newHarness(t, seed, "")
	f := h.run(*duration)
	t.Log(h.summary())
	if f != nil {
		t.Fatalf("seed %d: %s: %s\nevidence in %s", seed, f.check, f.reason, h.keep())
	}
}

func TestReplayHistory(t *testing.T) {
	if *historyFlag == "" {
		t.Skip("needs -chaos.history")
	}
	f, err := os.Open(*historyFlag)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	dec := gob.NewDecoder(f)
	r := check.NewReplayer()
	for k := 0; ; k++ {
		var calls []sim.Call
		if err := dec.Decode(&calls); errors.Is(err, io.EOF) {
			t.Logf("%d windows replay without a violation", k)
			return
		} else if err != nil {
			t.Fatalf("window %d: %v", k, err)
		}
		if v, ok := errors.AsType[*check.Violation](r.Window(calls)); ok {
			t.Fatalf("window %d: %s: %s", k, v.Check, v.Reason)
		}
	}
}

func (h *harness) run(d time.Duration) *finding {
	rounds := max(1, int(d/round))
	h.plan = plan(h.seed, rounds)
	r := check.NewReplayer()
	h.wg.Go(func() { h.replay(r) })
	if h.connect() {
		for k, cl := range h.clients {
			rng := rand.New(rand.NewPCG(h.seed, 10+uint64(k))) //nolint:gosec // G115: four clients
			h.wg.Go(func() { h.drive(k+1, cl, rng) })
		}
		h.conduct(rounds, r)
	}
	<-h.checked
	return h.finding()
}

func (h *harness) connect() bool {
	var addrs []string
	for id := 1; id <= nodes; id++ {
		addrs = append(addrs, h.links[route{0, id}].Addr())
	}
	ctx, cancel := context.WithTimeout(h.ctx, 30*time.Second)
	defer cancel()
	for k := range clients {
		cl, err := ledger.Dial(ctx, ledger.Config{Cluster: clusterID, Nodes: addrs})
		if err != nil {
			h.fail("dial", "client %d: %v", k+1, err)
			return false
		}
		h.clients = append(h.clients, cl)
	}
	return true
}

func (h *harness) conduct(rounds int, r *check.Replayer) {
	var late time.Duration
	for k := 0; k < rounds-int(late/round); k++ {
		offset := time.Duration(k) * round
		var ws []*window
		for _, w := range h.plan {
			if w.start >= offset && w.start < offset+round {
				ws = append(ws, w)
			}
		}
		if i := slices.IndexFunc(ws, func(w *window) bool { return w.kind == away }); i >= 0 {
			if !h.leave(ws[i]) {
				return
			}
			late += ws[i].waited
		}
		base := time.Now()
		h.begun = append(h.begun, base)
		if !h.storm(base, offset, ws) || !h.calm(base) {
			return
		}
	}
	h.final(r)
}

func (h *harness) leave(w *window) bool {
	h.aim(w)
	began := time.Now()
	for !h.filled(w.hit) && time.Since(began) < fillFor {
		if !h.sleep(100 * time.Millisecond) {
			return false
		}
	}
	last := h.reach()
	h.strike(w)
	w.behind = max(last, h.reach())
	for !h.compacted(w.hit, w.behind) {
		if time.Since(began) > awayFor {
			w.waited, w.absent = time.Since(began), time.Since(w.on)
			return true
		}
		if !h.sleep(100 * time.Millisecond) {
			return false
		}
	}
	w.waited, w.absent, w.passed = time.Since(began), time.Since(w.on), true
	return true
}

func (h *harness) reach() uint64 {
	var last uint64
	for _, st := range h.statuses() {
		if st != nil {
			last = max(last, st.Last)
		}
	}
	return last
}

func (h *harness) filled(missing int) bool {
	lead := h.leader()
	for _, n := range h.nodes {
		if n.id == missing || lead != 0 && lead != missing && n.id != lead {
			continue
		}
		if _, tail := n.segments(); tail >= segmentSize*15/16 {
			return true
		}
	}
	return false
}

func (h *harness) compacted(missing int, index uint64) bool {
	lead := h.leader()
	if lead == 0 || lead == missing {
		return false
	}
	first, _ := h.nodes[lead-1].segments()
	return first > index+1
}

func (h *harness) storm(base time.Time, offset time.Duration, ws []*window) bool {
	type event struct {
		at    time.Duration
		w     *window
		begin bool
	}
	var events []event
	for _, w := range ws {
		if w.kind != away {
			events = append(events, event{w.start - offset, w, true})
		}
		events = append(events, event{w.end - offset, w, false})
	}
	slices.SortStableFunc(events, func(a, b event) int {
		if a.at != b.at || a.begin == b.begin {
			return cmp.Compare(a.at, b.at)
		}
		if b.begin {
			return -1
		}
		return 1
	})
	for _, ev := range events {
		if !h.sleep(time.Until(base.Add(ev.at))) {
			return false
		}
		if ev.begin {
			h.begin(ev.w)
		} else {
			h.lift(ev.w)
		}
	}
	return h.sleep(time.Until(base.Add(storm)))
}

func (h *harness) begin(w *window) {
	h.aim(w)
	h.strike(w)
}

func (h *harness) aim(w *window) {
	switch w.act() {
	case crash, freeze, cut:
		w.hit = w.node
		if w.leader {
			if id := h.leader(); id != 0 {
				w.hit = id
			}
		}
	}
}

func (h *harness) strike(w *window) {
	h.kinds[w.kind]++
	w.on = time.Now()
	switch w.act() {
	case crash:
		h.nodes[w.hit-1].kill()
	case freeze:
		h.nodes[w.hit-1].signal(syscall.SIGSTOP)
	case cut:
		w.routes = w.side.routes(w.hit)
	}
	h.shape(w, true)
}

func (h *harness) lift(w *window) {
	switch w.act() {
	case crash:
		if err := h.nodes[w.hit-1].start(); err != nil {
			h.fail("restart", "n%d: %v", w.hit, err)
		}
	case freeze:
		h.nodes[w.hit-1].signal(syscall.SIGCONT)
	}
	h.shape(w, false)
	w.off = time.Now()
}

func (h *harness) shape(w *window, on bool) {
	h.fmu.Lock()
	defer h.fmu.Unlock()
	if on {
		h.active = append(h.active, w)
	} else {
		h.active = slices.DeleteFunc(h.active, func(o *window) bool { return o == w })
	}
	for _, r := range w.routes {
		var f faultnet.Fault
		for _, o := range h.active {
			if slices.Contains(o.routes, r) {
				f = merge(f, o.fault)
			}
		}
		h.links[r].Set(f)
		h.sets[r]++
		if set := h.sets[r]; r.from == 0 {
			h.wg.Go(func() { h.probe(r, set, f) })
		}
	}
}

func merge(a, b faultnet.Fault) faultnet.Fault {
	return faultnet.Fault{
		Cut:       a.Cut || b.Cut,
		Blackhole: a.Blackhole || b.Blackhole,
		Stall:     a.Stall || b.Stall,
		Delay:     max(a.Delay, b.Delay),
		Jitter:    max(a.Jitter, b.Jitter),
		Loss:      max(a.Loss, b.Loss),
		Flip:      max(a.Flip, b.Flip),
	}
}

func (h *harness) probe(r route, set int, f faultnet.Fault) {
	wait := f.Delay
	if f.Cut || f.Blackhole || f.Stall {
		wait = probeFor
	}
	if wait == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(h.ctx, wait)
	defer cancel()
	began := time.Now()
	_, err := ledger.Status(ctx, clusterID, h.links[r].Addr())
	took := time.Since(began)
	h.fmu.Lock()
	same := h.sets[r] == set
	h.fmu.Unlock()
	if err == nil && took < wait && same {
		h.fail("fault", "%s answered a status request after %v under %+v", r, took, f)
	}
}

func (h *harness) calm(base time.Time) bool {
	h.fmu.Lock()
	for r, l := range h.links {
		l.Set(faultnet.Fault{})
		h.sets[r]++
	}
	h.active = nil
	h.fmu.Unlock()
	healed := h.since()
	for _, n := range h.nodes {
		if !n.running() {
			h.fail("L2", "n%d is down when the calm begins", n.id)
			return false
		}
	}
	if !h.answered(healed) || !h.idle(answerWithin) {
		return false
	}
	if _, ok := h.agree(); !ok {
		return false
	}
	h.resume()
	return h.sound() && h.sleep(time.Until(base.Add(round)))
}

func (h *harness) answered(healed uint64) bool {
	deadline := time.Now().Add(answerWithin)
	for {
		h.mu.Lock()
		late := -1
		for i, start := range h.open {
			if start < healed {
				late = h.first + i
			}
		}
		h.mu.Unlock()
		switch {
		case late < 0:
			return true
		case time.Now().After(deadline):
			h.fail("L1", "call %d is still open %v after the faults healed", late, answerWithin)
			return false
		case !h.sleep(20 * time.Millisecond):
			return false
		}
	}
}

func (h *harness) agree() ([]*ledger.NodeStatus, bool) {
	deadline := time.Now().Add(agreeWithin)
	for {
		sts := h.statuses()
		why := h.judge(sts)
		switch {
		case h.ctx.Err() != nil:
			return nil, false
		case why == "":
			return sts, true
		case time.Now().After(deadline):
			h.fail("L2", "the nodes disagree %v after the load stopped: %s", agreeWithin, why)
			return nil, false
		case !h.sleep(50 * time.Millisecond):
			return nil, false
		}
	}
}

func (h *harness) judge(sts []*ledger.NodeStatus) string {
	var rows []string
	leaders, agreed := 0, true
	for i, st := range sts {
		if st == nil {
			rows = append(rows, fmt.Sprintf("n%d does not answer", i+1))
			agreed = false
			continue
		}
		if st.Repair {
			h.fail("L4", "n%d is in repair in term %d", st.ID, st.Term)
		}
		for _, o := range sts[:i] {
			if o != nil && o.Applied == st.Applied && o.ApplyHash != st.ApplyHash {
				h.fail("S7", "n%d and n%d applied up to %d with hashes %x and %x", o.ID, st.ID, st.Applied, o.ApplyHash[:8], st.ApplyHash[:8])
			}
		}
		if st.Role == ledger.Leader {
			leaders++
		}
		first := sts[0]
		if first == nil || st.Commit != first.Commit || st.Applied != st.Commit || st.Applied != first.Applied {
			agreed = false
		}
		rows = append(rows, fmt.Sprintf("n%d %s term %d commit %d applied %d", st.ID, st.Role, st.Term, st.Commit, st.Applied))
	}
	if agreed && leaders == 1 {
		return ""
	}
	return fmt.Sprintf("%d leaders; %s", leaders, strings.Join(rows, "; "))
}

func (h *harness) sound() bool {
	for _, n := range h.nodes {
		b, _ := os.ReadFile(n.log)
		if bytes.Contains(b, []byte("repair flag set")) || bytes.Contains(b, []byte("repair yes")) {
			h.fail("L4", "n%d went into repair, its log ends with:\n%s", n.id, tail(n.log, 20))
			return false
		}
	}
	return true
}

func (h *harness) final(r *check.Replayer) {
	if !h.idle(answerWithin) {
		return
	}
	accts, ok := h.lookups(h.clients[0])
	h.mu.Lock()
	h.closed = true
	h.cond.Broadcast()
	h.mu.Unlock()
	<-h.checked
	if !ok || h.finding() != nil {
		return
	}
	if err := balance(accts); err != nil {
		h.fail("balance", "%v", err)
		return
	}
	sts, ok := h.agree()
	if !ok {
		return
	}
	for _, st := range sts {
		if last := r.Ledger().Last(); st.Time != last {
			h.fail("time", "n%d ends at ledger time %d, the oracle at %d", st.ID, st.Time, last)
			return
		}
	}
	if !h.prove(h.clients[0], sts) {
		return
	}
	h.sound()
}

func (h *harness) sleep(d time.Duration) bool {
	if d <= 0 {
		return h.ctx.Err() == nil
	}
	tm := time.NewTimer(d)
	defer tm.Stop()
	select {
	case <-tm.C:
		return true
	case <-h.ctx.Done():
		return false
	}
}

func (h *harness) summary() string {
	h.mu.Lock()
	calls, windows := h.first+len(h.calls), h.windows
	h.mu.Unlock()
	var faults []string
	for k, n := range h.kinds {
		faults = append(faults, fmt.Sprintf("%s %d", kind(k), n))
	}
	passed, waited := 0, time.Duration(0)
	for _, w := range h.plan {
		if w.passed {
			passed++
		}
		waited += w.waited
	}
	streams, received, dropped := h.snapshots()
	sent, under := 0, 0
	for _, st := range streams {
		if st.sent {
			sent++
		}
		if h.disturbed(st) {
			under++
		}
	}
	return fmt.Sprintf("seed %d: %d calls in %d windows over %d rounds; faults: %s; the leader compacted past the absent node in %d long outages, %v before the storms; "+
		"a node became leader %d times; snapshots begun %d, sent %d, received %d, dropped %d, %d of the begun ones under a fault; %d receipts checked",
		h.seed, calls, windows, len(h.begun), strings.Join(faults, ", "), passed, waited.Round(time.Second),
		h.logged(" leader in term "), len(streams), sent, received, dropped, under, h.receipts)
}

type stream struct {
	from, to     int
	began, ended time.Time
	sent         bool
}

func (h *harness) snapshots() (streams []stream, received, dropped int) {
	for _, n := range h.nodes {
		b, _ := os.ReadFile(n.log)
		open := map[int]int{}
		end := func(to int, at time.Time, sent bool) {
			if i, ok := open[to]; ok {
				streams[i].ended, streams[i].sent = at, sent
				delete(open, to)
			}
		}
		for line := range strings.Lines(string(b)) {
			head := min(len(line), len(stampLayout))
			at, err := time.Parse(stampLayout, line[:head])
			f := strings.Fields(line[head:])
			if err != nil || len(f) < 6 {
				continue
			}
			switch {
			case f[0] == "sending" && f[1] == "snapshot":
				to, _ := strconv.Atoi(f[5])
				open[to] = len(streams)
				streams = append(streams, stream{from: n.id, to: to, began: at, ended: at})
			case f[0] == "sent" && f[1] == "snapshot":
				to, _ := strconv.Atoi(strings.TrimSuffix(f[5], ","))
				end(to, at, true)
			case f[0] == "snapshot":
				to, _ := strconv.Atoi(f[4])
				end(to, at, false)
			case f[0] == "node" && len(f) > 7 && f[7] == "started:":
				for _, i := range open {
					streams[i].ended = at
				}
				clear(open)
			case f[0] == "received" && f[1] == "snapshot":
				received++
			case f[0] == "dropped" && f[1] == "snapshot":
				dropped++
			}
		}
	}
	return streams, received, dropped
}

func (h *harness) disturbed(st stream) bool {
	for _, w := range h.plan {
		if w.on.IsZero() || w.on.After(st.ended) || !w.off.IsZero() && w.off.Before(st.began) {
			continue
		}
		switch w.act() {
		case crash, freeze:
			if w.hit == st.from || w.hit == st.to {
				return true
			}
		default:
			if slices.Contains(w.routes, route{st.from, st.to}) {
				return true
			}
		}
	}
	return false
}
