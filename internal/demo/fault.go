//go:build linux

package demo

import (
	"context"
	"fmt"
	"syscall"
	"time"

	"github.com/Demonisreal/dmn-ledger/internal/faultnet"
	"github.com/Demonisreal/dmn-ledger/internal/local"
)

type fault struct {
	kind   Kind
	target int
	node   int
	pid    int
	began  time.Time
	struck time.Time
	lifted time.Time
	routes []local.Route
}

func (s *Stage) act(r *run, w wish) {
	f := s.aim(r, w)
	s.disturb(r, f)
	healed := false
	if r.ctx.Err() == nil {
		s.change(func(st *State) { st.Action.Phase = "settling" })
		healed = s.count(r, true, f.kind)
	}
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.st.Action
	a.Ended = now.UnixMilli()
	a.Phase = "failed"
	if healed {
		a.Phase = "done"
		s.st.Totals.Healed++
	}
	if s.st.Phase == "action" {
		s.st.Phase = "steady"
	}
	s.acting, s.ended = false, now
	s.st.Rev++
}

func (s *Stage) aim(r *run, w wish) *fault {
	f := &fault{kind: w.kind, target: w.target, node: w.target, began: time.Now()}
	if f.node == 0 {
		ctx, cancel := context.WithTimeout(r.ctx, sightWithin)
		f.node = r.c.Leader(ctx)
		cancel()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if f.node == 0 {
		f.node = w.leader
	}
	st := s.sight.sts[f.node-1]
	a := &Action{Kind: f.kind, Target: f.target, Node: f.node, Began: f.began.UnixMilli()}
	if st != nil {
		a.Role, a.Term = st.Role.String(), st.Term
	}
	s.st.Action = a
	if s.st.Phase == "steady" {
		s.st.Phase = "action"
	}
	switch f.kind {
	case Kill:
		s.st.Totals.Actions.Kill++
	case Freeze:
		s.st.Totals.Actions.Freeze++
	case Cut:
		s.st.Totals.Actions.Cut++
	case Flip:
		s.st.Totals.Actions.Flip++
	}
	s.st.Rev++
	return f
}

func (s *Stage) disturb(r *run, f *fault) {
	lift := s.strike(r, f)
	defer lift()
	s.hold(r, f)
}

func (s *Stage) strike(r *run, f *fault) func() {
	n := r.c.Node(f.node)
	f.pid = n.Pid()
	s.mu.Lock()
	was := standing(s.sight.sts[f.node-1])
	s.mu.Unlock()
	switch f.kind {
	case Freeze:
		n.Signal(syscall.SIGSTOP)
		s.struck(r, f, func() { r.frozen[f.node-1], r.asked[f.node-1] = true, time.Time{} })
		s.say("demo", fmt.Sprintf(sayFreeze, f.node, f.pid, was))
		return func() {
			n.Signal(syscall.SIGCONT)
			s.lifted(r, f, noteFreeze, func() { r.frozen[f.node-1] = false })
			s.say("demo", fmt.Sprintf(sayThawed, f.node, secs(f.lifted.Sub(f.struck))))
		}
	case Cut:
		f.routes = r.c.Around(f.node)
		for _, rt := range f.routes {
			r.c.Link(rt).Set(faultnet.Fault{Cut: true})
		}
		s.struck(r, f, func() {
			for _, rt := range f.routes {
				r.cut[rt] = true
			}
		})
		a, b := others(f.node)
		s.say("demo", fmt.Sprintf(sayCut, f.node, a, b, f.node))
		return func() {
			for _, rt := range f.routes {
				r.c.Link(rt).Set(faultnet.Fault{})
			}
			s.lifted(r, f, noteCut, func() { clear(r.cut) })
			s.say("demo", fmt.Sprintf(sayHealed, f.node, secs(f.lifted.Sub(f.struck))))
		}
	}
	n.Kill()
	s.struck(r, f, func() { r.pids[f.node-1] = 0 })
	s.say("demo", fmt.Sprintf(sayKill, f.node, f.pid, was))
	if f.kind == Flip {
		return func() { s.mend(r, f) }
	}
	return func() {
		err := n.Start()
		pid := n.Pid()
		s.lifted(r, f, noteKill, func() { r.pids[f.node-1], r.up[f.node-1] = pid, f.lifted })
		if err != nil {
			s.fail(r, "fault", fmt.Sprintf(failStart, f.node, err))
			return
		}
		s.say("demo", fmt.Sprintf(sayKillLifted, f.node, secs(f.lifted.Sub(f.struck)), pid))
	}
}

func (s *Stage) struck(r *run, f *fault, mark func()) {
	f.struck = time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	mark()
	a := s.st.Action
	a.Phase = "struck"
	a.LiftAt = f.struck.Add(s.pace.holds[f.kind]).UnixMilli()
	s.draw(r)
	s.st.Rev++
}

func (s *Stage) lifted(r *run, f *fault, note string, mark func()) {
	f.lifted = time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	mark()
	s.st.Action.Phase = "lifted"
	s.st.Action.LiftAt = f.lifted.UnixMilli()
	if note != "" {
		r.notes[f.node-1] = Note{At: f.struck.UnixMilli(), Text: fmt.Sprintf(note, secs(f.lifted.Sub(f.struck)))}
	}
	s.draw(r)
	s.st.Rev++
}

func others(node int) (int, int) {
	switch node {
	case 1:
		return 2, 3
	case 2:
		return 1, 3
	}
	return 1, 2
}

func (s *Stage) hold(r *run, f *fault) {
	if s.during != nil {
		s.during(f.kind)
	}
	if f.kind != Flip {
		sleep(r.ctx, s.pace.holds[f.kind])
	} else if r.ctx.Err() == nil {
		s.tip(r, f)
	}
}
