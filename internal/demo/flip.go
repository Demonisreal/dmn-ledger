//go:build linux

package demo

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"
)

const repairWithin = 10 * time.Second

func pick(rng *rand.Rand, size int64) (int64, int) {
	return size/4 + rng.Int64N(size/2-size/4), rng.IntN(8)
}

type tipped struct {
	size, offset  int64
	bit           int
	before, after byte
}

func (s *Stage) tip(r *run, f *fault) {
	path, _ := newest(r.c.Node(f.node).Dir())
	t, err := flipBit(path, r.rng)
	if err != nil {
		s.fail(r, "fault", fmt.Sprintf(failTip, f.node, err))
		return
	}
	rel, _ := filepath.Rel(filepath.Join(s.cfg.Dir, "run"), path)
	s.change(func(st *State) {
		a := st.Action
		a.File, a.Size, a.Offset, a.Bit, a.Before, a.After = rel, t.size, t.offset, t.bit, t.before, t.after
	})
	s.say("demo", fmt.Sprintf(sayTipped, rel, grouped(fmt.Sprint(t.offset)), grouped(fmt.Sprint(t.size)), t.bit, t.before, t.after))
}

func flipBit(path string, rng *rand.Rand) (tipped, error) {
	var t tipped
	file, err := os.OpenFile(path, os.O_RDWR, 0) //nolint:gosec // G304: the newest segment in the node's own directory
	if err != nil {
		return t, err
	}
	defer file.Close()
	fi, err := file.Stat()
	if err != nil {
		return t, err
	}
	t.size = fi.Size()
	t.offset, t.bit = pick(rng, t.size)
	b := make([]byte, 1)
	if _, err := file.ReadAt(b, t.offset); err != nil {
		return t, err
	}
	t.before, t.after = b[0], b[0]^1<<t.bit
	if _, err := file.WriteAt([]byte{t.after}, t.offset); err != nil {
		return t, err
	}
	if err := file.Sync(); err != nil {
		return t, err
	}
	return t, file.Close()
}

func (s *Stage) mend(r *run, f *fault) {
	n := r.c.Node(f.node)
	s.mu.Lock()
	mark := r.ears[f.node-1].total
	r.flipping = f.node
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		r.flipping = 0
		s.mu.Unlock()
	}()
	err := n.Start()
	pid := n.Pid()
	s.lifted(r, f, "", func() { r.pids[f.node-1], r.up[f.node-1] = pid, f.lifted })
	if err != nil {
		s.fail(r, "fault", fmt.Sprintf(failStart, f.node, err))
		return
	}
	s.say("demo", fmt.Sprintf(sayStarted, f.node, pid))
	repair, nonvoter, ok := s.observe(r, f.node, mark)
	if !ok {
		if r.ctx.Err() == nil {
			s.fail(r, "fault", fmt.Sprintf(failSilent, f.node, repairWithin))
		}
		return
	}
	ms := nonvoter.Round(time.Millisecond).Milliseconds()
	s.change(func(st *State) {
		st.Action.Repair, st.Action.NonVoter = repair, ms
		r.notes[f.node-1] = Note{At: f.struck.UnixMilli(), Text: noteTruncated}
		if repair == "yes" {
			r.notes[f.node-1].Text = fmt.Sprintf(noteFlip, ms)
		}
		s.draw(r)
	})
	if repair == "yes" {
		s.say("demo", fmt.Sprintf(sayRepaired, f.node, ms, f.node))
	} else {
		s.say("demo", fmt.Sprintf(sayTruncated, f.node, f.node))
	}
}

func (s *Stage) observe(r *run, id, mark int) (string, time.Duration, bool) {
	ctx, cancel := context.WithTimeout(r.ctx, repairWithin)
	defer cancel()
	var began time.Time
	for {
		s.mu.Lock()
		lines := slices.Clone(r.ears[id-1].after(mark))
		fresh := r.fresh
		s.mu.Unlock()
		for _, h := range lines {
			switch {
			case strings.Contains(h.text, " started: ") && strings.Contains(h.text, " repair no,"):
				return "no", 0, true
			case strings.Contains(h.text, " started: "):
				began, _ = stamp(h.text)
			case !began.IsZero() && strings.Contains(h.text, " repair flag cleared "):
				if at, ok := stamp(h.text); ok {
					return "yes", at.Sub(began), true
				}
			}
		}
		select {
		case <-fresh:
		case <-ctx.Done():
			return "", 0, false
		}
	}
}
