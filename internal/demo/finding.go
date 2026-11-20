//go:build linux

package demo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/Demonisreal/dmn-ledger/internal/sim"
)

const (
	reasonMax   = 300
	findingsMax = 20
	evidenceMax = 3
)

func (s *Stage) fail(r *run, check, reason string) {
	now := time.Now()
	s.mu.Lock()
	if r.found != nil || r.broke != nil || r.ctx.Err() != nil && !r.ready {
		s.mu.Unlock()
		return
	}
	if !r.ready {
		r.broke = errors.New(reason)
		s.mu.Unlock()
		r.cancel()
		return
	}
	f := s.record(r, now, check, reason)
	s.draw(r)
	s.mu.Unlock()
	s.cfg.Log.Printf("finding %s in cluster %d, all three nodes stopped", f.Check, r.id)
	for id := 1; id <= 3; id++ {
		r.c.Node(id).Halt()
	}
	r.cancel()
	r.stopJudge()
}

func (s *Stage) record(r *run, now time.Time, check, reason string) Finding {
	if len(reason) > reasonMax {
		reason = strings.ToValidUTF8(reason[:reasonMax], "")
	}
	f := Finding{At: now.UnixMilli(), Check: check, Reason: reason}
	r.found = &f
	r.frozen = [3]bool{true, true, true}
	s.st.Phase, s.st.Load = "finding", "held"
	s.st.Finding = &f
	s.st.Totals.Found++
	s.st.Totals.Findings = append(s.st.Totals.Findings, f)
	if len(s.st.Totals.Findings) > findingsMax {
		s.st.Totals.Findings = slices.Delete(s.st.Totals.Findings, 0, 1)
	}
	s.journal.add(now, "check", fmt.Sprintf(sayFinding, check, reason))
	s.st.Rev++
	return f
}

func (s *Stage) exited(r *run, id int, err error) {
	s.mu.Lock()
	tail := r.ears[id-1].tail(20)
	s.mu.Unlock()
	how := "Exit 0"
	if err != nil {
		how = err.Error()
	}
	room := reasonMax - len(fmt.Sprintf(failExited, id, how, ""))
	for len(tail) > room && strings.Contains(tail, "\n") {
		_, tail, _ = strings.Cut(tail, "\n")
	}
	s.fail(r, "F1", fmt.Sprintf(failExited, id, how, tail))
}

func (s *Stage) finding(r *run) *Finding {
	s.mu.Lock()
	defer s.mu.Unlock()
	return r.found
}

func (s *Stage) stand(ctx context.Context, r *run) {
	s.beat()
	name := s.keep(r)
	until := time.Now().Add(s.pace.stand)
	s.mu.Lock()
	r.found.Evidence = name
	r.found.ResetAt = until.UnixMilli()
	f := *r.found
	s.st.Finding = &f
	s.st.Totals.Findings[len(s.st.Totals.Findings)-1] = f
	s.st.Rev++
	s.mu.Unlock()
	if name != "" {
		s.say("check", fmt.Sprintf(sayEvidence, name))
	}
	sleep(ctx, time.Until(until))
}

func (s *Stage) keep(r *run) string {
	kept, _ := filepath.Glob(filepath.Join(s.cfg.Dir, "finding-*"))
	if len(kept) >= evidenceMax {
		return ""
	}
	st := s.State()
	s.mu.Lock()
	name := fmt.Sprintf("finding-%d-%d", time.Now().Unix(), s.st.Totals.Found)
	var logs [3]string
	for i := range r.ears {
		logs[i] = r.ears[i].tail(earSize)
	}
	lines := s.journal.after(0)
	var current []sim.Call
	for _, call := range r.calls {
		current = append(current, call)
	}
	var waiting [][]sim.Call
	for _, w := range r.sealed {
		waiting = append(waiting, w.calls)
	}
	s.mu.Unlock()
	dst := filepath.Join(s.cfg.Dir, name)
	if err := os.Mkdir(dst, 0o700); err != nil {
		return ""
	}
	var failed error
	for id := 1; id <= 3; id++ {
		node := r.c.Node(id)
		failed = errors.Join(failed,
			os.CopyFS(filepath.Join(dst, filepath.Base(node.Dir())), os.DirFS(node.Dir())),
			os.WriteFile(filepath.Join(dst, fmt.Sprintf("n%d.log", id)), []byte(logs[id-1]+"\n"), 0o600))
	}
	var journal strings.Builder
	for _, l := range lines {
		fmt.Fprintf(&journal, "%d %s %s %s\n", l.N, time.UnixMilli(l.At).UTC().Format(time.RFC3339Nano), l.Src, l.Text)
	}
	failed = errors.Join(failed, os.WriteFile(filepath.Join(dst, "journal.txt"), []byte(journal.String()), 0o600))
	if b, err := json.MarshalIndent(st, "", "  "); err == nil {
		failed = errors.Join(failed, os.WriteFile(filepath.Join(dst, "state.json"), b, 0o600))
	}
	if b, err := json.Marshal(struct {
		Waiting [][]sim.Call `json:"waiting"`
		Current []sim.Call   `json:"current"`
	}{waiting, current}); err == nil {
		failed = errors.Join(failed, os.WriteFile(filepath.Join(dst, "calls.json"), b, 0o600))
	}
	if failed != nil {
		return ""
	}
	return name
}
