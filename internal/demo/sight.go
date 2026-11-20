//go:build linux

package demo

import (
	"context"
	"encoding/hex"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Demonisreal/dmn-ledger/internal/wal"
	"github.com/Demonisreal/dmn-ledger/ledger"
)

const (
	sightEvery  = 100 * time.Millisecond
	sightWithin = 300 * time.Millisecond
	frozenEvery = time.Second
	sizeEvery   = time.Second
)

func (s *Stage) watch(ctx context.Context) {
	tick := time.NewTicker(sightEvery)
	defer tick.Stop()
	var sized time.Time
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		measure := time.Since(sized) >= sizeEvery
		if measure {
			sized = time.Now()
		}
		s.look(ctx, measure)
	}
}

func (s *Stage) look(ctx context.Context, measure bool) {
	s.mu.Lock()
	r := s.cur
	var skip, frozen [3]bool
	if r != nil {
		frozen = r.frozen
		for i := range skip {
			skip[i] = frozen[i] && time.Since(r.asked[i]) < frozenEvery
		}
	}
	s.mu.Unlock()
	if r == nil {
		s.mu.Lock()
		s.beats.sight = time.Now()
		s.forget(s.beats.sight)
		s.mu.Unlock()
		return
	}
	sctx, cancel := context.WithTimeout(ctx, sightWithin)
	sts := poll(sctx, r, skip)
	cancel()
	var sizes [3]int64
	var pids [3]int
	for id := 1; id <= 3; id++ {
		pids[id-1] = r.c.Node(id).Pid()
		if measure {
			_, sizes[id-1] = newest(r.c.Node(id).Dir())
		}
	}
	calls, windows := r.rec.Judged()
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.beats.sight = now
	s.forget(now)
	if s.cur != r {
		return
	}
	s.sight.at = now
	s.sight.leader = 0
	for i, st := range sts {
		if !skip[i] && (frozen[i] || !r.frozen[i]) {
			s.sight.sts[i], r.asked[i] = st, now
		}
		if st := s.sight.sts[i]; st != nil && st.Role == ledger.Leader {
			s.sight.leader = i + 1
		}
		s.sight.sizes[i] = max(s.sight.sizes[i], sizes[i])
	}
	r.pids = pids
	for id := 1; id <= 3; id++ {
		s.flushEar(r, id, now)
	}
	for ; r.ruled < windows && len(r.sealed) > 0; r.ruled++ {
		r.upto = max(r.upto, r.sealed[0].upto)
		r.sealed = r.sealed[1:]
	}
	if windows != s.st.Books.Judged.Windows {
		s.st.Books.Judged = Judged{Windows: windows, Calls: calls, Upto: strconv.FormatUint(r.upto, 10), At: now.UnixMilli()}
		s.st.Totals.Windows = s.earlier + windows
		s.st.Rev++
	}
	s.draw(r)
}

func poll(ctx context.Context, r *run, skip [3]bool) [3]*ledger.NodeStatus {
	var sts [3]*ledger.NodeStatus
	var wg sync.WaitGroup
	for i, addr := range r.c.Addrs() {
		if skip[i] {
			continue
		}
		wg.Go(func() {
			if st, err := ledger.Status(ctx, r.id, addr); err == nil {
				sts[i] = &st
			}
		})
	}
	wg.Wait()
	return sts
}

func (s *Stage) draw(r *run) {
	var nodes [3]Node
	for i, st := range s.sight.sts {
		n := Node{ID: i + 1, Pid: r.pids[i], Proc: "up", Note: r.notes[i]}
		switch {
		case n.Pid == 0:
			n.Proc = "down"
		case r.frozen[i]:
			n.Proc = "frozen"
		}
		if n.Pid != 0 {
			n.Since = r.up[i].UnixMilli()
		}
		if st != nil {
			n.Answers = true
			n.Role = st.Role.String()
			n.Term, n.Leader, n.Commit, n.Applied, n.Last = st.Term, st.Leader, st.Commit, st.Applied, st.Last
			n.Apply, n.HeadHash, n.Head = hex.EncodeToString(st.ApplyHash[:8]), hex.EncodeToString(st.Head[:8]), st.HeadSeq
			n.Repair, n.Key = st.Repair, hex.EncodeToString(st.Key)
		}
		nodes[i] = n
	}
	var links [6]Link
	for i, rt := range r.c.Routes() {
		links[i] = Link{From: rt.From, To: rt.To, Cut: r.cut[rt]}
	}
	if nodes != s.st.Nodes || links != s.st.Links {
		s.st.Nodes, s.st.Links = nodes, links
		s.st.Rev++
	}
}

func newest(dir string) (string, int64) {
	segs, _ := os.ReadDir(filepath.Join(dir, "wal"))
	var name string
	var size int64
	for _, seg := range segs {
		seq, ok := strings.CutPrefix(seg.Name(), wal.SegmentPrefix)
		seq, done := strings.CutSuffix(seq, ".log")
		if _, err := strconv.ParseUint(seq, 10, 64); !ok || !done || err != nil {
			continue
		}
		if fi, err := seg.Info(); err == nil {
			name, size = filepath.Join(dir, "wal", seg.Name()), fi.Size()
		}
	}
	return name, size
}
