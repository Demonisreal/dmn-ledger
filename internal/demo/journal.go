//go:build linux

package demo

import (
	"bytes"
	"fmt"
	"strings"
	"time"
)

const (
	journalSize = 200
	earSize     = 200
	lineMax     = 400
	burstMax    = 20
	stampLayout = "2006/01/02 15:04:05.000000"
)

type journal struct {
	lines []Line
	next  uint64
}

func (j *journal) add(at time.Time, src, text string) {
	j.next++
	l := Line{N: j.next, At: at.UnixMilli(), Src: src, Text: text}
	if len(j.lines) < journalSize {
		j.lines = append(j.lines, l)
		return
	}
	copy(j.lines, j.lines[1:])
	j.lines[len(j.lines)-1] = l
}

func (j *journal) after(n uint64) []Line {
	var ls []Line
	for _, l := range j.lines {
		if l.N > n {
			ls = append(ls, l)
		}
	}
	return ls
}

type heard struct {
	at   time.Time
	text string
}

type ear struct {
	lines []heard
	total int

	since   time.Time
	shown   int
	skipped int
}

func (e *ear) add(h heard) {
	e.total++
	if len(e.lines) < earSize {
		e.lines = append(e.lines, h)
		return
	}
	copy(e.lines, e.lines[1:])
	e.lines[len(e.lines)-1] = h
}

func (e *ear) after(mark int) []heard {
	return e.lines[max(0, len(e.lines)-(e.total-mark)):]
}

func (e *ear) tail(n int) string {
	var b strings.Builder
	for _, h := range e.lines[max(0, len(e.lines)-n):] {
		b.WriteString(h.text)
		b.WriteByte('\n')
	}
	return strings.TrimSuffix(b.String(), "\n")
}

type tap struct {
	s    *Stage
	r    *run
	id   int
	rest []byte
}

func (t *tap) Write(b []byte) (int, error) {
	t.rest = append(t.rest, b...)
	for {
		line, rest, ok := bytes.Cut(t.rest, []byte{'\n'})
		if !ok {
			break
		}
		t.s.hear(t.r, t.id, string(line))
		t.rest = rest
	}
	if len(t.rest) > lineMax {
		t.rest = t.rest[:lineMax]
	}
	return len(b), nil
}

func (s *Stage) hear(r *run, id int, line string) {
	if strings.Contains(line, " stats ") {
		return
	}
	if len(line) > lineMax {
		line = strings.ToValidUTF8(line[:lineMax], "")
	}
	now := time.Now()
	s.mu.Lock()
	e := &r.ears[id-1]
	e.add(heard{now, line})
	s.flushEar(r, id, now)
	e.shown++
	if e.shown <= burstMax {
		s.journal.add(now, fmt.Sprintf("n%d", id), line)
	} else {
		e.skipped++
	}
	close(r.fresh)
	r.fresh = make(chan struct{})
	flipping := r.flipping == id
	s.mu.Unlock()
	if !flipping && r.ctx.Err() == nil && (strings.Contains(line, " repair flag set ") || strings.Contains(line, " started: ") && strings.Contains(line, " repair yes,")) {
		s.fail(r, "L4", fmt.Sprintf(failRepairLine, id, line))
	}
}

func (s *Stage) flushEar(r *run, id int, now time.Time) {
	e := &r.ears[id-1]
	if now.Sub(e.since) < time.Second {
		return
	}
	if e.skipped > 0 {
		s.journal.add(now, "demo", fmt.Sprintf(saySkipped, e.skipped, id))
	}
	e.since, e.shown, e.skipped = now, 0, 0
}

func stamp(line string) (time.Time, bool) {
	if len(line) < len(stampLayout) {
		return time.Time{}, false
	}
	at, err := time.Parse(stampLayout, line[:len(stampLayout)])
	return at, err == nil
}
