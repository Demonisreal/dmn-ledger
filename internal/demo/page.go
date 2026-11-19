package demo

import (
	"bytes"
	"fmt"
	"html/template"
	"net/http"
	"slices"
	"sync"
	"time"
)

const (
	renderEvery = time.Second
	catchingUp  = 100
)

var repo = ""

var pageTemplate = template.Must(template.New("page.html").Funcs(template.FuncMap{
	"args":    args,
	"num":     func(n any) string { return grouped(fmt.Sprint(n)) },
	"money":   money,
	"iso":     iso,
	"clock":   clock,
	"clockms": func(ms int64) string { return time.UnixMilli(ms).UTC().Format("15:04:05.000") },
	"day":     day,
	"left":    left,
	"sub":     func(a, b int64) int64 { return a - b },
	"behind":  func(applied, commit uint64) bool { return applied+catchingUp < commit },
	"tenths":  func(ms int64) string { return secs(time.Duration(ms) * time.Millisecond) },
	"plural":  plural,
	"locked":  locked,
	"accord":  accord,
	"commit":  leaderCommit,
	"severed": severed,
	"lately":  lately,
	"total":   func(a Actions) int { return a.Kill + a.Freeze + a.Cut + a.Flip },
	"latest":  latest,
	"repo":    func() string { return repo },
}).ParseFS(webFS, "web/page.html"))

type page struct {
	mu   sync.Mutex
	at   time.Time
	html []byte
}

func (p *page) render(show Show) ([]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if time.Since(p.at) < renderEvery {
		return p.html, nil
	}
	st := show.State()
	st.Lines = tail(show.Lines(0), logTail)
	var buf bytes.Buffer
	if err := pageTemplate.Execute(&buf, st); err != nil {
		return nil, err
	}
	p.html, p.at = buf.Bytes(), time.Now()
	return p.html, nil
}

func (web *Web) home(w http.ResponseWriter, _ *http.Request) {
	html, err := web.page.render(web.show)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(html)
}

func args(kv ...any) map[string]any {
	m := make(map[string]any, len(kv)/2)
	for i := 0; i+1 < len(kv); i += 2 {
		m[fmt.Sprint(kv[i])] = kv[i+1]
	}
	return m
}

func iso(at any) string {
	if ms, ok := at.(int64); ok {
		return time.UnixMilli(ms).UTC().Format("2006-01-02T15:04:05.000Z")
	}
	return fmt.Sprint(at)
}

func clock(at any) string {
	if ms, ok := at.(int64); ok {
		return time.UnixMilli(ms).UTC().Format("15:04:05")
	}
	return fmt.Sprint(at)
}

func day(at any) string {
	if ms, ok := at.(int64); ok {
		return time.UnixMilli(ms).UTC().Format("02.01.2006")
	}
	return fmt.Sprint(at)
}

func left(now, until int64) int64 {
	return max(0, (until-now+999)/1000)
}

func plural(n int, one, many string) string {
	if n == 1 {
		return one
	}
	return many
}

func locked(st State) bool {
	return st.Phase != "steady" || st.Next > st.Now
}

type agreement struct {
	Index   uint64
	Hash    string
	Pair    []int
	Odd     int
	Other   *Node
	Differ  bool
	Leaders int
	OneTerm bool
}

func accord(nodes [3]Node) agreement {
	var ag agreement
	at := map[uint64][]int{}
	var led []uint64
	for _, n := range nodes {
		if !n.Answers {
			continue
		}
		at[n.Applied] = append(at[n.Applied], n.ID)
		if n.Role == "leader" {
			ag.Leaders++
			ag.OneTerm = ag.OneTerm || slices.Contains(led, n.Term)
			led = append(led, n.Term)
		}
	}
	for index, ids := range at {
		if len(ids) >= 2 && (ag.Pair == nil || index > ag.Index) {
			ag.Index, ag.Pair = index, ids
		}
	}
	if ag.Pair == nil {
		return ag
	}
	ag.Hash = nodes[ag.Pair[0]-1].Apply
	for _, id := range ag.Pair[1:] {
		if nodes[id-1].Apply != ag.Hash {
			ag.Differ = true
		}
	}
	for i := range nodes {
		if !slices.Contains(ag.Pair, nodes[i].ID) {
			ag.Odd, ag.Other = nodes[i].ID, &nodes[i]
		}
	}
	return ag
}

func leaderCommit(nodes [3]Node) uint64 {
	var term, commit uint64
	for _, n := range nodes {
		if n.Answers && n.Role == "leader" && n.Term >= term {
			term, commit = n.Term, n.Commit
		}
	}
	return commit
}

func severed(links [6]Link, a, b int) bool {
	for _, l := range links {
		if l.Cut && (l.From == a && l.To == b || l.From == b && l.To == a) {
			return true
		}
	}
	return false
}

func lately(lines []Line) []Line {
	rev := slices.Clone(lines)
	slices.Reverse(rev)
	return rev
}

func latest(fs []Finding) *Finding {
	if len(fs) == 0 {
		return nil
	}
	return &fs[len(fs)-1]
}
