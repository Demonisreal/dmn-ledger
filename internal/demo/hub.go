package demo

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/netip"
	"sync"
	"time"
)

const (
	beatEvery      = 250 * time.Millisecond
	tickAtLeast    = time.Second
	viewerBacklog  = 32
	maxStreams     = 200
	streamsPerAddr = 4
	streamLife     = 10 * time.Minute
	streamWrite    = 5 * time.Second
	logTail        = 60
)

var retryLine = []byte("retry: 3000\n\n")

type viewer struct {
	from   netip.Prefix
	events chan []byte
	fresh  bool
}

type hub struct {
	show    Show
	mu      sync.Mutex
	viewers map[*viewer]struct{}
	per     map[netip.Prefix]int
	cluster int
	rev     uint64
	ticked  time.Time
	last    uint64
}

func newHub(show Show) *hub {
	return &hub{show: show, viewers: map[*viewer]struct{}{}, per: map[netip.Prefix]int{}}
}

func (h *hub) join(from netip.Prefix) *viewer {
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.viewers) >= maxStreams || h.per[from] >= streamsPerAddr {
		return nil
	}
	v := &viewer{from: from, events: make(chan []byte, viewerBacklog), fresh: true}
	h.viewers[v] = struct{}{}
	h.per[from]++
	return v
}

func (h *hub) leave(v *viewer) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.remove(v)
}

func (h *hub) remove(v *viewer) {
	if _, ok := h.viewers[v]; !ok {
		return
	}
	delete(h.viewers, v)
	if h.per[v.from]--; h.per[v.from] == 0 {
		delete(h.per, v.from)
	}
}

func (h *hub) run(ctx context.Context) {
	beat := time.NewTicker(beatEvery)
	defer beat.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-beat.C:
			h.beat()
		}
	}
}

func (h *hub) beat() {
	st := h.show.State()
	lines := h.show.Lines(h.last)
	if len(lines) > 0 {
		h.last = lines[len(lines)-1].N
	}
	reset := st.Cluster.N != h.cluster
	h.cluster = st.Cluster.N
	var news [][]byte
	if reset {
		h.rev, h.ticked = st.Rev, time.Now()
	} else {
		if st.Rev != h.rev || time.Since(h.ticked) >= tickAtLeast {
			h.rev, h.ticked = st.Rev, time.Now()
			news = append(news, event("tick", st))
		}
		for _, l := range lines {
			news = append(news, event("line", l))
		}
	}
	var greeting []byte
	h.mu.Lock()
	for v := range h.viewers {
		sends := news
		if reset || v.fresh {
			if greeting == nil {
				st.Lines = tail(h.show.Lines(0), logTail)
				greeting = event("state", st)
			}
			sends, v.fresh = [][]byte{greeting}, false
		}
		if !offer(v, sends) {
			h.remove(v)
			close(v.events)
		}
	}
	n := len(h.viewers)
	h.mu.Unlock()
	h.show.Watching(n)
}

func offer(v *viewer, sends [][]byte) bool {
	for _, ev := range sends {
		select {
		case v.events <- ev:
		default:
			return false
		}
	}
	return true
}

func tail(lines []Line, n int) []Line {
	return lines[max(len(lines)-n, 0):]
}

func event(name string, v any) []byte {
	js, _ := json.Marshal(v)
	return fmt.Appendf(nil, "event: %s\ndata: %s\n\n", name, js)
}

func (web *Web) events(w http.ResponseWriter, r *http.Request) {
	if foreign(r, web.origin) {
		w.WriteHeader(http.StatusForbidden)
		return
	}
	v := web.hub.join(visitor(r, web.proxies))
	if v == nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	defer web.hub.leave(v)
	w.Header().Set("Content-Type", "text/event-stream")
	rc := http.NewResponseController(w)
	send := func(b []byte) bool {
		if rc.SetWriteDeadline(time.Now().Add(streamWrite)) != nil {
			return false
		}
		if _, err := w.Write(b); err != nil {
			return false
		}
		return rc.Flush() == nil
	}
	if !send(retryLine) {
		return
	}
	end := time.NewTimer(streamLife)
	defer end.Stop()
	for {
		select {
		case ev, ok := <-v.events:
			if !ok || !send(ev) {
				return
			}
		case <-end.C:
			return
		case <-r.Context().Done():
			return
		}
	}
}
