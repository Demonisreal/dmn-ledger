package demo

import (
	"bytes"
	"html/template"
	"net/http"
	"sync"
	"time"
)

const renderEvery = time.Second

var pageTemplate = template.Must(template.ParseFS(webFS, "web/page.html"))

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
