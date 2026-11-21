package demo

import (
	"bytes"
	"context"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"log"
	"net"
	"net/http"
	"net/netip"
	"path"
	"strconv"
	"sync"
	"time"
)

const (
	maxConns   = 512
	writeLimit = 10 * time.Second
	closeLimit = 2 * time.Second
	proofHold  = time.Second
)

const policy = "default-src 'none'; script-src 'self'; style-src 'self'; img-src 'self'; font-src 'self'; connect-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'"

var guards = [...][2]string{
	{"Content-Security-Policy", policy},
	{"X-Content-Type-Options", "nosniff"},
	{"Referrer-Policy", "no-referrer"},
	{"Cross-Origin-Opener-Policy", "same-origin"},
	{"Cross-Origin-Resource-Policy", "same-origin"},
	{"X-Robots-Tag", "noindex"},
	{"Permissions-Policy", "camera=(), geolocation=(), microphone=()"},
	{"Cache-Control", "no-store"},
}

var targetOf = map[string]int{"1": 1, "2": 2, "3": 3, "leader": 0}

var assetTypes = map[string]string{
	".js":    "text/javascript; charset=utf-8",
	".css":   "text/css; charset=utf-8",
	".woff2": "font/woff2",
	".svg":   "image/svg+xml",
	".ico":   "image/x-icon",
	".png":   "image/png",
}

//go:embed web
var webFS embed.FS

type asset struct {
	body  []byte
	etag  string
	ctype string
}

var assets = loadAssets()

var missingPage = func() []byte {
	body, err := webFS.ReadFile("web/missing.html")
	if err != nil {
		panic(err)
	}
	return body
}()

func loadAssets() map[string]asset {
	files, err := fs.ReadDir(webFS, "web")
	if err != nil {
		panic(err)
	}
	loaded := map[string]asset{}
	for _, f := range files {
		ctype, ok := assetTypes[path.Ext(f.Name())]
		if !ok || f.IsDir() {
			continue
		}
		body, err := webFS.ReadFile("web/" + f.Name())
		if err != nil {
			panic(err)
		}
		sum := sha256.Sum256(body)
		loaded[f.Name()] = asset{body: body, etag: `"` + hex.EncodeToString(sum[:8]) + `"`, ctype: ctype}
	}
	return loaded
}

type Web struct {
	show    Show
	origin  string
	proxies []netip.Prefix
	mux     *http.ServeMux
	hub     *hub
	page    page
	proofs  proofs
}

type proofs struct {
	mu     sync.Mutex
	at     time.Time
	status int
	body   []byte
}

type accepted struct {
	Until int64 `json:"until"`
}

type refusal struct {
	Error string `json:"error"`
	Until int64  `json:"until,omitempty"`
}

func NewWeb(show Show, origin string, proxies []netip.Prefix) *Web {
	web := &Web{show: show, origin: origin, proxies: proxies, mux: http.NewServeMux(), hub: newHub(show)}
	web.mux.HandleFunc("GET /{$}", web.home)
	web.mux.HandleFunc("GET /events", web.events)
	web.mux.HandleFunc("POST /act/{kind}/{node}", web.act)
	web.mux.HandleFunc("GET /proof", web.proof)
	web.mux.HandleFunc("GET /static/{name}", web.static)
	web.mux.HandleFunc("GET /healthz", web.healthz)
	web.mux.HandleFunc("GET /robots.txt", robots)
	return web
}

func (web *Web) Serve(ctx context.Context, l net.Listener) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	srv := newServer(web)
	srv.BaseContext = func(net.Listener) context.Context { return ctx }
	var wg sync.WaitGroup
	wg.Go(func() { web.hub.run(ctx) })
	wg.Go(func() {
		<-ctx.Done()
		shut, stop := context.WithTimeout(context.WithoutCancel(ctx), closeLimit)
		defer stop()
		if srv.Shutdown(shut) != nil {
			_ = srv.Close()
		}
	})
	err := srv.Serve(capListener(l, maxConns))
	cancel()
	wg.Wait()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func newServer(h http.Handler) *http.Server {
	return &http.Server{
		Handler:           h,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    8 << 10,
		ErrorLog:          log.New(io.Discard, "", 0),
	}
}

func (web *Web) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h := w.Header()
	for _, g := range guards {
		h.Set(g[0], g[1])
	}
	_ = http.NewResponseController(w).SetWriteDeadline(time.Now().Add(writeLimit))
	if r.ContentLength != 0 {
		h.Set("Connection", "close")
		refuse(w, http.StatusBadRequest, "body", time.Time{})
		return
	}
	if (r.Method == http.MethodGet || r.Method == http.MethodHead) && web.unrouted(r) {
		h.Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write(missingPage)
		return
	}
	web.mux.ServeHTTP(w, r)
}

func (web *Web) unrouted(r *http.Request) bool {
	if _, pattern := web.mux.Handler(r); pattern != "" {
		return false
	}
	post := r.Clone(r.Context())
	post.Method = http.MethodPost
	_, pattern := web.mux.Handler(post)
	return pattern == ""
}

func refuse(w http.ResponseWriter, status int, reason string, until time.Time) {
	why := refusal{Error: reason}
	if !until.IsZero() {
		why.Until = until.UnixMilli()
	}
	body, _ := json.Marshal(why)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

func (web *Web) act(w http.ResponseWriter, r *http.Request) {
	if !sameOrigin(r, web.origin) {
		refuse(w, http.StatusForbidden, "origin", time.Time{})
		return
	}
	var kind Kind
	known := kind.UnmarshalText([]byte(r.PathValue("kind"))) == nil
	target, aimed := targetOf[r.PathValue("node")]
	if !known || !aimed {
		http.NotFound(w, r)
		return
	}
	until, err := web.show.Act(kind, target, visitor(r, web.proxies))
	switch {
	case err == nil:
		body, _ := json.Marshal(accepted{Until: until.UnixMilli()})
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write(body)
	case errors.Is(err, ErrBusy):
		refuse(w, http.StatusConflict, "busy", time.Time{})
	case errors.Is(err, ErrYoung):
		refuse(w, http.StatusConflict, "young", time.Time{})
	case errors.Is(err, ErrCooling):
		w.Header().Set("Retry-After", retryAfter(until))
		refuse(w, http.StatusTooManyRequests, "cooling", until)
	case errors.Is(err, ErrLimited):
		w.Header().Set("Retry-After", retryAfter(until))
		refuse(w, http.StatusTooManyRequests, "limited", until)
	case errors.Is(err, ErrRecovering):
		w.Header().Set("Retry-After", "2")
		refuse(w, http.StatusServiceUnavailable, "recovering", time.Time{})
	default:
		w.WriteHeader(http.StatusInternalServerError)
	}
}

func retryAfter(until time.Time) string {
	secs := (time.Until(until) + time.Second - 1) / time.Second
	return strconv.FormatInt(int64(max(secs, 1)), 10)
}

func (web *Web) proof(w http.ResponseWriter, r *http.Request) {
	if foreign(r, web.origin) {
		refuse(w, http.StatusForbidden, "origin", time.Time{})
		return
	}
	p := &web.proofs
	p.mu.Lock()
	if time.Since(p.at) >= proofHold {
		p.status, p.body = web.prove(context.WithoutCancel(r.Context()))
		p.at = time.Now()
	}
	status, body := p.status, p.body
	p.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

func (web *Web) prove(ctx context.Context) (int, []byte) {
	receipt, err := web.show.Proof(ctx)
	var reason string
	switch {
	case err == nil:
		body, _ := json.Marshal(receipt)
		return http.StatusOK, body
	case errors.Is(err, ErrNoLeader):
		reason = "noleader"
	case errors.Is(err, ErrEmpty):
		reason = "empty"
	case errors.Is(err, ErrFinding):
		reason = "finding"
	default:
		return http.StatusInternalServerError, nil
	}
	body, _ := json.Marshal(refusal{Error: reason})
	return http.StatusServiceUnavailable, body
}

func (web *Web) static(w http.ResponseWriter, r *http.Request) {
	a, ok := assets[r.PathValue("name")]
	if !ok {
		http.NotFound(w, r)
		return
	}
	h := w.Header()
	h.Set("Cache-Control", "no-cache")
	h.Set("ETag", a.etag)
	h.Set("Content-Type", a.ctype)
	http.ServeContent(w, r, "", time.Time{}, bytes.NewReader(a.body))
}

func (web *Web) healthz(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	if !web.show.Alive() {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	_, _ = w.Write([]byte("ok"))
}

func robots(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = w.Write([]byte("User-agent: *\nDisallow:\n"))
}

type capped struct {
	net.Listener
	slots chan struct{}
	done  chan struct{}
	once  sync.Once
}

type slotConn struct {
	net.Conn
	free func()
}

func capListener(l net.Listener, n int) *capped {
	return &capped{Listener: l, slots: make(chan struct{}, n), done: make(chan struct{})}
}

func (l *capped) Accept() (net.Conn, error) {
	select {
	case l.slots <- struct{}{}:
	case <-l.done:
		return nil, net.ErrClosed
	}
	conn, err := l.Listener.Accept()
	if err != nil {
		<-l.slots
		return nil, err
	}
	return &slotConn{Conn: conn, free: sync.OnceFunc(func() { <-l.slots })}, nil
}

func (l *capped) Close() error {
	l.once.Do(func() { close(l.done) })
	return l.Listener.Close()
}

func (c *slotConn) Close() error {
	c.free()
	return c.Conn.Close()
}
