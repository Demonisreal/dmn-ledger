package demo

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"
)

var wantGuards = map[string]string{
	"Content-Security-Policy":      "default-src 'none'; script-src 'self'; style-src 'self'; img-src 'self'; font-src 'self'; connect-src 'self'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'",
	"X-Content-Type-Options":       "nosniff",
	"Referrer-Policy":              "no-referrer",
	"Cross-Origin-Opener-Policy":   "same-origin",
	"Cross-Origin-Resource-Policy": "same-origin",
	"X-Robots-Tag":                 "noindex",
	"Permissions-Policy":           "camera=(), geolocation=(), microphone=()",
}

type route struct {
	name    string
	method  string
	target  string
	headers []string
	setup   func(f *fakeShow)
	status  int
}

var routes = []route{
	{name: "page", method: "GET", target: "/", status: http.StatusOK},
	{name: "static", method: "GET", target: "/static/receipt.js", status: http.StatusOK},
	{name: "static unchanged", method: "GET", target: "/static/receipt.js", headers: []string{"If-None-Match", assets["receipt.js"].etag}, status: http.StatusNotModified},
	{name: "static template", method: "GET", target: "/static/page.html", status: http.StatusNotFound},
	{name: "proof", method: "GET", target: "/proof", status: http.StatusOK},
	{name: "proof without leader", method: "GET", target: "/proof", setup: func(f *fakeShow) { f.proofErr = ErrNoLeader }, status: http.StatusServiceUnavailable},
	{name: "proof from a foreign origin", method: "GET", target: "/proof", headers: []string{"Origin", "https://evil.example"}, status: http.StatusForbidden},
	{name: "events from a foreign origin", method: "GET", target: "/events", headers: []string{"Origin", "https://evil.example"}, status: http.StatusForbidden},
	{name: "healthz", method: "GET", target: "/healthz", status: http.StatusOK},
	{name: "healthz down", method: "GET", target: "/healthz", setup: func(f *fakeShow) { f.alive = false }, status: http.StatusServiceUnavailable},
	{name: "robots", method: "GET", target: "/robots.txt", status: http.StatusOK},
	{name: "unknown path", method: "GET", target: "/nothing", status: http.StatusNotFound},
	{name: "act", method: "POST", target: "/act/kill/1", headers: []string{"Origin", testOrigin}, status: http.StatusAccepted},
	{name: "act unknown", method: "POST", target: "/act/boom/1", headers: []string{"Origin", testOrigin}, status: http.StatusNotFound},
	{name: "act without origin", method: "POST", target: "/act/kill/1", status: http.StatusForbidden},
	{name: "act busy", method: "POST", target: "/act/kill/1", headers: []string{"Origin", testOrigin}, setup: func(f *fakeShow) { f.actErr = ErrBusy }, status: http.StatusConflict},
	{name: "act cooling", method: "POST", target: "/act/kill/1", headers: []string{"Origin", testOrigin}, setup: func(f *fakeShow) { f.actErr, f.until = ErrCooling, time.Now().Add(3*time.Second) }, status: http.StatusTooManyRequests},
	{name: "act recovering", method: "POST", target: "/act/kill/1", headers: []string{"Origin", testOrigin}, setup: func(f *fakeShow) { f.actErr = ErrRecovering }, status: http.StatusServiceUnavailable},
	{name: "wrong method", method: "POST", target: "/", status: http.StatusMethodNotAllowed},
}

func (rt route) hit() *httptest.ResponseRecorder {
	show := newFake()
	if rt.setup != nil {
		rt.setup(show)
	}
	return hit(NewWeb(show, testOrigin, nil), rt.method, rt.target, rt.headers...)
}

func TestSecurityHeaders(t *testing.T) {
	check := func(name string, h http.Header, static bool) {
		t.Helper()
		for key, want := range wantGuards {
			if got := h.Values(key); len(got) != 1 || got[0] != want {
				t.Errorf("%s: %s is %q, want %q", name, key, got, want)
			}
		}
		cache := "no-store"
		if static {
			cache = "no-cache"
		}
		if got := h.Get("Cache-Control"); got != cache {
			t.Errorf("%s: Cache-Control %q, want %q", name, got, cache)
		}
		for key := range h {
			if strings.HasPrefix(key, "Access-Control-") {
				t.Errorf("%s: CORS header %s", name, key)
			}
		}
	}
	for _, rt := range routes {
		w := rt.hit()
		if w.Code != rt.status {
			t.Errorf("%s: status %d, want %d", rt.name, w.Code, rt.status)
		}
		check(rt.name, w.Header(), strings.HasPrefix(rt.target, "/static/") && w.Code != http.StatusNotFound)
	}
	if etag := routes[1].hit().Header().Get("ETag"); etag != assets["receipt.js"].etag || len(etag) != 18 {
		t.Errorf("ETag %q", etag)
	}
	synctest.Test(t, func(t *testing.T) {
		l := serveWeb(t, newFake())
		var open *stream
		for i := range streamsPerAddr + 1 {
			open = l.watch(t, "203.0.113.9:"+strconv.Itoa(i+1))
			if i == 0 {
				check("events", open.res.Header, false)
			}
		}
		if open.res.StatusCode != http.StatusServiceUnavailable {
			t.Fatalf("fifth stream: status %d, want 503", open.res.StatusCode)
		}
		check("events refused", open.res.Header, false)
		res, _ := l.open(t, "203.0.113.9:9").do(t, request("POST", "/act/kill/1", "Content-Length", "1")+"x")
		check("body", res.Header, false)
	})
}

func TestNoCookies(t *testing.T) {
	for _, rt := range routes {
		if cookies := rt.hit().Header().Values("Set-Cookie"); len(cookies) > 0 {
			t.Errorf("%s sets %q", rt.name, cookies)
		}
	}
	storage := regexp.MustCompile(`document\.cookie|localStorage|sessionStorage|indexedDB|caches\.open`)
	for name, a := range assets {
		if found := storage.FindString(string(a.body)); found != "" {
			t.Errorf("%s uses %s", name, found)
		}
	}
	page := hit(NewWeb(newFake(), testOrigin, nil), "GET", "/").Body.String()
	if found := storage.FindString(page); found != "" {
		t.Errorf("the page uses %s", found)
	}
	if loads := regexp.MustCompile(`(?i)\s(src|href)\s*=\s*["']?(https?:)?//`).FindAllString(page, -1); len(loads) > 0 {
		t.Errorf("the page names another origin: %q", loads)
	}
}

func TestActAnswers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		until := time.Now().Add(2500 * time.Millisecond)
		for _, c := range []struct {
			err    error
			status int
			reason string
			retry  string
		}{
			{nil, http.StatusAccepted, "", ""},
			{ErrBusy, http.StatusConflict, "busy", ""},
			{ErrYoung, http.StatusConflict, "young", ""},
			{fmt.Errorf("gate: %w", ErrCooling), http.StatusTooManyRequests, "cooling", "3"},
			{ErrLimited, http.StatusTooManyRequests, "limited", "3"},
			{ErrRecovering, http.StatusServiceUnavailable, "recovering", "2"},
			{errors.New("something else"), http.StatusInternalServerError, "", ""},
		} {
			show := newFake()
			show.actErr, show.until = c.err, until
			w := hit(NewWeb(show, testOrigin, nil), "POST", "/act/flip/leader", "Origin", testOrigin)
			if w.Code != c.status {
				t.Errorf("%v: status %d, want %d", c.err, w.Code, c.status)
			}
			if got := w.Header().Get("Retry-After"); got != c.retry {
				t.Errorf("%v: Retry-After %q, want %q", c.err, got, c.retry)
			}
			if c.reason == "" {
				if w.Body.Len() != 0 {
					t.Errorf("%v: body %q, want none", c.err, w.Body)
				}
				continue
			}
			var why refusal
			if err := json.Unmarshal(w.Body.Bytes(), &why); err != nil || why.Error != c.reason {
				t.Errorf("%v: body %q, want the error %s", c.err, w.Body, c.reason)
			}
			wantUntil := int64(0)
			if c.retry == "3" {
				wantUntil = until.UnixMilli()
			}
			if why.Until != wantUntil {
				t.Errorf("%v: until %d, want %d", c.err, why.Until, wantUntil)
			}
		}
	})
}

func TestUnknownActionIs404(t *testing.T) {
	show := newFake()
	web := NewWeb(show, testOrigin, nil)
	for _, target := range []string{
		"/act/boom/1", "/act/KILL/1", "/act/Kill/1", "/act/kill%20/1", "/act/kill/0", "/act/kill/4", "/act/kill/01",
		"/act/kill/-1", "/act/kill/+1", "/act/kill/1.0", "/act/kill/Leader", "/act/kill/leader2", "/act/kill/1/2",
		"/act/kill", "/act", "/act/flip/leader/x",
	} {
		if w := hit(web, "POST", target, "Origin", testOrigin); w.Code != http.StatusNotFound {
			t.Errorf("POST %s: status %d, want 404", target, w.Code)
		}
	}
	if acts := show.calls(); len(acts) != 0 {
		t.Fatalf("Act was called for unknown actions: %v", acts)
	}
	for name, kind := range map[string]Kind{"kill": Kill, "freeze": Freeze, "cut": Cut, "flip": Flip} {
		for node, target := range map[string]int{"1": 1, "2": 2, "3": 3, "leader": 0} {
			show := newFake()
			if w := hit(NewWeb(show, testOrigin, nil), "POST", "/act/"+name+"/"+node+"?kind=kill&node=2", "Origin", testOrigin); w.Code != http.StatusAccepted {
				t.Errorf("POST /act/%s/%s: status %d, want 202", name, node, w.Code)
			}
			if acts := show.calls(); len(acts) != 1 || acts[0].kind != kind || acts[0].target != target {
				t.Errorf("POST /act/%s/%s: Act got %v, want %v on %d", name, node, acts, kind, target)
			}
		}
	}
}

func TestWrongMethodIs405(t *testing.T) {
	show := newFake()
	web := NewWeb(show, testOrigin, nil)
	for _, c := range []struct{ method, target string }{
		{"GET", "/act/kill/1"}, {"HEAD", "/act/kill/1"}, {"PUT", "/act/kill/1"}, {"DELETE", "/act/kill/1"}, {"PATCH", "/act/flip/leader"},
		{"POST", "/"}, {"POST", "/events"}, {"POST", "/proof"}, {"DELETE", "/proof"}, {"POST", "/healthz"},
		{"POST", "/robots.txt"}, {"POST", "/static/receipt.js"}, {"PUT", "/static/receipt.js"},
	} {
		w := hit(web, c.method, c.target, "Origin", testOrigin)
		if w.Code != http.StatusMethodNotAllowed || w.Header().Get("Allow") == "" {
			t.Errorf("%s %s: status %d, Allow %q; want 405 with Allow", c.method, c.target, w.Code, w.Header().Get("Allow"))
		}
	}
	if acts := show.calls(); len(acts) != 0 {
		t.Fatalf("Act was called: %v", acts)
	}
}

type watchedBody struct {
	read bool
}

func (b *watchedBody) Read([]byte) (int, error) {
	b.read = true
	return 0, io.EOF
}

func TestBodyIsRefused(t *testing.T) {
	show := newFake()
	web := NewWeb(show, testOrigin, nil)
	for _, c := range []struct {
		method, target string
		length         int64
	}{
		{"POST", "/act/kill/1", 5},
		{"POST", "/act/kill/1", -1},
		{"POST", "/act/boom/1", 5},
		{"GET", "/", 3},
		{"GET", "/events", 3},
		{"GET", "/proof", -1},
	} {
		body := &watchedBody{}
		r := httptest.NewRequest(c.method, c.target, body)
		r.ContentLength = c.length
		r.Header.Set("Origin", testOrigin)
		w := httptest.NewRecorder()
		web.ServeHTTP(w, r)
		var why refusal
		if w.Code != http.StatusBadRequest || json.Unmarshal(w.Body.Bytes(), &why) != nil || why.Error != "body" {
			t.Errorf("%s %s with length %d: status %d, body %q; want 400 body", c.method, c.target, c.length, w.Code, w.Body)
		}
		if w.Header().Get("Connection") != "close" {
			t.Errorf("%s %s with length %d: the connection stays open", c.method, c.target, c.length)
		}
		if body.read {
			t.Errorf("%s %s with length %d: the body was read", c.method, c.target, c.length)
		}
	}
	synctest.Test(t, func(t *testing.T) {
		show := newFake()
		l := serveWeb(t, show)
		smuggled := request("GET", "/robots.txt")
		for _, c := range []struct {
			name string
			raw  string
			want int
		}{
			{"length", request("POST", "/act/kill/1", "Origin", testOrigin, "Content-Length", strconv.Itoa(len(smuggled))) + smuggled, http.StatusBadRequest},
			{"chunked", request("POST", "/act/kill/1", "Origin", testOrigin, "Transfer-Encoding", "chunked") + fmt.Sprintf("%x\r\n%s\r\n0\r\n\r\n", len(smuggled), smuggled) + smuggled, http.StatusBadRequest},
			{"chunked and length", request("POST", "/act/kill/1", "Origin", testOrigin, "Transfer-Encoding", "chunked", "Content-Length", "3") + "0\r\n\r\n" + smuggled, http.StatusBadRequest},
			{"a long body", request("POST", "/act/kill/1", "Origin", testOrigin, "Content-Length", "1048576") + strings.Repeat("x", 1<<20), http.StatusBadRequest},
			{"a large header", request("GET", "/robots.txt", "X-Pad", strings.Repeat("a", 16<<10)), http.StatusRequestHeaderFieldsTooLarge},
		} {
			client := l.open(t, "203.0.113.5:1")
			res, _ := client.do(t, c.raw)
			if res.StatusCode != c.want {
				t.Errorf("%s: status %d, want %d", c.name, res.StatusCode, c.want)
			}
			if !client.ended() {
				t.Errorf("%s: the connection served another request", c.name)
			}
		}
		if res, _ := l.open(t, "203.0.113.5:2").do(t, request("GET", "/robots.txt", "X-Pad", strings.Repeat("a", 6<<10))); res.StatusCode != http.StatusOK {
			t.Errorf("a header of 6 KiB: status %d, want 200", res.StatusCode)
		}
		if acts := show.calls(); len(acts) != 0 {
			t.Errorf("Act was called: %v", acts)
		}
	})
}

func TestServerLimits(t *testing.T) {
	srv := newServer(nil)
	if srv.ReadHeaderTimeout != 5*time.Second || srv.ReadTimeout != 10*time.Second || srv.IdleTimeout != time.Minute ||
		srv.MaxHeaderBytes != 8<<10 || srv.WriteTimeout != 0 {
		t.Errorf("server limits: header %v, read %v, idle %v, header bytes %d, write %v", srv.ReadHeaderTimeout, srv.ReadTimeout, srv.IdleTimeout, srv.MaxHeaderBytes, srv.WriteTimeout)
	}
	synctest.Test(t, func(t *testing.T) {
		l := serveWeb(t, newFake())
		began := time.Now()
		slow := l.open(t, "203.0.113.1:1")
		slow.send("GET / HTTP/1.1\r\nHost: demo.exa")
		if !slow.ended() {
			t.Fatal("an unfinished header kept the connection")
		}
		if took := time.Since(began); took != 5*time.Second {
			t.Errorf("an unfinished header was dropped after %v, want 5s", took)
		}

		idle := l.open(t, "203.0.113.2:1")
		idle.do(t, request("GET", "/robots.txt"))
		began = time.Now()
		if !idle.ended() {
			t.Fatal("an idle connection stayed open")
		}
		if took := time.Since(began); took != time.Minute {
			t.Errorf("an idle connection was closed after %v, want 1m0s", took)
		}

		for i := range maxConns {
			l.open(t, fmt.Sprintf("198.51.100.%d:%d", i%200+1, i+1)).send("GET / HTTP/1.1\r\n")
		}
		began = time.Now()
		accepted := make(chan time.Time, 1)
		go func() {
			if conn, ok := l.connect("192.0.2.1:1"); ok {
				accepted <- time.Now()
				_ = conn.Close()
			}
		}()
		synctest.Wait()
		select {
		case <-accepted:
			t.Fatalf("connection %d was accepted next to %d open ones", maxConns+1, maxConns)
		default:
		}
		if took := (<-accepted).Sub(began); took != 5*time.Second {
			t.Errorf("connection %d was accepted after %v, want 5s when the others time out", maxConns+1, took)
		}
	})
}

func TestPageRendersOncePerSecond(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		show := newFake()
		web := NewWeb(show, testOrigin, nil)
		first := hit(web, "GET", "/").Body.String()
		var wg sync.WaitGroup
		for range 50 {
			wg.Go(func() {
				if hit(web, "GET", "/").Body.String() != first {
					t.Error("a second request within the second got other bytes")
				}
			})
		}
		wg.Wait()
		time.Sleep(renderEvery - time.Millisecond)
		hit(web, "GET", "/")
		if states, _, _ := show.counted(); states != 1 {
			t.Fatalf("52 requests within a second rendered %d times, want 1", states)
		}
		time.Sleep(time.Millisecond)
		later := hit(web, "GET", "/").Body.String()
		if states, _, _ := show.counted(); states != 2 {
			t.Fatalf("a request after a second rendered %d times in all, want 2", states)
		}
		if !strings.Contains(later, strconv.FormatInt(time.Now().UnixMilli(), 10)) {
			t.Error("the page after a second does not show the new time")
		}
	})
}

func TestProofAsksOncePerSecond(t *testing.T) {
	show := newFake()
	show.held = make(chan struct{})
	web := NewWeb(show, testOrigin, nil)
	var wg sync.WaitGroup
	for range 20 {
		wg.Go(func() { hit(web, "GET", "/proof") })
	}
	for {
		if _, proofs, _ := show.counted(); proofs > 0 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	time.Sleep(100 * time.Millisecond)
	close(show.held)
	wg.Wait()
	if _, proofs, _ := show.counted(); proofs != 1 {
		t.Fatalf("20 requests during a slow proof asked %d times, want 1", proofs)
	}

	synctest.Test(t, func(t *testing.T) {
		show := newFake()
		web := NewWeb(show, testOrigin, nil)
		bodies := make(chan string, 20)
		var wg sync.WaitGroup
		for range 20 {
			wg.Go(func() { bodies <- hit(web, "GET", "/proof").Body.String() })
		}
		wg.Wait()
		close(bodies)
		if _, proofs, _ := show.counted(); proofs != 1 {
			t.Fatalf("20 requests asked %d times, want 1", proofs)
		}
		want, _ := json.Marshal(sampleProof())
		for body := range bodies {
			if body != string(want) {
				t.Fatalf("proof %s, want %s", body, want)
			}
		}
		time.Sleep(proofHold - time.Millisecond)
		hit(web, "GET", "/proof")
		if _, proofs, _ := show.counted(); proofs != 1 {
			t.Fatalf("a request within the second asked again: %d", proofs)
		}
		for i, c := range []struct {
			err    error
			status int
			body   string
		}{
			{ErrNoLeader, http.StatusServiceUnavailable, `{"error":"noleader"}`},
			{ErrEmpty, http.StatusServiceUnavailable, `{"error":"empty"}`},
			{ErrFinding, http.StatusServiceUnavailable, `{"error":"finding"}`},
			{errors.New("prove: deadline exceeded"), http.StatusInternalServerError, ""},
		} {
			time.Sleep(proofHold)
			show.mu.Lock()
			show.proofErr = c.err
			show.mu.Unlock()
			for range 3 {
				w := hit(web, "GET", "/proof")
				if w.Code != c.status || w.Body.String() != c.body {
					t.Errorf("%v: status %d, body %q; want %d, %q", c.err, w.Code, w.Body, c.status, c.body)
				}
			}
			if _, proofs, _ := show.counted(); proofs != i+2 {
				t.Errorf("%v: asked %d times in all, want %d", c.err, proofs, i+2)
			}
		}
	})
}

func noRequestDataInFake(t *testing.T) {
	const marker = "zq7x"
	synctest.Test(t, func(t *testing.T) {
		show := newFake()
		l := serveWeb(t, show)
		from := "203.0.113.77:4711"
		var bodies []string
		for _, raw := range []string{
			request("POST", "/act/kill/1?zq7x=%3Cscript%3E", "Origin", testOrigin, "User-Agent", "zq7x<script>", "X-Forwarded-For", "zq7x",
				"Referer", "https://zq7x.example/", "Cookie", "zq7x=1", "X-Zq7x", "<b>zq7x</b>", "Accept-Language", "zq7x"),
			request("POST", "/act/zq7x/1", "Origin", testOrigin),
			request("POST", "/act/kill/zq7x", "Origin", testOrigin),
			request("POST", "/act/flip/leader", "Origin", "https://zq7x.example"),
			request("POST", "/act/cut/2", "Origin", testOrigin, "X-Forwarded-For", "10.0.0.1, zq7x"),
			request("GET", "/zq7x/%3Cscript%3E"),
			request("GET", "/?zq7x=%3Cscript%3E", "User-Agent", "zq7x"),
			request("GET", "/proof?zq7x=1", "X-Zq7x", "zq7x"),
			request("GET", "/events?zq7x=1", "Origin", "https://zq7x.example"),
			request("GET", "/static/zq7x.js"),
			request("GET", "/healthz?zq7x"),
		} {
			_, body := l.open(t, from).do(t, raw)
			bodies = append(bodies, body)
		}
		time.Sleep(renderEvery)
		_, page := l.open(t, from).do(t, request("GET", "/"))
		_, proof := l.open(t, from).do(t, request("GET", "/proof"))
		s := l.watch(t, from, "User-Agent", "zq7x", "Last-Event-ID", "zq7x")
		s.event(t)
		greeting := s.event(t)
		bodies = append(bodies, page, proof, greeting.data)
		for _, body := range bodies {
			for _, bad := range []string{marker, "<script", "203.0.113.77"} {
				if strings.Contains(body, bad) {
					t.Errorf("an answer contains %q: %.200s", bad, body)
				}
			}
		}
		if !strings.Contains(page, "<dt>phase</dt>") {
			t.Error("the page was not rendered")
		}
		acts := show.calls()
		if len(acts) != 2 {
			t.Fatalf("Act was called %d times, want 2: %v", len(acts), acts)
		}
		for _, a := range acts {
			if a.kind < Kill || a.kind > Flip || a.target < 0 || a.target > 3 || a.from.String() != "203.0.113.77/32" {
				t.Errorf("Act got %v", a)
			}
		}
	})
}

var (
	tagPattern    = regexp.MustCompile(`(?is)<[a-z][^>]*>`)
	inlineAttr    = regexp.MustCompile(`(?i)\s(style|on[a-z]+)\s*=`)
	scriptPattern = regexp.MustCompile(`(?is)<script\b([^>]*)>(.*?)</script\s*>`)
	stylePattern  = regexp.MustCompile(`(?i)<style\b`)
)

func inlineCode(html string) []string {
	var found []string
	for _, tag := range tagPattern.FindAllString(html, -1) {
		if inlineAttr.MatchString(tag) {
			found = append(found, tag)
		}
	}
	for _, m := range scriptPattern.FindAllStringSubmatch(html, -1) {
		if strings.TrimSpace(m[2]) != "" || !strings.Contains(m[1], "src=") {
			found = append(found, m[0])
		}
	}
	found = append(found, stylePattern.FindAllString(html, -1)...)
	if strings.Contains(strings.ToLower(html), "javascript:") {
		found = append(found, "javascript:")
	}
	return found
}

func TestNoInlineScriptOrStyle(t *testing.T) {
	tmpl, err := webFS.ReadFile("web/page.html")
	if err != nil {
		t.Fatal(err)
	}
	show := newFake()
	show.change(func(st *State) {
		st.Phase = "finding"
		st.Finding = &Finding{Check: "<style>", Reason: `<script>alert(1)</script>" onclick="alert(2)`}
		st.Nodes[1].Note.Text = `" style="color:red`
	})
	show.write(`<img src=x onerror=alert(3)>`)
	page := hit(NewWeb(show, testOrigin, nil), "GET", "/").Body.String()
	if !strings.Contains(page, "&lt;script&gt;alert(1)") || !strings.Contains(page, "&lt;img src=x onerror=alert(3)&gt;") {
		t.Fatal("the page does not show the finding and the line")
	}
	for name, html := range map[string]string{"template": string(tmpl), "page": page} {
		if inline := inlineCode(html); len(inline) > 0 {
			t.Errorf("%s has inline script or style: %q", name, inline)
		}
	}
	for _, bad := range []string{`<p style="x">`, `<button onclick="x">`, `<script>x()</script>`, `<style>p{}</style>`, `<a href="javascript:x">`, `<script type="module">import "/x.js"</script>`} {
		if len(inlineCode(bad)) == 0 {
			t.Errorf("the check misses %s", bad)
		}
	}
	if found := inlineCode(`<script type="module" src="/static/app.js"></script>`); len(found) > 0 {
		t.Errorf("the check refuses an external script: %q", found)
	}
}

type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestServerLogsNoRequest(t *testing.T) {
	var out lockedBuffer
	log.SetOutput(&out)
	defer log.SetOutput(os.Stderr)
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := newServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { panic("handler broke") }))
	go func() { _ = srv.Serve(l) }()
	defer srv.Close()
	res, err := http.Get("http://" + l.Addr().String() + "/")
	if err == nil {
		_ = res.Body.Close()
		t.Fatalf("a handler that panics answered %s", res.Status)
	}
	if line := out.String(); line != "" {
		t.Fatalf("net/http wrote a line with the address of the caller: %q", line)
	}
}
