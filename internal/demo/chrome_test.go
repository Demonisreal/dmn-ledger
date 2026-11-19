//go:build linux

package demo

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/Demonisreal/dmn-ledger/ledger"
)

const (
	chromeLimit  = 30 * time.Second
	chromeMemory = 2500
	chromeZone   = "Asia/Kolkata"
	appScript    = `<script type="module" src="/static/app.js"></script>`
)

func chromeBin(t *testing.T) string {
	t.Helper()
	if bin := os.Getenv("DEMO_CHROME"); bin != "" {
		return bin
	}
	for _, name := range []string{"google-chrome", "chromium", "chromium-browser"} {
		if bin, err := exec.LookPath(name); err == nil {
			return bin
		}
	}
	t.Skip("no browser: neither DEMO_CHROME nor google-chrome, chromium or chromium-browser in PATH")
	return ""
}

func availableMB(t *testing.T) int {
	t.Helper()
	f, err := os.Open("/proc/meminfo")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	lines := bufio.NewScanner(f)
	for lines.Scan() {
		if kb, ok := strings.CutPrefix(lines.Text(), "MemAvailable:"); ok {
			n, err := strconv.Atoi(strings.TrimSpace(strings.TrimSuffix(kb, "kB")))
			if err != nil {
				t.Fatal(err)
			}
			return n / 1024
		}
	}
	t.Fatal("no MemAvailable in /proc/meminfo")
	return 0
}

func chrome(t *testing.T, args ...string) (stdout, stderr string) {
	t.Helper()
	bin := chromeBin(t)
	if mb := availableMB(t); mb < chromeMemory {
		t.Skipf("%d MB available, the browser starts with %d or more", mb, chromeMemory)
	}
	ctx, cancel := context.WithTimeout(context.Background(), chromeLimit)
	defer cancel()
	flags := []string{"--headless=new", "--disable-gpu", "--no-first-run", "--user-data-dir=" + t.TempDir(), "--enable-logging=stderr"}
	cmd := exec.CommandContext(ctx, bin, append(flags, args...)...) //nolint:gosec // G204: the browser of the machine, the flags are fixed
	cmd.Env = append(os.Environ(), "TZ="+chromeZone)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL}
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
	cmd.WaitDelay = 2 * time.Second
	var out, errs bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errs
	err := cmd.Run()
	_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	if ctx.Err() != nil {
		t.Fatalf("the browser did not finish within %v and was killed; stderr:\n%s", chromeLimit, errs.String())
	}
	if err != nil {
		t.Fatalf("browser: %v; stderr:\n%s", err, errs.String())
	}
	return out.String(), errs.String()
}

func guarded(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for _, g := range guards {
			w.Header().Set(g[0], g[1])
		}
		h.ServeHTTP(w, r)
	})
}

func file(name, ctype string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		body, err := os.ReadFile(name)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", ctype)
		_, _ = w.Write(body)
	}
}

func TestVectorsInChrome(t *testing.T) {
	const vectors = "../notary/testdata/vectors.json"
	raw, err := os.ReadFile(vectors)
	if err != nil {
		t.Fatal(err)
	}
	var cases []json.RawMessage
	if err := json.Unmarshal(raw, &cases); err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.Handle("GET /static/", NewWeb(newFake(), "", nil))
	mux.HandleFunc("GET /vectors.html", file("testdata/vectors.html", "text/html; charset=utf-8"))
	mux.HandleFunc("GET /vectors.js", file("testdata/vectors.js", "text/javascript; charset=utf-8"))
	mux.HandleFunc("GET /vectors.json", file(vectors, "application/json"))
	srv := httptest.NewServer(guarded(mux))
	defer srv.Close()
	dom, stderr := chrome(t, "--virtual-time-budget=8000", "--dump-dom", srv.URL+"/vectors.html")
	want := fmt.Sprintf("vectors %d differ 0", len(cases))
	if !strings.Contains(dom, want) {
		t.Fatalf("the page says %q, want %q; stderr:\n%s", dom, want, stderr)
	}
	t.Log("the page says", want)
	if strings.Contains(stderr, "Refused to") {
		t.Errorf("the policy refused something:\n%s", stderr)
	}
}

func pageWith(t *testing.T, web *Web, scripts string) http.HandlerFunc {
	t.Helper()
	page := hit(web, "GET", "/").Body.String()
	if !strings.Contains(page, appScript) {
		t.Fatalf("the page does not load %s", appScript)
	}
	page = strings.Replace(page, appScript, scripts, 1)
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = io.WriteString(w, page)
	}
}

var (
	noscriptPattern = regexp.MustCompile(`(?s)<noscript>.*?</noscript>`)
	blockPattern    = regexp.MustCompile(`</?(p|li|ul|ol|div|h[1-6]|dl|dt|dd|table|thead|tbody|tfoot|tr|th|td|fieldset|legend|label|pre)\b[^>]*>`)
	timePattern     = regexp.MustCompile(`<time[^>]*datetime="([^"]+)"[^>]*>[^<]*</time>`)
	mainPattern     = regexp.MustCompile(`(?s)(<main [^>]*data-now="(\d+)"[^>]*>)(.*?)</main>`)
	connPattern     = regexp.MustCompile(`(?s)<p class="note" id="conn"[^>]*>.*?</p>`)
	nowPattern      = regexp.MustCompile(`(?s)<p class="now">.*?</p>`)
	connSeen        = regexp.MustCompile(`<p data-conn="">([^<]*)</p>`)
	faultButton     = regexp.MustCompile(`<button[^>]*data-kind="[a-z]+"[^>]*>`)
	stepPattern     = regexp.MustCompile(`(?s)<li data-step="([a-z]+)"(?: data-ok="(true|false)")?>\s*<p class="step-h">.*?<span class="verdict-word" data-k="ok">([^<]*)</span>`)
	proofShot       = regexp.MustCompile(`(?s)<section class="proof blk"[^>]*data-shot="([a-z]+)">(.*?)</section>`)
	proofResult     = regexp.MustCompile(`<p class="result" id="proof-result" aria-live="polite">([^<]*)</p>`)
)

func section(t *testing.T, html, class string) string {
	t.Helper()
	s := regexp.MustCompile(`(?s)<section class="` + class + `[" ].*?</section>`).FindString(html)
	if s == "" {
		t.Fatalf("no section %s", class)
	}
	return s
}

func words(html string) string {
	return plain(blockPattern.ReplaceAllString(timePattern.ReplaceAllString(noscriptPattern.ReplaceAllString(html, ""), "$1"), " "))
}

func zoned(ms int64, layout string) string {
	return time.UnixMilli(ms).In(time.FixedZone(chromeZone, 5*3600+1800)).Format(layout)
}

func served(t *testing.T, st State) string {
	t.Helper()
	var buf bytes.Buffer
	if err := pageTemplate.Execute(&buf, st); err != nil {
		t.Fatal(err)
	}
	return buf.String()
}

type moment struct {
	event string
	st    State
}

const oddReason = "Buchung 12 fehlt {at} <b>fett</b>\nzweite Zeile"

func moments(lines, later []Line) []moment {
	at := func(k int, edit func(st *State)) State {
		st := sampleState()
		st.Now += int64(k) * 1000
		st.Lines = lines
		edit(&st)
		return st
	}
	return []moment{
		{"state", at(0, func(*State) {})},
		{"tick", at(1, func(st *State) {
			st.Phase = "action"
			st.Action = &Action{Kind: Kill, Node: 2, Role: "follower", Term: 7, Phase: "struck", Began: st.Now - 300, LiftAt: st.Now + 1700}
			st.Nodes[1].Proc, st.Nodes[1].Answers, st.Nodes[1].Pid, st.Nodes[1].Role = "down", false, 0, ""
		})},
		{"tick", at(2, func(st *State) {
			st.Phase = "action"
			st.Action = &Action{Kind: Cut, Node: 2, Phase: "struck", Began: st.Now - 1000, LiftAt: st.Now + 4500}
			for i := range st.Links {
				st.Links[i].Cut = st.Links[i].From == 2 || st.Links[i].To == 2
			}
			st.Nodes[0].Term, st.Nodes[1].Role = 8, "leader"
		})},
		{"tick", at(3, func(st *State) {
			st.Phase = "action"
			st.Action = &Action{Kind: Flip, Node: 3, Phase: "lifted", Began: st.Now - 500, LiftAt: st.Now - 400, Repair: "yes"}
			st.Nodes[2].Repair, st.Nodes[2].Applied = true, 4000
		})},
		{"tick", at(4, func(st *State) {
			st.Phase = "action"
			st.Action = &Action{Kind: Freeze, Node: 1, Phase: "settling", Began: st.Now - 6000, LiftAt: st.Now - 1000}
		})},
		{"state", at(5, func(st *State) {
			st.Phase, st.Load, st.Lines = "finding", "held", later
			st.Finding = &Finding{At: st.Now - 2000, Check: "replay", Reason: oddReason, ResetAt: st.Now + 28000}
			st.Nodes[2].Proc, st.Nodes[2].Answers, st.Nodes[2].Role = "frozen", false, ""
			st.Totals.Found = 2
			st.Totals.Findings = append(st.Totals.Findings, Finding{At: st.Now - 2000, Check: "replay", Reason: oddReason, ResetAt: st.Now + 28000, Evidence: "finding-1795036497202"})
		})},
		{"tick", at(6, func(st *State) {
			st.Phase, st.Load, st.Action, st.Finding, st.Recent = "setup", "held", nil, nil, nil
			st.Books = Books{Confirmed: 20000}
			st.Cluster.Why = "start"
			st.Totals.Found, st.Totals.Findings = 0, nil
			for i := range st.Nodes {
				st.Nodes[i].Answers, st.Nodes[i].Role, st.Nodes[i].Note = false, "", Note{}
			}
		})},
		{"tick", at(7, func(st *State) {
			st.Action = &Action{Kind: Kill, Node: 2, Phase: "done", Began: st.Now - 6000, Ended: st.Now - 3750}
			st.Next = st.Now + 7200
		})},
		{"tick", at(8, func(st *State) {
			st.Action = &Action{Kind: Kill, Node: 2, Phase: "done", Began: st.Now - 16000, Ended: st.Now - 13750}
			st.Next = st.Now - 1000
			st.Load, st.Rest = "resting", Rest{Since: st.Now - 90000}
			st.Books.Sum.Value = "5999900"
			st.Books.Recount.After = 0
			st.Viewers = 1
			st.Recent = []Booking{{ID: "18735", Debit: 2, Credit: 5, Amount: "4200", At: st.Now - 400}, st.Recent[0]}
		})},
	}
}

func clean(t *testing.T, stderr string) {
	t.Helper()
	for _, bad := range []string{"Refused to", "Uncaught"} {
		if strings.Contains(stderr, bad) {
			t.Errorf("the browser says %q:\n%s", bad, stderr)
		}
	}
}

func TestPageInChrome(t *testing.T) {
	show := newFake()
	web := NewWeb(show, "", nil)
	now := sampleState().Now
	lines := tail(show.Lines(0), logTail)
	news := []Line{
		{N: 101, At: now + 4100, Src: "demo", Text: "SIGKILL an n2 (PID 41231), Follower in Term 7."},
		{N: 101, At: now + 4100, Src: "demo", Text: "SIGKILL an n2 (PID 41231), Follower in Term 7."},
		{N: 102, At: now + 4200, Src: "check", Text: "Zählung: Summe 60 000,00."},
	}
	ms := moments(lines, tail(append(show.Lines(0), news[0], news[2]), logTail))
	stream := [][]byte{retryLine}
	for i, m := range ms {
		stream = append(stream, event(m.event, m.st))
		if i == 4 {
			for _, l := range news {
				stream = append(stream, event("line", l))
			}
		}
	}

	var opened atomic.Int32
	refused := make(chan struct{})
	mux := http.NewServeMux()
	mux.Handle("GET /static/", web)
	mux.HandleFunc("GET /{$}", pageWith(t, web, `<script type="module" src="/page.js"></script>`+appScript+`<img src="/held.svg" alt="" hidden>`))
	mux.HandleFunc("GET /page.js", file("testdata/page.js", "text/javascript; charset=utf-8"))
	mux.HandleFunc("GET /held.svg", func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-refused:
			time.Sleep(300 * time.Millisecond)
		case <-time.After(chromeLimit - 5*time.Second):
		}
		http.NotFound(w, r)
	})
	mux.HandleFunc("GET /events", func(w http.ResponseWriter, _ *http.Request) {
		if n := opened.Add(1); n > 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			if n == 2 {
				close(refused)
			}
			return
		}
		rc := http.NewResponseController(w)
		w.Header().Set("Content-Type", "text/event-stream")
		for _, ev := range stream {
			_, _ = w.Write(ev)
			_ = rc.Flush()
			time.Sleep(50 * time.Millisecond)
		}
		time.Sleep(3500 * time.Millisecond)
	})
	srv := httptest.NewServer(guarded(mux))
	defer srv.Close()
	dom, stderr := chrome(t, "--dump-dom", srv.URL+"/")
	clean(t, stderr)
	if n := opened.Load(); n < 2 {
		t.Errorf("the page opened the stream %d times, want a new try after the end", n)
	}

	mains := mainPattern.FindAllStringSubmatch(dom, -1)
	if len(mains) < 2 {
		t.Fatalf("no snapshot of the page; stderr:\n%s", stderr)
	}
	shots := map[string]string{}
	for _, m := range mains[1:] {
		shots[m[2]] = m[3]
	}
	for i, m := range ms {
		shot, ok := shots[strconv.FormatInt(m.st.Now, 10)]
		if !ok {
			t.Errorf("moment %d (%s) never reached the page", i, m.event)
			continue
		}
		want := served(t, m.st)
		classes := []string{"verdict", "nodes", "faults", "books"}
		if i == 0 {
			classes = append(classes, "log")
		}
		for _, class := range classes {
			if got, exp := words(section(t, shot, class)), words(section(t, want, class)); got != exp {
				t.Errorf("moment %d (%s), section %s:\n got %q\nwant %q", i, m.event, class, got, exp)
			}
		}
	}
	if !strings.Contains(dom, "Buchung 12 fehlt {at} &lt;b&gt;fett&lt;/b&gt;\nzweite Zeile") {
		t.Error("the wording of the finding is not on the page as text, line break and braces included")
	}

	head, live := mains[0][1], mains[0][3]
	last := ms[len(ms)-1].st
	want := served(t, last)
	for _, class := range []string{"nodes", "books"} {
		if got, exp := words(section(t, live, class)), words(section(t, want, class)); got != exp {
			t.Errorf("after the stream, section %s:\n got %q\nwant %q", class, got, exp)
		}
	}
	strip := func(html string, p *regexp.Regexp) string { return words(p.ReplaceAllString(html, "")) }
	if got, exp := strip(section(t, live, "verdict"), connPattern), strip(section(t, want, "verdict"), connPattern); got != exp {
		t.Errorf("after the stream, the verdict:\n got %q\nwant %q", got, exp)
	}
	if got, exp := strip(section(t, live, "faults"), nowPattern), strip(section(t, want, "faults"), nowPattern); got != exp {
		t.Errorf("after the stream, the faults:\n got %q\nwant %q", got, exp)
	}

	stale := "Stand " + zoned(last.Now, "15:04:05") + ". Die Verbindung ist unterbrochen, ein neuer Versuch läuft."
	crowded := "Gerade sehen zu viele zu. Das ist der Stand von " + zoned(last.Now, "15:04:05") + "; die Seite versucht es weiter."
	conn := connPattern.FindString(live)
	if strings.Contains(conn, "hidden") || plain(conn) != crowded {
		t.Errorf("the hint about the connection is %q, want %q", conn, crowded)
	}
	if got := plain(nowPattern.FindString(section(t, live, "faults"))); got != crowded {
		t.Errorf("the status line says %q, want %q", got, crowded)
	}
	var said []string
	for _, m := range connSeen.FindAllStringSubmatch(dom, -1) {
		said = append(said, m[1])
	}
	if i, j := slices.Index(said, stale), slices.Index(said, crowded); i < 0 || j < i {
		t.Errorf("the hint went %q, want %q first and then %q", said, stale, crowded)
	}
	if !strings.Contains(head, `data-stale=""`) {
		t.Errorf("the values are not marked as old: %s", head)
	}
	for _, b := range faultButton.FindAllString(live, -1) {
		if !strings.Contains(b, " disabled") || strings.Contains(b, " hidden") {
			t.Errorf("a fault button is not there and locked: %s", b)
		}
	}

	if !strings.Contains(live, `<ol id="lines"><li data-n="102" data-src="check">`) {
		t.Error("line 102 is not the first line of the log")
	}
	if n := strings.Count(live, `data-n="101"`); n != 1 {
		t.Errorf("line 101 stands %d times, want once", n)
	}
	if !strings.Contains(live, `<div class="vh" id="said" aria-live="polite">Zählung: Summe 60 000,00.</div>`) {
		t.Error("the live region does not hold the last line of check")
	}
	for _, stamp := range []string{
		`<time datetime="` + iso(last.Started) + `">` + zoned(last.Started, "15:04:05") + `</time>`,
		`<time datetime="` + iso(news[2].At) + `">` + zoned(news[2].At, "15:04:05.000") + `</time>`,
	} {
		if !strings.Contains(live, stamp) {
			t.Errorf("no time in the zone of the browser: %s", stamp)
		}
	}
}

func TestRefusalsInChrome(t *testing.T) {
	st := sampleState()
	st.Action, st.Next = nil, 0
	ready := "Bereit. Jede Störung endet von selbst. Es läuft immer nur eine, und alle Besucher sehen dieselbe."
	cases := []struct {
		name    string
		status  int
		body    string
		after   string
		locked  bool
		later   string
		retried bool
	}{
		{"origin", http.StatusForbidden, `{"error":"origin"}`, "Das hat der Server nicht angenommen (403).", false, "Das hat der Server nicht angenommen (403).", false},
		{"busy", http.StatusConflict, `{"error":"busy"}`, "Das hat der Server nicht angenommen (409).", false, "Das hat der Server nicht angenommen (409).", false},
		{"young", http.StatusConflict, `{"error":"young"}`, "Dieser Cluster ist zu jung für einen Bitfehler in bestätigten Einträgen. In einigen Sekunden geht es.", false,
			"Dieser Cluster ist zu jung für einen Bitfehler in bestätigten Einträgen. In einigen Sekunden geht es.", false},
		{"cooling", http.StatusTooManyRequests, fmt.Sprintf(`{"error":"cooling","until":%d}`, st.Now+5000), "Die nächste Störung geht in 4 s.", true, ready, false},
		{"limited", http.StatusTooManyRequests, fmt.Sprintf(`{"error":"limited","until":%d}`, st.Now+5000), "Von dieser Adresse geht die nächste Störung in 4 s.", true, ready, false},
		{"recovering", http.StatusServiceUnavailable, `{"error":"recovering"}`, "Der Cluster ist gerade nicht vollständig. Gleich wieder.", true, ready, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			web := NewWeb(newFake(), "", nil)
			acted := make(chan struct{})
			done := make(chan struct{})
			var once atomic.Bool
			mux := http.NewServeMux()
			mux.Handle("GET /static/", web)
			mux.HandleFunc("GET /{$}", pageWith(t, web, `<script type="module" src="/page.js"></script>`+appScript+
				`<script type="module" src="/act.js"></script><img src="/held.svg" alt="" hidden>`))
			mux.HandleFunc("GET /page.js", file("testdata/page.js", "text/javascript; charset=utf-8"))
			mux.HandleFunc("GET /act.js", file("testdata/act.js", "text/javascript; charset=utf-8"))
			mux.HandleFunc("GET /held.svg", func(w http.ResponseWriter, r *http.Request) {
				select {
				case <-done:
					time.Sleep(300 * time.Millisecond)
				case <-time.After(chromeLimit - 5*time.Second):
				}
				http.NotFound(w, r)
			})
			mux.HandleFunc("POST /act/{kind}/{node}", func(w http.ResponseWriter, r *http.Request) {
				if r.PathValue("kind") != "kill" || r.PathValue("node") != "leader" || r.Header.Get("Origin") == "" {
					t.Errorf("the page sent %s %s with Origin %q", r.Method, r.URL.Path, r.Header.Get("Origin"))
				}
				if c.retried {
					w.Header().Set("Retry-After", "2")
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(c.status)
				_, _ = io.WriteString(w, c.body)
				if once.CompareAndSwap(false, true) {
					close(acted)
				}
			})
			mux.HandleFunc("GET /events", func(w http.ResponseWriter, r *http.Request) {
				rc := http.NewResponseController(w)
				w.Header().Set("Content-Type", "text/event-stream")
				send := func(ev []byte) {
					_, _ = w.Write(ev)
					_ = rc.Flush()
				}
				send(retryLine)
				send(event("state", st))
				select {
				case <-acted:
				case <-time.After(10 * time.Second):
					t.Error("the page never sent the action")
				}
				time.Sleep(200 * time.Millisecond)
				for _, ahead := range []int64{1000, 6000} {
					next := st
					next.Now += ahead
					send(event("tick", next))
					time.Sleep(100 * time.Millisecond)
				}
				close(done)
				<-r.Context().Done()
			})
			srv := httptest.NewServer(guarded(mux))
			defer srv.Close()
			dom, stderr := chrome(t, "--dump-dom", srv.URL+"/")
			clean(t, stderr)
			mains := mainPattern.FindAllStringSubmatch(dom, -1)
			if len(mains) < 2 {
				t.Fatal("no snapshot of the page")
			}
			shots := map[string]string{}
			for _, m := range mains[1:] {
				shots[m[2]] = m[3]
			}
			for _, want := range []struct {
				ahead  int64
				says   string
				locked bool
			}{{1000, c.after, c.locked}, {6000, c.later, false}} {
				shot, ok := shots[strconv.FormatInt(st.Now+want.ahead, 10)]
				if !ok {
					t.Fatalf("no snapshot %d ms after the state", want.ahead)
				}
				faults := section(t, shot, "faults")
				if got := plain(nowPattern.FindString(faults)); got != want.says {
					t.Errorf("%d ms after the state the status line says %q, want %q", want.ahead, got, want.says)
				}
				for _, b := range faultButton.FindAllString(faults, -1) {
					if strings.Contains(b, " disabled") != want.locked {
						t.Errorf("%d ms after the state, locked %v: %s", want.ahead, want.locked, b)
					}
				}
			}
		})
	}
}

type vector struct {
	Name    string `json:"name"`
	Proof   string `json:"proof"`
	Cluster string `json:"cluster"`
	Key     string `json:"key"`
	Valid   bool   `json:"valid"`
	Seq     uint64 `json:"seq"`
	Head    string `json:"head"`
}

func receiptOf(t *testing.T, v vector) []byte {
	t.Helper()
	raw, err := hex.DecodeString(v.Proof)
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(Proof{
		At: 1, Cluster: v.Cluster,
		Keys:     [3]string{strings.Repeat("ab", 32), v.Key, strings.Repeat("ef", 32)},
		Transfer: Booking{ID: "999", Debit: 8, Credit: 9, Amount: "1", At: 1},
		Receipt:  v.Proof, Base64: base64.StdEncoding.EncodeToString(raw),
		Head: Head{Seq: v.Seq, Hash: v.Head, Signer: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func recorded(t *testing.T, v vector) ledger.Transfer {
	t.Helper()
	raw, err := hex.DecodeString(v.Proof)
	if err != nil {
		t.Fatal(err)
	}
	key, err := hex.DecodeString(v.Key)
	if err != nil {
		t.Fatal(err)
	}
	cluster, err := strconv.ParseUint(v.Cluster, 16, 64)
	if err != nil {
		t.Fatal(err)
	}
	p, err := ledger.VerifyProof(raw, cluster, [][]byte{key})
	if err != nil || p.Kind != ledger.ProofTransfer {
		t.Fatalf("vector %q: %v, kind %v", v.Name, err, p.Kind)
	}
	return p.Transfer
}

func TestProofInChrome(t *testing.T) {
	raw, err := os.ReadFile("../notary/testdata/vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	var vectors []vector
	if err := json.Unmarshal(raw, &vectors); err != nil {
		t.Fatal(err)
	}
	transfer, account := vectors[0], vectors[1]
	if !strings.HasPrefix(transfer.Proof, "0102") || !strings.HasPrefix(account.Proof, "0101") || !transfer.Valid || !account.Valid {
		t.Fatalf("the first two vectors are not a valid transfer and a valid account: %q, %q", transfer.Name, account.Name)
	}
	tr := recorded(t, transfer)
	receipts := [][]byte{receiptOf(t, transfer), receiptOf(t, account)}

	var asked atomic.Int32
	web := NewWeb(newFake(), "", nil)
	mux := http.NewServeMux()
	mux.Handle("GET /static/", web)
	mux.HandleFunc("GET /{$}", pageWith(t, web, `<script type="module" src="/run.js"></script>`))
	mux.HandleFunc("GET /run.js", file("testdata/proof.js", "text/javascript; charset=utf-8"))
	mux.HandleFunc("GET /proof", func(w http.ResponseWriter, _ *http.Request) {
		n := int(asked.Add(1)) - 1
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(receipts[min(n, len(receipts)-1)])
	})
	srv := httptest.NewServer(guarded(mux))
	defer srv.Close()
	dom, stderr := chrome(t, "--virtual-time-budget=8000", "--dump-dom", srv.URL+"/")
	clean(t, stderr)
	if i := strings.Index(dom, `<div id="shots" hidden="">error`); i >= 0 {
		t.Fatalf("the run failed: %.300s", dom[i:])
	}
	shots := map[string]string{}
	for _, m := range proofShot.FindAllStringSubmatch(dom, -1) {
		shots[m[1]] = m[2]
	}

	holds, fails := [2]string{"true", "stimmt"}, [2]string{"false", "stimmt nicht"}
	tampered := ledger.Uint128{Hi: tr.Amount.Hi, Lo: tr.Amount.Lo&^0xff | (tr.Amount.Lo+1)&0xff}
	cases := []struct {
		shot  string
		steps map[string][2]string
		said  string
		shows []string
	}{
		{"fetched", map[string][2]string{"transfer": holds, "leaf": holds, "path": holds, "head": holds, "stamp": holds, "owner": holds, "go": holds},
			"Der Beleg ist in sich schlüssig und trägt den Stempel von Knoten 2.",
			[]string{
				`<dd data-k="id">` + tr.ID.String() + `</dd>`,
				`<dd data-k="amount">` + money(tr.Amount.String()) + `</dd>`,
				`<dd data-k="debit">` + tr.DebitAccountID.String() + `</dd>`,
				`<dd data-k="credit">` + tr.CreditAccountID.String() + `</dd>`,
				`<p class="hex" data-k="head">` + transfer.Head + `</p>`,
				`<p class="hex" data-k="key">` + transfer.Key + `</p>`,
				`Wem der Schlüssel gehört: Knoten <span data-k="signer">2</span>`,
				`buchung-<span data-k="id">` + tr.ID.String() + `</span>.proof`,
				`LEDGER_CLUSTER=0x<span data-k="cluster">` + transfer.Cluster + `</span>`,
			}},
		{"tampered", map[string][2]string{"transfer": holds, "leaf": holds, "path": fails},
			"Der geänderte Beleg fällt durch: Der Pfad führt nicht mehr zur Wurzel im Kettenglied.",
			[]string{
				`<dd data-k="amount">` + money(tampered.String()) + `</dd>`,
				`<p class="hex" data-k="head"></p>`,
				`buchung-<span data-k="id">` + tr.ID.String() + `</span>.proof`,
			}},
		{"account", map[string][2]string{"transfer": fails}, "", []string{`<dd data-k="id"></dd>`}},
	}
	for _, c := range cases {
		shot, ok := shots[c.shot]
		if !ok {
			t.Errorf("no snapshot %q", c.shot)
			continue
		}
		steps := stepPattern.FindAllStringSubmatch(shot, -1)
		if len(steps) != 7 {
			t.Fatalf("%s: %d steps, want 7", c.shot, len(steps))
		}
		for _, s := range steps {
			if got, want := [2]string{s[2], s[3]}, c.steps[s[1]]; got != want {
				t.Errorf("%s: step %s says %q, want %q", c.shot, s[1], got, want)
			}
		}
		said := proofResult.FindStringSubmatch(shot)
		if said == nil || said[1] != c.said {
			t.Errorf("%s: the page sums up %q, want %q", c.shot, said, c.said)
		}
		for _, w := range c.shows {
			if !strings.Contains(shot, w) {
				t.Errorf("%s: the page does not show %s", c.shot, w)
			}
		}
		if c.shot != "fetched" && strings.Contains(shot, "schlüssig") {
			t.Errorf("%s: the page calls the receipt sound", c.shot)
		}
	}
}
