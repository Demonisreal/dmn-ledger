//go:build linux

package demo

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func held(done <-chan struct{}) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-done:
			time.Sleep(300 * time.Millisecond)
		case <-time.After(chromeLimit - 5*time.Second):
		}
		http.NotFound(w, r)
	}
}

func settle(sent int) time.Duration {
	if sent == 0 {
		return 500 * time.Millisecond
	}
	return 60 * time.Millisecond
}

func snapshots(t *testing.T, dom string) (live []string, shots map[string]string) {
	t.Helper()
	mains := mainPattern.FindAllStringSubmatch(dom, -1)
	if len(mains) < 2 {
		t.Fatal("no snapshot of the page")
	}
	shots = map[string]string{}
	for _, m := range mains[1:] {
		shots[m[2]] = m[3]
	}
	return mains[0], shots
}

func buttonsLocked(t *testing.T, html string, want bool, when string) {
	t.Helper()
	buttons := faultButton.FindAllString(html, -1)
	if len(buttons) != 4 {
		t.Fatalf("%s: %d fault buttons", when, len(buttons))
	}
	for _, b := range buttons {
		if strings.Contains(b, " disabled") != want {
			t.Errorf("%s: locked is not %v: %s", when, want, b)
		}
	}
}

func TestMoreStatesInChrome(t *testing.T) {
	show := newFake()
	web := NewWeb(show, "", nil)
	lines := tail(show.Lines(0), logTail)
	at := func(k int, edit func(st *State)) State {
		st := sampleState()
		st.Now += int64(k) * 1000
		st.Action, st.Next = nil, 0
		edit(&st)
		return st
	}
	down := func(st *State, n int) {
		st.Nodes[n-1].Proc, st.Nodes[n-1].Answers, st.Nodes[n-1].Pid, st.Nodes[n-1].Role = "down", false, 0, ""
	}
	ms := []State{
		at(0, func(st *State) { st.Lines = lines }),
		at(1, func(st *State) { st.Nodes[1].Apply = "00000000deadbeef" }),
		at(2, func(st *State) {
			for i, balance := range []string{"900000", "1100000", "1000001", "999999", "250", "1999750"} {
				st.Books.Sum.Accounts[i].Balance = balance
			}
		}),
		at(3, func(st *State) {
			st.Phase, st.Load, st.Books, st.Recent = "setup", "held", Books{}, nil
			for i := range st.Nodes {
				st.Nodes[i].Answers, st.Nodes[i].Role, st.Nodes[i].Note = false, "", Note{}
			}
		}),
		at(4, func(st *State) {
			st.Action = &Action{Kind: Cut, Node: 3, Phase: "done", Began: st.Now - 7000, Ended: st.Now - 250}
			st.Next = st.Now + 3750
		}),
		at(5, func(st *State) {
			st.Phase = "action"
			st.Action = &Action{Kind: Kill, Node: 2, Phase: "struck", Began: st.Now - 300, LiftAt: st.Now + 1700}
			st.Next = st.Now + 9700
			down(st, 2)
		}),
		at(6, func(st *State) {
			st.Phase, st.Load = "finding", "held"
			st.Finding = &Finding{At: st.Now - 100, Check: "S8", Reason: "transfer 12 lost"}
			st.Action = &Action{Kind: Kill, Node: 2, Phase: "struck", Began: st.Now - 1300, LiftAt: st.Now + 700}
			down(st, 2)
		}),
		at(7, func(st *State) {
			st.Phase, st.Load = "finding", "held"
			st.Finding = &Finding{At: st.Now - 1100, Check: "S8", Reason: "transfer 12 lost", ResetAt: st.Now + 28900, Evidence: "befund-1795036499-2"}
			down(st, 2)
		}),
		at(8, func(st *State) {
			st.Phase, st.Load, st.Cluster.Why = "setup", "held", "finding"
			st.Books.Confirmed = 20000
		}),
		at(9, func(st *State) { st.Nodes[1].Role = "leader" }),
		at(10, func(st *State) {
			st.Action = &Action{Kind: Flip, Node: 1, Phase: "done", Repair: "no", Began: st.Now - 2500, Ended: st.Now - 250}
			st.Next = st.Now + 7500
		}),
		at(11, func(*State) {}),
	}
	done := make(chan struct{})
	mux := http.NewServeMux()
	mux.Handle("GET /static/", web)
	mux.HandleFunc("GET /{$}", pageWith(t, web, `<script type="module" src="/page.js"></script>`+appScript+`<img src="/held.svg" alt="" hidden>`))
	mux.HandleFunc("GET /page.js", file("testdata/page.js", "text/javascript; charset=utf-8"))
	mux.HandleFunc("GET /held.svg", held(done))
	mux.HandleFunc("GET /events", func(w http.ResponseWriter, r *http.Request) {
		rc := http.NewResponseController(w)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write(retryLine)
		for i, st := range ms {
			name := "tick"
			if i == 0 {
				name = "state"
			}
			_, _ = w.Write(event(name, st))
			_ = rc.Flush()
			time.Sleep(settle(i))
		}
		close(done)
		<-r.Context().Done()
	})
	srv := httptest.NewServer(guarded(mux))
	defer srv.Close()
	dom, stderr := chrome(t, "--dump-dom", srv.URL+"/")
	clean(t, stderr)
	_, shots := snapshots(t, dom)
	for i, st := range ms {
		when := fmt.Sprintf("moment %d", i)
		shot, ok := shots[strconv.FormatInt(st.Now, 10)]
		if !ok {
			t.Errorf("%s never reached the page", when)
			continue
		}
		want := served(t, st)
		for _, class := range []string{"verdict", "nodes", "faults", "books"} {
			if got, exp := words(section(t, shot, class)), words(section(t, want, class)); got != exp {
				t.Errorf("%s, section %s:\n got %q\nwant %q", when, class, got, exp)
			}
		}
		buttonsLocked(t, section(t, shot, "faults"), locked(st), when)
		if hidden := strings.Contains(shot, `<table class="accounts" hidden="">`); hidden != (st.Books.Sum.At == 0) {
			t.Errorf("%s: the table of accounts is hidden %v, counted at %d", when, hidden, st.Books.Sum.At)
		}
		if counted := strings.Contains(section(t, shot, "faults"), `data-counted`); counted != (i > 0) {
			t.Errorf("%s: the number of viewers shows %v, want it from the first tick on", when, counted)
		}
	}
	if !strings.Contains(shots[strconv.FormatInt(ms[1].Now, 10)], "Bei Eintrag 4\u202f521 melden die Knoten verschiedene Zustände.") {
		t.Error("two nodes at one entry with two hashes, and the page does not say so")
	}
	if !strings.Contains(shots[strconv.FormatInt(ms[5].Now, 10)], `ist beendet und startet in <span aria-live="off">2</span> s`) {
		t.Error("the seconds left are not kept from the live region")
	}
	if shot := shots[strconv.FormatInt(ms[10].Now, 10)]; !strings.Contains(shot, "Erholt nach 2,2 s.") || !strings.Contains(shot, "Ein solches Ende gilt als abgebrochenes Schreiben") {
		t.Error("a flip without repair that took 2.25 s is not named as such")
	}
}

func TestDifferingHashesAreNamed(t *testing.T) {
	r := render(t, func(st *State) { st.Nodes[2].Apply = "00000000deadbeef" })
	r.shows(t, "Bei Eintrag 4'521 melden die Knoten verschiedene Zustände.")
	r.hides(t, "denselben Zustand")
}

func TestPageNamesTheDurationsThatRun(t *testing.T) {
	r := render(t, nil)
	secs := func(d time.Duration) int { return int(d / time.Second) }
	r.shows(t,
		fmt.Sprintf("Nach %d Sekunden startet der Prozess neu.", secs(standard.holds[Kill])),
		fmt.Sprintf("SIGSTOP für %d Sekunden.", secs(standard.holds[Freeze])),
		fmt.Sprintf("%d Sekunden lang erreicht der Knoten die beiden anderen nicht", secs(standard.holds[Cut])),
		fmt.Sprintf("%d Sekunden nach einer ausgelösten Störung", secs(standard.perAddr)),
		fmt.Sprintf("Diese Demo setzt ihren Cluster nach %s Buchungen neu auf", strings.ReplaceAll(num(standard.limit), "\u202f", " ")),
		fmt.Sprintf("Nach %s Buchungen wird der Cluster neu aufgesetzt", strings.ReplaceAll(num(standard.limit), "\u202f", " ")),
		fmt.Sprintf("Jedes Konto begann mit %s", strings.ReplaceAll(money(strconv.Itoa(funding)), "\u202f", " ")),
	)
}

func TestLogInChrome(t *testing.T) {
	show := newFake()
	web := NewWeb(show, "", nil)
	first := sampleState()
	first.Lines = tail(show.Lines(0), logTail)
	now := first.Now
	markup := `<b>fett</b> {at} <img src=x>`
	news := []Line{
		{N: 101, At: now + 100, Src: "n2", Text: markup},
		{N: 102, At: now + 200, Src: "check", Text: "Zählung: Summe 60 000,00."},
		{N: 103, At: now + 300, Src: "n3", Text: "node 3 of 3 says <i>nothing</i>"},
	}
	second := sampleState()
	second.Now += 1000
	third := sampleState()
	third.Now += 2000
	third.Totals.Found = 2
	third.Totals.Findings = append(third.Totals.Findings, Finding{At: now, Check: "S8", Reason: markup, ResetAt: now + 30000})
	for n := uint64(301); n <= 360; n++ {
		src := "n1"
		if n == 330 {
			src = "demo"
		}
		third.Lines = append(third.Lines, Line{N: n, At: now + int64(n), Src: src, Text: fmt.Sprintf("line %d", n)})
	}
	fourth := sampleState()
	fourth.Now += 3000
	fourth.Started += 3_600_000
	for n := uint64(1); n <= 5; n++ {
		fourth.Lines = append(fourth.Lines, Line{N: n, At: now + 3000, Src: "demo", Text: fmt.Sprintf("after the restart %d", n)})
	}
	fifth := fourth
	fifth.Now, fifth.Lines = fourth.Now+1000, nil
	var flood []byte
	for n := uint64(6); n <= 255; n++ {
		flood = append(flood, event("line", Line{N: n, At: now + 3500, Src: "n2", Text: fmt.Sprintf("flood %d", n)})...)
	}

	done := make(chan struct{})
	mux := http.NewServeMux()
	mux.Handle("GET /static/", web)
	mux.HandleFunc("GET /{$}", pageWith(t, web, `<script type="module" src="/page.js"></script>`+appScript+`<img src="/held.svg" alt="" hidden>`))
	mux.HandleFunc("GET /page.js", file("testdata/page.js", "text/javascript; charset=utf-8"))
	mux.HandleFunc("GET /held.svg", held(done))
	mux.HandleFunc("GET /events", func(w http.ResponseWriter, r *http.Request) {
		rc := http.NewResponseController(w)
		w.Header().Set("Content-Type", "text/event-stream")
		send := func(ev []byte) {
			_, _ = w.Write(ev)
			_ = rc.Flush()
			time.Sleep(50 * time.Millisecond)
		}
		send(retryLine)
		send(event("state", first))
		time.Sleep(settle(0))
		for _, l := range news {
			send(event("line", l))
		}
		send(event("tick", second))
		send(event("state", third))
		send(event("state", fourth))
		send(flood)
		send(event("tick", fifth))
		close(done)
		<-r.Context().Done()
	})
	srv := httptest.NewServer(guarded(mux))
	defer srv.Close()
	dom, stderr := chrome(t, "--dump-dom", srv.URL+"/")
	clean(t, stderr)
	_, shots := snapshots(t, dom)
	escaped := `&lt;b&gt;fett&lt;/b&gt; {at} &lt;img src=x&gt;`

	after := shots[strconv.FormatInt(second.Now, 10)]
	log := section(t, after, "log")
	if !strings.Contains(log, `<li data-n="101" data-src="n2" lang="en">`) || !strings.Contains(log, `<span class="said">`+escaped+`</span>`) {
		t.Error("a line of a node with markup in it is not in the log as text, marked as English")
	}
	if strings.Contains(log, "<b>fett</b>") || strings.Contains(log, "<i>nothing</i>") || strings.Contains(log, "<img") {
		t.Error("markup from a line of a node became markup of the page")
	}
	if !strings.Contains(log, `<div class="vh" aria-live="polite">Zählung: Summe 60 000,00.</div>`) {
		t.Error("the live region does not hold the last line of check after a later line of a node")
	}
	if !strings.Contains(log, `<li data-n="102" data-src="check">`) {
		t.Error("a line of check carries a language or is missing")
	}

	gap := section(t, shots[strconv.FormatInt(third.Now, 10)], "log")
	items := regexp.MustCompile(`<li data-n="(\d+)" data-src="([a-z0-9]+)"( lang="en")?`).FindAllStringSubmatch(gap, -1)
	if len(items) != 60 || items[0][1] != "360" || items[59][1] != "301" {
		t.Fatalf("after a gap in the numbers the log holds %d lines, want the 60 of the new state, newest first", len(items))
	}
	for _, it := range items {
		if english := it[3] != ""; english != (it[2] != "demo") {
			t.Errorf("line %s of %s: marked as English %v", it[1], it[2], english)
		}
	}
	pinned := regexp.MustCompile(`(?s)<p class="pinned">.*?</p>`).FindString(gap)
	if !strings.Contains(pinned, escaped) || strings.Contains(pinned, "<b>") || strings.Contains(pinned, "<img") {
		t.Errorf("the wording of the pinned finding is not on the page as text: %s", pinned)
	}

	numbered := regexp.MustCompile(`<li data-n="(\d+)"`)
	last := numbered.FindAllStringSubmatch(section(t, shots[strconv.FormatInt(fourth.Now, 10)], "log"), -1)
	if len(last) != 5 || last[0][1] != "5" {
		t.Errorf("after a restart of the controller the log holds %d lines, want its 5", len(last))
	}
	kept := numbered.FindAllStringSubmatch(section(t, shots[strconv.FormatInt(fifth.Now, 10)], "log"), -1)
	if len(kept) != 200 || kept[0][1] != "255" || kept[199][1] != "56" {
		t.Errorf("after 255 lines the log holds %d, want the newest 200", len(kept))
	}
}

func TestReconnectKeepsItsDistanceInChrome(t *testing.T) {
	web := NewWeb(newFake(), "", nil)
	var opened atomic.Int32
	done := make(chan struct{})
	mux := http.NewServeMux()
	mux.Handle("GET /static/", web)
	mux.HandleFunc("GET /{$}", pageWith(t, web, appScript+`<img src="/held.svg" alt="" hidden>`))
	mux.HandleFunc("GET /held.svg", held(done))
	mux.HandleFunc("GET /events", func(w http.ResponseWriter, _ *http.Request) {
		if opened.Add(1) == 1 {
			time.AfterFunc(2*time.Second, func() { close(done) })
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	})
	srv := httptest.NewServer(guarded(mux))
	defer srv.Close()
	dom, stderr := chrome(t, "--dump-dom", srv.URL+"/")
	clean(t, stderr)
	if n := opened.Load(); n != 1 {
		t.Errorf("the page asked for the stream %d times within 2.3 s of a 503, want once and then 3 s of waiting", n)
	}
	if !strings.Contains(dom, "Gerade sehen zu viele zu.") {
		t.Error("the page does not say that the stream is full")
	}
}

func TestPageRecoversAfterALostStreamInChrome(t *testing.T) {
	web := NewWeb(newFake(), "", nil)
	first, second := sampleState(), sampleState()
	first.Action, first.Next, second.Action, second.Next = nil, 0, nil, 0
	second.Now += 5000
	var opened atomic.Int32
	done := make(chan struct{})
	mux := http.NewServeMux()
	mux.Handle("GET /static/", web)
	mux.HandleFunc("GET /{$}", pageWith(t, web, `<script type="module" src="/page.js"></script>`+appScript+`<img src="/held.svg" alt="" hidden>`))
	mux.HandleFunc("GET /page.js", file("testdata/page.js", "text/javascript; charset=utf-8"))
	mux.HandleFunc("GET /held.svg", held(done))
	mux.HandleFunc("GET /events", func(w http.ResponseWriter, r *http.Request) {
		n := opened.Add(1)
		rc := http.NewResponseController(w)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write(retryLine)
		if n == 1 {
			_, _ = w.Write(event("state", first))
			_ = rc.Flush()
			select {
			case <-time.After(3300 * time.Millisecond):
			case <-r.Context().Done():
			}
			return
		}
		_, _ = w.Write(event("state", second))
		_ = rc.Flush()
		if n == 2 {
			close(done)
		}
		<-r.Context().Done()
	})
	srv := httptest.NewServer(guarded(mux))
	defer srv.Close()
	dom, stderr := chrome(t, "--dump-dom", srv.URL+"/")
	clean(t, stderr)
	live, _ := snapshots(t, dom)
	if !strings.Contains(dom, `<p data-conn="">Stand `) {
		t.Error("the page never said that its values were old")
	}
	if live[2] != strconv.FormatInt(second.Now, 10) {
		t.Fatalf("the page shows the state of %s, want the one of the second stream", live[2])
	}
	if strings.Contains(live[1], "data-stale") {
		t.Errorf("the values stay marked as old after the second stream: %s", live[1])
	}
	if conn := connPattern.FindString(live[3]); !strings.Contains(conn, " hidden") || plain(conn) != "" {
		t.Errorf("the hint about the connection stays: %s", conn)
	}
	buttonsLocked(t, section(t, live[3], "faults"), false, "after the second stream")
}

func TestAcceptedActionLocksUntilItShowsInChrome(t *testing.T) {
	st := sampleState()
	st.Action, st.Next = nil, 0
	waiting, running, after := st, st, st
	waiting.Now += 500
	running.Now += 1000
	running.Phase, running.Next = "action", st.Now+10100
	running.Action = &Action{Kind: Kill, Node: 1, Role: "leader", Term: 7, Phase: "struck", Began: st.Now + 100, LiftAt: st.Now + 2100}
	after.Now += 12000
	after.Action = &Action{Kind: Kill, Node: 1, Role: "leader", Term: 7, Phase: "done", Began: st.Now + 100, LiftAt: st.Now + 2100, Ended: st.Now + 2400}

	web := NewWeb(newFake(), "", nil)
	acted := make(chan struct{})
	done := make(chan struct{})
	var posts atomic.Int32
	mux := http.NewServeMux()
	mux.Handle("GET /static/", web)
	mux.HandleFunc("GET /{$}", pageWith(t, web, `<script type="module" src="/page.js"></script>`+appScript+
		`<script type="module" src="/act.js"></script><img src="/held.svg" alt="" hidden>`))
	mux.HandleFunc("GET /page.js", file("testdata/page.js", "text/javascript; charset=utf-8"))
	mux.HandleFunc("GET /act.js", file("testdata/act.js", "text/javascript; charset=utf-8"))
	mux.HandleFunc("GET /held.svg", held(done))
	mux.HandleFunc("POST /act/{kind}/{node}", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusAccepted)
		if posts.Add(1) == 1 {
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
		for _, next := range []State{waiting, running, after} {
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
	if n := posts.Load(); n != 1 {
		t.Errorf("the page sent %d actions, want 1", n)
	}
	_, shots := snapshots(t, dom)
	for _, want := range []struct {
		st     State
		locked bool
		when   string
	}{
		{waiting, true, "accepted, not yet in the state"},
		{running, true, "running"},
		{after, false, "over, 12 s later"},
	} {
		shot, ok := shots[strconv.FormatInt(want.st.Now, 10)]
		if !ok {
			t.Fatalf("no snapshot for %q", want.when)
		}
		buttonsLocked(t, section(t, shot, "faults"), want.locked, want.when)
	}
}

func TestReceiptValuesInChrome(t *testing.T) {
	raw, err := os.ReadFile("../notary/testdata/vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	var vectors []vector
	if err := json.Unmarshal(raw, &vectors); err != nil {
		t.Fatal(err)
	}
	transfer := vectors[0]
	strange := transfer
	strange.Head = strings.Repeat("5a", 32)
	tr := recorded(t, transfer)
	receipts := [][]byte{receiptOf(t, transfer), receiptOf(t, strange), receiptOf(t, transfer)}
	bin, err := hex.DecodeString(transfer.Proof)
	if err != nil {
		t.Fatal(err)
	}

	var asked atomic.Int32
	web := NewWeb(newFake(), "", nil)
	mux := http.NewServeMux()
	mux.Handle("GET /static/", web)
	mux.HandleFunc("GET /{$}", pageWith(t, web, `<script type="module" src="/run.js"></script>`))
	mux.HandleFunc("GET /run.js", file("testdata/gaps.js", "text/javascript; charset=utf-8"))
	mux.HandleFunc("GET /proof", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(receipts[min(int(asked.Add(1))-1, len(receipts)-1)])
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
	for _, name := range []string{"fetched", "strange", "blind"} {
		if shots[name] == "" {
			t.Fatalf("no snapshot %q", name)
		}
	}
	word := func(shot, step string) string {
		for _, s := range stepPattern.FindAllStringSubmatch(shot, -1) {
			if s[1] == step {
				return s[3]
			}
		}
		t.Fatalf("no step %s", step)
		return ""
	}

	booked := time.Unix(0, int64(tr.Timestamp)).UTC() //nolint:gosec // G115: a time of the test vector
	for _, w := range []string{
		`<dd data-k="at"><time datetime="` + booked.Format("2006-01-02T15:04:05.000Z") + `">`,
		`echo '<span data-k="base64">` + base64.StdEncoding.EncodeToString(bin) + `</span>' | base64 -d`,
		`<span data-k="key1">` + strings.Repeat("ab", 32) + `</span> <span data-k="key2">` + transfer.Key + `</span> <span data-k="key3">` + strings.Repeat("ef", 32) + `</span>`,
		`Knoten <span data-k="signer">2</span> hat erklärt, dass die Kette dieses Clusters bei Glied <span data-k="seq">` + num(int(transfer.Seq)) + `</span>`, //nolint:gosec // G115: a number of the test vector
	} {
		if !strings.Contains(shots["fetched"], w) {
			t.Errorf("fetched: the page does not show %s", w)
		}
	}

	if got := word(shots["strange"], "go"); got != "stimmt nicht" {
		t.Errorf("the controller names another head, and step 7 says %q", got)
	}
	if got := word(shots["blind"], "stamp"); got != "" {
		t.Errorf("without Ed25519 the step of the stamp says %q", got)
	}
	for _, name := range []string{"strange", "blind"} {
		if strings.Contains(shots[name], "schlüssig") {
			t.Errorf("%s: the page calls the receipt sound", name)
		}
	}
}

func TestHiddenTabClosesItsStreamInChrome(t *testing.T) {
	web := NewWeb(newFake(), "", nil)
	app, err := webFS.ReadFile("web/app.js")
	if err != nil {
		t.Fatal(err)
	}
	quick := strings.Replace(string(app), "const AWAY = 60000", "const AWAY = 1000", 1)
	if quick == string(app) {
		t.Fatal("app.js does not hold const AWAY = 60000")
	}
	began := time.Now()
	var mu sync.Mutex
	var opened, closed []time.Duration
	done := make(chan struct{})
	mux := http.NewServeMux()
	mux.Handle("GET /static/", web)
	mux.HandleFunc("GET /static/app.js", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/javascript; charset=utf-8")
		_, _ = w.Write([]byte(quick))
	})
	mux.HandleFunc("GET /{$}", pageWith(t, web, `<script type="module" src="/hidden.js"></script>`+appScript+`<img src="/held.svg" alt="" hidden>`))
	mux.HandleFunc("GET /hidden.js", file("testdata/hidden.js", "text/javascript; charset=utf-8"))
	mux.HandleFunc("GET /held.svg", held(done))
	mux.HandleFunc("GET /events", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		opened = append(opened, time.Since(began))
		mu.Unlock()
		rc := http.NewResponseController(w)
		w.Header().Set("Content-Type", "text/event-stream")
		st := sampleState()
		_, _ = w.Write(retryLine)
		_, _ = w.Write(event("state", st))
		_ = rc.Flush()
		beat := time.NewTicker(time.Second)
		defer beat.Stop()
		for r.Context().Err() == nil {
			select {
			case <-beat.C:
				st.Now += 1000
				_, _ = w.Write(event("tick", st))
				_ = rc.Flush()
			case <-r.Context().Done():
			}
		}
		mu.Lock()
		defer mu.Unlock()
		closed = append(closed, time.Since(began))
		if len(closed) == 2 {
			close(done)
		}
	})
	srv := httptest.NewServer(guarded(mux))
	defer srv.Close()
	_, stderr := chrome(t, "--dump-dom", srv.URL+"/")
	clean(t, stderr)
	mu.Lock()
	defer mu.Unlock()
	if len(opened) != 2 || len(closed) != 2 {
		t.Fatalf("streams opened at %v and closed at %v; want two, each closed by the page", opened, closed)
	}
	if closed[0] > opened[1] || closed[1]-opened[1] < time.Second || closed[1]-opened[1] > 10*time.Second {
		t.Errorf("streams opened at %v and closed at %v; want the first closed while the tab was hidden from the start, the second a while after the tab was hidden again", opened, closed)
	}
}
