package demo

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/netip"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

func (s *stream) until(t *testing.T, name string) sse {
	t.Helper()
	for {
		ev := s.event(t)
		for _, f := range ev.fields {
			if strings.HasPrefix(f, "id:") {
				t.Errorf("event %s carries %q", ev.name, f)
			}
		}
		if ev.name == name {
			return ev
		}
	}
}

type seenState struct {
	Phase   string
	Next    int64
	Cluster Cluster
	Nodes   [3]Node
	Books   struct{ Sum Sum }
	Action  json.RawMessage
	Lines   []Line
}

func decode[T any](t *testing.T, ev sse) T {
	t.Helper()
	var into T
	if err := json.Unmarshal([]byte(ev.data), &into); err != nil {
		t.Fatalf("%s: %v in %q", ev.name, err, ev.data)
	}
	return into
}

func TestStreamStartsWithState(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		show := newFake()
		l := serveWeb(t, show)
		time.Sleep(time.Second)
		s := l.watch(t, "203.0.113.7:1", "Last-Event-ID", "42")
		if s.res.StatusCode != http.StatusOK || s.res.Header.Get("Content-Type") != "text/event-stream" {
			t.Fatalf("status %d, type %q", s.res.StatusCode, s.res.Header.Get("Content-Type"))
		}
		if ev := s.event(t); len(ev.fields) != 1 || ev.fields[0] != "retry: 3000" {
			t.Fatalf("the stream begins with %q, want retry: 3000", ev.fields)
		}
		first := s.event(t)
		if first.name != "state" || len(first.fields) != 2 || !strings.HasPrefix(first.fields[1], "data: {") {
			t.Fatalf("first event %q, want state with one data line", first.fields)
		}
		st := decode[seenState](t, first)
		if len(st.Lines) != logTail || st.Lines[0].N != 41 || st.Lines[logTail-1].N != 100 {
			t.Fatalf("state has %d lines, want 41 to 100", len(st.Lines))
		}
		if st.Cluster != sampleState().Cluster || st.Nodes != sampleState().Nodes || st.Books.Sum.Value != "6000000" {
			t.Errorf("state %+v differs from the show", st)
		}

		show.write("node 2 stopped: context canceled")
		if l := decode[Line](t, s.until(t, "line")); l.N != 101 || l.Text != "node 2 stopped: context canceled" {
			t.Errorf("line %+v, want number 101", l)
		}
		tick := s.until(t, "tick")
		if strings.Contains(tick.data, `"lines"`) {
			t.Error("a tick carries the lines")
		}

		show.change(func(st *State) { st.Cluster.N, st.Cluster.ID, st.Cluster.Why = 4, "00000000000000a1", "limit" })
		show.write("cluster 4 set up")
		again := decode[seenState](t, s.until(t, "state"))
		if again.Cluster.N != 4 || len(again.Lines) != logTail || again.Lines[logTail-1].N != 102 {
			t.Errorf("state after the reset: cluster %d, %d lines", again.Cluster.N, len(again.Lines))
		}
		show.write("after the reset")
		if l := decode[Line](t, s.until(t, "line")); l.N != 103 {
			t.Errorf("after the state came line %d, want 103", l.N)
		}
	})
}

func TestLateViewerSeesTheRunningAction(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		show := newFake()
		l := serveWeb(t, show)
		l.watch(t, "203.0.113.7:1").drain()
		time.Sleep(2 * time.Second)
		now := time.Now().UnixMilli()
		running := Action{Kind: Freeze, Target: 0, Node: 1, Role: "leader", Term: 7, Phase: "struck", Began: now, LiftAt: now + 5000}
		show.change(func(st *State) {
			st.Phase, st.Next = "action", now+10000
			st.Action = &running
		})
		time.Sleep(3 * time.Second)
		late := l.watch(t, "198.51.100.4:1")
		st := decode[seenState](t, late.until(t, "state"))
		want, _ := json.Marshal(running)
		if string(st.Action) != string(want) || st.Phase != "action" || st.Next != now+10000 {
			t.Errorf("a late viewer sees action %s, phase %s, next %d; want %s", st.Action, st.Phase, st.Next, want)
		}
		_, page := l.open(t, "198.51.100.4:2").do(t, request("GET", "/"))
		if !strings.Contains(page, "Läuft: n1 ist eingefroren, noch ") {
			t.Error("the page does not show the running action")
		}
	})
}

func TestStreamCaps(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		show := newFake()
		l := serveWeb(t, show)
		open := func(from string) *stream {
			s := l.watch(t, from)
			if s.res.StatusCode == http.StatusOK {
				s.drain()
			}
			return s
		}
		refused := func(s *stream, status int) bool {
			if s.res.StatusCode != status || s.res.Header.Get("Retry-After") != "3" {
				return false
			}
			for key, want := range wantGuards {
				if s.res.Header.Get(key) != want {
					return false
				}
			}
			body, _ := io.ReadAll(s.res.Body)
			return len(body) == 0
		}
		var first []*stream
		for i := range streamsPerAddr {
			s := open(fmt.Sprintf("198.51.100.1:%d", i+1))
			if s.res.StatusCode != http.StatusOK {
				t.Fatalf("stream %d of one address: status %d", i+1, s.res.StatusCode)
			}
			first = append(first, s)
		}
		if !refused(open("198.51.100.1:9"), http.StatusTooManyRequests) {
			t.Error("the fifth stream of one address was not refused with an empty 429")
		}
		for i := range streamsPerAddr {
			if s := open(fmt.Sprintf("[2001:db8:1:2::%x]:1", i+1)); s.res.StatusCode != http.StatusOK {
				t.Fatalf("stream %d of one /64: status %d", i+1, s.res.StatusCode)
			}
		}
		if !refused(open("[2001:db8:1:2::ff]:1"), http.StatusTooManyRequests) {
			t.Error("the fifth stream of one /64 was not refused with an empty 429")
		}
		if s := open("[2001:db8:1:3::1]:1"); s.res.StatusCode != http.StatusOK {
			t.Errorf("a stream of the next /64: status %d", s.res.StatusCode)
		}
		for n := 2*streamsPerAddr + 1; n < maxStreams; n++ {
			if s := open(fmt.Sprintf("192.0.2.%d:%d", n/streamsPerAddr, n)); s.res.StatusCode != http.StatusOK {
				t.Fatalf("stream %d: status %d", n+1, s.res.StatusCode)
			}
		}
		if !refused(open("203.0.113.200:1"), http.StatusServiceUnavailable) {
			t.Errorf("stream %d was not refused with an empty 503", maxStreams+1)
		}
		time.Sleep(beatEvery)
		synctest.Wait()
		if _, _, watching := show.counted(); watching != maxStreams {
			t.Errorf("the show was told %d viewers, want %d", watching, maxStreams)
		}

		_ = first[0].conn.Close()
		synctest.Wait()
		if s := open("203.0.113.200:2"); s.res.StatusCode != http.StatusOK {
			t.Errorf("after a stream closed, a new one got status %d", s.res.StatusCode)
		}
		if !refused(open("203.0.113.201:1"), http.StatusServiceUnavailable) {
			t.Error("the closed stream freed two places")
		}
		_ = first[1].conn.Close()
		synctest.Wait()
		if s := open("198.51.100.1:10"); s.res.StatusCode != http.StatusOK {
			t.Errorf("after a stream of the address closed, a new one got status %d", s.res.StatusCode)
		}
	})
}

func TestSlowReaderIsDropped(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		show := newFake()
		show.moving = true
		l := serveWeb(t, show)
		fast := l.watch(t, "203.0.113.1:1")
		slow := l.watch(t, "203.0.113.2:1")
		began := time.Now()
		ticks := make(chan time.Duration, 4096)
		go func() {
			for {
				ev, err := fast.next()
				if err != nil {
					close(ticks)
					return
				}
				if ev.name == "tick" {
					ticks <- time.Since(began)
				}
			}
		}()
		pace := len(event("tick", sampleState())) / 4
		dropped := make(chan time.Duration, 1)
		go func() {
			buf := make([]byte, pace)
			for {
				if _, err := io.ReadFull(slow.body, buf); err != nil {
					dropped <- time.Since(began)
					return
				}
				time.Sleep(100 * time.Millisecond)
			}
		}()
		took := <-dropped
		if took > time.Minute {
			t.Fatalf("a reader of %d bytes a second was dropped after %v", pace*10, took)
		}
		time.Sleep(beatEvery)
		synctest.Wait()
		if _, _, watching := show.counted(); watching != 1 {
			t.Errorf("the show was told %d viewers after the drop, want 1", watching)
		}
		_ = fast.conn.Close()
		n := 0
		for at := range ticks {
			if at <= took {
				n++
			}
		}
		if want := int(took/beatEvery) - 2; n < want {
			t.Errorf("the fast reader got %d ticks in %v, want at least %d", n, took, want)
		}
		t.Logf("a reader of %d bytes a second was dropped after %v; the other got %d ticks", pace*10, took, n)
	})
}

func TestStreamEndsAfterTenMinutes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := serveWeb(t, newFake())
		s := l.watch(t, "203.0.113.7:1")
		began := time.Now()
		ended := s.drain()
		time.Sleep(10*time.Minute - time.Second)
		select {
		case at := <-ended:
			t.Fatalf("the stream ended after %v", at.Sub(began))
		default:
		}
		if took := (<-ended).Sub(began); took < 10*time.Minute || took > 10*time.Minute+beatEvery {
			t.Errorf("the stream ended after %v, want 10m0s", took)
		}
	})
}

func TestHubReportsTheLastViewerGone(t *testing.T) {
	show := newFake()
	h := newHub(show)
	v, _ := h.join(netip.MustParsePrefix("192.0.2.1/32"))
	h.beat()
	if _, _, watching := show.counted(); watching != 1 {
		t.Fatalf("%d viewers reported with one stream", watching)
	}
	h.leave(v)
	h.beat()
	if _, _, watching := show.counted(); watching != 0 {
		t.Fatalf("%d viewers reported after the last stream closed", watching)
	}
}

func TestTickRate(t *testing.T) {
	for _, moving := range []bool{true, false} {
		synctest.Test(t, func(t *testing.T) {
			show := newFake()
			show.moving = moving
			l := serveWeb(t, show)
			s := l.watch(t, "203.0.113.7:1")
			s.until(t, "state")
			beats := []time.Time{time.Now()}
			for len(beats) < 41 {
				if ev := s.event(t); ev.name == "tick" {
					beats = append(beats, time.Now())
				}
			}
			for i := 1; i < len(beats); i++ {
				if gap := beats[i].Sub(beats[i-1]); gap > time.Second {
					t.Errorf("changing %v: %v without a tick", moving, gap)
				}
				if i >= 4 && beats[i].Sub(beats[i-4]) < time.Second {
					t.Errorf("changing %v: five ticks within %v", moving, beats[i].Sub(beats[i-4]))
				}
			}
			t.Logf("changing %v: 40 ticks in %v", moving, beats[40].Sub(beats[0]))
		})
	}
	st := sampleState()
	t.Logf("a tick of the sample state is %d bytes", len(event("tick", st)))
	for i := range 20 {
		st.Totals.Findings = append(st.Totals.Findings, Finding{At: 1795032000000 + int64(i), Check: "S8", Reason: strings.Repeat("r", 300), ResetAt: 1795032030000, Evidence: "finding-1795032000000"})
	}
	st.Finding = &st.Totals.Findings[0]
	t.Logf("with 20 findings of 300 characters it is %d bytes", len(event("tick", st)))
}
