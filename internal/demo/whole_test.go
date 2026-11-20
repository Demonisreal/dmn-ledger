//go:build linux

package demo

import (
	"bufio"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/netip"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/Demonisreal/dmn-ledger/internal/local/localtest"
	"github.com/Demonisreal/dmn-ledger/ledger"
)

func publish(t *testing.T, s *Stage, proxies []netip.Prefix) (addr, origin string) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr = l.Addr().String()
	origin = "http://" + addr
	web := NewWeb(s, origin, proxies)
	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan error, 1)
	go func() { served <- web.Serve(ctx, l) }()
	t.Cleanup(func() {
		cancel()
		if err := <-served; err != nil {
			t.Errorf("serve: %v", err)
		}
	})
	return addr, origin
}

func dial(t *testing.T, addr string) *pipeClient {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return &pipeClient{conn: conn, br: bufio.NewReader(conn)}
}

func watchAt(t *testing.T, addr string, headers ...string) *stream {
	t.Helper()
	c := dial(t, addr)
	c.send(request("GET", "/events", headers...))
	res := c.read(t)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("GET /events: status %d", res.StatusCode)
	}
	return &stream{pipeClient: c, res: res, body: bufio.NewReader(res.Body)}
}

type logBook struct {
	mu   sync.Mutex
	text strings.Builder
}

func (h *logBook) Write(b []byte) (int, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.text.Write(b)
}

func (h *logBook) String() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.text.String()
}

type proofAnswer struct {
	status int
	reason string
	proof  Proof
}

func askProof(t *testing.T, addr string) proofAnswer {
	t.Helper()
	res, body := dial(t, addr).do(t, request("GET", "/proof"))
	pa := proofAnswer{status: res.StatusCode}
	if res.StatusCode != http.StatusOK {
		var why refusal
		_ = json.Unmarshal([]byte(body), &why)
		pa.reason = why.Error
		return pa
	}
	if err := json.Unmarshal([]byte(body), &pa.proof); err != nil {
		t.Fatalf("proof %q: %v", body, err)
	}
	return pa
}

func verify(p Proof) (ledger.Proof, error) {
	raw, err := hex.DecodeString(p.Receipt)
	if err != nil {
		return ledger.Proof{}, fmt.Errorf("receipt %q: %w", p.Receipt, err)
	}
	id, err := strconv.ParseUint(p.Cluster, 16, 64)
	if err != nil {
		return ledger.Proof{}, fmt.Errorf("cluster %q: %w", p.Cluster, err)
	}
	keys := make([][]byte, len(p.Keys))
	for i, k := range p.Keys {
		if keys[i], err = hex.DecodeString(k); err != nil {
			return ledger.Proof{}, fmt.Errorf("key %q: %w", k, err)
		}
	}
	vp, err := ledger.VerifyProof(raw, id, keys)
	if err != nil {
		return ledger.Proof{}, fmt.Errorf("the receipt for transfer %s does not verify with the keys it came with: %w", p.Transfer.ID, err)
	}
	return vp, nil
}

func TestProofVerifies(t *testing.T) {
	s := stage(t, func(s *Stage) { s.pace.pause = standard.pause })
	addr, _ := publish(t, s, nil)
	var mu sync.Mutex
	type sighting struct {
		b  Booking
		at time.Time
	}
	seen := map[string]sighting{}
	var last State
	watched := watchAt(t, addr)
	go func() {
		for {
			ev, err := watched.next()
			if err != nil {
				return
			}
			if ev.name != "state" && ev.name != "tick" {
				continue
			}
			var st State
			if json.Unmarshal([]byte(ev.data), &st) != nil {
				continue
			}
			mu.Lock()
			for _, b := range st.Recent {
				seen[b.ID] = sighting{b, time.Now()}
			}
			last = st
			mu.Unlock()
		}
	}()
	localtest.Await(t, 30*time.Second, "ten bookings in the stream", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(seen) >= 10
	})
	matched, tries := "", 0
	for ; tries < 20 && matched == ""; tries++ {
		time.Sleep(proofHold + 100*time.Millisecond)
		pa := askProof(t, addr)
		if pa.status != http.StatusOK {
			t.Fatalf("GET /proof: status %d, %q", pa.status, pa.reason)
		}
		p := pa.proof
		vp, err := verify(p)
		if err != nil {
			t.Fatal(err)
		}
		mu.Lock()
		st := last
		sg, ok := seen[p.Transfer.ID]
		mu.Unlock()
		if p.Cluster != st.Cluster.ID {
			t.Fatalf("proof of cluster %s, the stream shows cluster %s", p.Cluster, st.Cluster.ID)
		}
		for i, n := range st.Nodes {
			if p.Keys[i] != n.Key {
				t.Fatalf("key %d of the proof is %s, the stream shows %s for n%d", i+1, p.Keys[i], n.Key, n.ID)
			}
		}
		if vp.Transfer.ID.String() != p.Transfer.ID || vp.Transfer.Amount.String() != p.Transfer.Amount {
			t.Fatalf("the receipt holds transfer %v over %v, the answer names %s over %s", vp.Transfer.ID, vp.Transfer.Amount, p.Transfer.ID, p.Transfer.Amount)
		}
		if !ok || time.Since(sg.at) > 2*time.Second {
			continue
		}
		if b := sg.b; b.Debit != p.Transfer.Debit || b.Credit != p.Transfer.Credit || b.Amount != p.Transfer.Amount || b.Refused != "" {
			t.Fatalf("proof of %+v, the stream showed %+v", p.Transfer, sg.b)
		}
		matched = p.Transfer.ID
	}
	if matched == "" {
		t.Fatal("no proof named a booking that recent showed in the last two seconds")
	}
	t.Logf("request %d proved transfer %s, a booking recent showed in the last two seconds", tries, matched)
	mu.Lock()
	pids := []int{last.Nodes[0].Pid, last.Nodes[1].Pid, last.Nodes[2].Pid}
	mu.Unlock()
	thaw := func() {
		for _, pid := range pids {
			_ = syscall.Kill(pid, syscall.SIGCONT)
		}
	}
	defer thaw()
	for _, pid := range pids {
		if err := syscall.Kill(pid, syscall.SIGSTOP); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(proofHold)
	if pa := askProof(t, addr); pa.status != http.StatusServiceUnavailable || pa.reason != "noleader" {
		t.Fatalf("with all three nodes stopped: status %d, %q; want 503 noleader", pa.status, pa.reason)
	}
	thaw()
	localtest.Await(t, 30*time.Second, "a proof again", func() bool {
		time.Sleep(proofHold)
		return askProof(t, addr).status == http.StatusOK
	})
	if st := s.State(); st.Totals.Found != 0 {
		t.Fatalf("finding %s: %s", st.Totals.Findings[0].Check, st.Totals.Findings[0].Reason)
	}
}

func TestNoRequestDataInState(t *testing.T) {
	t.Run("fake", noRequestDataInFake)
	t.Run("stage", func(t *testing.T) {
		const visitor = "203.0.113.77"
		var logs logBook
		s := stage(t, func(s *Stage) { s.cfg.Log = log.New(io.MultiWriter(&logs, testLog{t}), "", 0) })
		addr, origin := publish(t, s, []netip.Prefix{netip.MustParsePrefix("127.0.0.1/32")})
		as := func(headers ...string) []string { return append([]string{"X-Forwarded-For", visitor}, headers...) }
		var bodies []string
		for _, raw := range []string{
			request("POST", "/act/kill/1?zq7x=%3Cscript%3E", as("Origin", origin, "User-Agent", "zq7x<script>", "Referer", "https://zq7x.example/",
				"Cookie", "zq7x=1", "X-Zq7x", "<b>zq7x</b>", "Accept-Language", "zq7x")...),
			request("POST", "/act/zq7x/1", as("Origin", origin)...),
			request("POST", "/act/kill/zq7x", as("Origin", origin)...),
			request("POST", "/act/flip/leader", as("Origin", "https://zq7x.example")...),
			request("POST", "/act/cut/2", "Origin", origin, "X-Forwarded-For", visitor+", zq7x"),
			request("GET", "/zq7x/%3Cscript%3E", as()...),
			request("GET", "/?zq7x=%3Cscript%3E", as("User-Agent", "zq7x")...),
			request("GET", "/proof?zq7x=1", as("X-Zq7x", "zq7x")...),
			request("GET", "/events?zq7x=1", as("Origin", "https://zq7x.example")...),
			request("GET", "/static/zq7x.js", as()...),
			request("GET", "/healthz?zq7x", as()...),
		} {
			_, body := dial(t, addr).do(t, raw)
			bodies = append(bodies, body)
		}
		localtest.Await(t, 30*time.Second, "the kill done", func() bool {
			st := s.State()
			return st.Action != nil && st.Action.Ended != 0
		})
		if st := s.State(); st.Totals.Actions.Kill != 1 || st.Action.Phase != "done" {
			t.Fatalf("kills %d, action %+v; want the kill of the visitor done", st.Totals.Actions.Kill, st.Action)
		}
		time.Sleep(renderEvery)
		_, page := dial(t, addr).do(t, request("GET", "/", as()...))
		_, proof := dial(t, addr).do(t, request("GET", "/proof", as()...))
		greeting := watchAt(t, addr, as("User-Agent", "zq7x", "Last-Event-ID", "zq7x")...).until(t, "state")
		st, err := json.Marshal(s.State())
		if err != nil {
			t.Fatal(err)
		}
		var journal strings.Builder
		for _, l := range s.Lines(0) {
			journal.WriteString(l.Src + " " + l.Text + "\n")
		}
		bodies = append(bodies, page, proof, greeting.data, string(st), journal.String(), logs.String())
		for _, body := range bodies {
			for _, bad := range []string{"zq7x", visitor} {
				if strings.Contains(body, bad) {
					t.Errorf("%q in %.300s", bad, body)
				}
			}
		}
		if !strings.Contains(page, `data-phase="`) || !strings.Contains(journal.String(), "SIGKILL an n1") {
			t.Error("the page was not rendered, or the kill is not in the journal")
		}
	})
}
