//go:build linux && soak

package demo

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"net"
	"net/http"
	"net/netip"
	"os"
	"runtime"
	"runtime/debug"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

var (
	soakDuration = flag.Duration("soak.duration", 2*time.Minute, "how long TestSoak runs")
	soakTarget   = flag.String("soak.target", "", "a demo that runs already, like http://127.0.0.1:18080; TestSoak then starts no stage of its own")
)

const (
	heapMost   = 96 << 20
	actionMost = 45 * time.Second
)

type tally struct {
	mu     sync.Mutex
	counts map[string]int
}

func (ta *tally) add(key string) {
	ta.mu.Lock()
	defer ta.mu.Unlock()
	ta.counts[key]++
}

func (ta *tally) String() string {
	keys := make([]string, 0, len(ta.counts))
	for k := range ta.counts {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = fmt.Sprintf("%s %d", k, ta.counts[k])
	}
	return strings.Join(parts, ", ")
}

type counted struct {
	io.Reader
	n int64
}

func (c *counted) Read(p []byte) (int, error) {
	n, err := c.Reader.Read(p)
	c.n += int64(n)
	return n, err
}

type watcher struct {
	from   string
	rate   int
	leave  time.Duration
	opens  int
	drops  int
	shut   int
	bytes  int64
	events func(ev sse)
}

func (w *watcher) run(ctx context.Context, hc *http.Client, base string) {
	life := streamLife + time.Minute
	if w.leave > 0 {
		life = w.leave
	}
	for ctx.Err() == nil {
		watch, cancel := context.WithTimeout(ctx, life)
		req, _ := http.NewRequestWithContext(watch, http.MethodGet, base+"/events", nil)
		req.Header.Set("X-Forwarded-For", w.from)
		res, err := hc.Do(req)
		if err != nil || res.StatusCode != http.StatusOK {
			if err == nil {
				_ = res.Body.Close()
				w.shut++
			}
			cancel()
			sleep(ctx, time.Second)
			continue
		}
		w.opens++
		began := time.Now()
		body := &counted{Reader: res.Body}
		if w.rate > 0 {
			w.trickle(watch, body)
		} else {
			w.follow(body)
		}
		w.bytes += body.n
		_ = res.Body.Close()
		if watch.Err() == nil && time.Since(began) < streamLife-time.Second {
			w.drops++
		}
		cancel()
	}
}

func (w *watcher) follow(body io.Reader) {
	s := &stream{body: bufio.NewReader(body)}
	for {
		ev, err := s.next()
		if err != nil {
			return
		}
		if w.events != nil {
			w.events(ev)
		}
	}
}

func (w *watcher) trickle(ctx context.Context, body io.Reader) {
	buf := make([]byte, w.rate/4)
	for sleep(ctx, 250*time.Millisecond) {
		if _, err := body.Read(buf); err != nil {
			return
		}
	}
}

type shown struct {
	fail       func(format string, args ...any)
	cluster    int
	phase      string
	actionAt   int64
	setupAt    int64
	longSetup  time.Duration
	longAction time.Duration
	longSettle time.Duration
	done       map[int64]bool
	kinds      tally
	last       State
	states     int
	lines      int
}

func (sh *shown) see(ev sse) {
	switch ev.name {
	case "line":
		sh.lines++
		return
	case "":
		return
	}
	var st State
	if err := json.Unmarshal([]byte(ev.data), &st); err != nil {
		sh.fail("%s: %v in %.200s", ev.name, err, ev.data)
		return
	}
	sh.states++
	sh.last = st
	if f := st.Finding; f != nil {
		sh.fail("finding %s: %s", f.Check, f.Reason)
	} else if st.Totals.Found > 0 {
		f := st.Totals.Findings[len(st.Totals.Findings)-1]
		sh.fail("finding %s: %s", f.Check, f.Reason)
	}
	if v := st.Books.Sum.Value; v != "" && v != fmt.Sprint(want) {
		sh.fail("sum %s in a %s of cluster %d, want %d", v, ev.name, st.Cluster.N, want)
	}
	if sh.cluster != 0 && st.Cluster.N != sh.cluster && st.Cluster.Why != "limit" {
		sh.fail("cluster %d set up %s", st.Cluster.N, st.Cluster.Why)
	}
	sh.cluster = st.Cluster.N
	if st.Phase != sh.phase {
		switch {
		case st.Phase == "action":
			sh.actionAt = st.Now
		case st.Phase == "setup":
			sh.setupAt = st.Now
		case st.Phase == "steady" && sh.phase == "setup" && sh.setupAt != 0:
			sh.longSetup = max(sh.longSetup, time.Duration(st.Now-sh.setupAt)*time.Millisecond)
		}
		sh.phase = st.Phase
	}
	if since := time.Duration(st.Now-sh.actionAt) * time.Millisecond; st.Phase == "action" && since > actionMost {
		sh.fail("phase action for %v", since)
	}
	a, t := st.Action, st.Totals.Actions
	ended := t.Kill + t.Freeze + t.Cut + t.Flip
	if a != nil && a.Ended == 0 {
		ended--
	}
	if st.Totals.Healed < ended {
		sh.fail("%d healed after %d actions, the last %+v", st.Totals.Healed, ended, a)
	}
	if a == nil || a.Ended == 0 || sh.done[a.Began] {
		return
	}
	sh.done[a.Began] = true
	kind := a.Kind.String()
	if a.Kind == Flip {
		kind += " repair " + a.Repair
	}
	sh.kinds.add(kind)
	sh.longAction = max(sh.longAction, time.Duration(a.Ended-a.Began)*time.Millisecond)
	sh.longSettle = max(sh.longSettle, time.Duration(a.Ended-a.LiftAt)*time.Millisecond)
}

func cpu(who int) time.Duration {
	var ru syscall.Rusage
	_ = syscall.Getrusage(who, &ru)
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}

func TestSoak(t *testing.T) {
	base := strings.TrimSuffix(*soakTarget, "/")
	goroutines := runtime.NumGoroutine()
	selfBefore, nodesBefore := cpu(syscall.RUSAGE_SELF), cpu(syscall.RUSAGE_CHILDREN)
	var lower func()
	if base == "" {
		old := debug.SetMemoryLimit(heapMost)
		defer debug.SetMemoryLimit(old)
		s := New(Config{Dir: t.TempDir(), Ledgerd: bin, Log: log.New(os.Stderr, "", log.LstdFlags|log.Lmicroseconds|log.LUTC)})
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		base = "http://" + l.Addr().String()
		web := NewWeb(s, base, []netip.Prefix{netip.MustParsePrefix("127.0.0.1/32")})
		ctx, cancel := context.WithCancel(context.Background())
		var wg sync.WaitGroup
		var runErr, serveErr error
		wg.Go(func() { runErr = s.Run(ctx) })
		wg.Go(func() { serveErr = web.Serve(ctx, l) })
		lower = sync.OnceFunc(func() {
			cancel()
			wg.Wait()
			if runErr != nil || serveErr != nil {
				t.Errorf("Run: %v, Serve: %v", runErr, serveErr)
			}
		})
		defer lower()
	}
	ctx, stop := context.WithTimeout(context.Background(), *soakDuration)
	defer stop()
	fail := func(format string, args ...any) {
		t.Errorf(format, args...)
		stop()
	}
	transport := &http.Transport{MaxIdleConnsPerHost: 4}
	hc := &http.Client{Transport: transport, Timeout: 10 * time.Second}
	streams := &http.Client{Transport: transport}
	sh := &shown{fail: fail, done: map[int64]bool{}, kinds: tally{counts: map[string]int{}}}
	all := &watcher{from: "192.0.2.1", events: sh.see}
	slow := &watcher{from: "192.0.2.2", rate: 1 << 10}
	leaving := &watcher{from: "192.0.2.3", leave: 20 * time.Second}
	refused := &tally{counts: map[string]int{}}
	accepted, proved, heap := 0, 0, uint64(0)
	began := time.Now()
	var wg sync.WaitGroup
	for _, w := range []*watcher{all, slow, leaving} {
		wg.Go(func() { w.run(ctx, streams, base) })
	}
	wg.Go(func() {
		rng := rand.New(rand.NewPCG(uint64(began.UnixNano()), 1))
		for sleep(ctx, 3*time.Second+time.Duration(rng.Int64N(int64(9*time.Second)))) {
			kind := kinds[rng.IntN(len(kinds))].String()
			target := []string{"leader", "1", "2", "3"}[rng.IntN(4)]
			var body io.Reader
			origin, want := base, 0
			switch rng.IntN(30) {
			case 0:
				origin, want = "", http.StatusForbidden
			case 1:
				kind, want = "reboot", http.StatusNotFound
			case 2:
				body, want = strings.NewReader("{}"), http.StatusBadRequest
			}
			req, _ := http.NewRequestWithContext(ctx, http.MethodPost, base+"/act/"+kind+"/"+target, body)
			req.Header.Set("X-Forwarded-For", fmt.Sprintf("198.51.100.%d", rng.IntN(64)))
			if origin != "" {
				req.Header.Set("Origin", origin)
			}
			res, err := hc.Do(req)
			if err != nil {
				if ctx.Err() == nil {
					fail("POST %s: %v", req.URL.Path, err)
				}
				return
			}
			var why refusal
			_ = json.NewDecoder(res.Body).Decode(&why)
			_ = res.Body.Close()
			switch {
			case want != 0 && res.StatusCode != want:
				fail("POST %s, origin %q, body %v: status %d, want %d", req.URL.Path, origin, body != nil, res.StatusCode, want)
			case want != 0:
				refused.add(strconv.Itoa(res.StatusCode))
			case res.StatusCode == http.StatusAccepted:
				accepted++
			case why.Error == "":
				fail("POST %s: status %d", req.URL.Path, res.StatusCode)
			default:
				refused.add(why.Error)
			}
		}
	})
	wg.Go(func() {
		for sleep(ctx, time.Second) {
			for _, path := range []string{"/", "/proof"} {
				req, _ := http.NewRequestWithContext(ctx, http.MethodGet, base+path, nil)
				res, err := hc.Do(req)
				if err != nil {
					if ctx.Err() == nil {
						fail("GET %s: %v", path, err)
					}
					return
				}
				got, _ := io.ReadAll(res.Body)
				_ = res.Body.Close()
				var p Proof
				switch {
				case path == "/" && res.StatusCode != http.StatusOK:
					fail("GET /: status %d", res.StatusCode)
				case path == "/":
				case res.StatusCode == http.StatusServiceUnavailable:
					var why refusal
					_ = json.Unmarshal(got, &why)
					refused.add("proof " + why.Error)
				case res.StatusCode != http.StatusOK:
					fail("GET /proof: status %d, %q", res.StatusCode, got)
				case json.Unmarshal(got, &p) != nil:
					fail("GET /proof: %q", got)
				default:
					if _, err := verify(p); err != nil {
						fail("GET /proof: %v", err)
					}
					proved++
				}
			}
		}
	})
	if lower != nil {
		wg.Go(func() {
			var ms runtime.MemStats
			for sleep(ctx, 10*time.Second) {
				runtime.ReadMemStats(&ms)
				heap = max(heap, ms.HeapAlloc)
				if ms.HeapAlloc > heapMost {
					fail("heap %d MiB", ms.HeapAlloc>>20)
				}
			}
		})
	}
	wg.Wait()
	ran := time.Since(began)
	transport.CloseIdleConnections()
	usage := "cpu not measured, the demo runs elsewhere"
	if lower != nil {
		lower()
		self, nodes := cpu(syscall.RUSAGE_SELF)-selfBefore, cpu(syscall.RUSAGE_CHILDREN)-nodesBefore
		usage = fmt.Sprintf("cpu of this process %v (%.1f%% of a core), of the nodes %v (%.1f%%)",
			self.Round(time.Millisecond), 100*self.Seconds()/ran.Seconds(), nodes.Round(time.Millisecond), 100*nodes.Seconds()/ran.Seconds())
		if left := children(t); len(left) > 0 {
			t.Errorf("ledgerd still running after the soak: %v", left)
		}
		await(t, 5*time.Second, fmt.Sprintf("at most %d goroutines", goroutines+10), func() bool { return runtime.NumGoroutine() <= goroutines+10 })
	}
	allowed := int(ran / standard.gap)
	if accepted*2 < allowed {
		t.Errorf("%d actions accepted, A3 allows %d in %v", accepted, allowed, ran.Round(time.Second))
	}
	st := sh.last
	t.Logf("soak %v: %d clusters, %d bookings, %d windows; actions %s; accepted %d of %d allowed; refused %s; "+
		"longest action %v, lifted to counted %v, longest setup %v; reader of all: %d states, %d lines, %.2f MB, %d drops in %d streams; "+
		"slow reader: %d drops in %d streams, %d refused; leaving reader: %d streams; %d proofs verified; heap at most %.1f MiB; %s",
		ran.Round(time.Second), st.Totals.Clusters, st.Totals.Confirmed, st.Totals.Windows, sh.kinds.String(), accepted, allowed, refused.String(),
		sh.longAction, sh.longSettle, sh.longSetup, sh.states, sh.lines, float64(all.bytes)/1e6, all.drops, all.opens,
		slow.drops, slow.opens, slow.shut, leaving.opens, proved, float64(heap)/(1<<20), usage)
}
