package main

import (
	"bytes"
	"context"
	"errors"
	"log"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

func TestServeTurnsTheEndIntoTheExitCode(t *testing.T) {
	gaveUp := errors.New("demo: gave up after 5 attempts to set up a cluster: exit status 1")
	broke := errors.New("accept tcp 127.0.0.1:8080: accept4: too many open files")
	stopped, stop := context.WithCancelCause(context.Background())
	stop(errors.New("terminated signal received"))
	for _, tc := range []struct {
		name             string
		ctx              context.Context
		stageErr, webErr error
		alive            bool
		code             int
		last             string
		order            []string
	}{
		{"signal", stopped, nil, nil, true, 0, "demo stopped: terminated signal received", []string{"web", "stage"}},
		{"the stage gives up", context.Background(), gaveUp, nil, true, 1, "demo halted: " + gaveUp.Error(), []string{"stage", "web"}},
		{"the server fails", context.Background(), nil, broke, true, 1, "demo halted: " + broke.Error(), []string{"web", "stage"}},
		{"the watchdog", context.Background(), nil, nil, false, 1, "demo halted: the stage or its sight stopped reporting", []string{"web", "stage"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var mu sync.Mutex
			var ended []string
			part := func(name string, err error) func(context.Context) error {
				return func(ctx context.Context) error {
					if err == nil {
						<-ctx.Done()
					}
					mu.Lock()
					defer mu.Unlock()
					ended = append(ended, name)
					return err
				}
			}
			var logs bytes.Buffer
			code := serve(tc.ctx, part("stage", tc.stageErr), part("web", tc.webErr), func() bool { return tc.alive }, log.New(&logs, "", 0))
			if lines := strings.TrimSpace(logs.String()); code != tc.code || lines != tc.last || !slices.Equal(ended, tc.order) {
				t.Fatalf("exit %d, log %q, ended %v; want exit %d, %q, %v", code, lines, ended, tc.code, tc.last, tc.order)
			}
		})
	}
}

func TestCheckAsksHealthz(t *testing.T) {
	var status atomic.Int32
	status.Store(http.StatusOK)
	var paths sync.Map
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths.Store(r.URL.Path, true)
		w.WriteHeader(int(status.Load()))
	}))
	at := netip.MustParseAddrPort(srv.Listener.Addr().String())
	env := func(addr string) []string { return []string{"DEMO_DIR=/data", "DEMO_ADDR=" + addr} }
	check := func(environ []string) (int, string) {
		var stderr bytes.Buffer
		code := probe(environ, &stderr)
		return code, stderr.String()
	}
	if code, out := check(env(at.String())); code != 0 || out != "" {
		t.Fatalf("alive: exit %d, stderr %q", code, out)
	}
	if code, out := check(env(netip.AddrPortFrom(netip.IPv4Unspecified(), at.Port()).String())); code != 0 || out != "" {
		t.Fatalf("on 0.0.0.0: exit %d, stderr %q", code, out)
	}
	if _, ok := paths.Load("/healthz"); !ok {
		t.Fatal("the check did not ask /healthz")
	}
	status.Store(http.StatusServiceUnavailable)
	want := "demo: http://" + at.String() + "/healthz answered 503 Service Unavailable\n"
	if code, out := check(env(at.String())); code != 1 || out != want {
		t.Fatalf("stuck: exit %d, stderr %q, want 1 and %q", code, out, want)
	}
	srv.Close()
	if code, out := check(env(at.String())); code != 1 || !strings.Contains(out, "connection refused") {
		t.Fatalf("nothing listens: exit %d, stderr %q", code, out)
	}
	if code, out := check([]string{"DEMO_ADDR=" + at.String()}); code != 1 || out != "demo: DEMO_DIR is missing\n" {
		t.Fatalf("no DEMO_DIR: exit %d, stderr %q", code, out)
	}
}
