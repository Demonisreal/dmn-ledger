//go:build linux

package demo

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

const (
	chromeLimit  = 30 * time.Second
	chromeMemory = 2500
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
	flags := []string{"--headless=new", "--disable-gpu", "--no-first-run", "--user-data-dir=" + t.TempDir(),
		"--virtual-time-budget=8000", "--enable-logging=stderr"}
	cmd := exec.CommandContext(ctx, bin, append(flags, args...)...) //nolint:gosec // G204: the browser of the machine, the flags are fixed
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
	dom, stderr := chrome(t, "--dump-dom", srv.URL+"/vectors.html")
	want := fmt.Sprintf("vectors %d differ 0", len(cases))
	if !strings.Contains(dom, want) {
		t.Fatalf("the page says %q, want %q; stderr:\n%s", dom, want, stderr)
	}
	t.Log("the page says", want)
	if strings.Contains(stderr, "Refused to") {
		t.Errorf("the policy refused something:\n%s", stderr)
	}
}
