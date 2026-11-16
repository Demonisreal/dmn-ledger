//go:build pprof && linux

package main

import (
	"context"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Demonisreal/dmn-ledger/ledger"
)

const profileRefused = ""

func TestProfileIsWritten(t *testing.T) {
	addrs := freeAddrs(t, 2)
	path := filepath.Join(t.TempDir(), "ledgerd.prof")
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	var logs lockedBuffer
	exited := make(chan int)
	go func() {
		exited <- run(ctx, nodeEnv(1, addrs[:1], addrs[1], t.TempDir(), "LEDGER_PROFILE="+path), &logs)
	}()
	awaitStatus(t, addrs[1], func(st ledger.NodeStatus) bool { return st.Role == ledger.Leader })
	calls, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	cl, err := ledger.Dial(calls, ledger.Config{Cluster: 1, Nodes: addrs[1:]})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cl.CreateAccounts(calls, []ledger.Account{{ID: ledger.From64(1), Ledger: 1, Code: 1}}); err != nil {
		t.Fatal(err)
	}
	if err := cl.Close(); err != nil {
		t.Fatal(err)
	}
	stop()
	if code := <-exited; code != 0 {
		t.Fatalf("exit %d, log:\n%s", code, logs.String())
	}
	out, err := exec.Command("go", "tool", "pprof", "-raw", path).CombinedOutput() //nolint:gosec // G204: the path comes from TempDir
	if err != nil {
		t.Fatalf("go tool pprof -raw: %v\n%s", err, out)
	}
	if !strings.Contains(string(out), "PeriodType: cpu nanoseconds\nPeriod: 4000000\n") {
		t.Fatalf("want a CPU profile at 250 Hz, pprof shows:\n%.400s", out)
	}
}

func TestProfileWantsAnAbsolutePath(t *testing.T) {
	vars := required()
	vars["LEDGER_PROFILE"] = "ledgerd.prof"
	if _, err := parse(environ(vars)); err == nil || err.Error() != `LEDGER_PROFILE="ledgerd.prof": want an absolute path` {
		t.Fatalf("relative LEDGER_PROFILE: %v", err)
	}
}
