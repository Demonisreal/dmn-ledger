//go:build linux

package main

import (
	"context"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/Demonisreal/dmn-ledger/internal/disk"
	"github.com/Demonisreal/dmn-ledger/ledger"
)

var statsFields = []string{"up", "rounds", "wait", "process", "slowest", "logsync", "logsync_ns", "metasync", "metasync_ns", "othersync", "othersync_ns", "written"}

func parseStats(t *testing.T, line string) map[string]uint64 {
	t.Helper()
	_, rest, ok := strings.Cut(line, " stats ")
	fields := strings.Fields(rest)
	if !ok || len(fields) != len(statsFields) {
		t.Fatalf("not a stats line with %d fields: %q", len(statsFields), line)
	}
	counts := make(map[string]uint64)
	for i, field := range fields {
		name, given, _ := strings.Cut(field, "=")
		n, err := strconv.ParseUint(given, 10, 64)
		if name != statsFields[i] || err != nil {
			t.Fatalf("field %d of %q: want %s=<number>", i+1, line, statsFields[i])
		}
		counts[name] = n
	}
	return counts
}

func (p *proc) statsLines(t *testing.T) []string {
	t.Helper()
	var lines []string
	for line := range strings.SplitSeq(p.logs(t), "\n") {
		if strings.Contains(line, " stats ") {
			lines = append(lines, line)
		}
	}
	return lines
}

func (p *proc) askStats(t *testing.T) map[string]uint64 {
	t.Helper()
	had := len(p.statsLines(t))
	p.signal(t, syscall.SIGUSR1)
	deadline := time.Now().Add(5 * time.Second)
	for {
		if lines := p.statsLines(t); len(lines) > had {
			return parseStats(t, lines[len(lines)-1])
		}
		if time.Now().After(deadline) {
			t.Fatalf("no stats line 5s after SIGUSR1, log:\n%s", p.logs(t))
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestStatsLine(t *testing.T) {
	addrs := freeAddrs(t, 2)
	p := launch(t, nodeEnv(1, addrs[:1], addrs[1], t.TempDir())...)
	awaitStatus(t, addrs[1], func(st ledger.NodeStatus) bool { return st.Role == ledger.Leader })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cl, err := ledger.Dial(ctx, ledger.Config{Cluster: 1, Nodes: addrs[1:]})
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	if res, err := cl.CreateAccounts(ctx, []ledger.Account{{ID: ledger.From64(1), Ledger: 1, Code: 1}, {ID: ledger.From64(2), Ledger: 1, Code: 1}}); err != nil || len(res.Failures) > 0 {
		t.Fatalf("create accounts: %+v, %v", res, err)
	}
	before := p.askStats(t)
	if res, err := cl.CreateTransfers(ctx, []ledger.Transfer{{ID: ledger.From64(10), DebitAccountID: ledger.From64(1), CreditAccountID: ledger.From64(2), Amount: ledger.From64(5), Ledger: 1, Code: 1}}); err != nil || len(res.Failures) > 0 {
		t.Fatalf("transfer: %+v, %v", res, err)
	}
	after := p.askStats(t)
	grew := func(name string) uint64 { return after[name] - before[name] }
	switch {
	case grew("logsync") != 1 || grew("metasync") != 1:
		t.Fatalf("one transfer: %d syncs of the log and %d of meta, want one each\nbefore %v\nafter  %v", grew("logsync"), grew("metasync"), before, after)
	case grew("logsync_ns") == 0 || grew("metasync_ns") == 0 || grew("written") == 0:
		t.Fatalf("one transfer: %d ns in the log sync, %d ns in the meta sync, %d bytes written", grew("logsync_ns"), grew("metasync_ns"), grew("written"))
	case grew("rounds") == 0 || grew("up") == 0 || after["slowest"] == 0:
		t.Fatalf("one transfer: %d rounds more, up %d ns more, slowest %d ns", grew("rounds"), grew("up"), after["slowest"])
	case grew("process") < grew("logsync_ns")+grew("metasync_ns"):
		t.Fatalf("Process grew by %d ns, less than the %d ns of its syncs", grew("process"), grew("logsync_ns")+grew("metasync_ns"))
	case after["wait"]+after["process"] > after["up"]:
		t.Fatalf("wait %d ns and process %d ns add up to more than up %d ns", after["wait"], after["process"], after["up"])
	}
	p.signal(t, syscall.SIGTERM)
	if code := p.exit(t); code != 0 {
		t.Fatalf("exit %d, log:\n%s", code, p.logs(t))
	}
	logged := strings.Split(strings.TrimSpace(p.logs(t)), "\n")
	stats := p.statsLines(t)
	if len(stats) != 3 || len(logged) < 2 || logged[len(logged)-2] != stats[2] || !strings.Contains(logged[len(logged)-1], " node 1 stopped: ") {
		t.Fatalf("want the third stats line right before the last line, log:\n%s", p.logs(t))
	}
	if last := parseStats(t, stats[2]); last["logsync"] < after["logsync"] || last["up"] <= after["up"] {
		t.Fatalf("stats at the end %v, after the transfer %v", last, after)
	}
}

func TestDisksAreCountedByKind(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "wal")
	c := &countingFS{FS: disk.OS{}, dir: dir}
	for _, sub := range []string{"wal", "snap", "recv"} {
		if err := c.MkdirAll(filepath.Join(dir, sub)); err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range []string{"wal/00000000000000000001.wal", "wal/00000000000000000009.wal.tmp", "meta", "meta.tmp", "snap/snap-1.snap", "recv/2.tmp", "LOCK"} {
		f, err := c.Create(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.WriteAt([]byte("abc"), 0); err != nil {
			t.Fatal(err)
		}
		if err := f.Sync(); err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
	}
	f, err := c.Open(filepath.Join(dir, "meta"))
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if err := c.SyncDir(filepath.Join(dir, "wal")); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		kind string
		s    *syncs
		want uint64
	}{{"log", &c.log, 2}, {"meta", &c.meta, 3}, {"other", &c.other, 4}} {
		if n, ns := tc.s.n.Load(), tc.s.ns.Load(); n != tc.want || ns <= 0 {
			t.Errorf("%s: %d syncs in %d ns, want %d", tc.kind, n, ns, tc.want)
		}
	}
	if n := c.written.Load(); n != 21 {
		t.Errorf("%d bytes written, want 21", n)
	}
}
