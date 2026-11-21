//go:build linux

package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io/fs"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/Demonisreal/dmn-ledger/internal/disk"
	"github.com/Demonisreal/dmn-ledger/internal/notary"
	"github.com/Demonisreal/dmn-ledger/internal/replica"
	"github.com/Demonisreal/dmn-ledger/internal/wal"
	"github.com/Demonisreal/dmn-ledger/ledger"
)

var bin string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "ledgerd-bin-")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	build := exec.Command("go", "build", "-o", dir+string(filepath.Separator), ".", "../ledgerctl") //nolint:gosec // G204: the directory comes from MkdirTemp
	if out, err := build.CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "build ledgerd and ledgerctl: %v\n%s", err, out)
		_ = os.RemoveAll(dir)
		os.Exit(1)
	}
	bin = dir
	code := m.Run()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

type proc struct {
	cmd    *exec.Cmd
	log    string
	exited chan struct{}
}

func launch(t *testing.T, env ...string) *proc {
	t.Helper()
	logf, err := os.CreateTemp(t.TempDir(), "ledgerd-*.log")
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(filepath.Join(bin, "ledgerd")) //nolint:gosec // G204: the binary TestMain built
	cmd.Env = env
	cmd.Stderr = logf
	cmd.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGKILL}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	p := &proc{cmd: cmd, log: logf.Name(), exited: make(chan struct{})}
	go func() {
		_ = cmd.Wait()
		_ = logf.Close()
		close(p.exited)
	}()
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		<-p.exited
	})
	return p
}

func (p *proc) exit(t *testing.T) int {
	t.Helper()
	select {
	case <-p.exited:
		return p.cmd.ProcessState.ExitCode()
	case <-time.After(5 * time.Second):
		t.Fatalf("ledgerd still runs after 5s, log:\n%s", p.logs(t))
		return 0
	}
}

func (p *proc) signal(t *testing.T, sig os.Signal) {
	t.Helper()
	if err := p.cmd.Process.Signal(sig); err != nil {
		t.Fatal(err)
	}
}

func (p *proc) logs(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile(p.log)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func freeAddrs(t *testing.T, n int) []string {
	t.Helper()
	addrs := make([]string, n)
	for i := range addrs {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer l.Close()
		addrs[i] = l.Addr().String()
	}
	return addrs
}

func dataDir(t *testing.T, id, nodes int) string {
	t.Helper()
	dir := t.TempDir()
	if err := replica.Init(disk.OS{}, dir, wal.Identity{Cluster: 1, Node: uint64(id), Nodes: uint64(nodes)}); err != nil {
		t.Fatal(err)
	}
	return dir
}

func earlierMeta(t *testing.T, dir string) {
	t.Helper()
	raw := make([]byte, wal.MetaStride+wal.MetaSlotLen)
	for seq, off := range []int{0, wal.MetaStride} {
		slot := raw[off : off+wal.MetaSlotLen]
		binary.LittleEndian.PutUint64(slot, uint64(seq))
		binary.LittleEndian.PutUint64(slot[8:], 3)
		binary.LittleEndian.PutUint64(slot[16:], 2)
		binary.LittleEndian.PutUint32(slot[36:], crc32.Checksum(slot[:36], crc32.MakeTable(crc32.Castagnoli)))
	}
	if err := os.WriteFile(filepath.Join(dir, wal.MetaName), raw, 0o600); err != nil {
		t.Fatal(err)
	}
}

func loggedAlone(logs, line string) bool {
	return regexp.MustCompile(`^\d{4}/\d\d/\d\d \d\d:\d\d:\d\d\.\d{6} ` + regexp.QuoteMeta(line) + "\n$").MatchString(logs)
}

func nodeEnv(id int, peers []string, client, dir string, extra ...string) []string {
	return append([]string{
		"LEDGER_ID=" + strconv.Itoa(id),
		"LEDGER_CLUSTER=1",
		"LEDGER_PEERS=" + strings.Join(peers, ","),
		"LEDGER_CLIENT_ADDR=" + client,
		"LEDGER_DIR=" + dir,
	}, extra...)
}

func awaitStatus(t *testing.T, addr string, ready func(ledger.NodeStatus) bool) ledger.NodeStatus {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		st, err := ledger.Status(ctx, 1, addr)
		cancel()
		if err == nil && ready(st) {
			return st
		}
		if time.Now().After(deadline) {
			t.Fatalf("node at %s: status %+v, %v after 5s", addr, st, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func anyStatus(ledger.NodeStatus) bool { return true }

func TestExitCodes(t *testing.T) {
	t.Run("0 after SIGTERM and SIGINT", func(t *testing.T) {
		addrs := freeAddrs(t, 2)
		env := nodeEnv(1, addrs[:1], addrs[1], dataDir(t, 1, 1))
		for term, sig := range []os.Signal{syscall.SIGTERM, os.Interrupt} {
			p := launch(t, env...)
			awaitStatus(t, addrs[1], func(st ledger.NodeStatus) bool { return st.Role == ledger.Leader })
			p.signal(t, sig)
			code := p.exit(t)
			lines := strings.Split(strings.TrimSpace(p.logs(t)), "\n")
			first := fmt.Sprintf(" node 1 of 1 in cluster 1 started: peers on %s, clients on %s, term %d, repair no, no key", addrs[0], addrs[1], term)
			if code != 0 || !strings.HasSuffix(lines[0], first) || !strings.HasSuffix(lines[len(lines)-1], " node 1 stopped: "+sig.String()+" signal received") {
				t.Fatalf("%v: exit %d, log:\n%s\nwant it to start with %q", sig, code, p.logs(t), first)
			}
		}
	})
	t.Run("2 on a configuration error", func(t *testing.T) {
		open := filepath.Join(t.TempDir(), "node.key")
		if err := notary.WriteKey(open, ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize))); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(open, 0o644); err != nil { //nolint:gosec // G302: the key file is opened to its group on purpose
			t.Fatal(err)
		}
		for _, tc := range []struct {
			env  []string
			want string
		}{
			{[]string{"LEDGER_ID=1"}, "ledgerd: LEDGER_PEERS is missing\n"},
			{append(nodeEnv(1, []string{"127.0.0.1:7201"}, "127.0.0.1:7101", "/var/lib/ledger"), "LEDGER_SNAPSHOTS=5"), "ledgerd: unknown variable LEDGER_SNAPSHOTS\n"},
			{nodeEnv(1, []string{"127.0.0.1:7201"}, "127.0.0.1:7101", "/var/lib/ledger", "LEDGER_BUG=B11"), "ledgerd: unknown variable LEDGER_BUG\n"},
			{nodeEnv(1, []string{"127.0.0.1:7201"}, "127.0.0.1:7101", "/var/lib/ledger", "LEDGER_PROFILE=/var/tmp/ledgerd.prof"), "ledgerd: unknown variable LEDGER_PROFILE\n"},
			{nodeEnv(1, []string{"127.0.0.1:7201"}, "127.0.0.1:7101", "/var/lib/ledger", "LEDGER_TICK=2s"), "ledgerd: LEDGER_TICK=\"2s\": want a duration from 1ms to 1s\n"},
			{nodeEnv(1, []string{"127.0.0.1:7201"}, "127.0.0.1:7101", "/var/lib/ledger", "LEDGER_KEY_FILE="+open), "ledgerd: LEDGER_KEY_FILE=\"" + open + "\": notary: bad key file: " + open + " is open to group or others (mode 0644)\n"},
		} {
			p := launch(t, tc.env...)
			if code := p.exit(t); code != 2 || p.logs(t) != tc.want {
				t.Errorf("%v: exit %d, stderr %q, want exit 2 and %q", tc.env, code, p.logs(t), tc.want)
			}
		}
	})
	t.Run("2 when LEDGER_MAX_SESSIONS does not fit the directory", func(t *testing.T) {
		addrs := freeAddrs(t, 2)
		dir := dataDir(t, 1, 1)
		env := nodeEnv(1, addrs[:1], addrs[1], dir, "LEDGER_SNAPSHOT_EVERY=1")
		p := launch(t, env...)
		awaitStatus(t, addrs[1], func(st ledger.NodeStatus) bool { return st.Role == ledger.Leader })
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		cl, err := ledger.Dial(ctx, ledger.Config{Cluster: 1, Nodes: addrs[1:]})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := cl.CreateAccounts(ctx, []ledger.Account{{ID: ledger.From64(1), Ledger: 1, Code: 1}}); err != nil {
			t.Fatal(err)
		}
		_ = cl.Close()
		p.signal(t, syscall.SIGTERM)
		if code := p.exit(t); code != 0 {
			t.Fatalf("first start: exit %d, log:\n%s", code, p.logs(t))
		}
		p = launch(t, append(env, "LEDGER_MAX_SESSIONS=17")...)
		want := "node 1 cannot start: LEDGER_MAX_SESSIONS=17 does not fit the data directory, set the value the cluster runs with: replica: MaxSessions differs from the snapshot: the snapshot was written with 1024, this node has 17"
		if code := p.exit(t); code != 2 || !strings.Contains(p.logs(t), want) {
			t.Fatalf("exit %d, log:\n%s\nwant exit 2 and %q", code, p.logs(t), want)
		}
	})
	t.Run("2 or 3 on a directory that is not the node's", func(t *testing.T) {
		addrs := freeAddrs(t, 4)
		missing := filepath.Join(t.TempDir(), "missing")
		empty := t.TempDir()
		other, wider, single := dataDir(t, 2, 3), dataDir(t, 1, 3), dataDir(t, 1, 1)
		earlier := t.TempDir()
		earlierMeta(t, earlier)
		const noMeta = `node 1 cannot start: LEDGER_DIR=%s is not initialised, "ledgerctl init" makes the directory of a node that never ran, a node that lost its directory must stay down: wal: no meta: %s`
		const foreign = "node 1 cannot start: LEDGER_DIR=%[1]s belongs to another node or cluster, compare LEDGER_ID, LEDGER_PEERS, LEDGER_CLUSTER and LEDGER_DIR: replica: the directory belongs to another node or cluster: %[1]s holds %s, not %s"
		for _, tc := range []struct {
			name string
			env  []string
			code int
			want string
		}{
			{"no directory", nodeEnv(1, addrs[:1], addrs[3], missing), 2, fmt.Sprintf(noMeta, missing, missing+" does not exist")},
			{"empty directory", nodeEnv(1, addrs[:1], addrs[3], empty), 2, fmt.Sprintf(noMeta, empty, empty+"/meta")},
			{"directory of another node", nodeEnv(1, addrs[:3], addrs[3], other), 2, fmt.Sprintf(foreign, other, "node 2 of 3 in cluster 1", "node 1 of 3 in cluster 1")},
			{"of another number of nodes", nodeEnv(1, addrs[:1], addrs[3], wider), 2, fmt.Sprintf(foreign, wider, "node 1 of 3 in cluster 1", "node 1 of 1 in cluster 1")},
			{"of another cluster", nodeEnv(1, addrs[:1], addrs[3], single, "LEDGER_CLUSTER=2"), 2, fmt.Sprintf(foreign, single, "node 1 of 1 in cluster 1", "node 1 of 1 in cluster 2")},
			{"meta of an earlier build", nodeEnv(1, addrs[:1], addrs[3], earlier), 3, "node 1 cannot start: wal: unknown format version: " + earlier + "/meta holds no identity, an earlier build wrote it"},
		} {
			p := launch(t, tc.env...)
			if code := p.exit(t); code != tc.code || !loggedAlone(p.logs(t), tc.want) {
				t.Errorf("%s: exit %d, log:\n%s\nwant exit %d and the line %q", tc.name, code, p.logs(t), tc.code, tc.want)
			}
		}
	})
	t.Run("3 on a locked directory", func(t *testing.T) {
		addrs := freeAddrs(t, 4)
		dir := dataDir(t, 1, 1)
		first := launch(t, nodeEnv(1, addrs[:1], addrs[1], dir)...)
		awaitStatus(t, addrs[1], anyStatus)
		second := launch(t, nodeEnv(1, addrs[2:3], addrs[3], dir)...)
		if code := second.exit(t); code != 3 || !strings.Contains(second.logs(t), "node 1 cannot start: ") || !strings.Contains(second.logs(t), "directory locked") {
			t.Fatalf("exit %d, log:\n%s", code, second.logs(t))
		}
		awaitStatus(t, addrs[1], anyStatus)
		first.signal(t, syscall.SIGTERM)
		if code := first.exit(t); code != 0 {
			t.Fatalf("first node: exit %d", code)
		}
	})
	t.Run("3 on a busy port", func(t *testing.T) {
		busy, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer busy.Close()
		addrs := freeAddrs(t, 1)
		for name, env := range map[string][]string{
			"client port": nodeEnv(1, addrs, busy.Addr().String(), dataDir(t, 1, 1)),
			"peer port":   nodeEnv(1, []string{busy.Addr().String()}, addrs[0], dataDir(t, 1, 1)),
		} {
			p := launch(t, env...)
			if code := p.exit(t); code != 3 || !strings.Contains(p.logs(t), "address already in use") {
				t.Errorf("%s: exit %d, log:\n%s", name, code, p.logs(t))
			}
		}
	})
}

func TestLedgerdCreatesNoDirectory(t *testing.T) {
	addrs := freeAddrs(t, 2)
	missing := filepath.Join(t.TempDir(), "missing")
	empty := t.TempDir()
	for _, dir := range []string{missing, empty} {
		p := launch(t, nodeEnv(1, addrs[:1], addrs[1], dir)...)
		if code := p.exit(t); code != 2 {
			t.Fatalf("%s: exit %d, log:\n%s", dir, code, p.logs(t))
		}
	}
	if _, err := os.Stat(missing); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("ledgerd made %s: %v", missing, err)
	}
	ents, err := os.ReadDir(empty)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range ents {
		names = append(names, e.Name())
	}
	if !slices.Equal(names, []string{disk.LockName}) {
		t.Errorf("ledgerd left %v in an empty directory, want only %s", names, disk.LockName)
	}
}

func TestInitOutput(t *testing.T) {
	parent := t.TempDir()
	cmd := exec.Command(filepath.Join(bin, "ledgerctl"), "init", "-id", "2", "-nodes", "3", "n2") //nolint:gosec // G204: the binary TestMain built
	cmd.Dir = parent
	cmd.Env = []string{"LEDGER_CLUSTER=1"}
	var out, errs bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errs
	if err := cmd.Run(); err != nil {
		t.Fatalf("ledgerctl init: %v, stderr %q", err, errs.String())
	}
	dir := filepath.Join(parent, "n2")
	want := "initialised " + dir + " as node 2 of 3 in cluster 1\n" +
		"start it only if node 2 has never run in cluster 1: a node that ran has voted and acknowledged entries, and this directory knows none of that\n"
	if out.String() != want || errs.String() != "" {
		t.Fatalf("stdout %q, stderr %q, want %q", out.String(), errs.String(), want)
	}

	addrs := freeAddrs(t, 4)
	wrong := launch(t, nodeEnv(3, addrs[:3], addrs[3], dir)...)
	refused := "node 3 cannot start: LEDGER_DIR=" + dir + " belongs to another node or cluster, compare LEDGER_ID, LEDGER_PEERS, LEDGER_CLUSTER and LEDGER_DIR: replica: the directory belongs to another node or cluster: " + dir + " holds node 2 of 3 in cluster 1, not node 3 of 3 in cluster 1"
	if code := wrong.exit(t); code != 2 || !loggedAlone(wrong.logs(t), refused) {
		t.Fatalf("node 3: exit %d, log:\n%s\nwant exit 2 and the line %q", code, wrong.logs(t), refused)
	}
	right := launch(t, nodeEnv(2, addrs[:3], addrs[3], dir)...)
	awaitStatus(t, addrs[3], anyStatus)
	right.signal(t, syscall.SIGTERM)
	started := fmt.Sprintf(" node 2 of 3 in cluster 1 started: peers on %s, clients on %s, term 0, repair no, no key", addrs[1], addrs[3])
	if code := right.exit(t); code != 0 || !strings.Contains(right.logs(t), started+"\n") {
		t.Fatalf("node 2: exit %d, log:\n%s\nwant a line ending in %q", code, right.logs(t), started)
	}
}

func TestLedgerdRefusesStrangersAtThePeerPort(t *testing.T) {
	addrs := freeAddrs(t, 4)
	_, port, _ := net.SplitHostPort(addrs[0])
	peers := []string{addrs[0], addrs[1], addrs[2]}
	p := launch(t, nodeEnv(1, peers, addrs[3], dataDir(t, 1, 3))...)
	awaitStatus(t, addrs[3], anyStatus)
	stranger := dialFrom(t, "127.0.0.2", net.JoinHostPort("127.0.0.1", port))
	_ = stranger.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := stranger.Read(make([]byte, 1)); err == nil || isTimeout(err) {
		t.Fatalf("the peer port kept a connection from 127.0.0.2 open: %v", err)
	}
	p.signal(t, syscall.SIGTERM)
	if code := p.exit(t); code != 0 || !strings.Contains(p.logs(t), "refused peer from 127.0.0.2:") {
		t.Fatalf("exit %d, log:\n%s", code, p.logs(t))
	}
}
