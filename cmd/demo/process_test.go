//go:build linux

package main

import (
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/Demonisreal/dmn-ledger/internal/local/localtest"
)

var bin string

func TestMain(m *testing.M) {
	var remove func()
	bin, remove = localtest.Build("", ".", "../ledgerd")
	code := m.Run()
	remove()
	os.Exit(code)
}

type proc struct {
	cmd    *exec.Cmd
	log    string
	exited chan struct{}
}

func launch(t *testing.T, env []string, args ...string) *proc {
	t.Helper()
	logf, err := os.CreateTemp(t.TempDir(), "demo-*.log")
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(filepath.Join(bin, "demo"), args...) //nolint:gosec // G204: the binary TestMain built
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

func (p *proc) exit(t *testing.T, within time.Duration) int {
	t.Helper()
	select {
	case <-p.exited:
		return p.cmd.ProcessState.ExitCode()
	case <-time.After(within):
		t.Fatalf("demo still runs after %v, log:\n%s", within, p.logs(t))
		return 0
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

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

func check(t *testing.T, env []string) (int, string) {
	t.Helper()
	cmd := exec.Command(filepath.Join(bin, "demo"), "-check") //nolint:gosec // G204: the binary TestMain built
	cmd.Env = env
	out, err := cmd.CombinedOutput()
	if _, exited := errors.AsType[*exec.ExitError](err); err != nil && !exited {
		t.Fatal(err)
	}
	return cmd.ProcessState.ExitCode(), string(out)
}

func TestExitCodes(t *testing.T) {
	t.Run("0 after SIGTERM, with -check before and after", func(t *testing.T) {
		addr := freeAddr(t)
		env := []string{"DEMO_DIR=" + filepath.Join(t.TempDir(), "data"), "DEMO_ADDR=" + addr}
		p := launch(t, env)
		deadline := time.Now().Add(30 * time.Second)
		var nodes []int
		for len(nodes) < 3 {
			if time.Now().After(deadline) {
				t.Fatalf("%d ledgerd after 30s, log:\n%s", len(nodes), p.logs(t))
			}
			time.Sleep(50 * time.Millisecond)
			nodes = localtest.Children(t, p.cmd.Process.Pid)
		}
		if code, out := check(t, env); code != 0 || out != "" {
			t.Fatalf("-check on the running demo: exit %d, %q", code, out)
		}
		if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
			t.Fatal(err)
		}
		code := p.exit(t, 20*time.Second)
		lines := strings.Split(strings.TrimSpace(p.logs(t)), "\n")
		first := fmt.Sprintf(" demo started: http on %s, origin http://%s, proxies none, ledgerd %s, data in ", addr, addr, filepath.Join(bin, "ledgerd"))
		if code != 0 || !strings.Contains(lines[0], first) || !strings.HasSuffix(lines[len(lines)-1], " demo stopped: terminated signal received") {
			t.Fatalf("exit %d, log:\n%s\nwant it to start with %q", code, p.logs(t), first)
		}
		for _, pid := range nodes {
			if _, err := os.Stat(fmt.Sprintf("/proc/%d", pid)); err == nil {
				t.Errorf("ledgerd %d lives after the demo ended", pid)
			}
		}
		if code, out := check(t, env); code != 1 || !strings.Contains(out, "connection refused") {
			t.Fatalf("-check after the end: exit %d, %q", code, out)
		}
	})
	t.Run("1 when no cluster comes up", func(t *testing.T) {
		quit, err := exec.LookPath("true")
		if err != nil {
			t.Skip(err)
		}
		p := launch(t, []string{"DEMO_DIR=" + t.TempDir(), "DEMO_ADDR=" + freeAddr(t), "DEMO_LEDGERD=" + quit})
		if code := p.exit(t, 60*time.Second); code != 1 || !strings.Contains(p.logs(t), " demo halted: demo: gave up after 5 attempts to set up a cluster: ") {
			t.Fatalf("exit %d, log:\n%s", code, p.logs(t))
		}
	})
	t.Run("2 on a configuration error", func(t *testing.T) {
		for _, tc := range []struct {
			env  []string
			args []string
			want string
		}{
			{[]string{"DEMO_ADDR=127.0.0.1:8080"}, nil, "demo: DEMO_DIR is missing\n"},
			{[]string{"DEMO_DIR=/data", "DEMO_RATE=10"}, nil, "demo: unknown variable DEMO_RATE\n"},
			{[]string{"DEMO_DIR=/data", "DEMO_ORIGIN=https://ledger.dmn-software.com/"}, nil, "demo: DEMO_ORIGIN=\"https://ledger.dmn-software.com/\": want https://ledger.dmn-software.com, as a browser sends it\n"},
			{[]string{"DEMO_DIR=/data"}, []string{"serve"}, "demo: no arguments, the only flag is -check\n"},
		} {
			p := launch(t, tc.env, tc.args...)
			if code := p.exit(t, 5*time.Second); code != 2 || p.logs(t) != tc.want {
				t.Errorf("%v %v: exit %d, stderr %q, want exit 2 and %q", tc.env, tc.args, code, p.logs(t), tc.want)
			}
		}
	})
	t.Run("3 when it cannot start", func(t *testing.T) {
		busy, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer busy.Close()
		locked := t.TempDir()
		if err := os.Chmod(locked, 0o500); err != nil { //nolint:gosec // G302: the directory is closed to writes on purpose
			t.Fatal(err)
		}
		for _, tc := range []struct {
			env  []string
			want string
		}{
			{[]string{"DEMO_DIR=" + t.TempDir(), "DEMO_ADDR=" + busy.Addr().String()}, "address already in use"},
			{[]string{"DEMO_DIR=" + t.TempDir(), "DEMO_ADDR=" + freeAddr(t), "DEMO_LEDGERD=/nonexistent/ledgerd"}, "no such file or directory"},
			{[]string{"DEMO_DIR=" + locked, "DEMO_ADDR=" + freeAddr(t)}, "permission denied"},
		} {
			p := launch(t, tc.env)
			if code, logs := p.exit(t, 5*time.Second), p.logs(t); code != 3 || !strings.Contains(logs, " demo cannot start: ") || !strings.Contains(logs, tc.want) {
				t.Errorf("%v: exit %d, log %q, want exit 3 and %q", tc.env, code, logs, tc.want)
			}
		}
	})
}
