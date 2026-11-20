//go:build linux

package localtest

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func Build(tags string, pkgs ...string) (dir string, remove func()) {
	dir, err := os.MkdirTemp("", "ledgerd-bin-")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	remove = func() { _ = os.RemoveAll(dir) }
	args := append([]string{"build", "-tags", tags, "-o", dir + string(filepath.Separator)}, pkgs...)
	build := exec.Command("go", args...) //nolint:gosec // G204: packages and tags of the calling test
	if out, err := build.CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "build %s: %v\n%s", strings.Join(pkgs, " "), err, out)
		remove()
		os.Exit(1)
	}
	return dir, remove
}

func Children(t *testing.T, parent int) []int {
	t.Helper()
	entries, err := os.ReadDir("/proc")
	if err != nil {
		t.Fatal(err)
	}
	var pids []int
	for _, e := range entries {
		pid, err := strconv.Atoi(e.Name())
		if err != nil {
			continue
		}
		stat, err := os.ReadFile(filepath.Join("/proc", e.Name(), "stat")) //nolint:gosec // G304: a file under /proc
		if err != nil {
			continue
		}
		comm, rest, _ := strings.Cut(string(stat), ") ")
		f := strings.Fields(rest)
		if strings.HasSuffix(comm, "(ledgerd") && len(f) > 1 && f[1] == strconv.Itoa(parent) {
			pids = append(pids, pid)
		}
	}
	return pids
}

func ProcState(pid int) string {
	stat, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid)) //nolint:gosec // G304: a file under /proc
	if err != nil {
		return "gone"
	}
	_, rest, _ := strings.Cut(string(stat), ") ")
	return strings.Fields(rest)[0]
}

func Await(t *testing.T, within time.Duration, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(within)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatalf("%s: not within %v", what, within)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
