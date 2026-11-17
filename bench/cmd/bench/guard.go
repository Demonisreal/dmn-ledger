package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/Demonisreal/dmn-ledger/bench/pg"
)

var (
	errNoLimit  = errors.New("no memory limit on this process, start the plan with scripts/bench.sh")
	errMemory   = errors.New("not enough memory")
	errLeftover = errors.New("left over from an earlier run, scripts/bench.sh clean removes it")
	errRunning  = errors.New("the container " + pg.Name + " already runs, scripts/bench.sh clean removes it")
	errDevice   = errors.New("the Postgres data and the bench directory are on different devices")
)

const (
	spare      = 1 << 30
	memoryWait = time.Minute
	memoryPoll = 5 * time.Second
)

func ownCgroup(fsys fs.FS) (string, error) {
	b, err := fs.ReadFile(fsys, "proc/self/cgroup")
	if err != nil {
		return "", err
	}
	for line := range strings.Lines(string(b)) {
		if rest, ok := strings.CutPrefix(line, "0::"); ok {
			return strings.Trim(strings.TrimSpace(rest), "/"), nil
		}
	}
	return "", fmt.Errorf("no cgroup v2 line in /proc/self/cgroup")
}

func cgroupMemory(fsys fs.FS, cg string) (limit, used uint64, err error) {
	read := func(name string) (string, error) {
		b, err := fs.ReadFile(fsys, path.Join("sys/fs/cgroup", cg, name))
		return strings.TrimSpace(string(b)), err
	}
	most, err := read("memory.max")
	if err != nil {
		return 0, 0, err
	}
	now, err := read("memory.current")
	if err != nil {
		return 0, 0, err
	}
	if used, err = strconv.ParseUint(now, 10, 64); err != nil {
		return 0, 0, err
	}
	if most == "max" {
		return 0, used, nil
	}
	limit, err = strconv.ParseUint(most, 10, 64)
	return limit, used, err
}

func available(fsys fs.FS) (uint64, error) {
	b, err := fs.ReadFile(fsys, "proc/meminfo")
	if err != nil {
		return 0, err
	}
	for line := range strings.Lines(string(b)) {
		if rest, ok := strings.CutPrefix(line, "MemAvailable:"); ok {
			kb, err := strconv.ParseUint(strings.TrimSuffix(strings.TrimSpace(rest), " kB"), 10, 64)
			return kb << 10, err
		}
	}
	return 0, fmt.Errorf("no MemAvailable in /proc/meminfo")
}

func headroom(fsys fs.FS, cgroups []string) (uint64, error) {
	room := uint64(spare)
	for _, cg := range cgroups {
		limit, used, err := cgroupMemory(fsys, cg)
		if err != nil {
			return 0, err
		}
		if limit == 0 {
			return 0, fmt.Errorf("%w: cgroup %s", errNoLimit, cg)
		}
		room += limit - min(used, limit)
	}
	return room, nil
}

func (b *bench) capTo(ctx context.Context, bytes uint64) error {
	if b.limit == 0 {
		return nil
	}
	if limit, _, err := cgroupMemory(b.root, b.cgroup); err != nil || limit == bytes {
		return err
	}
	unit := path.Base(b.cgroup)
	out, err := exec.CommandContext(ctx, "systemctl", "--user", "set-property", "--runtime", unit, "MemoryMax="+strconv.FormatUint(bytes, 10)).CombinedOutput() //nolint:gosec // G204: the scope this plan runs in
	if err != nil {
		return fmt.Errorf("set the memory limit of %s to %d MiB: %w: %s", unit, bytes>>20, err, strings.TrimSpace(string(out)))
	}
	limit, _, err := cgroupMemory(b.root, b.cgroup)
	if err == nil && limit != bytes {
		err = fmt.Errorf("the memory limit of %s is %d MiB after setting it to %d MiB", unit, limit>>20, bytes>>20)
	}
	return err
}

func awaitMemory(ctx context.Context, fsys fs.FS, cgroups []string, log io.Writer) error {
	deadline := time.Now().Add(memoryWait)
	said := false
	for {
		want, err := headroom(fsys, cgroups)
		if err != nil {
			return err
		}
		have, err := available(fsys)
		if err != nil {
			return err
		}
		if have >= want {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%w: %d MiB available, a run may take %d MiB more and 1 GiB must stay free; close something and start the plan again, it goes on where it stopped",
				errMemory, have>>20, (want-spare)>>20)
		}
		if !said {
			fmt.Fprintf(log, "waiting for memory: %d MiB available, want %d MiB\n", have>>20, want>>20)
			said = true
		}
		if !pause(ctx, memoryPoll) {
			return ctx.Err()
		}
	}
}

func pause(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func leftovers(dir string) error {
	for _, sub := range []string{"run", "pg"} {
		entries, err := os.ReadDir(path.Join(dir, sub))
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
		if len(entries) > 0 {
			return fmt.Errorf("%w: %s holds %d entries", errLeftover, path.Join(dir, sub), len(entries))
		}
	}
	return nil
}

func containerExists(ctx context.Context) (bool, error) {
	var stderr bytes.Buffer
	cmd := exec.CommandContext(ctx, "docker", "container", "inspect", "--format", "{{.Name}}", pg.Name) //nolint:gosec // G204: looks up our own container by its fixed name
	cmd.Stderr = &stderr
	err := cmd.Run()
	var exit *exec.ExitError
	switch {
	case err == nil:
		return true, nil
	case errors.As(err, &exit) && strings.Contains(strings.ToLower(stderr.String()), "no such"):
		return false, nil
	}
	return false, fmt.Errorf("docker container inspect %s: %w: %s", pg.Name, err, strings.TrimSpace(stderr.String()))
}

func sameDevice(a, b string) error {
	var sa, sb syscall.Stat_t
	if err := syscall.Stat(a, &sa); err != nil {
		return err
	}
	if err := syscall.Stat(b, &sb); err != nil {
		return err
	}
	if sa.Dev != sb.Dev {
		return fmt.Errorf("%w: %s on %d, %s on %d", errDevice, a, sa.Dev, b, sb.Dev)
	}
	return nil
}

func ownLedgerd(fsys fs.FS) []int {
	comms, err := fs.Glob(fsys, "proc/*/comm")
	if err != nil {
		return nil
	}
	self := os.Getpid()
	var pids []int
	for _, name := range comms {
		b, err := fs.ReadFile(fsys, name)
		if err != nil || strings.TrimSpace(string(b)) != "ledgerd" {
			continue
		}
		dir := path.Dir(name)
		stat, err := fs.ReadFile(fsys, path.Join(dir, "stat"))
		if err != nil {
			continue
		}
		f := strings.Fields(string(stat[bytes.LastIndexByte(stat, ')')+1:]))
		if len(f) > 1 && f[1] == strconv.Itoa(self) {
			pid, _ := strconv.Atoi(path.Base(dir))
			pids = append(pids, pid)
		}
	}
	return pids
}
