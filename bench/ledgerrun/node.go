package ledgerrun

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Demonisreal/dmn-ledger/bench/load"
	"github.com/Demonisreal/dmn-ledger/ledger"
)

var statsFields = []string{"up", "rounds", "wait", "process", "slowest", "logsync", "logsync_ns", "metasync", "metasync_ns", "othersync", "othersync_ns", "written"}

type node struct {
	id      int
	addr    string
	dir     string
	log     string
	env     []string
	cores   string
	cmd     *exec.Cmd
	stats   *statsLog
	exited  chan struct{}
	stopped bool
	status  ledger.NodeStatus
	err     error
}

type statsLog struct {
	file  *os.File
	mu    sync.Mutex
	rest  []byte
	last  string
	fresh chan struct{}
}

func (n *node) name() string {
	return "n" + strconv.Itoa(n.id)
}

func (n *node) start(bin string) error {
	logf, err := os.OpenFile(filepath.Clean(n.log), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	cmd := exec.Command(bin) //nolint:gosec // G204: the ledgerd that Build made
	if n.cores != "" {
		cmd = exec.Command("taskset", "-c", n.cores, bin) //nolint:gosec // G204: the ledgerd that Build made, on the cores of its node
	}
	n.stats = &statsLog{file: logf, fresh: make(chan struct{})}
	cmd.Env = n.env
	cmd.Stderr = n.stats
	cmd.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGKILL, Setpgid: true}
	if err := cmd.Start(); err != nil {
		_ = logf.Close()
		return err
	}
	n.cmd, n.exited = cmd, make(chan struct{})
	go func() {
		n.err = cmd.Wait()
		_ = logf.Close()
		close(n.exited)
	}()
	return nil
}

func (n *node) stop() error {
	select {
	case <-n.exited:
		return n.failure("ended on its own")
	default:
	}
	_ = n.cmd.Process.Signal(syscall.SIGTERM)
	timer := time.NewTimer(stopWithin)
	defer timer.Stop()
	select {
	case <-n.exited:
		if n.err != nil {
			return n.failure("did not stop cleanly")
		}
		return nil
	case <-timer.C:
	}
	_ = n.cmd.Process.Kill()
	<-n.exited
	return fmt.Errorf("%w: %s still ran %v after SIGTERM and was killed", ErrNode, n.name(), stopWithin)
}

func (n *node) ready(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, readyWithin)
	defer cancel()
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		asked, done := context.WithTimeout(ctx, sampleWithin)
		st, err := ledger.Status(asked, cluster, n.addr)
		done()
		if err == nil && st.Leader != 0 {
			n.status = st
			return nil
		}
		select {
		case <-n.exited:
			return n.failure("ended while starting")
		case <-ctx.Done():
			return fmt.Errorf("%w that %s knows of: %w", ErrNoLeader, n.name(), errors.Join(context.Cause(ctx), err))
		case <-tick.C:
		}
	}
}

func (n *node) failure(what string) error {
	return fmt.Errorf("%w: %s %s (%v), its log ends with:\n%s", ErrNode, n.name(), what, n.err, tail(n.log, 10))
}

func (n *node) sample(ctx context.Context) (load.Part, error) {
	fresh := n.stats.next()
	if err := n.cmd.Process.Signal(syscall.SIGUSR1); err != nil {
		return load.Part{}, fmt.Errorf("%w: %s: %w", ErrNode, n.name(), err)
	}
	type answer struct {
		st  ledger.NodeStatus
		err error
	}
	asked := make(chan answer, 1)
	go func() {
		st, err := ledger.Status(ctx, cluster, n.addr)
		asked <- answer{st, err}
	}()
	select {
	case <-fresh:
	case <-n.exited:
		return load.Part{}, n.failure("ended")
	case <-ctx.Done():
		return load.Part{}, fmt.Errorf("%w: no stats line from %s: %w", ErrNode, n.name(), ctx.Err())
	}
	fields, err := parseStats(n.stats.line())
	if err != nil {
		return load.Part{}, fmt.Errorf("%s: %w", n.name(), err)
	}
	a := <-asked
	if a.err == nil {
		n.status = a.st
	}
	st := n.status
	fields["applied"] = st.Applied
	if kb, err := peakRSS(n.cmd.Process.Pid); err == nil {
		fields["peak_rss"] = kb << 10
	}
	if size := newestSnapshot(filepath.Join(n.dir, "snap")); size > 0 {
		fields["snapshot_bytes"] = size
	}
	p := load.Part{
		Name:     n.name(),
		Role:     st.Role.String(),
		Term:     st.Term,
		Syncs:    fields["logsync"] + fields["metasync"] + fields["othersync"],
		SyncTime: time.Duration(fields["logsync_ns"] + fields["metasync_ns"] + fields["othersync_ns"]), //nolint:gosec // G115: nanoseconds since the node started
		Counters: fields,
		Stale:    a.err != nil,
	}
	if st.Role == ledger.Leader {
		p.Requests = st.Applied
	}
	return p, nil
}

func (s *statsLog) Write(b []byte) (int, error) {
	n, err := s.file.Write(b)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rest = append(s.rest, b...)
	for {
		line, rest, ok := bytes.Cut(s.rest, []byte{'\n'})
		if !ok {
			break
		}
		if bytes.Contains(line, []byte(" stats ")) {
			s.last = string(line)
			close(s.fresh)
			s.fresh = make(chan struct{})
		}
		s.rest = rest
	}
	return n, err
}

func (s *statsLog) next() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.fresh
}

func (s *statsLog) line() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.last
}

func parseStats(line string) (map[string]uint64, error) {
	_, rest, ok := strings.Cut(line, " stats ")
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrStats, line)
	}
	fields := make(map[string]uint64, len(statsFields)+1)
	for field := range strings.FieldsSeq(rest) {
		name, num, ok := strings.Cut(field, "=")
		n, err := strconv.ParseUint(num, 10, 64)
		if !ok || err != nil {
			return nil, fmt.Errorf("%w: field %q in %q", ErrStats, field, line)
		}
		fields[name] = n
	}
	for _, name := range statsFields {
		if _, ok := fields[name]; !ok {
			return nil, fmt.Errorf("%w: no %s in %q", ErrStats, name, line)
		}
	}
	return fields, nil
}

func peakRSS(pid int) (uint64, error) {
	b, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
	if err != nil {
		return 0, err
	}
	for line := range strings.Lines(string(b)) {
		if rest, ok := strings.CutPrefix(line, "VmHWM:"); ok {
			return strconv.ParseUint(strings.TrimSuffix(strings.TrimSpace(rest), " kB"), 10, 64)
		}
	}
	return 0, fmt.Errorf("ledgerrun: no VmHWM for process %d", pid)
}

func newestSnapshot(dir string) uint64 {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "snap-") && strings.HasSuffix(e.Name(), ".snap") {
			names = append(names, e.Name())
		}
	}
	if len(names) == 0 {
		return 0
	}
	fi, err := os.Stat(filepath.Join(dir, slices.Max(names)))
	if err != nil {
		return 0
	}
	return uint64(fi.Size()) //nolint:gosec // G115: the size of a file
}

func tail(path string, lines int) string {
	b, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return err.Error()
	}
	all := strings.Split(strings.TrimRight(string(b), "\n"), "\n")
	return strings.Join(all[max(0, len(all)-lines):], "\n")
}
