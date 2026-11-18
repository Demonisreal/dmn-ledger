//go:build linux

package local

import (
	"fmt"
	"os/exec"
	"sync"
	"syscall"
)

type Node struct {
	c   *Cluster
	id  int
	dir string
	env []string

	mu     sync.Mutex
	cmd    *exec.Cmd
	exited chan struct{}
	held   bool
}

func (n *Node) ID() int {
	return n.id
}

func (n *Node) Dir() string {
	return n.dir
}

func (n *Node) Pid() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.cmd == nil {
		return 0
	}
	return n.cmd.Process.Pid
}

func (n *Node) Start() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	switch {
	case n.held:
		return nil
	case n.cmd != nil:
		return fmt.Errorf("%w: n%d as process %d", ErrRunning, n.id, n.cmd.Process.Pid)
	}
	cmd := exec.Command(n.c.cfg.Bin) //nolint:gosec // G204: the ledgerd the caller built, without arguments
	cmd.Env = n.env
	cmd.Stderr = n.c.cfg.Stderr(n.id)
	cmd.SysProcAttr = &syscall.SysProcAttr{Pdeathsig: syscall.SIGKILL}
	if err := cmd.Start(); err != nil {
		return err
	}
	exited := make(chan struct{})
	n.cmd, n.exited = cmd, exited
	go func() {
		err := cmd.Wait()
		n.mu.Lock()
		alone := n.cmd == cmd
		n.mu.Unlock()
		if alone && n.c.cfg.Exited != nil {
			n.c.cfg.Exited(n.id, err)
		}
		n.mu.Lock()
		if n.cmd == cmd {
			n.cmd = nil
		}
		n.mu.Unlock()
		close(exited)
	}()
	return nil
}

func (n *Node) Running() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.cmd != nil
}

func (n *Node) Signal(sig syscall.Signal) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.cmd != nil && !n.held {
		_ = n.cmd.Process.Signal(sig)
	}
}

func (n *Node) Kill() {
	n.end(false)
}

func (n *Node) Stop() {
	n.end(true)
}

func (n *Node) end(hold bool) {
	n.mu.Lock()
	if n.cmd == nil || n.held && !hold {
		n.held = n.held || hold
		n.mu.Unlock()
		return
	}
	n.held = n.held || hold
	_ = n.cmd.Process.Kill()
	exited := n.exited
	n.cmd = nil
	n.mu.Unlock()
	<-exited
}

func (n *Node) Halt() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.held = true
	if n.cmd != nil {
		_ = n.cmd.Process.Signal(syscall.SIGSTOP)
	}
}
