//go:build linux

package main

import (
	"syscall"
	"time"
)

const (
	tcpUserTimeout = 18
	userTimeout    = 5 * time.Second
)

func control(_, _ string, c syscall.RawConn) error {
	var opt error
	err := c.Control(func(fd uintptr) {
		opt = syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, tcpUserTimeout, int(userTimeout.Milliseconds())) //nolint:gosec // G115: a file descriptor fits an int
	})
	if err != nil {
		return err
	}
	return opt
}
