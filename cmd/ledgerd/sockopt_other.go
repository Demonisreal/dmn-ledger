//go:build !linux

package main

import "syscall"

var control func(network, address string, c syscall.RawConn) error
