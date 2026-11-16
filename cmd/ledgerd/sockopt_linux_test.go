//go:build linux

package main

import (
	"net"
	"syscall"
	"testing"
)

func sockopts(t *testing.T, conn net.Conn) map[string]int {
	raw, err := conn.(*net.TCPConn).SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	opts := map[string][2]int{
		"user timeout": {syscall.IPPROTO_TCP, tcpUserTimeout},
		"keepalive":    {syscall.SOL_SOCKET, syscall.SO_KEEPALIVE},
		"idle":         {syscall.IPPROTO_TCP, syscall.TCP_KEEPIDLE},
		"interval":     {syscall.IPPROTO_TCP, syscall.TCP_KEEPINTVL},
		"probes":       {syscall.IPPROTO_TCP, syscall.TCP_KEEPCNT},
	}
	got := make(map[string]int)
	var gerr error
	if err := raw.Control(func(fd uintptr) {
		for name, o := range opts {
			v, err := syscall.GetsockoptInt(int(fd), o[0], o[1])
			if err != nil {
				gerr = err
			}
			got[name] = v
		}
	}); err != nil || gerr != nil {
		t.Fatal(err, gerr)
	}
	return got
}

func TestPeerSocketsDetectDeadConnections(t *testing.T) {
	l, err := listenPeers("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	dialed, err := dialPeer(t.Context(), l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer dialed.Close()
	accepted, err := l.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer accepted.Close()
	want := map[string]int{"user timeout": 5000, "keepalive": 1, "idle": 5, "interval": 1, "probes": 3}
	for side, conn := range map[string]net.Conn{"dialed": dialed, "accepted": accepted} {
		got := sockopts(t, conn)
		for name, w := range want {
			if got[name] != w {
				t.Errorf("%s connection: %s %d, want %d", side, name, got[name], w)
			}
		}
	}
}
