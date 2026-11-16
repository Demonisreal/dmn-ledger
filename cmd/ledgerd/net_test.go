package main

import (
	"bytes"
	"errors"
	"log"
	"net"
	"net/netip"
	"strings"
	"sync"
	"testing"
	"time"
)

type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func acceptAll(t *testing.T, l net.Listener) <-chan net.Conn {
	accepted := make(chan net.Conn, 8)
	var wg sync.WaitGroup
	wg.Go(func() {
		defer close(accepted)
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			accepted <- conn
		}
	})
	t.Cleanup(func() {
		_ = l.Close()
		wg.Wait()
		for conn := range accepted {
			_ = conn.Close()
		}
	})
	return accepted
}

func dialFrom(t *testing.T, from, addr string) net.Conn {
	d := net.Dialer{LocalAddr: &net.TCPAddr{IP: net.ParseIP(from)}, Timeout: time.Second}
	conn, err := d.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial %s from %s: %v", addr, from, err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func closedByPeer(conn net.Conn) bool {
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, err := conn.Read(make([]byte, 1))
	return err != nil && !isTimeout(err)
}

func isTimeout(err error) bool {
	ne, ok := errors.AsType[net.Error](err)
	return ok && ne.Timeout()
}

func TestPeerListenerRefusesStrangers(t *testing.T) {
	for _, listen := range []string{"127.0.0.1:0", "[::]:0"} {
		t.Run(listen, func(t *testing.T) {
			inner, err := net.Listen("tcp", listen)
			if err != nil {
				t.Skipf("cannot listen on %s: %v", listen, err)
			}
			var logs lockedBuffer
			peers := []netip.AddrPort{netip.MustParseAddrPort("10.9.9.9:7201"), netip.MustParseAddrPort("[::ffff:127.0.0.1]:7202"), netip.MustParseAddrPort("[::1]:7203")}
			accepted := acceptAll(t, &knownPeers{Listener: inner, ips: peerIPs(peers), log: log.New(&logs, "", 0)})
			_, port, _ := net.SplitHostPort(inner.Addr().String())
			addr := net.JoinHostPort("127.0.0.1", port)

			for _, from := range []string{"127.0.0.2", "127.0.0.3"} {
				if conn := dialFrom(t, from, addr); !closedByPeer(conn) {
					t.Fatalf("a connection from %s stayed open", from)
				}
			}
			known := dialFrom(t, "127.0.0.1", addr)
			select {
			case conn := <-accepted:
				if conn.RemoteAddr().String() != known.LocalAddr().String() {
					t.Fatalf("accepted %s, want the peer at %s", conn.RemoteAddr(), known.LocalAddr())
				}
			case <-time.After(5 * time.Second):
				t.Fatal("the connection from a peer IP was not accepted")
			}
			select {
			case conn := <-accepted:
				t.Fatalf("a second connection came through from %s", conn.RemoteAddr())
			default:
			}
			lines := strings.Split(strings.TrimSpace(logs.String()), "\n")
			if len(lines) != 1 || !strings.HasPrefix(lines[0], "refused peer from 127.0.0.2:") || !strings.HasSuffix(lines[0], ": not in LEDGER_PEERS") {
				t.Fatalf("log %q, want one line for the first stranger", lines)
			}
		})
	}
}

func TestRefusedPeersAreLoggedOncePerSecond(t *testing.T) {
	inner, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var logs lockedBuffer
	l := &knownPeers{Listener: inner, ips: []netip.Addr{netip.MustParseAddr("127.0.0.1")}, log: log.New(&logs, "", 0)}
	accepted := acceptAll(t, l)
	for range 3 {
		closedByPeer(dialFrom(t, "127.0.0.2", inner.Addr().String()))
	}
	time.Sleep(refusalGap)
	closedByPeer(dialFrom(t, "127.0.0.2", inner.Addr().String()))
	dialFrom(t, "127.0.0.1", inner.Addr().String())
	<-accepted
	if n := strings.Count(logs.String(), "refused peer"); n != 2 {
		t.Fatalf("%d lines for four strangers over a second, want 2:\n%s", n, logs.String())
	}
}
