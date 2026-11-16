package main

import (
	"context"
	"log"
	"net"
	"net/netip"
	"slices"
	"time"
)

const refusalGap = time.Second

var (
	peerKeepAlive = net.KeepAliveConfig{Enable: true, Idle: 5 * time.Second, Interval: time.Second, Count: 3}
	peerDialer    = net.Dialer{KeepAliveConfig: peerKeepAlive, Control: control}
)

func listenPeers(addr string) (net.Listener, error) {
	lc := net.ListenConfig{KeepAliveConfig: peerKeepAlive, Control: control}
	return lc.Listen(context.Background(), "tcp", addr)
}

func dialPeer(ctx context.Context, addr string) (net.Conn, error) {
	return peerDialer.DialContext(ctx, "tcp", addr)
}

type knownPeers struct {
	net.Listener
	ips     []netip.Addr
	log     *log.Logger
	refused time.Time
}

func (l *knownPeers) Accept() (net.Conn, error) {
	for {
		conn, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}
		from, _ := netip.ParseAddrPort(conn.RemoteAddr().String())
		if slices.Contains(l.ips, from.Addr()) {
			return conn, nil
		}
		_ = conn.Close()
		if time.Since(l.refused) >= refusalGap {
			l.refused = time.Now()
			l.log.Printf("refused peer from %s: not in LEDGER_PEERS", conn.RemoteAddr())
		}
	}
}

func peerIPs(peers []netip.AddrPort) []netip.Addr {
	ips := make([]netip.Addr, len(peers))
	for i, p := range peers {
		ips[i] = p.Addr().Unmap()
	}
	return ips
}
