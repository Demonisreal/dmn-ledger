package demo

import (
	"net/http"
	"net/netip"
	"slices"
	"strings"
)

func visitor(r *http.Request, proxies []netip.Prefix) netip.Prefix {
	peer, _ := netip.ParseAddrPort(r.RemoteAddr)
	conn := peer.Addr().Unmap()
	if !behind(proxies, conn) {
		return addressKey(conn)
	}
	lines := r.Header.Values("X-Forwarded-For")
	for i := len(lines) - 1; i >= 0; i-- {
		hops := strings.Split(lines[i], ",")
		for j := len(hops) - 1; j >= 0; j-- {
			hop, err := netip.ParseAddr(strings.TrimSpace(hops[j]))
			if err != nil {
				return addressKey(conn)
			}
			if !behind(proxies, hop.Unmap()) {
				return addressKey(hop.Unmap())
			}
		}
	}
	return addressKey(conn)
}

func behind(proxies []netip.Prefix, addr netip.Addr) bool {
	return slices.ContainsFunc(proxies, func(p netip.Prefix) bool { return p.Contains(addr) })
}

func addressKey(addr netip.Addr) netip.Prefix {
	if addr.Is4() {
		return netip.PrefixFrom(addr, 32)
	}
	key, _ := addr.WithZone("").Prefix(64)
	return key
}
