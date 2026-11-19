package main

import (
	"fmt"
	"io"
	"net/http"
	"net/netip"
	"time"
)

const checkWithin = 2 * time.Second

func probe(environ []string, stderr io.Writer) int {
	cfg, err := parse(environ, "")
	if err != nil {
		fmt.Fprintln(stderr, "demo:", err)
		return 1
	}
	addr := cfg.addr
	if addr.Addr().IsUnspecified() {
		loopback := netip.IPv6Loopback()
		if addr.Addr().Is4() {
			loopback = netip.AddrFrom4([4]byte{127, 0, 0, 1})
		}
		addr = netip.AddrPortFrom(loopback, addr.Port())
	}
	hc := &http.Client{Timeout: checkWithin}
	res, err := hc.Get("http://" + addr.String() + "/healthz")
	if err != nil {
		fmt.Fprintln(stderr, "demo:", err)
		return 1
	}
	_ = res.Body.Close()
	if res.StatusCode != http.StatusOK {
		fmt.Fprintf(stderr, "demo: %s answered %s\n", res.Request.URL, res.Status)
		return 1
	}
	return 0
}
