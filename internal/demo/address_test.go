package demo

import (
	"net/http/httptest"
	"net/netip"
	"testing"
)

func visitorOf(remote string, forwarded ...string) netip.Prefix {
	r := httptest.NewRequest("POST", "/act/kill/1", nil)
	r.RemoteAddr = remote
	for _, line := range forwarded {
		r.Header.Add("X-Forwarded-For", line)
	}
	return visitor(r, testProxies)
}

func TestAddressKey(t *testing.T) {
	for _, c := range []struct {
		remote string
		want   string
	}{
		{"203.0.113.7:4711", "203.0.113.7/32"},
		{"[::ffff:203.0.113.7]:4711", "203.0.113.7/32"},
		{"[2001:db8:1:2:3:4:5:6]:4711", "2001:db8:1:2::/64"},
		{"[2001:db8:1:2::ffff]:4711", "2001:db8:1:2::/64"},
		{"[2001:db8:1:3::1]:4711", "2001:db8:1:3::/64"},
		{"[fe80::1%eth0]:4711", "fe80::/64"},
	} {
		if got := visitorOf(c.remote); got != netip.MustParsePrefix(c.want) {
			t.Errorf("%s: key %s, want %s", c.remote, got, c.want)
		}
	}
	if visitorOf("[2001:db8:1:2::1]:1") != visitorOf("[2001:db8:1:2:ffff:ffff:ffff:ffff]:2") {
		t.Error("two addresses in one /64 have different keys")
	}
	if visitorOf("203.0.113.7:1") == visitorOf("203.0.113.8:1") {
		t.Error("two IPv4 addresses share a key")
	}
	if got := visitorOf("10.0.0.2:1", "2001:db8:5:6:7::9"); got != netip.MustParsePrefix("2001:db8:5:6::/64") {
		t.Errorf("forwarded IPv6 has key %s, want its /64", got)
	}
}

func TestAddressOnlyFromTheProxy(t *testing.T) {
	for _, c := range []struct {
		name      string
		remote    string
		forwarded []string
		want      string
	}{
		{"a stranger with the header", "198.51.100.9:1", []string{"203.0.113.7"}, "198.51.100.9/32"},
		{"a stranger in an IPv4-mapped form", "[::ffff:198.51.100.9]:1", []string{"203.0.113.7"}, "198.51.100.9/32"},
		{"the proxy with one entry", "10.0.0.2:1", []string{"203.0.113.7"}, "203.0.113.7/32"},
		{"the proxy with a forged entry on the left", "10.0.0.2:1", []string{"1.2.3.4, 203.0.113.7"}, "203.0.113.7/32"},
		{"the proxy behind a second proxy", "10.0.0.2:1", []string{"1.2.3.4, 203.0.113.7, 10.0.0.5"}, "203.0.113.7/32"},
		{"the proxy without the header", "10.0.0.2:1", nil, "10.0.0.2/32"},
		{"the proxy with only proxies in the header", "10.0.0.2:1", []string{"10.0.0.3, 10.0.0.4"}, "10.0.0.2/32"},
		{"two header lines", "10.0.0.2:1", []string{"1.2.3.4", "203.0.113.7"}, "203.0.113.7/32"},
		{"two header lines, the last one a proxy", "10.0.0.2:1", []string{"1.2.3.4, 203.0.113.7", "10.0.0.9"}, "203.0.113.7/32"},
		{"no address on the right, the entries left of it are anyone's", "10.0.0.2:1", []string{"1.2.3.4, 203.0.113.7, unknown"}, "10.0.0.2/32"},
		{"an address with a port on the right", "10.0.0.2:1", []string{"1.2.3.4, 203.0.113.7:4711"}, "10.0.0.2/32"},
		{"no address on the left", "10.0.0.2:1", []string{"unknown, 203.0.113.7"}, "203.0.113.7/32"},
		{"the proxy over IPv6", "[fd00::1]:1", []string{"2001:db8::1"}, "2001:db8::/64"},
		{"the proxy in an IPv4-mapped form", "[::ffff:10.0.0.2]:1", []string{"203.0.113.7"}, "203.0.113.7/32"},
		{"a forwarded IPv4-mapped entry", "10.0.0.2:1", []string{"::ffff:203.0.113.7"}, "203.0.113.7/32"},
	} {
		if got := visitorOf(c.remote, c.forwarded...); got != netip.MustParsePrefix(c.want) {
			t.Errorf("%s: key %s, want %s", c.name, got, c.want)
		}
	}
}
