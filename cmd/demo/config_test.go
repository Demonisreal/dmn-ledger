package main

import (
	"net/netip"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func environ(vars map[string]string) []string {
	var env []string
	for name, given := range vars {
		env = append(env, name+"="+given)
	}
	return env
}

func TestConfigDefaults(t *testing.T) {
	cfg, err := parse([]string{"DEMO_DIR=/data", "HOME=/home/x", "DEMOX=1", "LEDGER_ID=1", "PATH=/bin"}, "/opt/demo/demo")
	if err != nil {
		t.Fatal(err)
	}
	want := config{
		dir:     "/data",
		addr:    netip.MustParseAddrPort("127.0.0.1:8080"),
		origin:  "http://127.0.0.1:8080",
		ledgerd: "/opt/demo/ledgerd",
	}
	if !reflect.DeepEqual(cfg, want) {
		t.Fatalf("got %+v\nwant %+v", cfg, want)
	}
}

func TestConfig(t *testing.T) {
	const missing = "\x00"
	for _, tc := range []struct {
		name, given string
		want        string
		got         func(config) any
		wantVal     any
	}{
		{"DEMO_DIR", "/", "", func(c config) any { return c.dir }, "/"},
		{"DEMO_DIR", "/var/lib/demo", "", func(c config) any { return c.dir }, "/var/lib/demo"},
		{"DEMO_DIR", "data", `DEMO_DIR="data": want an absolute path`, nil, nil},
		{"DEMO_DIR", "", `DEMO_DIR="": want an absolute path`, nil, nil},
		{"DEMO_DIR", missing, "DEMO_DIR is missing", nil, nil},

		{"DEMO_ADDR", "0.0.0.0:8080", "", func(c config) any { return c.addr.String() }, "0.0.0.0:8080"},
		{"DEMO_ADDR", "[::1]:18080", "", func(c config) any { return c.addr.String() }, "[::1]:18080"},
		{"DEMO_ADDR", "0.0.0.0:8080", "", func(c config) any { return c.origin }, "http://0.0.0.0:8080"},
		{"DEMO_ADDR", "localhost:8080", `DEMO_ADDR="localhost:8080": want IP:port, no host names`, nil, nil},
		{"DEMO_ADDR", ":8080", `DEMO_ADDR=":8080": want IP:port`, nil, nil},
		{"DEMO_ADDR", "127.0.0.1", `DEMO_ADDR="127.0.0.1": want IP:port`, nil, nil},
		{"DEMO_ADDR", "127.0.0.1:0", `DEMO_ADDR="127.0.0.1:0": want a port other than 0`, nil, nil},
		{"DEMO_ADDR", "127.0.0.1:65536", `DEMO_ADDR="127.0.0.1:65536"`, nil, nil},
		{"DEMO_ADDR", "", `DEMO_ADDR=""`, nil, nil},

		{"DEMO_ORIGIN", "https://ledger.dmn-software.com", "", func(c config) any { return c.origin }, "https://ledger.dmn-software.com"},
		{"DEMO_ORIGIN", "http://127.0.0.1:18080", "", func(c config) any { return c.origin }, "http://127.0.0.1:18080"},
		{"DEMO_ORIGIN", "http://[::1]:8080", "", func(c config) any { return c.origin }, "http://[::1]:8080"},
		{"DEMO_ORIGIN", "https://ledger.dmn-software.com:8443", "", func(c config) any { return c.origin }, "https://ledger.dmn-software.com:8443"},
		{"DEMO_ORIGIN", "https://ledger.dmn-software.com/", `DEMO_ORIGIN="https://ledger.dmn-software.com/": want https://ledger.dmn-software.com, as a browser sends it`, nil, nil},
		{"DEMO_ORIGIN", "https://ledger.dmn-software.com/demo", "want https://ledger.dmn-software.com, as a browser sends it", nil, nil},
		{"DEMO_ORIGIN", "https://ledger.dmn-software.com?x=1", "want https://ledger.dmn-software.com, as a browser sends it", nil, nil},
		{"DEMO_ORIGIN", "https://ledger.dmn-software.com#top", "want https://ledger.dmn-software.com, as a browser sends it", nil, nil},
		{"DEMO_ORIGIN", "https://admin@ledger.dmn-software.com", "want https://ledger.dmn-software.com, as a browser sends it", nil, nil},
		{"DEMO_ORIGIN", "https://Ledger.DMN-Software.com", "want https://ledger.dmn-software.com, as a browser sends it", nil, nil},
		{"DEMO_ORIGIN", "HTTPS://ledger.dmn-software.com", "want https://ledger.dmn-software.com, as a browser sends it", nil, nil},
		{"DEMO_ORIGIN", "https://ledger.dmn-software.com:443", "want https://ledger.dmn-software.com, as a browser sends it", nil, nil},
		{"DEMO_ORIGIN", "http://127.0.0.1:80", "want http://127.0.0.1, as a browser sends it", nil, nil},
		{"DEMO_ORIGIN", "http://127.0.0.1:08080", "want http://127.0.0.1:8080, as a browser sends it", nil, nil},
		{"DEMO_ORIGIN", "http://127.0.0.1:0", "want a port from 1 to 65535", nil, nil},
		{"DEMO_ORIGIN", "http://127.0.0.1:65536", "want a port from 1 to 65535", nil, nil},
		{"DEMO_ORIGIN", "ftp://ledger.dmn-software.com", "want http:// or https://, a host and an optional port", nil, nil},
		{"DEMO_ORIGIN", "ledger.dmn-software.com", "want http:// or https://", nil, nil},
		{"DEMO_ORIGIN", "https://", "want http:// or https://", nil, nil},
		{"DEMO_ORIGIN", "https://:8443", "want http:// or https://", nil, nil},
		{"DEMO_ORIGIN", "", `DEMO_ORIGIN="": want http:// or https://`, nil, nil},

		{"DEMO_PROXIES", "172.30.0.0/24", "", func(c config) any { return c.proxies }, []netip.Prefix{netip.MustParsePrefix("172.30.0.0/24")}},
		{"DEMO_PROXIES", "127.0.0.1/32,fd00:30::/64", "", func(c config) any { return c.proxies }, []netip.Prefix{netip.MustParsePrefix("127.0.0.1/32"), netip.MustParsePrefix("fd00:30::/64")}},
		{"DEMO_PROXIES", "", "", func(c config) any { return c.proxies }, []netip.Prefix(nil)},
		{"DEMO_PROXIES", "172.30.0.5", `DEMO_PROXIES="172.30.0.5": "172.30.0.5": want a network like 172.30.0.0/24`, nil, nil},
		{"DEMO_PROXIES", "172.30.0.5/24", `"172.30.0.5/24": want the network 172.30.0.0/24`, nil, nil},
		{"DEMO_PROXIES", "172.30.0.0/24, 10.0.0.0/8", `" 10.0.0.0/8": want a network`, nil, nil},
		{"DEMO_PROXIES", "172.30.0.0/24,", `"": want a network`, nil, nil},
		{"DEMO_PROXIES", "caddy", `"caddy": want a network`, nil, nil},

		{"DEMO_LEDGERD", "/usr/local/bin/ledgerd", "", func(c config) any { return c.ledgerd }, "/usr/local/bin/ledgerd"},
		{"DEMO_LEDGERD", "ledgerd", `DEMO_LEDGERD="ledgerd": want an absolute path`, nil, nil},
		{"DEMO_LEDGERD", "", `DEMO_LEDGERD="": want an absolute path`, nil, nil},

		{"DEMO_PORT", "8080", "unknown variable DEMO_PORT", nil, nil},
		{"DEMO_RATE", "100", "unknown variable DEMO_RATE", nil, nil},
		{"DEMO_", "", "unknown variable DEMO_", nil, nil},
	} {
		t.Run(tc.name+"="+strings.ReplaceAll(tc.given, missing, "missing"), func(t *testing.T) {
			vars := map[string]string{"DEMO_DIR": "/data"}
			if tc.given == missing {
				delete(vars, tc.name)
			} else {
				vars[tc.name] = tc.given
			}
			cfg, err := parse(environ(vars), "/opt/demo/demo")
			switch {
			case tc.want == "" && err != nil:
				t.Fatalf("unexpected error: %v", err)
			case tc.want == "":
				if got := tc.got(cfg); !reflect.DeepEqual(got, tc.wantVal) {
					t.Fatalf("got %v, want %v", got, tc.wantVal)
				}
			case err == nil:
				t.Fatalf("accepted, want an error with %q", tc.want)
			case !strings.Contains(err.Error(), tc.want):
				t.Fatalf("error %q, want %q", err, tc.want)
			}
		})
	}
}

func TestUnknownVariablesAreAllNamed(t *testing.T) {
	_, err := parse([]string{"DEMO_DIR=/data", "DEMO_RATE=5", "DEMO_LIMIT=100", "DEMO_ORIGN=https://x"}, "/demo")
	if err == nil || err.Error() != "unknown variable DEMO_LIMIT, DEMO_ORIGN, DEMO_RATE" {
		t.Fatalf("got %v", err)
	}
}

func TestPrepare(t *testing.T) {
	bin := filepath.Join(t.TempDir(), "ledgerd")
	if err := os.WriteFile(bin, []byte("#!/bin/true\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(t.TempDir(), "a", "data")
	if err := (config{dir: dir, ledgerd: bin}).prepare(); err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(dir)
	if err != nil || fi.Mode().Perm() != 0o700 {
		t.Fatalf("%s: %v, %v, want a directory with mode 0700", dir, fi.Mode(), err)
	}
	if left, _ := os.ReadDir(dir); len(left) != 0 {
		t.Fatalf("prepare left %v", left)
	}
	plain := filepath.Join(t.TempDir(), "plain")
	if err := os.WriteFile(plain, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	locked := t.TempDir()
	if err := os.Chmod(locked, 0o500); err != nil { //nolint:gosec // G302: the directory is closed to writes on purpose
		t.Fatal(err)
	}
	for _, tc := range []struct {
		cfg  config
		want string
	}{
		{config{dir: dir, ledgerd: filepath.Join(dir, "ledgerd")}, "no such file or directory"},
		{config{dir: dir, ledgerd: plain}, "permission denied"},
		{config{dir: filepath.Join(plain, "data"), ledgerd: bin}, "not a directory"},
		{config{dir: locked, ledgerd: bin}, "permission denied"},
	} {
		if err := tc.cfg.prepare(); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%+v: %v, want %q", tc.cfg, err, tc.want)
		}
	}
}

func TestGreetingNamesWhatWasRead(t *testing.T) {
	cfg, err := parse([]string{"DEMO_DIR=/data", "DEMO_ADDR=0.0.0.0:8080", "DEMO_ORIGIN=https://ledger.dmn-software.com", "DEMO_PROXIES=172.30.0.0/24,fd00:30::/64"}, "/demo")
	if err != nil {
		t.Fatal(err)
	}
	want := "demo started: http on 0.0.0.0:8080, origin https://ledger.dmn-software.com, proxies 172.30.0.0/24,fd00:30::/64, ledgerd /ledgerd, data in /data"
	if got := greeting(cfg); got != want {
		t.Errorf("got %q\nwant %q", got, want)
	}
	cfg.proxies = nil
	if got := greeting(cfg); !strings.Contains(got, ", proxies none, ") {
		t.Errorf("without proxies: %q", got)
	}
}
