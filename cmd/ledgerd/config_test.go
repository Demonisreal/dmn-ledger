package main

import (
	"net/netip"
	"reflect"
	"strings"
	"testing"
	"time"
)

const threePeers = "127.0.0.1:7201,127.0.0.1:7202,127.0.0.1:7203"

func environ(vars map[string]string) []string {
	var env []string
	for name, given := range vars {
		env = append(env, name+"="+given)
	}
	return env
}

func required() map[string]string {
	return map[string]string{
		"LEDGER_ID":          "2",
		"LEDGER_CLUSTER":     "7",
		"LEDGER_PEERS":       threePeers,
		"LEDGER_CLIENT_ADDR": "127.0.0.1:7102",
		"LEDGER_DIR":         "/var/lib/ledger",
	}
}

func TestConfigDefaults(t *testing.T) {
	cfg, err := parse(append(environ(required()), "HOME=/home/x", "LEDGERX=1", "LEDGERD_CHILD=1", "PATH=/bin"))
	if err != nil {
		t.Fatal(err)
	}
	want := config{
		id:            2,
		cluster:       7,
		peers:         []netip.AddrPort{netip.MustParseAddrPort("127.0.0.1:7201"), netip.MustParseAddrPort("127.0.0.1:7202"), netip.MustParseAddrPort("127.0.0.1:7203")},
		clients:       netip.MustParseAddrPort("127.0.0.1:7102"),
		dir:           "/var/lib/ledger",
		tick:          10 * time.Millisecond,
		snapshotEvery: 100000,
		segmentSize:   64 << 20,
		maxSessions:   1024,
		maxClients:    128,
		sweep:         true,
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
		{"LEDGER_ID", "1", "", func(c config) any { return c.id }, uint64(1)},
		{"LEDGER_ID", "3", "", func(c config) any { return c.id }, uint64(3)},
		{"LEDGER_ID", "0", `LEDGER_ID="0": want a number from 1 to 3`, nil, nil},
		{"LEDGER_ID", "4", `LEDGER_ID="4": want a number from 1 to 3`, nil, nil},
		{"LEDGER_ID", "two", `LEDGER_ID="two"`, nil, nil},
		{"LEDGER_ID", "", `LEDGER_ID=""`, nil, nil},
		{"LEDGER_ID", missing, "LEDGER_ID is missing", nil, nil},

		{"LEDGER_CLUSTER", "1", "", func(c config) any { return c.cluster }, uint64(1)},
		{"LEDGER_CLUSTER", "18446744073709551615", "", func(c config) any { return c.cluster }, uint64(1<<64 - 1)},
		{"LEDGER_CLUSTER", "0xffffffffffffffff", "", func(c config) any { return c.cluster }, uint64(1<<64 - 1)},
		{"LEDGER_CLUSTER", "0x1", "", func(c config) any { return c.cluster }, uint64(1)},
		{"LEDGER_CLUSTER", "0", `LEDGER_CLUSTER="0": want a number from 1 to 18446744073709551615`, nil, nil},
		{"LEDGER_CLUSTER", "0x0", `LEDGER_CLUSTER="0x0"`, nil, nil},
		{"LEDGER_CLUSTER", "18446744073709551616", `LEDGER_CLUSTER="18446744073709551616"`, nil, nil},
		{"LEDGER_CLUSTER", "0x10000000000000000", `LEDGER_CLUSTER="0x10000000000000000"`, nil, nil},
		{"LEDGER_CLUSTER", "18446744073709551617", `LEDGER_CLUSTER="18446744073709551617"`, nil, nil},
		{"LEDGER_CLUSTER", "0x", `LEDGER_CLUSTER="0x"`, nil, nil},
		{"LEDGER_CLUSTER", "-1", `LEDGER_CLUSTER="-1"`, nil, nil},
		{"LEDGER_CLUSTER", missing, "LEDGER_CLUSTER is missing", nil, nil},

		{"LEDGER_PEERS", "127.0.0.1:7201,127.0.0.1:7202,127.0.0.1:7203,127.0.0.1:7204,127.0.0.1:7205", "", func(c config) any { return len(c.peers) }, 5},
		{"LEDGER_PEERS", "[::1]:7201,[::1]:7202,[::1]:7203", "", func(c config) any { return c.peers[1].String() }, "[::1]:7202"},
		{"LEDGER_PEERS", "127.0.0.1:7201,127.0.0.1:7202", `LEDGER_PEERS="127.0.0.1:7201,127.0.0.1:7202": 2 peers, want 1, 3 or 5`, nil, nil},
		{"LEDGER_PEERS", "127.0.0.1:7201,127.0.0.1:7202,127.0.0.1:7203,127.0.0.1:7204", "4 peers, want 1, 3 or 5", nil, nil},
		{"LEDGER_PEERS", "127.0.0.1:1,127.0.0.1:2,127.0.0.1:3,127.0.0.1:4,127.0.0.1:5,127.0.0.1:6", "6 peers, want 1, 3 or 5", nil, nil},
		{"LEDGER_PEERS", "127.0.0.1:7201,127.0.0.1:7202,127.0.0.1:7201", "127.0.0.1:7201 listed twice", nil, nil},
		{"LEDGER_PEERS", "127.0.0.1:7201,localhost:7202,127.0.0.1:7203", `"localhost:7202": want IP:port, no host names`, nil, nil},
		{"LEDGER_PEERS", "127.0.0.1:7201,127.0.0.1:0,127.0.0.1:7203", `"127.0.0.1:0": want a port other than 0`, nil, nil},
		{"LEDGER_PEERS", threePeers + ",", `"": want IP:port`, nil, nil},
		{"LEDGER_PEERS", "127.0.0.1:7201, 127.0.0.1:7202,127.0.0.1:7203", `" 127.0.0.1:7202"`, nil, nil},
		{"LEDGER_PEERS", missing, "LEDGER_PEERS is missing", nil, nil},

		{"LEDGER_CLIENT_ADDR", "0.0.0.0:65535", "", func(c config) any { return c.clients.String() }, "0.0.0.0:65535"},
		{"LEDGER_CLIENT_ADDR", "localhost:7102", `LEDGER_CLIENT_ADDR="localhost:7102": want IP:port, no host names`, nil, nil},
		{"LEDGER_CLIENT_ADDR", ":7102", `LEDGER_CLIENT_ADDR=":7102": want IP:port`, nil, nil},
		{"LEDGER_CLIENT_ADDR", "127.0.0.1:0", `LEDGER_CLIENT_ADDR="127.0.0.1:0": want a port other than 0`, nil, nil},
		{"LEDGER_CLIENT_ADDR", "127.0.0.1:65536", `LEDGER_CLIENT_ADDR="127.0.0.1:65536"`, nil, nil},
		{"LEDGER_CLIENT_ADDR", missing, "LEDGER_CLIENT_ADDR is missing", nil, nil},

		{"LEDGER_DIR", "/", "", func(c config) any { return c.dir }, "/"},
		{"LEDGER_DIR", "data/n1", `LEDGER_DIR="data/n1": want an absolute path`, nil, nil},
		{"LEDGER_DIR", "", `LEDGER_DIR="": want an absolute path`, nil, nil},
		{"LEDGER_DIR", missing, "LEDGER_DIR is missing", nil, nil},

		{"LEDGER_TICK", "1ms", "", func(c config) any { return c.tick }, time.Millisecond},
		{"LEDGER_TICK", "1s", "", func(c config) any { return c.tick }, time.Second},
		{"LEDGER_TICK", "999999ns", `LEDGER_TICK="999999ns": want a duration from 1ms to 1s`, nil, nil},
		{"LEDGER_TICK", "1000000001ns", `LEDGER_TICK="1000000001ns": want a duration from 1ms to 1s`, nil, nil},
		{"LEDGER_TICK", "10", `LEDGER_TICK="10"`, nil, nil},
		{"LEDGER_TICK", "-5ms", `LEDGER_TICK="-5ms"`, nil, nil},

		{"LEDGER_SNAPSHOT_EVERY", "1", "", func(c config) any { return c.snapshotEvery }, uint64(1)},
		{"LEDGER_SNAPSHOT_EVERY", "4294967296", "", func(c config) any { return c.snapshotEvery }, uint64(1 << 32)},
		{"LEDGER_SNAPSHOT_EVERY", "0", `LEDGER_SNAPSHOT_EVERY="0": want a number from 1 to 4294967296`, nil, nil},
		{"LEDGER_SNAPSHOT_EVERY", "4294967297", `LEDGER_SNAPSHOT_EVERY="4294967297": want a number from 1 to 4294967296`, nil, nil},

		{"LEDGER_SEGMENT_SIZE", "2097152", "", func(c config) any { return c.segmentSize }, int64(2 << 20)},
		{"LEDGER_SEGMENT_SIZE", "67108864", "", func(c config) any { return c.segmentSize }, int64(64 << 20)},
		{"LEDGER_SEGMENT_SIZE", "2097151", `LEDGER_SEGMENT_SIZE="2097151": want a number from 2097152 to 67108864`, nil, nil},
		{"LEDGER_SEGMENT_SIZE", "67108865", `LEDGER_SEGMENT_SIZE="67108865": want a number from 2097152 to 67108864`, nil, nil},
		{"LEDGER_SEGMENT_SIZE", "64MiB", `LEDGER_SEGMENT_SIZE="64MiB"`, nil, nil},

		{"LEDGER_MAX_SESSIONS", "1", "", func(c config) any { return c.maxSessions }, 1},
		{"LEDGER_MAX_SESSIONS", "1000000", "", func(c config) any { return c.maxSessions }, 1000000},
		{"LEDGER_MAX_SESSIONS", "0", `LEDGER_MAX_SESSIONS="0": want a number from 1 to 1000000`, nil, nil},
		{"LEDGER_MAX_SESSIONS", "1000001", `LEDGER_MAX_SESSIONS="1000001": want a number from 1 to 1000000`, nil, nil},

		{"LEDGER_MAX_CLIENTS", "1", "", func(c config) any { return c.maxClients }, 1},
		{"LEDGER_MAX_CLIENTS", "4096", "", func(c config) any { return c.maxClients }, 4096},
		{"LEDGER_MAX_CLIENTS", "0", `LEDGER_MAX_CLIENTS="0": want a number from 1 to 4096`, nil, nil},
		{"LEDGER_MAX_CLIENTS", "4097", `LEDGER_MAX_CLIENTS="4097": want a number from 1 to 4096`, nil, nil},

		{"LEDGER_SWEEP", "on", "", func(c config) any { return c.sweep }, true},
		{"LEDGER_SWEEP", "off", "", func(c config) any { return c.sweep }, false},
		{"LEDGER_SWEEP", "yes", `LEDGER_SWEEP="yes": want on or off`, nil, nil},
		{"LEDGER_SWEEP", "", `LEDGER_SWEEP="": want on or off`, nil, nil},

		{"LEDGER_TIKC", "5ms", "unknown variable LEDGER_TIKC", nil, nil},
		{"LEDGER_NODES", "127.0.0.1:7101", "unknown variable LEDGER_NODES", nil, nil},
		{"LEDGER_", "", "unknown variable LEDGER_", nil, nil},
	} {
		t.Run(tc.name+"="+strings.ReplaceAll(tc.given, missing, "missing"), func(t *testing.T) {
			vars := required()
			if tc.given == missing {
				delete(vars, tc.name)
			} else {
				vars[tc.name] = tc.given
			}
			cfg, err := parse(environ(vars))
			switch {
			case tc.want == "" && err != nil:
				t.Fatalf("unexpected error: %v", err)
			case tc.want == "":
				if got := tc.got(cfg); got != tc.wantVal {
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

func TestConfigSinglePeer(t *testing.T) {
	vars := required()
	vars["LEDGER_PEERS"], vars["LEDGER_ID"] = "10.0.0.1:7201", "1"
	if cfg, err := parse(environ(vars)); err != nil || len(cfg.peers) != 1 {
		t.Fatalf("one peer: %v, %v", cfg.peers, err)
	}
	vars["LEDGER_ID"] = "2"
	if _, err := parse(environ(vars)); err == nil || !strings.Contains(err.Error(), `LEDGER_ID="2": want a number from 1 to 1`) {
		t.Fatalf("ID beyond a single peer: %v", err)
	}
}

func TestUnknownVariablesAreAllNamed(t *testing.T) {
	_, err := parse(append(environ(required()), "LEDGER_TIKC=5ms", "LEDGER_SWEEEP=off"))
	if err == nil || err.Error() != "unknown variable LEDGER_SWEEEP, LEDGER_TIKC" {
		t.Fatalf("got %v", err)
	}
}
