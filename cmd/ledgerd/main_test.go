package main

import (
	"context"
	"errors"
	"io"
	"log"
	"net"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/Demonisreal/dmn-ledger/internal/disk"
)

func TestServeTurnsTheEndOfRunIntoTheExitCode(t *testing.T) {
	cfg := config{id: 2, cluster: 7, peers: []netip.AddrPort{
		netip.MustParseAddrPort("127.0.0.1:1"), netip.MustParseAddrPort("127.0.0.1:2"), netip.MustParseAddrPort("127.0.0.1:3"),
	}}
	halt := errors.New("replica: checksum mismatch in entry 42")
	stopped, stop := context.WithCancelCause(context.Background())
	stop(errors.New("terminated signal received"))
	for _, tc := range []struct {
		name string
		ctx  context.Context
		err  error
		code int
		last string
	}{
		{"fail-stop", context.Background(), halt, 1, "node 2 halted: replica: checksum mismatch in entry 42"},
		{"signal", stopped, nil, 0, "node 2 stopped: terminated signal received"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var logs lockedBuffer
			ran := false
			code := serve(tc.ctx, func(context.Context) error { ran = true; return tc.err }, cfg, "127.0.0.1:1", log.New(&logs, "", 0))
			lines := strings.Split(strings.TrimSpace(logs.String()), "\n")
			if !ran || code != tc.code || len(lines) != 2 || lines[1] != tc.last {
				t.Fatalf("ran %v, exit %d, log %q, want exit %d ending in %q", ran, code, lines, tc.code, tc.last)
			}
			if !strings.HasPrefix(lines[0], "node 2 of 3 in cluster 7 started: peers on 127.0.0.1:2, clients on 127.0.0.1:1, no status: ") {
				t.Fatalf("start line %q", lines[0])
			}
		})
	}
}

func TestServerGetsEveryVariable(t *testing.T) {
	cfg, err := parse([]string{
		"LEDGER_ID=3",
		"LEDGER_CLUSTER=0x99",
		"LEDGER_PEERS=10.0.0.1:7201,10.0.0.2:7201,10.0.0.3:7201,10.0.0.4:7201,[fd00::5]:7201",
		"LEDGER_CLIENT_ADDR=10.0.0.3:7101",
		"LEDGER_DIR=/srv/ledger/n3",
		"LEDGER_TICK=7ms",
		"LEDGER_SNAPSHOT_EVERY=1234",
		"LEDGER_MAX_SESSIONS=4321",
		"LEDGER_MAX_CLIENTS=77",
		"LEDGER_SWEEP=off",
	})
	if err != nil {
		t.Fatal(err)
	}
	clients, peering := &net.TCPListener{}, &net.TCPListener{}
	logger := log.New(io.Discard, "", 0)
	sc := cfg.server(clients, peering, logger)
	known, ok := sc.Peering.(*knownPeers)
	switch {
	case sc.ID != 3 || sc.Cluster != 0x99 || sc.Dir != "/srv/ledger/n3" || sc.FS != (disk.OS{}):
		t.Fatalf("id %d, cluster %d, dir %q, fs %T", sc.ID, sc.Cluster, sc.Dir, sc.FS)
	case strings.Join(sc.Peers, ",") != "10.0.0.1:7201,10.0.0.2:7201,10.0.0.3:7201,10.0.0.4:7201,[fd00::5]:7201":
		t.Fatalf("peers %v", sc.Peers)
	case sc.Tick != 7*time.Millisecond || sc.SnapshotEvery != 1234 || sc.MaxSessions != 4321 || sc.MaxClients != 77 || sc.Sweep:
		t.Fatalf("tick %v, snapshot every %d, sessions %d, clients %d, sweep %v", sc.Tick, sc.SnapshotEvery, sc.MaxSessions, sc.MaxClients, sc.Sweep)
	case sc.Clients != clients || !ok || known.Listener != peering || len(known.ips) != 5 || sc.Dial == nil || sc.Log != logger:
		t.Fatalf("clients %v, peering %#v, dial set %v, log %v", sc.Clients, sc.Peering, sc.Dial != nil, sc.Log)
	}
	cfg.sweep = true
	if !cfg.server(clients, peering, logger).Sweep {
		t.Fatal("LEDGER_SWEEP=on did not reach the server")
	}
}

func TestConfigErrorExitsTwo(t *testing.T) {
	var stderr lockedBuffer
	if code := run(context.Background(), []string{"LEDGER_TICK=5ms"}, &stderr); code != 2 || stderr.String() != "ledgerd: LEDGER_PEERS is missing\n" {
		t.Fatalf("exit %d, stderr %q", code, stderr.String())
	}
}
