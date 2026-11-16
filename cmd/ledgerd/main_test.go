package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/netip"
	"os"
	"strings"
	"syscall"
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
			code := serve(tc.ctx, func(context.Context) error { ran = true; return tc.err }, cfg, log.New(&logs, "", 0), nil, func() string { return "stats up=1" })
			want := "stats up=1\n" + tc.last
			if lines := strings.TrimSpace(logs.String()); !ran || code != tc.code || lines != want {
				t.Fatalf("ran %v, exit %d, log %q, want exit %d and %q", ran, code, lines, tc.code, want)
			}
		})
	}
}

func TestServeWritesStatsOnSignalAndAtTheEnd(t *testing.T) {
	cfg := config{id: 1, peers: []netip.AddrPort{netip.MustParseAddrPort("127.0.0.1:1")}}
	stopped, stop := context.WithCancelCause(context.Background())
	asked, halt := make(chan os.Signal), make(chan struct{})
	var logs lockedBuffer
	lines := 0
	exited := make(chan int)
	go func() {
		exited <- serve(stopped, func(context.Context) error { <-halt; return nil }, cfg, log.New(&logs, "", 0), asked, func() string {
			lines++
			return fmt.Sprintf("stats %d", lines)
		})
	}()
	asked <- syscall.SIGUSR1
	asked <- syscall.SIGUSR1
	stop(errors.New("terminated signal received"))
	close(halt)
	want := "stats 1\nstats 2\nstats 3\nnode 1 stopped: terminated signal received"
	if code := <-exited; code != 0 || strings.TrimSpace(logs.String()) != want {
		t.Fatalf("exit %d, log %q, want 0 and %q", code, logs.String(), want)
	}
}

func TestGreetingShowsWhatOpenFound(t *testing.T) {
	cfg := config{id: 2, cluster: 7, peers: []netip.AddrPort{
		netip.MustParseAddrPort("127.0.0.1:1"), netip.MustParseAddrPort("127.0.0.1:2"), netip.MustParseAddrPort("127.0.0.1:3"),
	}}
	for _, tc := range []struct {
		term   uint64
		repair bool
		want   string
	}{
		{0, false, "node 2 of 3 in cluster 7 started: peers on 127.0.0.1:2, clients on 127.0.0.1:9, term 0, repair no, no key"},
		{41, true, "node 2 of 3 in cluster 7 started: peers on 127.0.0.1:2, clients on 127.0.0.1:9, term 41, repair yes, no key"},
	} {
		if got := greeting(cfg, "127.0.0.1:9", tc.term, tc.repair); got != tc.want {
			t.Errorf("got %q, want %q", got, tc.want)
		}
	}
	cfg.key = ed25519.NewKeyFromSeed(bytes.Repeat([]byte{1}, ed25519.SeedSize))
	want := "node 2 of 3 in cluster 7 started: peers on 127.0.0.1:2, clients on 127.0.0.1:9, term 3, repair no, key 8a88e3dd7409f195fd52db2d3cba5d72ca6709bf1d94121bf3748801b40f6f5c"
	if got := greeting(cfg, "127.0.0.1:9", 3, false); got != want {
		t.Errorf("with a key: got %q, want %q", got, want)
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
		"LEDGER_SEGMENT_SIZE=3000000",
		"LEDGER_MAX_SESSIONS=4321",
		"LEDGER_MAX_CLIENTS=77",
		"LEDGER_SWEEP=off",
	})
	if err != nil {
		t.Fatal(err)
	}
	clients, peering := &net.TCPListener{}, &net.TCPListener{}
	logger := log.New(io.Discard, "", 0)
	disks := &countingFS{FS: disk.OS{}, dir: cfg.dir}
	sc := cfg.server(disks, clients, peering, logger)
	known, ok := sc.Peering.(*knownPeers)
	switch {
	case sc.ID != 3 || sc.Cluster != 0x99 || sc.Dir != "/srv/ledger/n3" || sc.FS != disks:
		t.Fatalf("id %d, cluster %d, dir %q, fs %T", sc.ID, sc.Cluster, sc.Dir, sc.FS)
	case strings.Join(sc.Peers, ",") != "10.0.0.1:7201,10.0.0.2:7201,10.0.0.3:7201,10.0.0.4:7201,[fd00::5]:7201":
		t.Fatalf("peers %v", sc.Peers)
	case sc.Tick != 7*time.Millisecond || sc.SnapshotEvery != 1234 || sc.SegmentSize != 3000000 || sc.MaxSessions != 4321 || sc.MaxClients != 77 || sc.Sweep:
		t.Fatalf("tick %v, snapshot every %d, segments of %d, sessions %d, clients %d, sweep %v", sc.Tick, sc.SnapshotEvery, sc.SegmentSize, sc.MaxSessions, sc.MaxClients, sc.Sweep)
	case sc.Clients != clients || !ok || known.Listener != peering || len(known.ips) != 5 || sc.Dial == nil || sc.Log != logger:
		t.Fatalf("clients %v, peering %#v, dial set %v, log %v", sc.Clients, sc.Peering, sc.Dial != nil, sc.Log)
	case sc.Key != nil:
		t.Fatalf("a key without LEDGER_KEY_FILE: %x", sc.Key)
	}
	cfg.key = ed25519.NewKeyFromSeed(bytes.Repeat([]byte{1}, ed25519.SeedSize))
	if !cfg.server(disks, clients, peering, logger).Key.Equal(cfg.key) {
		t.Fatal("the key did not reach the server")
	}
	cfg.sweep = true
	if !cfg.server(disks, clients, peering, logger).Sweep {
		t.Fatal("LEDGER_SWEEP=on did not reach the server")
	}
}

func TestConfigErrorExitsTwo(t *testing.T) {
	var stderr lockedBuffer
	if code := run(context.Background(), []string{"LEDGER_TICK=5ms"}, &stderr); code != 2 || stderr.String() != "ledgerd: LEDGER_PEERS is missing\n" {
		t.Fatalf("exit %d, stderr %q", code, stderr.String())
	}
}
