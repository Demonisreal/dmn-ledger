package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/Demonisreal/dmn-ledger/internal/disk"
	"github.com/Demonisreal/dmn-ledger/internal/server"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	code := run(ctx, os.Environ(), os.Stderr)
	stop()
	os.Exit(code)
}

func run(ctx context.Context, environ []string, stderr io.Writer) int {
	cfg, err := parse(environ)
	if err != nil {
		fmt.Fprintln(stderr, "ledgerd:", err)
		return 2
	}
	logger := log.New(stderr, "", log.LstdFlags|log.Lmicroseconds|log.LUTC)
	srv, clients, err := start(cfg, logger)
	if err != nil {
		logger.Printf("node %d cannot start: %v", cfg.id, err)
		return 3
	}
	term, repair := srv.Opened()
	logger.Print(greeting(cfg, clients, term, repair))
	return serve(ctx, srv.Run, cfg, logger)
}

func start(cfg config, logger *log.Logger) (*server.Server, string, error) {
	clients, err := net.Listen("tcp", cfg.clients.String())
	if err != nil {
		return nil, "", err
	}
	peering, err := listenPeers(cfg.peers[cfg.id-1].String())
	if err != nil {
		_ = clients.Close()
		return nil, "", err
	}
	srv, err := server.Open(cfg.server(clients, peering, logger))
	if err != nil {
		_ = clients.Close()
		_ = peering.Close()
		return nil, "", err
	}
	return srv, clients.Addr().String(), nil
}

func (cfg config) server(clients, peering net.Listener, logger *log.Logger) server.Config {
	addrs := make([]string, len(cfg.peers))
	for i, p := range cfg.peers {
		addrs[i] = p.String()
	}
	return server.Config{
		ID:            cfg.id,
		Cluster:       cfg.cluster,
		Peers:         addrs,
		Dir:           cfg.dir,
		FS:            disk.OS{},
		Tick:          cfg.tick,
		SnapshotEvery: cfg.snapshotEvery,
		SegmentSize:   cfg.segmentSize,
		MaxSessions:   cfg.maxSessions,
		MaxClients:    cfg.maxClients,
		Sweep:         cfg.sweep,
		Clients:       clients,
		Peering:       &knownPeers{Listener: peering, ips: peerIPs(cfg.peers), log: logger},
		Dial:          dialPeer,
		Log:           logger,
	}
}

func greeting(cfg config, clients string, term uint64, repair bool) string {
	flag := "no"
	if repair {
		flag = "yes"
	}
	return fmt.Sprintf("node %d of %d in cluster %d started: peers on %s, clients on %s, term %d, repair %s",
		cfg.id, len(cfg.peers), cfg.cluster, cfg.peers[cfg.id-1], clients, term, flag)
}

func serve(ctx context.Context, runServer func(context.Context) error, cfg config, logger *log.Logger) int {
	if err := runServer(ctx); err != nil {
		logger.Printf("node %d halted: %v", cfg.id, err)
		return 1
	}
	logger.Printf("node %d stopped: %v", cfg.id, context.Cause(ctx))
	return 0
}
