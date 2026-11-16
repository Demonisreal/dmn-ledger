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
	"time"

	"github.com/Demonisreal/dmn-ledger/internal/disk"
	"github.com/Demonisreal/dmn-ledger/internal/server"
	"github.com/Demonisreal/dmn-ledger/ledger"
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
	return serve(ctx, srv.Run, cfg, clients, logger)
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
		MaxSessions:   cfg.maxSessions,
		MaxClients:    cfg.maxClients,
		Sweep:         cfg.sweep,
		Clients:       clients,
		Peering:       &knownPeers{Listener: peering, ips: peerIPs(cfg.peers), log: logger},
		Dial:          dialPeer,
		Log:           logger,
	}
}

func greeting(ctx context.Context, cfg config, clients string) string {
	line := fmt.Sprintf("node %d of %d in cluster %d started: peers on %s, clients on %s",
		cfg.id, len(cfg.peers), cfg.cluster, cfg.peers[cfg.id-1], clients)
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	st, err := ledger.Status(ctx, cfg.cluster, clients)
	if err != nil {
		return fmt.Sprintf("%s, no status: %v", line, err)
	}
	repair := "no"
	if st.Repair {
		repair = "yes"
	}
	return fmt.Sprintf("%s, term %d, repair %s", line, st.Term, repair)
}

func serve(ctx context.Context, runServer func(context.Context) error, cfg config, clients string, logger *log.Logger) int {
	halted := make(chan error, 1)
	go func() { halted <- runServer(ctx) }()
	logger.Print(greeting(ctx, cfg, clients))
	if err := <-halted; err != nil {
		logger.Printf("node %d halted: %v", cfg.id, err)
		return 1
	}
	logger.Printf("node %d stopped: %v", cfg.id, context.Cause(ctx))
	return 0
}
