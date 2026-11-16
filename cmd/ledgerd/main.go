package main

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Demonisreal/dmn-ledger/internal/disk"
	"github.com/Demonisreal/dmn-ledger/internal/replica"
	"github.com/Demonisreal/dmn-ledger/internal/server"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	code := run(ctx, os.Environ(), os.Stderr)
	stop()
	os.Exit(code)
}

func run(ctx context.Context, environ []string, stderr io.Writer) int {
	began := time.Now()
	asked := make(chan os.Signal, 1)
	signal.Notify(asked, syscall.SIGUSR1)
	defer signal.Stop(asked)
	cfg, err := parse(environ)
	if err != nil {
		fmt.Fprintln(stderr, "ledgerd:", err)
		return 2
	}
	logger := log.New(stderr, "", log.LstdFlags|log.Lmicroseconds|log.LUTC)
	stopProfile, err := startProfile(cfg.profile)
	if err != nil {
		logger.Printf("node %d cannot start: %v", cfg.id, err)
		return 3
	}
	defer func() {
		if err := stopProfile(); err != nil {
			logger.Printf("node %d could not finish the profile: %v", cfg.id, err)
		}
	}()
	disks := &countingFS{FS: disk.OS{}, dir: cfg.dir}
	srv, clients, err := start(cfg, disks, logger)
	if errors.Is(err, replica.ErrMaxSessions) {
		logger.Printf("node %d cannot start: LEDGER_MAX_SESSIONS=%d does not fit the data directory, set the value the cluster runs with: %v", cfg.id, cfg.maxSessions, err)
		return 2
	}
	if err != nil {
		logger.Printf("node %d cannot start: %v", cfg.id, err)
		return 3
	}
	term, repair := srv.Opened()
	logger.Print(greeting(cfg, clients, term, repair))
	return serve(ctx, srv.Run, cfg, logger, asked, func() string {
		return statsLine(time.Since(began), srv.Stats(), disks)
	})
}

func start(cfg config, fsys disk.FS, logger *log.Logger) (*server.Server, string, error) {
	clients, err := net.Listen("tcp", cfg.clients.String())
	if err != nil {
		return nil, "", err
	}
	peering, err := listenPeers(cfg.peers[cfg.id-1].String())
	if err != nil {
		_ = clients.Close()
		return nil, "", err
	}
	srv, err := server.Open(cfg.server(fsys, clients, peering, logger))
	if err != nil {
		_ = clients.Close()
		_ = peering.Close()
		return nil, "", err
	}
	return srv, clients.Addr().String(), nil
}

func (cfg config) server(fsys disk.FS, clients, peering net.Listener, logger *log.Logger) server.Config {
	addrs := make([]string, len(cfg.peers))
	for i, p := range cfg.peers {
		addrs[i] = p.String()
	}
	return server.Config{
		ID:            cfg.id,
		Cluster:       cfg.cluster,
		Peers:         addrs,
		Dir:           cfg.dir,
		FS:            fsys,
		Tick:          cfg.tick,
		SnapshotEvery: cfg.snapshotEvery,
		SegmentSize:   cfg.segmentSize,
		MaxSessions:   cfg.maxSessions,
		MaxClients:    cfg.maxClients,
		Sweep:         cfg.sweep,
		Key:           cfg.key,
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
	key := "no key"
	if cfg.key != nil {
		key = "key " + hex.EncodeToString(cfg.key.Public().(ed25519.PublicKey))
	}
	return fmt.Sprintf("node %d of %d in cluster %d started: peers on %s, clients on %s, term %d, repair %s, %s",
		cfg.id, len(cfg.peers), cfg.cluster, cfg.peers[cfg.id-1], clients, term, flag, key)
}

func serve(ctx context.Context, runServer func(context.Context) error, cfg config, logger *log.Logger, asked <-chan os.Signal, stats func() string) int {
	ran := make(chan error, 1)
	go func() { ran <- runServer(ctx) }()
	for {
		select {
		case <-asked:
			logger.Print(stats())
		case err := <-ran:
			logger.Print(stats())
			if err != nil {
				logger.Printf("node %d halted: %v", cfg.id, err)
				return 1
			}
			logger.Printf("node %d stopped: %v", cfg.id, context.Cause(ctx))
			return 0
		}
	}
}
