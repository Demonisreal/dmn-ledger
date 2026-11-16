package main

import (
	"errors"
	"fmt"
	"math"
	"net/netip"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/Demonisreal/dmn-ledger/ledger"
)

type config struct {
	id            uint64
	cluster       uint64
	peers         []netip.AddrPort
	clients       netip.AddrPort
	dir           string
	tick          time.Duration
	snapshotEvery uint64
	maxSessions   int
	maxClients    int
	sweep         bool
}

func parse(environ []string) (config, error) {
	vars := make(map[string]string)
	for _, kv := range environ {
		if name, val, ok := strings.Cut(kv, "="); ok && strings.HasPrefix(name, "LEDGER_") {
			vars[name] = val
		}
	}
	if err := takeBug(vars); err != nil {
		return config{}, err
	}
	cfg := config{tick: 10 * time.Millisecond, snapshotEvery: 100000, maxSessions: 1024, maxClients: 128, sweep: true}
	steps := []struct {
		name     string
		required bool
		set      func(string) error
	}{
		{"LEDGER_PEERS", true, func(s string) (err error) { cfg.peers, err = peerList(s); return err }},
		{"LEDGER_ID", true, func(s string) (err error) { cfg.id, err = number(s, 1, uint64(len(cfg.peers))); return err }},
		{"LEDGER_CLUSTER", true, func(s string) (err error) { cfg.cluster, err = clusterID(s); return err }},
		{"LEDGER_CLIENT_ADDR", true, func(s string) (err error) { cfg.clients, err = address(s); return err }},
		{"LEDGER_DIR", true, func(s string) error {
			if !filepath.IsAbs(s) {
				return errors.New("want an absolute path")
			}
			cfg.dir = s
			return nil
		}},
		{"LEDGER_TICK", false, func(s string) error {
			d, err := time.ParseDuration(s)
			if err != nil || d < time.Millisecond || d > time.Second {
				return errors.New("want a duration from 1ms to 1s")
			}
			cfg.tick = d
			return nil
		}},
		{"LEDGER_SNAPSHOT_EVERY", false, func(s string) (err error) { cfg.snapshotEvery, err = number(s, 1, 1<<32); return err }},
		{"LEDGER_MAX_SESSIONS", false, func(s string) error {
			n, err := number(s, 1, 1000000)
			cfg.maxSessions = int(n) //nolint:gosec // G115: at most 1000000
			return err
		}},
		{"LEDGER_MAX_CLIENTS", false, func(s string) error {
			n, err := number(s, 1, 4096)
			cfg.maxClients = int(n) //nolint:gosec // G115: at most 4096
			return err
		}},
		{"LEDGER_SWEEP", false, func(s string) error {
			if s != "on" && s != "off" {
				return errors.New("want on or off")
			}
			cfg.sweep = s == "on"
			return nil
		}},
	}
	for _, st := range steps {
		val, ok := vars[st.name]
		delete(vars, st.name)
		if !ok {
			if st.required {
				return config{}, fmt.Errorf("%s is missing", st.name)
			}
			continue
		}
		if err := st.set(val); err != nil {
			return config{}, fmt.Errorf("%s=%q: %w", st.name, val, err)
		}
	}
	if len(vars) > 0 {
		names := make([]string, 0, len(vars))
		for name := range vars {
			names = append(names, name)
		}
		slices.Sort(names)
		return config{}, fmt.Errorf("unknown variable %s", strings.Join(names, ", "))
	}
	return cfg, nil
}

func number(s string, lo, hi uint64) (uint64, error) {
	n, err := strconv.ParseUint(s, 10, 64)
	if err != nil || n < lo || n > hi {
		return 0, fmt.Errorf("want a number from %d to %d", lo, hi)
	}
	return n, nil
}

func clusterID(s string) (uint64, error) {
	u, err := ledger.Parse(s)
	if err != nil || u.Hi != 0 || u.Lo == 0 {
		return 0, fmt.Errorf("want a number from 1 to %d, decimal or 0x", uint64(math.MaxUint64))
	}
	return u.Lo, nil
}

func address(s string) (netip.AddrPort, error) {
	ap, err := netip.ParseAddrPort(s)
	switch {
	case err != nil:
		return netip.AddrPort{}, errors.New("want IP:port, no host names")
	case ap.Port() == 0:
		return netip.AddrPort{}, errors.New("want a port other than 0")
	}
	return ap, nil
}

func peerList(s string) ([]netip.AddrPort, error) {
	var peers []netip.AddrPort
	for entry := range strings.SplitSeq(s, ",") {
		ap, err := address(entry)
		if err != nil {
			return nil, fmt.Errorf("%q: %w", entry, err)
		}
		if slices.Contains(peers, ap) {
			return nil, fmt.Errorf("%s twice", ap)
		}
		peers = append(peers, ap)
	}
	if n := len(peers); n != 1 && n != 3 && n != 5 {
		return nil, fmt.Errorf("%d peers, want 1, 3 or 5", n)
	}
	return peers, nil
}
