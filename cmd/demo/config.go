package main

import (
	"errors"
	"fmt"
	"io/fs"
	"net/netip"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
)

type config struct {
	dir     string
	addr    netip.AddrPort
	origin  string
	proxies []netip.Prefix
	ledgerd string
}

var defaultPorts = map[string]uint64{"http": 80, "https": 443}

var runFiles = []string{"n1", "n2", "n3", "n1.key", "n2.key", "n3.key"}

func parse(environ []string, exe string) (config, error) {
	vars := make(map[string]string)
	for _, kv := range environ {
		if name, given, ok := strings.Cut(kv, "="); ok && strings.HasPrefix(name, "DEMO_") {
			vars[name] = given
		}
	}
	cfg := config{addr: netip.MustParseAddrPort("127.0.0.1:8080"), ledgerd: filepath.Join(filepath.Dir(exe), "ledgerd")}
	steps := []struct {
		name     string
		required bool
		set      func(string) error
	}{
		{"DEMO_DIR", true, func(s string) (err error) { cfg.dir, err = absolute(s); return err }},
		{"DEMO_ADDR", false, func(s string) (err error) { cfg.addr, err = address(s); return err }},
		{"DEMO_ORIGIN", false, func(s string) (err error) { cfg.origin, err = origin(s); return err }},
		{"DEMO_PROXIES", false, func(s string) (err error) { cfg.proxies, err = networks(s); return err }},
		{"DEMO_LEDGERD", false, func(s string) (err error) { cfg.ledgerd, err = absolute(s); return err }},
	}
	for _, st := range steps {
		given, ok := vars[st.name]
		delete(vars, st.name)
		if !ok {
			if st.required {
				return config{}, fmt.Errorf("%s is missing", st.name)
			}
			continue
		}
		if err := st.set(given); err != nil {
			return config{}, fmt.Errorf("%s=%q: %w", st.name, given, err)
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
	if cfg.origin == "" {
		cfg.origin = "http://" + cfg.addr.String()
	}
	return cfg, nil
}

func absolute(s string) (string, error) {
	if !filepath.IsAbs(s) {
		return "", errors.New("want an absolute path")
	}
	return s, nil
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

func origin(s string) (string, error) {
	u, err := url.Parse(s)
	if err != nil || u.Scheme != "http" && u.Scheme != "https" || u.Hostname() == "" {
		return "", errors.New("want http:// or https://, a host and an optional port")
	}
	host := strings.ToLower(u.Hostname())
	if strings.Contains(host, ":") {
		host = "[" + host + "]"
	}
	want := u.Scheme + "://" + host
	if port := u.Port(); port != "" {
		n, err := strconv.ParseUint(port, 10, 16)
		if err != nil || n == 0 {
			return "", errors.New("want a port from 1 to 65535")
		}
		if n != defaultPorts[u.Scheme] {
			want += ":" + strconv.FormatUint(n, 10)
		}
	}
	if s != want {
		return "", fmt.Errorf("want %s, as a browser sends it", want)
	}
	return s, nil
}

func networks(s string) ([]netip.Prefix, error) {
	if s == "" {
		return nil, nil
	}
	var nets []netip.Prefix
	for entry := range strings.SplitSeq(s, ",") {
		p, err := netip.ParsePrefix(entry)
		switch {
		case err != nil:
			return nil, fmt.Errorf("%q: want a network like 172.30.0.0/24", entry)
		case p != p.Masked():
			return nil, fmt.Errorf("%q: want the network %s", entry, p.Masked())
		}
		nets = append(nets, p)
	}
	return nets, nil
}

func (cfg config) prepare() error {
	if _, err := exec.LookPath(cfg.ledgerd); err != nil {
		return err
	}
	if err := os.MkdirAll(cfg.dir, 0o700); err != nil {
		return err
	}
	if err := ownRun(filepath.Join(cfg.dir, "run")); err != nil {
		return err
	}
	probe, err := os.CreateTemp(cfg.dir, ".probe-")
	if err != nil {
		return err
	}
	_ = probe.Close()
	return os.Remove(probe.Name())
}

func ownRun(run string) error {
	left, err := os.ReadDir(run)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, e := range left {
		if !slices.Contains(runFiles, e.Name()) {
			return fmt.Errorf("%s holds %s, which no demo wrote; DEMO_DIR must be a directory of its own", run, e.Name())
		}
	}
	return nil
}

func greeting(cfg config) string {
	proxies := "none"
	if len(cfg.proxies) > 0 {
		nets := make([]string, len(cfg.proxies))
		for i, p := range cfg.proxies {
			nets[i] = p.String()
		}
		proxies = strings.Join(nets, ",")
	}
	return fmt.Sprintf("demo started: http on %s, origin %s, proxies %s, ledgerd %s, data in %s", cfg.addr, cfg.origin, proxies, cfg.ledgerd, cfg.dir)
}
