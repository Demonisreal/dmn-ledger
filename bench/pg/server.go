package pg

import (
	"context"
	"crypto/rand"
	_ "embed"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
)

var (
	ErrFilesystem = errors.New("pg: data directory not on ext4")
	ErrLeftover   = errors.New("pg: data directory not empty")
	ErrPortTaken  = errors.New("pg: port taken")
	ErrNotReady   = errors.New("pg: server did not come up")
)

const (
	Name  = "dmn-ledger-bench-pg"
	Port  = 55439
	Cores = "0-5"

	ext4        = 0xef53
	readyWithin = 60 * time.Second
)

//go:embed image.txt
var image string

func Image() string {
	return strings.TrimSpace(image)
}

var settings = []string{
	"listen_addresses=127.0.0.1",
	"port=" + strconv.Itoa(Port),
	"shared_buffers=1GB",
	"max_wal_size=8GB",
	"checkpoint_timeout=15min",
	"wal_sync_method=fdatasync",
	"track_wal_io_timing=on",
}

type Server struct {
	dir      string
	password string
	cgroup   string
}

func Start(ctx context.Context, dir string) (*Server, error) {
	dir, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dir, 0o700); err != nil { //nolint:gosec // G703: the data directory the caller chose
		return nil, err
	}
	var fs syscall.Statfs_t
	if err := syscall.Statfs(dir, &fs); err != nil {
		return nil, err
	}
	if fs.Type != ext4 {
		return nil, fmt.Errorf("%w: %s has file system %#x", ErrFilesystem, dir, fs.Type)
	}
	if left, _ := os.ReadDir(dir); len(left) > 0 {
		return nil, fmt.Errorf("%w: %s holds %d entries", ErrLeftover, dir, len(left))
	}
	l, err := net.Listen("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(Port)))
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrPortTaken, err)
	}
	_ = l.Close()

	s := &Server{dir: dir, password: rand.Text()}
	secret := filepath.Join(dir, "pw")
	if err := os.WriteFile(secret, []byte(s.password), 0o600); err != nil { //nolint:gosec // G703: inside the data directory the caller chose
		return nil, err
	}
	data := filepath.Join(dir, "data")
	if err := os.Mkdir(data, 0o700); err != nil { //nolint:gosec // G703: inside the data directory the caller chose
		return nil, err
	}
	args := []string{"run", "-d", "--name", Name, "--network", "host",
		"--cpuset-cpus", Cores, "--memory", "3g", "--memory-swap", "3g", "--shm-size", "256m",
		"--user", fmt.Sprintf("%d:%d", os.Getuid(), os.Getgid()),
		"-v", data + ":/var/lib/postgresql",
		"-e", "POSTGRES_PASSWORD_FILE=/run/secrets/pw", "-v", secret + ":/run/secrets/pw:ro",
		Image()}
	for _, kv := range settings {
		args = append(args, "-c", kv)
	}
	var stderr strings.Builder
	run := exec.CommandContext(ctx, "docker", args...) //nolint:gosec // G204: docker run of the pinned image under our own name
	run.Stderr = &stderr
	id, err := run.Output()
	if err != nil {
		return nil, errors.Join(fmt.Errorf("pg: docker run: %w\n%s", err, stderr.String()), os.RemoveAll(dir)) //nolint:gosec // G703: the data directory the caller chose, empty before Start
	}
	s.cgroup = "system.slice/docker-" + strings.TrimSpace(string(id)) + ".scope"
	if err := s.ready(ctx); err != nil {
		logs, _ := exec.Command("docker", "logs", "--tail", "20", Name).CombinedOutput()
		return nil, errors.Join(fmt.Errorf("%w: %w\n%s", ErrNotReady, err, logs), s.Stop())
	}
	return s, nil
}

func (s *Server) ready(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, readyWithin)
	defer cancel()
	tick := time.NewTicker(200 * time.Millisecond)
	defer tick.Stop()
	for {
		conn, err := pgx.Connect(ctx, s.dsn("postgres"))
		if err == nil {
			return conn.Close(ctx)
		}
		select {
		case <-ctx.Done():
			return errors.Join(context.Cause(ctx), err)
		case <-tick.C:
		}
	}
}

func (s *Server) dsn(db string) string {
	u := url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword("postgres", s.password),
		Host:     net.JoinHostPort("127.0.0.1", strconv.Itoa(Port)),
		Path:     db,
		RawQuery: "sslmode=disable",
	}
	return u.String()
}

func (s *Server) Dir() string {
	return s.dir
}

func (s *Server) Cgroup() string {
	return s.cgroup
}

func (s *Server) Stop() error {
	var errs []error
	for _, args := range [][]string{{"stop", Name}, {"rm", Name}} {
		if msg, err := exec.Command("docker", args...).CombinedOutput(); err != nil { //nolint:gosec // G204: stop and remove our own container by its name
			errs = append(errs, fmt.Errorf("pg: docker %s: %w\n%s", args[0], err, msg))
		}
	}
	return errors.Join(append(errs, os.RemoveAll(s.dir))...)
}
