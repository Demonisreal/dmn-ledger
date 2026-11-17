package main

import (
	"errors"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

const fakeDocker = `#!/bin/sh
echo "$*" >>"$DOCKER_LOG"
if [ "$1 $2" = "container inspect" ] && [ "$FAKE_RUNNING" != 1 ]; then
	echo "Error: No such container: $3" >&2
	exit 1
fi
exit 0
`

type script struct {
	t    *testing.T
	bin  string
	log  string
	root string
}

func newScript(t *testing.T) *script {
	t.Helper()
	bin := t.TempDir()
	if err := os.WriteFile(filepath.Join(bin, "docker"), []byte(fakeDocker), 0o700); err != nil { //nolint:gosec // G306: a script the test runs
		t.Fatal(err)
	}
	root, err := os.MkdirTemp("/var/tmp", "bench-clean-")
	if err != nil {
		t.Skipf("no /var/tmp to try clean in: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	return &script{t: t, bin: bin, log: filepath.Join(bin, "docker.log"), root: root}
}

func (s *script) run(dir string, running bool, args ...string) (string, error) {
	s.t.Helper()
	_ = os.Remove(s.log)
	cmd := exec.Command("bash", append([]string{"../../../scripts/bench.sh"}, args...)...) //nolint:gosec // G204: the script under test with the arguments of the case
	cmd.Env = append(os.Environ(), "BENCH_DIR="+dir, "PATH="+s.bin+":"+os.Getenv("PATH"), "DOCKER_LOG="+s.log, "FAKE_RUNNING=0")
	if running {
		cmd.Env = append(cmd.Env, "FAKE_RUNNING=1")
	}
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func (s *script) docker() string {
	b, err := os.ReadFile(s.log)
	if errors.Is(err, fs.ErrNotExist) {
		return ""
	}
	if err != nil {
		s.t.Fatal(err)
	}
	return string(b)
}

func TestCleanRefusesForeignPaths(t *testing.T) {
	s := newScript(t)
	foreign := filepath.Join(s.root, "foreign")
	if err := os.MkdirAll(filepath.Join(foreign, "keep"), 0o750); err != nil {
		t.Fatal(err)
	}
	plain := filepath.Join(s.root, "plain")
	if err := os.WriteFile(plain, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	outside, err := os.MkdirTemp(".", "outside-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(outside) })
	if err := os.WriteFile(filepath.Join(outside, ".dmn-ledger-bench"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if outside, err = filepath.Abs(outside); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(s.root, "link")
	if err := os.Symlink("/etc", link); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		name string
		dir  string
		args []string
	}{
		{"the root", "/", []string{"clean"}},
		{"the home folder", os.Getenv("HOME"), []string{"clean"}},
		{"/var/tmp itself", "/var/tmp", []string{"clean"}},
		{"/var/tmp through dots", s.root + "/../..", []string{"clean"}},
		{"a folder with files and no marker", foreign, []string{"clean"}},
		{"a file", plain, []string{"clean"}},
		{"a link out of /var/tmp", link, []string{"clean"}},
		{"a marked folder outside /var/tmp", outside, []string{"clean"}},
		{"the root, for a plan", "/", []string{"all"}},
	} {
		out, err := s.run(c.dir, true, c.args...)
		var exit *exec.ExitError
		if !errors.As(err, &exit) || exit.ExitCode() != 1 || !strings.Contains(out, "refusing") {
			t.Errorf("%s: exit %v, output %q; want a refusal", c.name, err, out)
		}
		if calls := s.docker(); calls != "" {
			t.Errorf("%s: docker called before the path was checked: %q", c.name, calls)
		}
	}
	if _, err := os.Stat(filepath.Join(foreign, "keep")); err != nil {
		t.Errorf("the foreign folder lost its content: %v", err)
	}
	if _, err := os.Stat(outside); err != nil {
		t.Errorf("the marked folder outside /var/tmp is gone: %v", err)
	}
	if _, err := os.Stat(plain); err != nil {
		t.Errorf("the file is gone: %v", err)
	}
}

func TestCleanRemovesItsOwn(t *testing.T) {
	s := newScript(t)
	own := filepath.Join(s.root, "bench")
	if err := os.MkdirAll(filepath.Join(own, "pg", "data"), 0o750); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{".dmn-ledger-bench", "pg/pw"} {
		if err := os.WriteFile(filepath.Join(own, name), nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	out, err := s.run(own, true, "clean")
	if err != nil {
		t.Fatalf("clean: %v\n%s", err, out)
	}
	if want := "container inspect dmn-ledger-bench-pg\nstop dmn-ledger-bench-pg\nrm dmn-ledger-bench-pg\n"; s.docker() != want {
		t.Errorf("docker calls %q, want %q", s.docker(), want)
	}
	if _, err := os.Stat(own); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("%s still there: %v", own, err)
	}
	empty := filepath.Join(s.root, "empty")
	if err := os.Mkdir(empty, 0o750); err != nil {
		t.Fatal(err)
	}
	if out, err := s.run(empty, false, "clean"); err != nil {
		t.Fatalf("clean of an empty folder: %v\n%s", err, out)
	}
	if calls := s.docker(); calls != "container inspect dmn-ledger-bench-pg\n" {
		t.Errorf("docker calls without a container: %q", calls)
	}
	if _, err := os.Stat(empty); !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("%s still there: %v", empty, err)
	}
	if out, err := s.run(own, false, "clean", "extra"); err == nil {
		t.Errorf("clean with an argument: %s", out)
	}
}
