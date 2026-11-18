package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"testing/fstest"
	"testing/synctest"
)

func machine(limit, used, free string) fstest.MapFS {
	return fstest.MapFS{
		"proc/self/cgroup": {Data: []byte("0::/user.slice/user-1000.slice/user@1000.service/app.slice/run-r1.scope\n")},
		"sys/fs/cgroup/user.slice/user-1000.slice/user@1000.service/app.slice/run-r1.scope/memory.max":     {Data: []byte(limit + "\n")},
		"sys/fs/cgroup/user.slice/user-1000.slice/user@1000.service/app.slice/run-r1.scope/memory.current": {Data: []byte(used + "\n")},
		"sys/fs/cgroup/system.slice/docker-abc.scope/memory.max":                                           {Data: []byte("3221225472\n")},
		"sys/fs/cgroup/system.slice/docker-abc.scope/memory.current":                                       {Data: []byte("1073741824\n")},
		"proc/meminfo": {Data: []byte("MemTotal:       16045084 kB\nMemFree:         1185000 kB\nMemAvailable:    " + free + " kB\n")},
	}
}

func TestMemoryBeforeEachRun(t *testing.T) {
	fsys := machine("6442450944", "1073741824", "7340032")
	cg, err := ownCgroup(fsys)
	if err != nil {
		t.Fatal(err)
	}
	if cg != "user.slice/user-1000.slice/user@1000.service/app.slice/run-r1.scope" {
		t.Fatalf("cgroup %q", cg)
	}
	room, err := headroom(fsys, []string{cg})
	if err != nil || room != 6<<30 {
		t.Fatalf("headroom %d MiB, %v; want 5 GiB left under the limit and 1 GiB to spare", room>>20, err)
	}
	if room, _ = headroom(fsys, []string{cg, "system.slice/docker-abc.scope"}); room != 8<<30 {
		t.Fatalf("with the container: %d MiB, want 8 GiB", room>>20)
	}
	if err := awaitMemory(t.Context(), fsys, []string{cg}, io.Discard); err != nil {
		t.Fatalf("7 GiB available for 6 GiB: %v", err)
	}
	stopped, cancel := context.WithCancel(t.Context())
	cancel()
	if err := awaitMemory(stopped, fsys, []string{cg, "system.slice/docker-abc.scope"}, io.Discard); !errors.Is(err, context.Canceled) {
		t.Fatalf("7 GiB available for 8 GiB: %v, want to wait until the context ends", err)
	}
	synctest.Test(t, func(t *testing.T) {
		err := awaitMemory(t.Context(), fsys, []string{cg, "system.slice/docker-abc.scope"}, io.Discard)
		for _, want := range []string{"7168 MiB available, 1024 MiB missing", "limits 6144 MiB and 3072 MiB", "BENCH_MEMORY=5G"} {
			if !errors.Is(err, errMemory) || !strings.Contains(err.Error(), want) {
				t.Errorf("7 GiB available for 8 GiB after a minute: %v, want it to say %q", err, want)
			}
		}
	})
	for _, c := range []struct{ limit, want uint64 }{{6 << 30, 3 << 30}, {5 << 30, 2 << 30}, {4 << 30, 1536 << 20}, {3 << 30, 1536 << 20}, {8 << 30, 3 << 30}} {
		if got := (&bench{limit: c.limit}).postgresLimit(); got != c.want {
			t.Errorf("plan limit %d MiB: Postgres runs get %d MiB, want %d MiB next to the 3 GiB of the container; "+
				"the generator held 680 MiB with 100 000 calls open", c.limit>>20, got>>20, c.want>>20)
		}
	}
	unlimited := machine("max", "1073741824", "7340032")
	if limit, _, err := cgroupMemory(unlimited, cg); err != nil || limit != 0 {
		t.Fatalf("memory.max of max: limit %d, %v", limit, err)
	}
	if _, err := headroom(unlimited, []string{cg}); !errors.Is(err, errNoLimit) {
		t.Fatalf("no limit: %v, want errNoLimit", err)
	}
}

func TestOwnLedgerd(t *testing.T) {
	self := os.Getpid()
	fsys := fstest.MapFS{
		"proc/101/comm": {Data: []byte("ledgerd\n")},
		"proc/101/stat": {Data: []byte(fmt.Sprintf("101 (ledgerd) S %d 101 101 0 -1", self))},
		"proc/102/comm": {Data: []byte("ledgerd\n")},
		"proc/102/stat": {Data: []byte("102 (ledgerd) S 1 102 102 0 -1")},
		"proc/103/comm": {Data: []byte("bash\n")},
		"proc/103/stat": {Data: []byte(fmt.Sprintf("103 (bash) S %d 103 103 0 -1", self))},
	}
	if got := ownLedgerd(fsys); !slices.Equal(got, []int{101}) {
		t.Fatalf("own ledgerd %v, want only 101: 102 belongs to someone else", got)
	}
}

func TestLeftovers(t *testing.T) {
	dir := t.TempDir()
	if err := leftovers(dir); err != nil {
		t.Fatalf("empty bench directory: %v", err)
	}
	for _, sub := range []string{"run", "pg"} {
		if err := os.MkdirAll(filepath.Join(dir, sub, "n1"), 0o750); err != nil {
			t.Fatal(err)
		}
		if err := leftovers(dir); !errors.Is(err, errLeftover) {
			t.Fatalf("%s not empty: %v, want errLeftover", sub, err)
		}
		if err := os.RemoveAll(filepath.Join(dir, sub)); err != nil {
			t.Fatal(err)
		}
	}
}

func TestPlanRefusesARunningContainer(t *testing.T) {
	bin := t.TempDir()
	if err := os.WriteFile(filepath.Join(bin, "docker"), []byte(fakeDocker), 0o700); err != nil { //nolint:gosec // G306: a script the test runs
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+":/usr/bin:/bin")
	t.Setenv("DOCKER_LOG", filepath.Join(bin, "docker.log"))
	for _, postgres := range []bool{false, true} {
		t.Setenv("FAKE_RUNNING", "1")
		if err := refuseRunning(t.Context(), postgres); !errors.Is(err, errRunning) {
			t.Errorf("postgres %v, container runs: %v, want errRunning", postgres, err)
		}
		t.Setenv("FAKE_RUNNING", "0")
		if err := refuseRunning(t.Context(), postgres); err != nil {
			t.Errorf("postgres %v, no container: %v", postgres, err)
		}
	}
	t.Setenv("PATH", t.TempDir())
	if err := refuseRunning(t.Context(), false); err != nil {
		t.Errorf("no docker and no Postgres in the plan: %v", err)
	}
	if err := refuseRunning(t.Context(), true); err == nil {
		t.Error("no docker and Postgres in the plan: no error")
	}
}
