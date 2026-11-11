package disk_test

import (
	"bytes"
	"errors"
	"io"
	"io/fs"
	"math/rand/v2"
	"path/filepath"
	"slices"
	"syscall"
	"testing"

	"github.com/Demonisreal/dmn-ledger/internal/disk"
)

func backends(t *testing.T) map[string]func() (disk.FS, string) {
	return map[string]func() (disk.FS, string){
		"os":  func() (disk.FS, string) { return disk.OS{}, t.TempDir() },
		"sim": func() (disk.FS, string) { return disk.NewSim(rand.New(rand.NewPCG(1, 2))), "/d" },
	}
}

func TestFileContract(t *testing.T) {
	for name, mk := range backends(t) {
		t.Run(name, func(t *testing.T) {
			fsys, dir := mk()
			if err := fsys.MkdirAll(dir); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(dir, "a")
			f, err := fsys.Create(path)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := f.WriteAt([]byte("hello"), 0); err != nil {
				t.Fatal(err)
			}
			if _, err := f.WriteAt([]byte("x"), 8); err != nil {
				t.Fatal(err)
			}
			if size, _ := f.Size(); size != 9 {
				t.Fatalf("size = %d, want 9", size)
			}
			got := make([]byte, 9)
			if _, err := f.ReadAt(got, 0); err != nil {
				t.Fatal(err)
			}
			if want := []byte("hello\x00\x00\x00x"); !bytes.Equal(got, want) {
				t.Fatalf("read %q, want %q", got, want)
			}
			n, err := f.ReadAt(make([]byte, 4), 7)
			if n != 2 || !errors.Is(err, io.EOF) {
				t.Fatalf("short read = %d, %v; want 2, EOF", n, err)
			}
			if n, err := f.ReadAt(nil, 9); n != 0 || err != nil {
				t.Fatalf("empty read at end = %d, %v", n, err)
			}
			if err := f.Truncate(2); err != nil {
				t.Fatal(err)
			}
			if err := f.Truncate(4); err != nil {
				t.Fatal(err)
			}
			got = make([]byte, 4)
			if _, err := f.ReadAt(got, 0); err != nil {
				t.Fatal(err)
			}
			if want := []byte("he\x00\x00"); !bytes.Equal(got, want) {
				t.Fatalf("after truncate %q, want %q", got, want)
			}
			if err := f.Sync(); err != nil {
				t.Fatal(err)
			}
			if err := f.Close(); err != nil {
				t.Fatal(err)
			}

			if _, err := fsys.Open(filepath.Join(dir, "missing")); !errors.Is(err, fs.ErrNotExist) {
				t.Fatalf("open missing: %v", err)
			}
			if err := fsys.Rename(path, filepath.Join(dir, "b")); err != nil {
				t.Fatal(err)
			}
			if err := fsys.SyncDir(dir); err != nil {
				t.Fatal(err)
			}
			names, err := fsys.List(dir)
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(names, []string{"b"}) {
				t.Fatalf("list = %v", names)
			}
			if err := fsys.Remove(filepath.Join(dir, "b")); err != nil {
				t.Fatal(err)
			}
			if err := fsys.Remove(filepath.Join(dir, "b")); !errors.Is(err, fs.ErrNotExist) {
				t.Fatalf("second remove: %v", err)
			}
		})
	}
}

func newSim(seed uint64) *disk.Sim {
	return disk.NewSim(rand.New(rand.NewPCG(seed, seed)))
}

func write(t *testing.T, f disk.File, p []byte, off int64) {
	t.Helper()
	if _, err := f.WriteAt(p, off); err != nil {
		t.Fatal(err)
	}
}

func contents(t *testing.T, fsys disk.FS, name string) []byte {
	t.Helper()
	f, err := fsys.Open(name)
	if err != nil {
		t.Fatal(err)
	}
	size, _ := f.Size()
	b := make([]byte, size)
	if _, err := f.ReadAt(b, 0); err != nil && !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	return b
}

func TestSimCrashKeepsOnlySynced(t *testing.T) {
	sim := newSim(1)
	f, _ := sim.Create("/d/a")
	write(t, f, []byte("old"), 0)
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := sim.SyncDir("/d"); err != nil {
		t.Fatal(err)
	}
	if _, err := sim.Create("/d/unsynced"); err != nil {
		t.Fatal(err)
	}
	write(t, f, []byte("new"), 0)

	sim.Crash()

	if got := contents(t, sim, "/d/a"); string(got) != "old" && string(got) != "new" {
		t.Fatalf("a = %q", got)
	}
	names, _ := sim.List("/d")
	if !slices.Equal(names, []string{"a"}) {
		t.Fatalf("after crash list = %v, want [a]", names)
	}
}

func TestSimNamespaceNeedsSyncDir(t *testing.T) {
	sim := newSim(1)
	f, _ := sim.Create("/d/a")
	write(t, f, []byte("x"), 0)
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	sim.Crash()
	if names, _ := sim.List("/d"); len(names) != 0 {
		t.Fatalf("create survived without SyncDir: %v", names)
	}

	f, _ = sim.Create("/d/a")
	write(t, f, []byte("x"), 0)
	_ = f.Sync()
	_ = sim.SyncDir("/d")
	if err := sim.Rename("/d/a", "/d/b"); err != nil {
		t.Fatal(err)
	}
	if err := sim.Remove("/d/b"); err != nil {
		t.Fatal(err)
	}
	sim.Crash()
	if names, _ := sim.List("/d"); !slices.Equal(names, []string{"a"}) {
		t.Fatalf("rename/remove survived without SyncDir: %v", names)
	}
}

func TestSimTornWriteBySector(t *testing.T) {
	const sectors = 64
	sawOld, sawNew := false, false
	for seed := range uint64(20) {
		sim := newSim(seed)
		f, _ := sim.Create("/d/a")
		_ = sim.SyncDir("/d")
		write(t, f, bytes.Repeat([]byte{1}, sectors*disk.SectorSize), 0)
		_ = f.Sync()
		write(t, f, bytes.Repeat([]byte{2}, sectors*disk.SectorSize), 0)
		sim.Crash()

		got := contents(t, sim, "/d/a")
		if len(got) != sectors*disk.SectorSize {
			t.Fatalf("size = %d", len(got))
		}
		for sec := range sectors {
			s := got[sec*disk.SectorSize : (sec+1)*disk.SectorSize]
			switch {
			case bytes.Equal(s, bytes.Repeat([]byte{1}, disk.SectorSize)):
				sawOld = true
			case bytes.Equal(s, bytes.Repeat([]byte{2}, disk.SectorSize)):
				sawNew = true
			default:
				t.Fatalf("seed %d: sector %d mixed inside", seed, sec)
			}
		}
	}
	if !sawOld || !sawNew {
		t.Fatalf("torn writes never mixed sectors: old %v new %v", sawOld, sawNew)
	}
}

func TestSimTornAppendLeavesZeros(t *testing.T) {
	for seed := range uint64(20) {
		sim := newSim(seed)
		f, _ := sim.Create("/d/a")
		_ = sim.SyncDir("/d")
		write(t, f, bytes.Repeat([]byte{7}, 4*disk.SectorSize), 0)
		sim.Crash()

		got := contents(t, sim, "/d/a")
		if len(got) != 0 && len(got) != 4*disk.SectorSize {
			t.Fatalf("seed %d: size %d", seed, len(got))
		}
		for _, b := range got {
			if b != 0 && b != 7 {
				t.Fatalf("seed %d: garbage byte %d", seed, b)
			}
		}
	}
}

func TestSimDeterministic(t *testing.T) {
	run := func() []byte {
		sim := newSim(42)
		f, _ := sim.Create("/d/a")
		_ = sim.SyncDir("/d")
		g, _ := sim.Create("/d/b")
		_ = sim.SyncDir("/d")
		for i := range 8 {
			write(t, f, bytes.Repeat([]byte{byte(i)}, 3*disk.SectorSize), int64(i)*700)
			write(t, g, bytes.Repeat([]byte{byte(i)}, 2*disk.SectorSize), int64(i)*300)
		}
		sim.Crash()
		return append(contents(t, sim, "/d/a"), contents(t, sim, "/d/b")...)
	}
	if !bytes.Equal(run(), run()) {
		t.Fatal("same seed, different crash")
	}
}

func TestSimCloneIsIndependent(t *testing.T) {
	sim := newSim(1)
	f, _ := sim.Create("/d/a")
	_ = sim.SyncDir("/d")
	write(t, f, []byte("abc"), 0)
	_ = f.Sync()

	c := sim.Clone()
	write(t, f, []byte("xyz"), 0)
	_ = f.Sync()
	c.Crash()

	if got := contents(t, c, "/d/a"); string(got) != "abc" {
		t.Fatalf("clone sees %q", got)
	}
	if got := contents(t, sim, "/d/a"); string(got) != "xyz" {
		t.Fatalf("original sees %q", got)
	}
}

func TestSimCorruptFlipsOneBit(t *testing.T) {
	sim := newSim(3)
	f, _ := sim.Create("/d/a")
	_ = sim.SyncDir("/d")
	orig := bytes.Repeat([]byte{0x55}, 16)
	write(t, f, orig, 0)
	_ = f.Sync()

	if err := sim.Corrupt("/d/a", 5); err != nil {
		t.Fatal(err)
	}
	sim.Crash()
	got := contents(t, sim, "/d/a")
	for i := range got {
		diff := got[i] ^ orig[i]
		if (i == 5) != (diff != 0) || diff&(diff-1) != 0 {
			t.Fatalf("byte %d: %08b vs %08b", i, got[i], orig[i])
		}
	}
	if err := sim.Corrupt("/d/a", 16); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("corrupt past end: %v", err)
	}
}

func TestSimInjectedErrors(t *testing.T) {
	sim := newSim(9)
	sim.WriteErrRate, sim.SyncErrRate = 0.5, 0.5
	f, _ := sim.Create("/d/a")
	var writes, syncs int
	for range 200 {
		if _, err := f.WriteAt([]byte("x"), 0); errors.Is(err, syscall.EIO) {
			writes++
		}
		if err := f.Sync(); errors.Is(err, syscall.EIO) {
			syncs++
		}
	}
	if writes < 50 || writes > 150 || syncs < 50 || syncs > 150 {
		t.Fatalf("EIO writes %d syncs %d out of 200", writes, syncs)
	}
}
