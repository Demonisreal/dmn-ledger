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
		"sim": func() (disk.FS, string) { return disk.NewSim(rand.New(rand.NewPCG(1, 2))), "/d/e" },
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

			l, err := fsys.Lock(dir)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := fsys.Lock(dir); !errors.Is(err, disk.ErrLocked) {
				t.Fatalf("second lock: %v", err)
			}
			if err := l.Close(); err != nil {
				t.Fatal(err)
			}
			l, err = fsys.Lock(dir)
			if err != nil {
				t.Fatalf("lock after release: %v", err)
			}
			_ = l.Close()
		})
	}
}

func newSim(seed uint64) *disk.Sim {
	sim := disk.NewSim(rand.New(rand.NewPCG(seed, seed)))
	_ = sim.MkdirAll("/d")
	return sim
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
	for _, sector := range []int{disk.SectorSize, 4096} {
		const sectors = 16
		size := sectors * sector
		sawOld, sawNew := false, false
		for seed := range uint64(20) {
			sim := newSim(seed)
			sim.Sector = int64(sector)
			f, _ := sim.Create("/d/a")
			_ = sim.SyncDir("/d")
			write(t, f, bytes.Repeat([]byte{1}, size), 0)
			_ = f.Sync()
			write(t, f, bytes.Repeat([]byte{2}, size), 0)
			sim.Crash()

			got := contents(t, sim, "/d/a")
			if len(got) != size {
				t.Fatalf("size = %d", len(got))
			}
			for sec := range sectors {
				s := got[sec*sector : (sec+1)*sector]
				switch {
				case bytes.Equal(s, bytes.Repeat([]byte{1}, sector)):
					sawOld = true
				case bytes.Equal(s, bytes.Repeat([]byte{2}, sector)):
					sawNew = true
				default:
					t.Fatalf("sector size %d, seed %d: sector %d mixed inside", sector, seed, sec)
				}
			}
		}
		if !sawOld || !sawNew {
			t.Fatalf("sector size %d: old %v new %v", sector, sawOld, sawNew)
		}
	}
}

func TestSimTornAppendLeavesGarbage(t *testing.T) {
	garbage := 0
	for seed := range uint64(20) {
		sim := newSim(seed)
		f, _ := sim.Create("/d/a")
		_ = sim.SyncDir("/d")
		write(t, f, []byte("synced"), 0)
		_ = f.Sync()
		write(t, f, bytes.Repeat([]byte{7}, 4*disk.SectorSize), 6)
		sim.Crash()

		got := contents(t, sim, "/d/a")
		if len(got) < 6 || len(got) > 6+4*disk.SectorSize || string(got[:6]) != "synced" {
			t.Fatalf("seed %d: %d bytes, starts %q", seed, len(got), got[:min(len(got), 6)])
		}
		for off := 0; off < len(got); off += disk.SectorSize {
			sec := got[max(off, 6):min(off+disk.SectorSize, len(got))]
			if !bytes.Equal(sec, bytes.Repeat([]byte{7}, len(sec))) && bytes.Count(sec, []byte{0}) < len(sec)/2 {
				garbage++
			}
		}
	}
	if garbage == 0 {
		t.Fatal("lost sectors of an append always read as the new data")
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

	c := sim.Clone(rand.New(rand.NewPCG(7, 7)))
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
	f, _ := sim.Create("/d/a")
	sim.WriteErrRate, sim.SyncErrRate, sim.MetaErrRate, sim.ReadErrRate = 0.5, 0.5, 0.5, 0.5
	counts := map[string]int{}
	for range 200 {
		if _, err := f.WriteAt([]byte("x"), 0); errors.Is(err, syscall.EIO) {
			counts["write"]++
		}
		if err := f.Sync(); errors.Is(err, syscall.EIO) {
			counts["sync"]++
		}
		if _, err := f.ReadAt(make([]byte, 1), 0); errors.Is(err, syscall.EIO) {
			counts["read"]++
		}
		if err := f.Truncate(1); errors.Is(err, syscall.EIO) {
			counts["truncate"]++
		}
		if err := sim.SyncDir("/d"); errors.Is(err, syscall.EIO) {
			counts["syncdir"]++
		}
		if _, err := sim.Create("/d/b"); errors.Is(err, syscall.EIO) {
			counts["create"]++
		}
		if _, err := sim.Open("/d/a"); errors.Is(err, syscall.EIO) {
			counts["open"]++
		}
		if _, err := sim.List("/d"); errors.Is(err, syscall.EIO) {
			counts["list"]++
		}
		if _, err := f.Size(); errors.Is(err, syscall.EIO) {
			counts["size"]++
		}
		if err := sim.MkdirAll("/d/sub"); errors.Is(err, syscall.EIO) {
			counts["mkdir"]++
		}
	}
	for op, n := range counts {
		if n < 50 || n > 150 {
			t.Errorf("%s: EIO %d out of 200", op, n)
		}
	}
	if len(counts) != 10 {
		t.Fatalf("injected errors: %v", counts)
	}
}

func TestSimShortWrite(t *testing.T) {
	short := false
	for seed := range uint64(20) {
		sim := newSim(seed)
		f, _ := sim.Create("/d/a")
		sim.WriteErrRate = 1
		n, err := f.WriteAt(bytes.Repeat([]byte{1}, 100), 0)
		if !errors.Is(err, syscall.EIO) {
			t.Fatalf("write = %v", err)
		}
		if size, _ := f.Size(); size != int64(n) {
			t.Fatalf("wrote %d, size %d", n, size)
		}
		short = short || (n > 0 && n < 100)
	}
	if !short {
		t.Fatal("never a short write")
	}
}

func TestSimFailedSyncDropsDirtyPages(t *testing.T) {
	lost := 0
	for seed := range uint64(40) {
		sim := newSim(seed)
		f, _ := sim.Create("/d/a")
		_ = sim.SyncDir("/d")
		write(t, f, []byte("old"), 0)
		_ = f.Sync()
		write(t, f, []byte("new"), 0)
		sim.SyncErrRate = 1
		if err := f.Sync(); !errors.Is(err, syscall.EIO) {
			t.Fatalf("sync = %v", err)
		}
		sim.SyncErrRate = 0
		if err := f.Sync(); err != nil {
			t.Fatal(err)
		}
		if got := contents(t, sim, "/d/a"); string(got) != "new" {
			t.Fatalf("page cache shows %q", got)
		}
		sim.Crash()
		if string(contents(t, sim, "/d/a")) == "old" {
			lost++
		}
	}
	if lost == 0 || lost == 40 {
		t.Fatalf("second sync after EIO lost the write in %d of 40 runs", lost)
	}
}

func TestSimNeedsDirectory(t *testing.T) {
	sim := disk.NewSim(rand.New(rand.NewPCG(1, 1)))
	if _, err := sim.Create("/x/a"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("create without dir: %v", err)
	}
	if err := sim.SyncDir("/x"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("syncdir without dir: %v", err)
	}
	if err := sim.MkdirAll("/x/y"); err != nil {
		t.Fatal(err)
	}
	f, err := sim.Create("/x/y/a")
	if err != nil {
		t.Fatal(err)
	}
	_ = f.Sync()
	_ = sim.SyncDir("/x/y")
	sim.Crash()
	if names, _ := sim.List("/x/y"); !slices.Equal(names, []string{"a"}) {
		t.Fatalf("after crash %v", names)
	}
}

func TestSimCrashLengthAnywhereBetween(t *testing.T) {
	seen := map[int]bool{}
	for seed := range uint64(60) {
		sim := newSim(seed)
		f, _ := sim.Create("/d/a")
		_ = sim.SyncDir("/d")
		write(t, f, make([]byte, 10), 0)
		_ = f.Sync()
		write(t, f, make([]byte, 10), 10)
		sim.Crash()
		n := len(contents(t, sim, "/d/a"))
		if n < 10 || n > 20 {
			t.Fatalf("length %d", n)
		}
		seen[n] = true
	}
	if len(seen) < 5 {
		t.Fatalf("lengths after crash: %v", seen)
	}
}

func TestSimDirectoryNeedsSyncedParent(t *testing.T) {
	sim := newSim(1)
	sim.MetaErrRate = 1
	if err := sim.MkdirAll("/new"); !errors.Is(err, syscall.EIO) {
		t.Fatalf("mkdir = %v", err)
	}
	sim.MetaErrRate = 0
	f, err := sim.Create("/new/a")
	if err != nil {
		t.Fatal(err)
	}
	_ = f.Sync()
	if err := sim.SyncDir("/new"); err != nil {
		t.Fatal(err)
	}
	c := sim.Clone(rand.New(rand.NewPCG(1, 1)))
	c.Crash()
	if _, err := c.List("/new"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("directory without synced parent survived: %v", err)
	}

	if err := sim.MkdirAll("/new"); err != nil {
		t.Fatal(err)
	}
	sim.Crash()
	if names, err := sim.List("/new"); err != nil || !slices.Equal(names, []string{"a"}) {
		t.Fatalf("after second mkdir and crash: %v, %v", names, err)
	}
}

func TestSimJournalMakesNamesDurableEarly(t *testing.T) {
	sim := newSim(1)
	sim.JournalRate = 1
	f, _ := sim.Create("/d/a")
	write(t, f, []byte("x"), 0)
	_ = f.Sync()
	sim.Crash()
	if got := contents(t, sim, "/d/a"); string(got) != "x" {
		t.Fatalf("after crash %q", got)
	}
}

func TestSimLockEndsWithProcess(t *testing.T) {
	sim := newSim(1)
	old, err := sim.Lock("/d")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sim.Lock("/missing"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("lock missing dir: %v", err)
	}
	if _, err := sim.Clone(rand.New(rand.NewPCG(1, 1))).Lock("/d"); err != nil {
		t.Fatalf("clone inherited the lock: %v", err)
	}
	sim.Crash()
	l, err := sim.Lock("/d")
	if err != nil {
		t.Fatalf("lock after crash: %v", err)
	}
	_ = old.Close()
	if _, err := sim.Lock("/d"); !errors.Is(err, disk.ErrLocked) {
		t.Fatalf("stale handle released the new lock: %v", err)
	}
	_ = l.Close()
}

func TestSimCloneDoesNotDisturbOriginal(t *testing.T) {
	run := func(check bool) []byte {
		sim := newSim(5)
		f, _ := sim.Create("/d/a")
		_ = sim.SyncDir("/d")
		for i := range 6 {
			write(t, f, bytes.Repeat([]byte{byte(i + 1)}, 2*disk.SectorSize), int64(i)*900)
			if check {
				sim.Clone(rand.New(rand.NewPCG(uint64(i), 3))).Crash()
			}
		}
		sim.Crash()
		return contents(t, sim, "/d/a")
	}
	if !bytes.Equal(run(false), run(true)) {
		t.Fatal("crashing a clone changed the original's crash")
	}
}

func TestSimFailAfter(t *testing.T) {
	sim := newSim(3)
	f, err := sim.Create("/d/a")
	if err != nil {
		t.Fatal(err)
	}
	const k = 7
	sim.FailAfter(k)
	block := bytes.Repeat([]byte{1}, 1000)
	var failed []int
	short := -1
	for i := 1; i <= 20; i++ {
		n, err := f.WriteAt(block, int64(i-1)*1000)
		switch {
		case errors.Is(err, syscall.EIO):
			failed = append(failed, i)
			short = n
		case err != nil:
			t.Fatal(err)
		}
	}
	if len(failed) != 1 || failed[0] != k {
		t.Fatalf("failed writes %v, want exactly write %d", failed, k)
	}
	if short == len(block) {
		t.Fatal("the failed write wrote everything; pick another seed")
	}
	got := contents(t, sim, "/d/a")[(k-1)*1000 : k*1000]
	if want := append(bytes.Repeat([]byte{1}, short), make([]byte, 1000-short)...); !bytes.Equal(got, want) {
		t.Fatalf("write %d failed after %d bytes, but the file holds something else", k, short)
	}

	sim.FailAfter(2)
	sim.FailAfter(0)
	for range 5 {
		if err := f.Sync(); err != nil {
			t.Fatalf("disarmed sync: %v", err)
		}
	}
	sim.FailAfter(1)
	if _, err := sim.Clone(rand.New(rand.NewPCG(1, 1))).Open("/d/a"); err != nil {
		t.Fatalf("clone inherited the armed fault: %v", err)
	}
	if _, err := sim.Open("/d/a"); !errors.Is(err, syscall.EIO) {
		t.Fatalf("armed open = %v, want EIO", err)
	}
}

func TestSimFailAfterCountsEveryAccess(t *testing.T) {
	sim := newSim(4)
	f, _ := sim.Create("/d/a")
	sim.FailAfter(4)
	steps := []func() error{
		func() error { _, err := f.WriteAt([]byte("x"), 0); return err },
		f.Sync,
		func() error { _, err := sim.List("/d"); return err },
		func() error { return sim.Rename("/d/a", "/d/b") },
		func() error { return sim.SyncDir("/d") },
	}
	var failed []int
	for i, step := range steps {
		if err := step(); errors.Is(err, syscall.EIO) {
			failed = append(failed, i+1)
		} else if err != nil {
			t.Fatal(err)
		}
	}
	if !slices.Equal(failed, []int{4}) {
		t.Fatalf("failed steps %v, want the rename", failed)
	}
}

func TestSimFailSyncAfterDropsPages(t *testing.T) {
	sim := newSim(5)
	f, _ := sim.Create("/d/a")
	_ = sim.SyncDir("/d")
	write(t, f, []byte("old"), 0)
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	sim.FailSyncAfter(2)
	write(t, f, []byte("mid"), 0)
	if err := f.Sync(); err != nil {
		t.Fatalf("first sync: %v", err)
	}
	write(t, f, []byte("new"), 0)
	if err := f.Sync(); !errors.Is(err, syscall.EIO) {
		t.Fatalf("second sync = %v, want EIO", err)
	}
	if err := f.Sync(); err != nil {
		t.Fatalf("sync after the failure: %v", err)
	}
	if got := contents(t, sim, "/d/a"); string(got) != "new" {
		t.Fatalf("page cache shows %q", got)
	}
	sim.Crash()
	if got := contents(t, sim, "/d/a"); string(got) != "mid" {
		t.Fatalf("after crash %q, want the last synced %q", got, "mid")
	}
}

func TestSimDamageReachesOnlyCleanCache(t *testing.T) {
	harms := map[string]func(*disk.Sim, string, int64) error{
		"corrupt": (*disk.Sim).Corrupt,
		"garble":  (*disk.Sim).Garble,
	}
	for name, harm := range harms {
		t.Run(name, func(t *testing.T) {
			sim := newSim(3)
			f, _ := sim.Create("/d/a")
			_ = sim.SyncDir("/d")
			write(t, f, bytes.Repeat([]byte{0x55}, 3*disk.SectorSize), 0)
			_ = f.Sync()
			dirty := int64(2 * disk.SectorSize)
			write(t, f, []byte{0xaa, 0xaa}, dirty)

			for _, off := range []int64{10, dirty + 1} {
				if err := harm(sim, "/d/a", off); err != nil {
					t.Fatal(err)
				}
			}
			got := contents(t, sim, "/d/a")
			if got[10] == 0x55 {
				t.Fatal("a running reader does not see the damage in a clean page")
			}
			if got[dirty+1] != 0xaa {
				t.Fatal("the damage reached a dirty page")
			}
			if err := f.Sync(); err != nil {
				t.Fatal(err)
			}
			sim.Crash()
			got = contents(t, sim, "/d/a")
			if got[10] == 0x55 {
				t.Fatal("the damage in a clean page is gone after fsync")
			}
			if got[dirty+1] != 0xaa {
				t.Fatal("fsync of the dirty page did not overwrite the damage")
			}
		})
	}
}

func TestSimGarbleHitsOneSector(t *testing.T) {
	size := int64(3*disk.SectorSize + 100)
	for _, off := range []int64{0, disk.SectorSize + 7, 2*disk.SectorSize - 1, size - 1} {
		sim := newSim(4)
		f, _ := sim.Create("/d/a")
		_ = sim.SyncDir("/d")
		orig := bytes.Repeat([]byte{0x55}, int(size))
		write(t, f, orig, 0)
		_ = f.Sync()

		if err := sim.Garble("/d/a", off); err != nil {
			t.Fatal(err)
		}
		lo := off / disk.SectorSize * disk.SectorSize
		hi := min(lo+disk.SectorSize, size)
		for _, when := range []string{"in the cache", "after power loss"} {
			got := contents(t, sim, "/d/a")
			if int64(len(got)) != size || !bytes.Equal(got[:lo], orig[:lo]) || !bytes.Equal(got[hi:], orig[hi:]) {
				t.Fatalf("garble at %d, %s: damage outside the sector [%d, %d)", off, when, lo, hi)
			}
			if same := bytes.Count(got[lo:hi], []byte{0x55}); same > 20 {
				t.Fatalf("garble at %d, %s: %d of %d bytes in the sector unchanged", off, when, same, hi-lo)
			}
			sim.Crash()
		}
	}

	sim := newSim(4)
	f, _ := sim.Create("/d/a")
	_ = sim.SyncDir("/d")
	write(t, f, make([]byte, 10), 0)
	if err := sim.Garble("/d/a", 0); !errors.Is(err, fs.ErrInvalid) {
		t.Fatalf("garble in unsynced bytes: %v", err)
	}
	if err := sim.Garble("/d/b", 0); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("garble in a missing file: %v", err)
	}
}

const filler = 0x55

func shredded(b []byte) []int {
	var secs []int
	for off := 0; off < len(b); off += disk.SectorSize {
		sec := b[off:min(off+disk.SectorSize, len(b))]
		if bytes.Count(sec, []byte{filler}) != len(sec) {
			secs = append(secs, off/disk.SectorSize)
		}
	}
	return secs
}

func syncedFile(seed uint64, sectors int, rate float64) (*disk.Sim, disk.File) {
	sim := newSim(seed)
	sim.ShredRate = rate
	f, _ := sim.Create("/d/a")
	_ = sim.SyncDir("/d")
	_, _ = f.WriteAt(bytes.Repeat([]byte{filler}, sectors*disk.SectorSize), 0)
	_ = f.Sync()
	return sim, f
}

func TestSimShredTakesWrittenSectors(t *testing.T) {
	rewrite := func(t *testing.T, f disk.File) {
		write(t, f, []byte{filler}, disk.SectorSize+3)
		write(t, f, []byte{filler, filler}, 4*disk.SectorSize-1)
	}
	cases := map[string]struct {
		rate  float64
		touch func(t *testing.T, sim *disk.Sim, f disk.File)
		want  []int
	}{
		"same bytes written back": {1, func(t *testing.T, _ *disk.Sim, f disk.File) {
			rewrite(t, f)
		}, []int{1, 3, 4}},
		"synced again": {1, func(t *testing.T, _ *disk.Sim, f disk.File) {
			rewrite(t, f)
			if err := f.Sync(); err != nil {
				t.Fatal(err)
			}
		}, nil},
		"pages dropped by a failed fsync": {1, func(t *testing.T, sim *disk.Sim, f disk.File) {
			rewrite(t, f)
			sim.FailSyncAfter(1)
			if err := f.Sync(); !errors.Is(err, syscall.EIO) {
				t.Fatalf("sync = %v", err)
			}
			write(t, f, []byte{filler}, 5*disk.SectorSize)
		}, []int{5}},
		"written after the fsync": {1, func(t *testing.T, _ *disk.Sim, f disk.File) {
			rewrite(t, f)
			_ = f.Sync()
			write(t, f, []byte{filler}, 5*disk.SectorSize)
		}, []int{5}},
		"end of a truncate": {1, func(t *testing.T, _ *disk.Sim, f disk.File) {
			if err := f.Truncate(3*disk.SectorSize + 1); err != nil {
				t.Fatal(err)
			}
		}, []int{3}},
		"truncate on a sector boundary": {1, func(t *testing.T, _ *disk.Sim, f disk.File) {
			if err := f.Truncate(3 * disk.SectorSize); err != nil {
				t.Fatal(err)
			}
		}, nil},
		"empty write": {1, func(t *testing.T, _ *disk.Sim, f disk.File) {
			write(t, f, nil, disk.SectorSize+3)
		}, nil},
		"rate 0": {0, func(t *testing.T, _ *disk.Sim, f disk.File) {
			rewrite(t, f)
		}, nil},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			sim, f := syncedFile(1, 6, tc.rate)
			tc.touch(t, sim, f)
			clone := sim.Clone(rand.New(rand.NewPCG(8, 8)))
			sim.Crash()
			if got := shredded(contents(t, sim, "/d/a")); !slices.Equal(got, tc.want) {
				t.Fatalf("shredded sectors %v, want %v", got, tc.want)
			}
			clone.Crash()
			if got := shredded(contents(t, clone, "/d/a")); !slices.Equal(got, tc.want) {
				t.Fatalf("clone shredded sectors %v, want %v", got, tc.want)
			}
		})
	}
}

func TestSimShredAfterShortWrite(t *testing.T) {
	short := 0
	for seed := range uint64(10) {
		sim, f := syncedFile(seed, 6, 1)
		sim.WriteErrRate = 1
		n, err := f.WriteAt(bytes.Repeat([]byte{filler}, 3*disk.SectorSize), disk.SectorSize)
		if !errors.Is(err, syscall.EIO) {
			t.Fatalf("write = %v", err)
		}
		sim.Crash()
		var want []int
		for sec := 1; sec*disk.SectorSize < disk.SectorSize+n; sec++ {
			want = append(want, sec)
		}
		if got := shredded(contents(t, sim, "/d/a")); !slices.Equal(got, want) {
			t.Fatalf("short write of %d bytes: shredded %v, want %v", n, got, want)
		}
		if n > 0 && n < 3*disk.SectorSize {
			short++
		}
	}
	if short == 0 {
		t.Fatal("never a short write")
	}
}

func TestSimShredRateIsPerSector(t *testing.T) {
	sim, f := syncedFile(5, 128, 0.5)
	for sec := int64(0); sec < 128; sec += 2 {
		write(t, f, []byte{filler}, sec*disk.SectorSize+9)
	}
	sim.Crash()
	got := shredded(contents(t, sim, "/d/a"))
	if len(got) < 16 || len(got) > 48 {
		t.Fatalf("%d of 64 written sectors shredded at rate 0.5", len(got))
	}
	for _, sec := range got {
		if sec%2 != 0 {
			t.Fatalf("sector %d shredded, it was not written", sec)
		}
	}
}

func TestSimDurableIsSorted(t *testing.T) {
	sim := newSim(1)
	_ = sim.MkdirAll("/d/sub")
	for _, name := range []string{"/d/b", "/d/sub/c", "/d/a", "/d/unsynced"} {
		f, _ := sim.Create(name)
		write(t, f, make([]byte, len(name)), 0)
		_ = f.Sync()
		if name != "/d/unsynced" {
			_ = sim.SyncDir(filepath.Dir(name))
		}
	}
	f, _ := sim.Open("/d/a")
	write(t, f, make([]byte, 100), 0)
	want := []disk.DurableFile{{Name: "/d/a", Size: 4}, {Name: "/d/b", Size: 4}, {Name: "/d/sub/c", Size: 8}}
	if got := sim.Durable(); !slices.Equal(got, want) {
		t.Fatalf("durable %v, want %v", got, want)
	}
}
