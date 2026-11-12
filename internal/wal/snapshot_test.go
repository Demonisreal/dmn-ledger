package wal

import (
	"bytes"
	"errors"
	"io"
	"math/rand/v2"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"

	"github.com/Demonisreal/dmn-ledger/internal/disk"
)

func snapBody(meta SnapshotMeta) []byte {
	return bytes.Repeat([]byte{byte(meta.Index)}, int(meta.Index%7)*300)
}

func saveSnap(fsys disk.FS, meta SnapshotMeta) error {
	return SaveSnapshot(fsys, dir, meta, func(w io.Writer) error {
		b := snapBody(meta)
		if _, err := w.Write(b[:len(b)/2]); err != nil {
			return err
		}
		_, err := w.Write(b[len(b)/2:])
		return err
	})
}

func loadSnap(fsys disk.FS) (SnapshotMeta, []byte, error) {
	var b []byte
	meta, _, err := LoadSnapshot(fsys, dir, func(r io.Reader) error {
		var err error
		b, err = io.ReadAll(r)
		return err
	})
	return meta, b, err
}

func brokenSnaps(t *testing.T, fsys disk.FS) (SnapshotMeta, []SnapshotMeta) {
	t.Helper()
	meta, broken, err := LoadSnapshot(fsys, dir, func(r io.Reader) error {
		_, err := io.Copy(io.Discard, r)
		return err
	})
	if err != nil && !errors.Is(err, ErrNoSnapshot) {
		t.Fatal(err)
	}
	return meta, broken
}

func snapNames(metas ...SnapshotMeta) []string {
	var names []string
	for _, meta := range metas {
		names = append(names, snapshotName(meta))
	}
	return names
}

func snapFiles(t *testing.T, fsys disk.FS) []string {
	t.Helper()
	names, _ := fsys.List(dir)
	return slices.DeleteFunc(names, func(name string) bool { return !strings.HasPrefix(name, "snap-") })
}

func TestSnapshotKeepsNewestTwo(t *testing.T) {
	sim := newSim(1)
	if _, _, err := loadSnap(sim); !errors.Is(err, ErrNoSnapshot) {
		t.Fatalf("empty dir: %v", err)
	}
	for _, meta := range []SnapshotMeta{{10, 1}, {20, 1}, {30, 2}, {40, 2}} {
		if err := saveSnap(sim, meta); err != nil {
			t.Fatal(err)
		}
	}
	sim.Crash()

	meta, body, err := loadSnap(sim)
	if err != nil {
		t.Fatal(err)
	}
	if want := (SnapshotMeta{40, 2}); meta != want || !bytes.Equal(body, snapBody(want)) {
		t.Fatalf("loaded %+v (%d bytes)", meta, len(body))
	}
	want := []string{
		"snap-00000000000000000030-00000000000000000002.snap",
		"snap-00000000000000000040-00000000000000000002.snap",
	}
	if got := snapFiles(t, sim); !slices.Equal(got, want) {
		t.Fatalf("files %v", got)
	}
}

func TestSnapshotHalfWrittenIsIgnored(t *testing.T) {
	sim := newSim(1)
	old := SnapshotMeta{Index: 10, Term: 1}
	_ = saveSnap(sim, old)

	tmp := filepath.Join(dir, snapshotName(SnapshotMeta{Index: 20, Term: 1})+".tmp")
	f, _ := sim.Create(tmp)
	_, _ = f.WriteAt(bytes.Repeat([]byte{9}, 100), 0)
	_ = f.Sync()
	_ = sim.SyncDir(dir)

	meta, _, err := loadSnap(sim)
	if err != nil || meta != old {
		t.Fatalf("loaded %+v, %v; want %+v", meta, err, old)
	}
	next := SnapshotMeta{Index: 30, Term: 1}
	_ = saveSnap(sim, next)
	if got := snapFiles(t, sim); slices.ContainsFunc(got, func(name string) bool { return strings.HasSuffix(name, ".tmp") }) {
		t.Fatalf("stale temp file left: %v", got)
	}
}

func TestSnapshotCrashDuringSave(t *testing.T) {
	sim := newSim(2)
	var done, saving SnapshotMeta
	checks := 0
	fsys := hookFS{Sim: sim, after: func(string) {
		checks++
		c := fork(sim, uint64(checks))
		c.Crash()
		meta, body, err := loadSnap(c)
		switch {
		case errors.Is(err, ErrNoSnapshot) && done == (SnapshotMeta{}):
		case err != nil:
			t.Fatalf("saving %+v: %v", saving, err)
		case meta != done && meta != saving:
			t.Fatalf("loaded %+v, want %+v or %+v", meta, done, saving)
		case !bytes.Equal(body, snapBody(meta)):
			t.Fatalf("snapshot %+v has wrong body", meta)
		}
	}}
	for i := uint64(1); i <= 12; i++ {
		saving = SnapshotMeta{Index: i * 100, Term: i / 3}
		if err := saveSnap(fsys, saving); err != nil {
			t.Fatal(err)
		}
		done = saving
	}
	if checks < 12*6 {
		t.Fatalf("only %d crash points", checks)
	}
}

func TestSnapshotLoadFallsBack(t *testing.T) {
	older, newer := SnapshotMeta{Index: 10, Term: 1}, SnapshotMeta{Index: 20, Term: 1}
	cases := map[string]struct {
		harmed []SnapshotMeta
		want   SnapshotMeta
		broken []SnapshotMeta
	}{
		"newest holds":      {nil, newer, nil},
		"newest broken":     {[]SnapshotMeta{newer}, older, []SnapshotMeta{newer}},
		"only older broken": {[]SnapshotMeta{older}, newer, nil},
		"both broken":       {[]SnapshotMeta{older, newer}, SnapshotMeta{}, []SnapshotMeta{newer, older}},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			sim := newSim(3)
			for _, meta := range []SnapshotMeta{older, newer} {
				if err := saveSnap(sim, meta); err != nil {
					t.Fatal(err)
				}
			}
			for k, meta := range tc.harmed {
				harm := []func(string, int64) error{sim.Corrupt, sim.Garble}[k%2]
				if err := harm(filepath.Join(dir, snapshotName(meta)), 50); err != nil {
					t.Fatal(err)
				}
			}
			sim.Crash()

			var body []byte
			meta, broken, err := LoadSnapshot(sim, dir, func(r io.Reader) error {
				var err error
				body, err = io.ReadAll(r)
				return err
			})
			if tc.want == (SnapshotMeta{}) {
				if !errors.Is(err, ErrNoSnapshot) || meta != tc.want {
					t.Fatalf("load = %+v, %v", meta, err)
				}
			} else if err != nil || meta != tc.want || !bytes.Equal(body, snapBody(tc.want)) {
				t.Fatalf("loaded %+v (%d bytes), %v; want %+v", meta, len(body), err, tc.want)
			}
			if !slices.Equal(broken, tc.broken) {
				t.Fatalf("reported %+v as broken, want %+v", broken, tc.broken)
			}
			if got := snapFiles(t, sim); !slices.Equal(got, snapNames(older, newer)) {
				t.Fatalf("load changed the files: %v", got)
			}
		})
	}
}

func TestSnapshotBrokenFileWouldEvictTheHealthyOne(t *testing.T) {
	older, newer, next := SnapshotMeta{Index: 10, Term: 1}, SnapshotMeta{Index: 20, Term: 1}, SnapshotMeta{Index: 15, Term: 1}
	for _, cleanup := range []bool{false, true} {
		sim := newSim(3)
		_ = saveSnap(sim, older)
		_ = saveSnap(sim, newer)
		if err := sim.Corrupt(filepath.Join(dir, snapshotName(newer)), 50); err != nil {
			t.Fatal(err)
		}
		base, broken := brokenSnaps(t, sim)
		if base != older || !slices.Equal(broken, []SnapshotMeta{newer}) {
			t.Fatalf("loaded %+v, broken %+v", base, broken)
		}
		want := snapNames(next, newer)
		if cleanup {
			if err := RemoveSnapshot(sim, dir, newer); err != nil {
				t.Fatal(err)
			}
			sim.Crash()
			want = snapNames(older, next)
		}
		if err := saveSnap(sim, next); err != nil {
			t.Fatal(err)
		}
		if got := snapFiles(t, sim); !slices.Equal(got, want) {
			t.Fatalf("cleanup %v: files after the next snapshot %v, want %v", cleanup, got, want)
		}
	}
}

func TestCrashWhileRemovingBrokenSnapshots(t *testing.T) {
	older, newer := SnapshotMeta{Index: 10, Term: 1}, SnapshotMeta{Index: 20, Term: 1}
	for _, both := range []bool{false, true} {
		sim := newSim(3)
		_ = saveSnap(sim, older)
		_ = saveSnap(sim, newer)
		_ = sim.Corrupt(filepath.Join(dir, snapshotName(newer)), 50)
		var left []string
		if both {
			_ = sim.Corrupt(filepath.Join(dir, snapshotName(older)), 50)
		} else {
			left = snapNames(older)
		}
		base, broken := brokenSnaps(t, sim)
		if both != (base == SnapshotMeta{}) || len(broken) == 0 {
			t.Fatalf("loaded %+v, broken %+v", base, broken)
		}
		points := 0
		check := func(c *disk.Sim, tag string) {
			again, rest := brokenSnaps(t, c)
			if again != base {
				t.Fatalf("%s: loaded %+v, want %+v", tag, again, base)
			}
			for _, meta := range rest {
				if err := RemoveSnapshot(c, dir, meta); err != nil {
					t.Fatal(err)
				}
			}
			c.Crash()
			if got := snapFiles(t, c); !slices.Equal(got, left) {
				t.Fatalf("%s: files after the second cleanup %v, want %v", tag, got, left)
			}
		}
		fsys := hookFS{Sim: sim, after: func(op string) {
			points++
			lost := fork(sim, uint64(points))
			lost.Crash()
			check(lost, "power loss after "+op)
			check(fork(sim, uint64(points)), "process crash after "+op)
		}}
		for _, meta := range broken {
			if err := RemoveSnapshot(fsys, dir, meta); err != nil {
				t.Fatal(err)
			}
		}
		if points != 2*len(broken) {
			t.Fatalf("%d crash points for %d broken snapshots", points, len(broken))
		}
	}
}

func TestSnapshotNameMustMatchFooter(t *testing.T) {
	sim := newSim(4)
	older, meta := SnapshotMeta{Index: 5, Term: 1}, SnapshotMeta{Index: 10, Term: 1}
	_ = saveSnap(sim, older)
	_ = saveSnap(sim, meta)
	_ = sim.Rename(filepath.Join(dir, snapshotName(meta)), filepath.Join(dir, snapshotName(SnapshotMeta{Index: 11, Term: 1})))
	loaded, _, err := loadSnap(sim)
	if err == nil || errors.Is(err, ErrCorrupt) || errors.Is(err, ErrNoSnapshot) {
		t.Fatalf("renamed snapshot: loaded %+v, %v; a wrong name is not a disk fault and must stop the start", loaded, err)
	}
}

func TestSnapshotUnreadableContentStopsTheLoad(t *testing.T) {
	sim := newSim(4)
	_ = saveSnap(sim, SnapshotMeta{Index: 5, Term: 1})
	_ = saveSnap(sim, SnapshotMeta{Index: 10, Term: 1})
	errMagic := errors.New("bad magic")
	calls := 0
	meta, broken, err := LoadSnapshot(sim, dir, func(io.Reader) error {
		calls++
		return errMagic
	})
	if !errors.Is(err, errMagic) || calls != 1 || len(broken) != 0 {
		t.Fatalf("loaded %+v, broken %+v, %v after %d reads; want the error of the newest snapshot and no fallback", meta, broken, err, calls)
	}
}

func TestSnapshotLoadMakesRenameDurable(t *testing.T) {
	sim := newSim(5)
	old := SnapshotMeta{Index: 10, Term: 1}
	_ = saveSnap(sim, old)
	armed := true
	fsys, frozen := freezeAt(sim, "rename", &armed)
	next := SnapshotMeta{Index: 20, Term: 1}
	if err := saveSnap(fsys, next); err != nil {
		t.Fatal(err)
	}

	proc := frozen()
	meta, _, err := loadSnap(proc)
	if err != nil || meta != next {
		t.Fatalf("after process crash: %+v, %v", meta, err)
	}
	proc.Crash()
	if meta, _, err = loadSnap(proc); err != nil || meta != next {
		t.Fatalf("loaded %+v before power loss, %+v after, %v", next, meta, err)
	}
}

func TestReadSnapshot(t *testing.T) {
	sim := newSim(6)
	older, newer := SnapshotMeta{Index: 10, Term: 1}, SnapshotMeta{Index: 20, Term: 2}
	for _, meta := range []SnapshotMeta{older, newer} {
		if err := saveSnap(sim, meta); err != nil {
			t.Fatal(err)
		}
	}
	var got []byte
	read := func(r io.Reader) error {
		var err error
		got, err = io.ReadAll(r)
		return err
	}
	if err := ReadSnapshot(sim, dir, older, read); err != nil || !bytes.Equal(got, snapBody(older)) {
		t.Fatalf("older snapshot: %d bytes, %v", len(got), err)
	}
	if err := ReadSnapshot(sim, dir, SnapshotMeta{Index: 20, Term: 1}, read); !errors.Is(err, ErrNoSnapshot) {
		t.Fatalf("missing snapshot: %v", err)
	}
	if err := sim.Corrupt(filepath.Join(dir, snapshotName(older)), 3); err != nil {
		t.Fatal(err)
	}
	sim.Crash()
	if err := ReadSnapshot(sim, dir, older, read); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("damaged snapshot: %v", err)
	}
}

func TestSnapshotFaultsAndRestarts(t *testing.T) {
	seeds := uint64(150)
	if testing.Short() {
		seeds = 20
	}
	checks := 0
	for seed := range seeds {
		rng := rand.New(rand.NewPCG(seed, 12))
		sim := newSim(seed + 30000)
		sim.Sector = []int64{disk.SectorSize, 4096}[seed%2]
		sim.JournalRate = []float64{0, 0.5}[seed/2%2]
		var saved SnapshotMeta
		var attempted []SnapshotMeta
		step := 0
		verify := func(c *disk.Sim, tag string) {
			checks++
			c.WriteErrRate, c.SyncErrRate, c.MetaErrRate, c.ReadErrRate = 0.05, 0.05, 0.1, 0.05
			for try := 0; ; try++ {
				if try == 6 {
					quiet(c)
				}
				meta, body, err := loadSnap(c)
				if errors.Is(err, syscall.EIO) {
					if rng.IntN(2) == 0 {
						c.Crash()
					}
					continue
				}
				if errors.Is(err, ErrNoSnapshot) && saved == (SnapshotMeta{}) {
					return
				}
				if err != nil {
					t.Fatalf("seed %d step %d %s: %v", seed, step, tag, err)
				}
				if meta.Index < saved.Index || !slices.Contains(attempted, meta) || !bytes.Equal(body, snapBody(meta)) {
					t.Fatalf("seed %d step %d %s: loaded %+v, last successful save %+v", seed, step, tag, meta, saved)
				}
				quiet(c).Crash()
				if again, _, err := loadSnap(c); err != nil || again != meta {
					t.Fatalf("seed %d step %d %s: load returned %+v, after power loss %+v, %v", seed, step, tag, meta, again, err)
				}
				return
			}
		}
		fsys := hookFS{Sim: sim, after: func(op string) {
			c := fork(sim, uint64(checks))
			c.Crash()
			verify(c, "power loss after "+op)
			verify(fork(sim, uint64(checks)), "process crash after "+op)
		}}
		sim.WriteErrRate, sim.SyncErrRate, sim.MetaErrRate = 0.03, 0.06, 0.04
		for step = range 30 {
			meta := SnapshotMeta{Index: uint64(step+1) * 10, Term: uint64(step / 5)}
			if rng.IntN(4) == 0 && saved.Index > 0 {
				meta = saved
			}
			attempted = append(attempted, meta)
			if err := saveSnap(fsys, meta); err != nil {
				if !errors.Is(err, syscall.EIO) {
					t.Fatalf("seed %d step %d: save: %v", seed, step, err)
				}
				if rng.IntN(2) == 0 {
					sim.Crash()
				}
				continue
			}
			if meta.Index > saved.Index {
				saved = meta
			}
		}
	}
	t.Logf("%d checked snapshot recoveries", checks)
}
