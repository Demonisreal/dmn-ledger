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
	meta, err := LoadSnapshot(fsys, dir, func(r io.Reader) error {
		var err error
		b, err = io.ReadAll(r)
		return err
	})
	return meta, b, err
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

func TestSnapshotCorruptIsReported(t *testing.T) {
	sim := newSim(3)
	for _, meta := range []SnapshotMeta{{10, 1}, {20, 1}} {
		_ = saveSnap(sim, meta)
	}
	name := filepath.Join(dir, snapshotName(SnapshotMeta{20, 1}))
	if err := sim.Corrupt(name, 50); err != nil {
		t.Fatal(err)
	}
	sim.Crash()
	var ce *CorruptError
	if _, _, err := loadSnap(sim); !errors.As(err, &ce) || ce.File != name {
		t.Fatalf("load = %v", err)
	}
}

func TestSnapshotNameMustMatchFooter(t *testing.T) {
	sim := newSim(4)
	meta := SnapshotMeta{Index: 10, Term: 1}
	_ = saveSnap(sim, meta)
	_ = sim.Rename(filepath.Join(dir, snapshotName(meta)), filepath.Join(dir, snapshotName(SnapshotMeta{Index: 11, Term: 1})))
	if _, _, err := loadSnap(sim); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("renamed snapshot: %v", err)
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
