package wal

import (
	"bytes"
	"errors"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/Demonisreal/dmn-ledger/internal/disk"
)

func snapBody(meta SnapshotMeta) []byte {
	return bytes.Repeat([]byte{byte(meta.Index)}, int(meta.Index%7)*300)
}

func snapFiles(t *testing.T, fsys disk.FS) []string {
	t.Helper()
	names, _ := fsys.List(dir)
	return slices.DeleteFunc(names, func(name string) bool { return !strings.HasPrefix(name, "snap-") })
}

func TestSnapshotKeepsNewestTwo(t *testing.T) {
	sim := newSim(1)
	if _, _, err := LoadSnapshot(sim, dir); !errors.Is(err, ErrNoSnapshot) {
		t.Fatalf("empty dir: %v", err)
	}
	for _, meta := range []SnapshotMeta{{10, 1}, {20, 1}, {30, 2}, {40, 2}} {
		if err := SaveSnapshot(sim, dir, meta, snapBody(meta)); err != nil {
			t.Fatal(err)
		}
	}
	sim.Crash()

	meta, body, err := LoadSnapshot(sim, dir)
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
	_ = SaveSnapshot(sim, dir, old, snapBody(old))

	tmp := filepath.Join(dir, snapshotName(SnapshotMeta{Index: 20, Term: 1})+".tmp")
	f, _ := sim.Create(tmp)
	_, _ = f.WriteAt(bytes.Repeat([]byte{9}, 100), 0)
	_ = f.Sync()
	_ = sim.SyncDir(dir)

	meta, _, err := LoadSnapshot(sim, dir)
	if err != nil || meta != old {
		t.Fatalf("loaded %+v, %v; want %+v", meta, err, old)
	}
	next := SnapshotMeta{Index: 30, Term: 1}
	_ = SaveSnapshot(sim, dir, next, snapBody(next))
	if got := snapFiles(t, sim); slices.ContainsFunc(got, func(name string) bool { return strings.HasSuffix(name, ".tmp") }) {
		t.Fatalf("stale temp file left: %v", got)
	}
}

func TestSnapshotCrashDuringSave(t *testing.T) {
	sim := newSim(2)
	var done, saving SnapshotMeta
	checks := 0
	fsys := hookFS{Sim: sim, after: func() {
		checks++
		meta, body, err := LoadSnapshot(crashedCopy(sim), dir)
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
		if err := SaveSnapshot(fsys, dir, saving, snapBody(saving)); err != nil {
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
		_ = SaveSnapshot(sim, dir, meta, snapBody(meta))
	}
	name := filepath.Join(dir, snapshotName(SnapshotMeta{20, 1}))
	if err := sim.Corrupt(name, 50); err != nil {
		t.Fatal(err)
	}
	sim.Crash()
	var ce *CorruptError
	if _, _, err := LoadSnapshot(sim, dir); !errors.As(err, &ce) || ce.File != name {
		t.Fatalf("load = %v", err)
	}
}

func TestSnapshotNameMustMatchFooter(t *testing.T) {
	sim := newSim(4)
	meta := SnapshotMeta{Index: 10, Term: 1}
	_ = SaveSnapshot(sim, dir, meta, snapBody(meta))
	_ = sim.Rename(filepath.Join(dir, snapshotName(meta)), filepath.Join(dir, snapshotName(SnapshotMeta{Index: 11, Term: 1})))
	if _, _, err := LoadSnapshot(sim, dir); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("renamed snapshot: %v", err)
	}
}
