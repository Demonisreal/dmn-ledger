package wal

import (
	"bytes"
	"errors"
	"io"
	"path/filepath"
	"slices"
	"strings"
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
		c := sim.Clone()
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
