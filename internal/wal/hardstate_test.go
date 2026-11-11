package wal

import (
	"errors"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/Demonisreal/dmn-ledger/internal/disk"
)

func mustOpenHardState(t *testing.T, fsys disk.FS) (*HardStateFile, HardState) {
	t.Helper()
	h, hs, err := OpenHardState(fsys, dir)
	if err != nil {
		t.Fatal(err)
	}
	return h, hs
}

func TestHardStateRoundTrip(t *testing.T) {
	sim := newSim(1)
	h, hs := mustOpenHardState(t, sim)
	if hs != (HardState{}) {
		t.Fatalf("fresh state = %+v", hs)
	}
	for term := uint64(1); term <= 5; term++ {
		if err := h.Save(HardState{Term: term, Vote: term * 10}); err != nil {
			t.Fatal(err)
		}
	}
	_ = h.Close()
	sim.Crash()

	h, hs = mustOpenHardState(t, sim)
	if want := (HardState{Term: 5, Vote: 50}); hs != want {
		t.Fatalf("state = %+v, want %+v", hs, want)
	}
	if err := h.Save(HardState{Term: 6, Vote: 1}); err != nil {
		t.Fatal(err)
	}
	_ = h.Close()
	sim.Crash()
	if _, hs = mustOpenHardState(t, sim); hs != (HardState{Term: 6, Vote: 1}) {
		t.Fatalf("after reopen and save: %+v", hs)
	}
}

func TestHardStateCrashDuringSave(t *testing.T) {
	for _, sector := range []int64{disk.SectorSize, 4096} {
		sim := newSim(2)
		sim.Sector = sector
		var old, saving HardState
		power, process := 0, 0
		fsys := hookFS{Sim: sim, after: func(string) {
			power++
			c := sim.Clone()
			c.Crash()
			_, hs, err := OpenHardState(c, dir)
			if err != nil {
				t.Fatalf("sector %d: open after power loss: %v", sector, err)
			}
			if hs != old && hs != saving {
				t.Fatalf("sector %d: got %+v, want %+v or %+v", sector, hs, old, saving)
			}

			process++
			proc := sim.Clone()
			h, seen, err := OpenHardState(proc, dir)
			if err != nil || (seen != old && seen != saving) {
				t.Fatalf("sector %d: after process crash %+v, %v", sector, seen, err)
			}
			_ = h.Close()
			proc.Crash()
			if _, hs, err = OpenHardState(proc, dir); err != nil || hs != seen {
				t.Fatalf("sector %d: process saw %+v, after power loss %+v, %v", sector, seen, hs, err)
			}
		}}
		h, _ := mustOpenHardState(t, fsys)
		for term := uint64(1); term <= 30; term++ {
			saving = HardState{Term: term, Vote: term % 4}
			if err := h.Save(saving); err != nil {
				t.Fatal(err)
			}
			old = saving
		}
		if power < 60 || process < 60 {
			t.Fatalf("sector %d: only %d/%d crash points", sector, power, process)
		}
	}
}

func TestHardStateSurvivesFailedSyncAndRestart(t *testing.T) {
	dropped := 0
	for seed := range uint64(30) {
		sim := newSim(seed)
		h, _ := mustOpenHardState(t, sim)
		_ = h.Save(HardState{Term: 5, Vote: 1})
		sim.SyncErrRate = 1
		if err := h.Save(HardState{Term: 6, Vote: 2}); !errors.Is(err, syscall.EIO) {
			t.Fatalf("save = %v", err)
		}
		sim.SyncErrRate = 0
		_ = h.Close()

		c := sim.Clone()
		c.Crash()
		if _, hs, _ := OpenHardState(c, dir); hs.Term == 5 {
			dropped++
		}

		h, seen := mustOpenHardState(t, sim)
		_ = h.Close()
		sim.Crash()
		if _, hs := mustOpenHardState(t, sim); hs != seen {
			t.Fatalf("seed %d: restart saw %+v, after power loss %+v", seed, seen, hs)
		}
	}
	if dropped == 0 {
		t.Fatal("failed sync never lost the write")
	}
}

func TestHardStateDamagedSlot(t *testing.T) {
	sim := newSim(3)
	h, _ := mustOpenHardState(t, sim)
	_ = h.Save(HardState{Term: 1, Vote: 7})
	_ = h.Save(HardState{Term: 2, Vote: 8})
	_ = h.Close()
	path := filepath.Join(dir, hardStateName)

	if err := sim.Corrupt(path, 9); err != nil {
		t.Fatal(err)
	}
	sim.Crash()
	if _, hs := mustOpenHardState(t, sim); hs != (HardState{Term: 1, Vote: 7}) {
		t.Fatalf("with newer slot damaged: %+v", hs)
	}

	if err := sim.Corrupt(path, slotStride+9); err != nil {
		t.Fatal(err)
	}
	sim.Crash()
	var ce *CorruptError
	if _, _, err := OpenHardState(sim, dir); !errors.As(err, &ce) || ce.Offset != 0 {
		t.Fatalf("both slots damaged: %v", err)
	}
}

func TestHardStateSyncErrorIsSticky(t *testing.T) {
	sim := newSim(4)
	h, _ := mustOpenHardState(t, sim)
	sim.SyncErrRate = 1
	if err := h.Save(HardState{Term: 1}); !errors.Is(err, syscall.EIO) {
		t.Fatalf("save = %v", err)
	}
	sim.SyncErrRate = 0
	if err := h.Save(HardState{Term: 1}); !errors.Is(err, syscall.EIO) {
		t.Fatalf("save after failed sync = %v", err)
	}
}

func TestHardStateCreateSurvivesProcessCrash(t *testing.T) {
	sim := newSim(5)
	armed := true
	fsys, frozen := freezeAt(sim, "create", &armed)
	h, _ := mustOpenHardState(t, fsys)
	_ = h.Close()

	proc := frozen()
	h, _ = mustOpenHardState(t, proc)
	if err := h.Save(HardState{Term: 7, Vote: 3}); err != nil {
		t.Fatal(err)
	}
	proc.Crash()
	if _, hs := mustOpenHardState(t, proc); hs != (HardState{Term: 7, Vote: 3}) {
		t.Fatalf("saved {7 3}, after power loss %+v", hs)
	}
}
