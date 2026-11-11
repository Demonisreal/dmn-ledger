package wal

import (
	"errors"
	"path/filepath"
	"syscall"
	"testing"
)

func TestHardStateRoundTrip(t *testing.T) {
	sim := newSim(1)
	h, hs, err := OpenHardState(sim, dir)
	if err != nil {
		t.Fatal(err)
	}
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

	h, hs, err = OpenHardState(sim, dir)
	if err != nil {
		t.Fatal(err)
	}
	if want := (HardState{Term: 5, Vote: 50}); hs != want {
		t.Fatalf("state = %+v, want %+v", hs, want)
	}
	if err := h.Save(HardState{Term: 6, Vote: 1}); err != nil {
		t.Fatal(err)
	}
	_ = h.Close()
	sim.Crash()
	if _, hs, _ = OpenHardState(sim, dir); hs != (HardState{Term: 6, Vote: 1}) {
		t.Fatalf("after reopen and save: %+v", hs)
	}
}

func TestHardStateCrashDuringSave(t *testing.T) {
	sim := newSim(2)
	var old, saving HardState
	checks := 0
	fsys := hookFS{Sim: sim, after: func() {
		checks++
		_, hs, err := OpenHardState(crashedCopy(sim), dir)
		if err != nil {
			t.Fatalf("open after crash: %v", err)
		}
		if hs != old && hs != saving {
			t.Fatalf("got %+v, want %+v or %+v", hs, old, saving)
		}
	}}
	h, _, err := OpenHardState(fsys, dir)
	if err != nil {
		t.Fatal(err)
	}
	for term := uint64(1); term <= 30; term++ {
		saving = HardState{Term: term, Vote: term % 4}
		if err := h.Save(saving); err != nil {
			t.Fatal(err)
		}
		old = saving
	}
	if checks < 60 {
		t.Fatalf("only %d crash points", checks)
	}
}

func TestHardStateDamagedSlot(t *testing.T) {
	sim := newSim(3)
	h, _, _ := OpenHardState(sim, dir)
	_ = h.Save(HardState{Term: 1, Vote: 7})
	_ = h.Save(HardState{Term: 2, Vote: 8})
	_ = h.Close()
	path := filepath.Join(dir, hardStateName)

	if err := sim.Corrupt(path, 9); err != nil {
		t.Fatal(err)
	}
	sim.Crash()
	_, hs, err := OpenHardState(sim, dir)
	if err != nil {
		t.Fatal(err)
	}
	if want := (HardState{Term: 1, Vote: 7}); hs != want {
		t.Fatalf("with newer slot damaged: %+v, want %+v", hs, want)
	}

	if err := sim.Corrupt(path, 512+9); err != nil {
		t.Fatal(err)
	}
	sim.Crash()
	if _, _, err := OpenHardState(sim, dir); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("both slots damaged: %v", err)
	}
}

func TestHardStateSyncErrorIsSticky(t *testing.T) {
	sim := newSim(4)
	h, _, _ := OpenHardState(sim, dir)
	sim.SyncErrRate = 1
	if err := h.Save(HardState{Term: 1}); !errors.Is(err, syscall.EIO) {
		t.Fatalf("save = %v", err)
	}
	sim.SyncErrRate = 0
	if err := h.Save(HardState{Term: 1}); !errors.Is(err, syscall.EIO) {
		t.Fatalf("save after failed sync = %v", err)
	}
}
