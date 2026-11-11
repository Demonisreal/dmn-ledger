package wal

import (
	"errors"
	"math/rand/v2"
	"path/filepath"
	"slices"
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
			c := fork(sim, uint64(power))
			c.Crash()
			_, hs, err := OpenHardState(c, dir)
			if err != nil {
				t.Fatalf("sector %d: open after power loss: %v", sector, err)
			}
			if hs != old && hs != saving {
				t.Fatalf("sector %d: got %+v, want %+v or %+v", sector, hs, old, saving)
			}

			process++
			proc := fork(sim, uint64(power))
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

		c := fork(sim, seed)
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

	for _, off := range []int64{9, slotStride + 9} {
		c := fork(sim, uint64(off))
		if err := c.Corrupt(path, off); err != nil {
			t.Fatal(err)
		}
		c.Crash()
		if _, hs := mustOpenHardState(t, c); hs != (HardState{Term: 2, Vote: 8}) {
			t.Fatalf("slot at %d damaged: %+v", off, hs)
		}
	}

	for _, off := range []int64{9, slotStride + 9} {
		if err := sim.Corrupt(path, off); err != nil {
			t.Fatal(err)
		}
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
	fsys, frozen := freezeAt(sim, "rename", &armed)
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

func TestHardStateFirstSaveSurvivesPowerLoss(t *testing.T) {
	saved := HardState{Term: 1, Vote: 2}
	for _, sector := range []int64{disk.SectorSize, 4096} {
		points := 0
		for seed := range uint64(1500) {
			sim := newSim(seed)
			sim.Sector = sector
			saving := false
			fsys := hookFS{Sim: sim, after: func(op string) {
				points++
				c := fork(sim, uint64(points))
				c.Crash()
				_, hs, err := OpenHardState(c, dir)
				if err != nil {
					t.Fatalf("sector %d seed %d: power loss after %s: %v", sector, seed, op, err)
				}
				if hs != (HardState{}) && (!saving || hs != saved) {
					t.Fatalf("sector %d seed %d: got %+v", sector, seed, hs)
				}
			}}
			h, _ := mustOpenHardState(t, fsys)
			saving = true
			if err := h.Save(saved); err != nil {
				t.Fatal(err)
			}
		}
		if points < 1500*8 {
			t.Fatalf("sector %d: only %d crash points", sector, points)
		}
	}
}

type windowFS struct {
	*disk.Sim
	open   map[int64]bool
	widest int
	bytes  int64
}

type windowFile struct {
	disk.File
	fs *windowFS
}

func (w *windowFS) Open(name string) (disk.File, error) {
	f, err := w.Sim.Open(name)
	if err != nil {
		return nil, err
	}
	return windowFile{f, w}, nil
}

func (f windowFile) WriteAt(p []byte, off int64) (int, error) {
	for page := off / slotStride; page*slotStride < off+int64(len(p)); page++ {
		f.fs.open[page] = true
	}
	f.fs.widest = max(f.fs.widest, len(f.fs.open))
	f.fs.bytes += int64(len(p))
	return f.File.WriteAt(p, off)
}

func (f windowFile) Sync() error {
	clear(f.fs.open)
	return f.File.Sync()
}

func TestHardStateOpenWritesOneSlotAtATime(t *testing.T) {
	sim := newSim(1)
	h, _ := mustOpenHardState(t, sim)
	_ = h.Save(HardState{Term: 1, Vote: 1})
	_ = h.Save(HardState{Term: 2, Vote: 2})
	_ = h.Close()

	fsys := &windowFS{Sim: sim, open: map[int64]bool{}}
	h, hs, err := OpenHardState(fsys, dir)
	if err != nil || hs != (HardState{Term: 2, Vote: 2}) {
		t.Fatalf("open = %+v, %v", hs, err)
	}
	if fsys.widest != 1 || fsys.bytes != 2*slotLen {
		t.Fatalf("open wrote %d bytes with up to %d slots before one fsync", fsys.bytes, fsys.widest)
	}
	_ = h.Close()
}

func TestHardStateIsLocked(t *testing.T) {
	sim := newSim(1)
	h, _ := mustOpenHardState(t, sim)
	if _, _, err := OpenHardState(sim, dir); !errors.Is(err, disk.ErrLocked) {
		t.Fatalf("second open = %v", err)
	}
	_ = h.Close()
	h, _ = mustOpenHardState(t, sim)
	_ = h.Close()
}

func TestHardStateFaultsAndRestarts(t *testing.T) {
	seeds := uint64(300)
	if testing.Short() {
		seeds = 40
	}
	checks := 0
	for seed := range seeds {
		rng := rand.New(rand.NewPCG(seed, 11))
		sim := newSim(seed + 20000)
		sim.Sector = []int64{disk.SectorSize, 4096}[seed%2]
		sim.JournalRate = []float64{0, 0.5}[seed/2%2]
		var promised HardState
		attempted := []HardState{{}}
		allowed := func(hs HardState) bool {
			return hs.Term >= promised.Term && slices.Contains(attempted, hs)
		}
		step := 0
		verify := func(c *disk.Sim, tag string) {
			checks++
			for try := 0; ; try++ {
				if try == 6 {
					quiet(c)
				}
				h, hs, err := OpenHardState(c, dir)
				if errors.Is(err, syscall.EIO) {
					if rng.IntN(2) == 0 {
						c.Crash()
					}
					continue
				}
				if err != nil {
					t.Fatalf("seed %d step %d %s: %v", seed, step, tag, err)
				}
				_ = h.Close()
				if !allowed(hs) {
					t.Fatalf("seed %d step %d %s: got %+v, promised %+v", seed, step, tag, hs, promised)
				}
				quiet(c).Crash()
				if _, again, err := OpenHardState(c, dir); err != nil || again != hs {
					t.Fatalf("seed %d step %d %s: open returned %+v, after power loss %+v, %v", seed, step, tag, hs, again, err)
				}
				return
			}
		}
		fsys := hookFS{Sim: sim, after: func(op string) {
			c := fork(sim, uint64(checks))
			c.Crash()
			c.WriteErrRate, c.SyncErrRate, c.MetaErrRate = 0.1, 0.1, 0.1
			verify(c, "power loss after "+op)
			c = fork(sim, uint64(checks))
			c.WriteErrRate, c.SyncErrRate, c.MetaErrRate = 0.1, 0.1, 0.1
			verify(c, "process crash after "+op)
		}}
		reopen := func() *HardStateFile {
			for {
				h, hs, err := OpenHardState(fsys, dir)
				if err == nil {
					if !allowed(hs) {
						t.Fatalf("seed %d step %d: reopen got %+v, promised %+v", seed, step, hs, promised)
					}
					promised = hs
					return h
				}
				if !errors.Is(err, syscall.EIO) {
					t.Fatalf("seed %d step %d: reopen: %v", seed, step, err)
				}
				if rng.IntN(2) == 0 {
					sim.Crash()
				}
			}
		}
		sim.WriteErrRate, sim.SyncErrRate, sim.MetaErrRate = 0.03, 0.06, 0.03
		h := reopen()
		for step = range 40 {
			hs := HardState{Term: promised.Term + 1 + uint64(step), Vote: rng.Uint64N(3)}
			attempted = append(attempted, hs)
			if err := h.Save(hs); err != nil {
				_ = h.Close()
				if rng.IntN(2) == 0 {
					sim.Crash()
				}
				h = reopen()
				continue
			}
			promised = hs
		}
		_ = h.Close()
	}
	t.Logf("%d checked hard state recoveries", checks)
}
