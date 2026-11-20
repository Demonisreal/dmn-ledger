package wal

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io/fs"
	"maps"
	"math/rand/v2"
	"path/filepath"
	"slices"
	"syscall"
	"testing"

	"github.com/Demonisreal/dmn-ledger/internal/disk"
)

var (
	metaPath = filepath.Join(dir, MetaName)
	ident    = Identity{Cluster: 7, Node: 2, Nodes: 3}
)

func mustOpenMeta(t *testing.T, fsys disk.FS) (*MetaFile, Meta) {
	t.Helper()
	m, st, err := CreateMeta(fsys, dir, ident)
	if err != nil {
		t.Fatal(err)
	}
	return m, st
}

func mustSettle(t *testing.T, fsys disk.FS) (*MetaFile, Meta) {
	t.Helper()
	m, st := mustOpenMeta(t, fsys)
	if err := m.Settle(); err != nil {
		t.Fatal(err)
	}
	return m, st
}

func metaSlots(t *testing.T, sim *disk.Sim) (slots [2]Meta, valid [2]bool) {
	t.Helper()
	raw := contents(t, sim, metaPath)
	if len(raw) != metaLen {
		t.Fatalf("meta is %d bytes long, want %d", len(raw), metaLen)
	}
	for k := range slots {
		_, _, slots[k], valid[k] = decodeMeta(raw[k*MetaStride : k*MetaStride+MetaSlotLen])
	}
	return slots, valid
}

func newestSlot(m *MetaFile) int64 {
	return int64(m.seq%2) * MetaStride
}

func TestMetaRoundTrip(t *testing.T) {
	sim := newSim(1)
	m, st := mustSettle(t, sim)
	if st != (Meta{}) || !m.Intact() {
		t.Fatalf("fresh meta = %+v, intact %v", st, m.Intact())
	}
	for term := uint64(1); term <= 5; term++ {
		if err := m.Save(term, term*10, term%2 == 1); err != nil {
			t.Fatal(err)
		}
		if err := m.Seal(term * 100); err != nil {
			t.Fatal(err)
		}
		if m.Sealed() != term*100 {
			t.Fatalf("sealed %d after Seal(%d)", m.Sealed(), term*100)
		}
	}
	_ = m.Close()
	sim.Crash()

	m, st = mustSettle(t, sim)
	if want := (Meta{Term: 5, Vote: 50, Sealed: 500, Repair: true}); st != want || !m.Intact() || m.Sealed() != 500 {
		t.Fatalf("meta = %+v, intact %v, want %+v", st, m.Intact(), want)
	}
	if err := m.Save(6, 1, false); err != nil {
		t.Fatal(err)
	}
	_ = m.Close()
	sim.Crash()
	if _, st = mustOpenMeta(t, sim); st != (Meta{Term: 6, Vote: 1, Sealed: 500}) {
		t.Fatalf("after reopen and save: %+v", st)
	}
}

func TestMetaStartsWithTwoValidSlots(t *testing.T) {
	for _, off := range []int64{3, MetaStride + 3} {
		sim := newSim(1)
		m, _ := mustSettle(t, sim)
		_ = m.Close()
		if slots, valid := metaSlots(t, sim); slots != [2]Meta{} || valid != [2]bool{true, true} {
			t.Fatalf("fresh meta holds %+v, valid %v", slots, valid)
		}
		if err := sim.Garble(metaPath, off); err != nil {
			t.Fatal(err)
		}
		sim.Crash()
		m, st, err := OpenMeta(sim, dir)
		if err != nil || st != (Meta{}) || m.Intact() {
			t.Fatalf("sector at %d garbled before the first start finished: %+v, %v", off, st, err)
		}
	}
}

func TestMetaSaveLeavesBothSlotsEqual(t *testing.T) {
	sim := newSim(1)
	m, _ := mustSettle(t, sim)
	for term := uint64(1); term <= 6; term++ {
		if err := m.Save(term, term+7, term%3 == 0); err != nil {
			t.Fatal(err)
		}
		want := Meta{Term: term, Vote: term + 7, Sealed: (term - 1) * 10, Repair: term%3 == 0}
		if slots, valid := metaSlots(t, sim); slots != [2]Meta{want, want} || valid != [2]bool{true, true} {
			t.Fatalf("after Save: slots %+v, valid %v, want twice %+v", slots, valid, want)
		}

		if err := m.Seal(term * 10); err != nil {
			t.Fatal(err)
		}
		sealed := want
		sealed.Sealed = term * 10
		slots, _ := metaSlots(t, sim)
		newest := newestSlot(m) / MetaStride
		if slots[newest] != sealed || slots[1-newest] != want {
			t.Fatalf("after Seal: slots %+v, want %+v newest and %+v untouched", slots, sealed, want)
		}
	}
}

type slotLog struct {
	*disk.Sim
	offs     []int64
	unsynced int
	widest   int
}

type slotLogFile struct {
	disk.File
	log *slotLog
}

func (l *slotLog) Open(name string) (disk.File, error) {
	f, err := l.Sim.Open(name)
	if err != nil {
		return nil, err
	}
	return slotLogFile{f, l}, nil
}

func (f slotLogFile) WriteAt(p []byte, off int64) (int, error) {
	f.log.offs = append(f.log.offs, off)
	f.log.unsynced++
	f.log.widest = max(f.log.widest, f.log.unsynced)
	return f.File.WriteAt(p, off)
}

func (f slotLogFile) Sync() error {
	f.log.unsynced = 0
	return f.File.Sync()
}

func (l *slotLog) take() []int64 {
	offs := l.offs
	l.offs = nil
	return offs
}

func TestMetaWritesOneSlotAtATime(t *testing.T) {
	sim := newSim(1)
	m, _ := mustSettle(t, sim)
	_ = m.Save(1, 1, false)
	_ = m.Seal(4)
	_ = m.Close()

	log := &slotLog{Sim: sim}
	m, st := mustOpenMeta(t, log)
	if st != (Meta{Term: 1, Vote: 1, Sealed: 4}) {
		t.Fatalf("meta = %+v", st)
	}
	if offs := log.take(); len(offs) != 0 {
		t.Fatalf("OpenMeta wrote at %v", offs)
	}
	read := newestSlot(m)
	spare := MetaStride - read

	if err := m.Settle(); err != nil {
		t.Fatal(err)
	}
	if offs := log.take(); !slices.Equal(offs, []int64{read}) {
		t.Fatalf("Settle wrote at %v, want the slot it read at %d", offs, read)
	}
	if err := m.Seal(9); err != nil {
		t.Fatal(err)
	}
	if offs := log.take(); !slices.Equal(offs, []int64{spare}) {
		t.Fatalf("Seal wrote at %v, want only the older slot at %d", offs, spare)
	}
	if err := m.Save(2, 3, true); err != nil {
		t.Fatal(err)
	}
	if offs := log.take(); !slices.Equal(offs, []int64{read, spare}) {
		t.Fatalf("Save wrote at %v, want the older slot at %d first, then %d", offs, read, spare)
	}
	if log.widest != 1 {
		t.Fatalf("up to %d slots written before one fsync", log.widest)
	}
	_ = m.Close()
	if _, st = mustOpenMeta(t, sim); st != (Meta{Term: 2, Vote: 3, Sealed: 9, Repair: true}) {
		t.Fatalf("meta = %+v", st)
	}
}

func TestMetaSettleLeavesTheOnlyValidSlotAlone(t *testing.T) {
	for _, off := range []int64{20, MetaStride + 20} {
		sim := newSim(1)
		m, _ := mustSettle(t, sim)
		_ = m.Save(3, 2, false)
		_ = m.Seal(8)
		_ = m.Close()
		if err := sim.Corrupt(metaPath, off); err != nil {
			t.Fatal(err)
		}

		log := &slotLog{Sim: sim}
		m, st := mustOpenMeta(t, log)
		if m.Intact() || st.Term != 3 || st.Vote != 2 {
			t.Fatalf("slot at %d damaged: %+v, intact %v", off, st, m.Intact())
		}
		valid := newestSlot(m)
		if err := m.Settle(); err != nil {
			t.Fatal(err)
		}
		if offs := log.take(); len(offs) != 0 {
			t.Fatalf("Settle wrote at %v with one valid slot at %d", offs, valid)
		}
		if err := m.Seal(st.Sealed + 1); err != nil {
			t.Fatal(err)
		}
		if offs := log.take(); !slices.Equal(offs, []int64{MetaStride - valid}) {
			t.Fatalf("first write went to %v, the only valid slot is at %d", offs, valid)
		}
		_ = m.Close()

		sim.Crash()
		m, again := mustOpenMeta(t, sim)
		if want := (Meta{Term: 3, Vote: 2, Sealed: st.Sealed + 1}); again != want || !m.Intact() {
			t.Fatalf("after the next write: %+v, intact %v, want %+v with both slots", again, m.Intact(), want)
		}
	}
}

func TestMetaDamagedSlotKeepsTheVote(t *testing.T) {
	harms := map[string]func(*disk.Sim, string, int64) error{
		"bit":    (*disk.Sim).Corrupt,
		"sector": (*disk.Sim).Garble,
	}
	sim := newSim(3)
	m, _ := mustSettle(t, sim)
	_ = m.Save(1, 7, false)
	_ = m.Seal(10)
	_ = m.Save(2, 8, true)
	_ = m.Seal(20)
	_ = m.Seal(30)
	newest := newestSlot(m)
	_ = m.Close()

	for name, harm := range harms {
		for _, off := range []int64{9, MetaStride + 9} {
			c := fork(sim, uint64(off))
			if err := harm(c, metaPath, off); err != nil {
				t.Fatal(err)
			}
			c.Crash()
			m, st := mustOpenMeta(t, c)
			want := Meta{Term: 2, Vote: 8, Sealed: 30, Repair: true}
			if off-9 == newest {
				want.Sealed = 20
			}
			if st != want || m.Intact() {
				t.Fatalf("%s at %d: %+v, intact %v, want %+v", name, off, st, m.Intact(), want)
			}
			_ = m.Close()
		}
	}

	for _, off := range []int64{9, MetaStride + 9} {
		if err := sim.Corrupt(metaPath, off); err != nil {
			t.Fatal(err)
		}
	}
	sim.Crash()
	var ce *CorruptError
	if _, _, err := OpenMeta(sim, dir); !errors.As(err, &ce) || ce.File != metaPath || ce.Offset != 0 {
		t.Fatalf("both slots damaged: %v", err)
	}
	if _, _, err := OpenMeta(sim, dir); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("second open with both slots damaged: %v, the lock was not released", err)
	}
}

func TestMetaShortFileIsOneBrokenSlot(t *testing.T) {
	sim := newSim(1)
	m, _ := mustSettle(t, sim)
	_ = m.Save(4, 1, false)
	_ = m.Close()
	f, _ := sim.Open(metaPath)
	_ = f.Truncate(MetaStride + MetaSlotLen/2)
	_ = f.Sync()

	m, st := mustSettle(t, sim)
	if st != (Meta{Term: 4, Vote: 1}) || m.Intact() {
		t.Fatalf("cut file: %+v, intact %v", st, m.Intact())
	}
	if err := m.Seal(1); err != nil {
		t.Fatal(err)
	}
	_ = m.Close()
	sim.Crash()
	if m, st = mustOpenMeta(t, sim); st != (Meta{Term: 4, Vote: 1, Sealed: 1}) || !m.Intact() {
		t.Fatalf("after the next write: %+v, intact %v", st, m.Intact())
	}
	_ = m.Close()

	f, _ = sim.Open(metaPath)
	_ = f.Truncate(MetaSlotLen - 1)
	_ = f.Sync()
	if _, _, err := OpenMeta(sim, dir); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("file without a whole slot: %v", err)
	}
}

func TestMetaUnknownFlagIsAVersionError(t *testing.T) {
	for _, newest := range []bool{false, true} {
		sim := newSim(1)
		m, _ := mustSettle(t, sim)
		_ = m.Save(2, 1, true)
		_ = m.Seal(5)
		off := MetaStride - newestSlot(m)
		if newest {
			off = newestSlot(m)
		}
		_ = m.Close()

		f, _ := sim.Open(metaPath)
		var slot [MetaSlotLen]byte
		if _, err := f.ReadAt(slot[:], off); err != nil {
			t.Fatal(err)
		}
		slot[metaOffFlags] |= 2
		binary.LittleEndian.PutUint32(slot[metaOffCRC:], crc32.Checksum(slot[:metaOffCRC], castagnoli))
		_, _ = f.WriteAt(slot[:], off)
		_ = f.Sync()

		_, _, err := OpenMeta(sim, dir)
		if !errors.Is(err, ErrVersion) || errors.Is(err, ErrCorrupt) {
			t.Fatalf("unknown flag in the slot at %d: %v", off, err)
		}
	}
}

func TestMetaFlipThenTornWriteStopsTheNode(t *testing.T) {
	stopped, started := 0, 0
	for seed := range uint64(40) {
		sim := newSim(seed)
		m, _ := mustSettle(t, sim)
		_ = m.Save(3, 1, false)
		_ = m.Seal(7)
		if err := sim.Corrupt(metaPath, newestSlot(m)+metaOffVote); err != nil {
			t.Fatal(err)
		}
		sim.WriteErrRate = 1
		if err := m.Seal(8); !errors.Is(err, syscall.EIO) {
			t.Fatalf("seed %d: seal = %v", seed, err)
		}
		_ = m.Close()
		quiet(sim).Crash()

		_, st, err := OpenMeta(sim, dir)
		switch {
		case errors.Is(err, ErrCorrupt):
			stopped++
		case err != nil || st.Term != 3 || st.Vote != 1:
			t.Fatalf("seed %d: %+v, %v", seed, st, err)
		default:
			started++
		}
	}
	if stopped == 0 || started == 0 {
		t.Fatalf("%d runs ended without a valid slot, %d with one: the test does not sit on the limit", stopped, started)
	}
}

func TestMetaErrorIsSticky(t *testing.T) {
	cases := map[string]func(*disk.Sim, *MetaFile) error{
		"write": func(sim *disk.Sim, m *MetaFile) error {
			sim.WriteErrRate = 1
			return m.Save(1, 0, false)
		},
		"sync": func(sim *disk.Sim, m *MetaFile) error {
			sim.SyncErrRate = 1
			return m.Seal(1)
		},
		"sync of the directory": func(sim *disk.Sim, m *MetaFile) error {
			sim.MetaErrRate = 1
			return m.Settle()
		},
	}
	for name, failing := range cases {
		t.Run(name, func(t *testing.T) {
			sim := newSim(4)
			m, _ := mustSettle(t, sim)
			if err := failing(sim, m); !errors.Is(err, syscall.EIO) {
				t.Fatalf("with the fault: %v", err)
			}
			quiet(sim)
			if err := m.Save(1, 0, false); !errors.Is(err, syscall.EIO) {
				t.Fatalf("save after the fault = %v", err)
			}
			if err := m.Seal(1); !errors.Is(err, syscall.EIO) {
				t.Fatalf("seal after the fault = %v", err)
			}
			if err := m.Settle(); !errors.Is(err, syscall.EIO) {
				t.Fatalf("settle after the fault = %v", err)
			}
		})
	}
}

func TestMetaIsLocked(t *testing.T) {
	sim := newSim(1)
	m, _ := mustOpenMeta(t, sim)
	if _, _, err := OpenMeta(sim, dir); !errors.Is(err, disk.ErrLocked) {
		t.Fatalf("second open = %v", err)
	}
	_ = m.Close()
	m, _ = mustOpenMeta(t, sim)
	_ = m.Close()
}

func TestMetaSettleMakesTheFileDurable(t *testing.T) {
	sim := newSim(5)
	armed := true
	fsys, frozen := freezeAt(sim, "rename", &armed)
	m, _ := mustOpenMeta(t, fsys)
	_ = m.Close()

	proc := frozen()
	m, _ = mustSettle(t, proc)
	if err := m.Save(7, 3, false); err != nil {
		t.Fatal(err)
	}
	if err := m.Seal(2); err != nil {
		t.Fatal(err)
	}
	proc.Crash()
	if _, st := mustOpenMeta(t, proc); st != (Meta{Term: 7, Vote: 3, Sealed: 2}) {
		t.Fatalf("saved {7 3} and sealed 2, after power loss %+v", st)
	}
}

func TestMetaFirstWritesSurvivePowerLoss(t *testing.T) {
	saved := Meta{Term: 1, Vote: 2, Repair: true}
	sealed := Meta{Term: 1, Vote: 2, Sealed: 5, Repair: true}
	for _, sector := range []int64{disk.SectorSize, 4096} {
		points := 0
		for seed := range uint64(1000) {
			sim := newSim(seed)
			sim.Sector = sector
			sim.ShredRate = float64(seed % 2)
			allowed := []Meta{{}}
			fsys := hookFS{Sim: sim, after: func(op string) {
				points++
				c := fork(sim, uint64(points))
				c.Crash()
				_, st, err := CreateMeta(c, dir, ident)
				if err != nil {
					t.Fatalf("sector %d seed %d: power loss after %s: %v", sector, seed, op, err)
				}
				if !slices.Contains(allowed, st) {
					t.Fatalf("sector %d seed %d: power loss after %s: got %+v, want one of %+v", sector, seed, op, st, allowed)
				}
			}}
			m, _ := mustSettle(t, fsys)
			allowed = append(allowed, saved)
			if err := m.Save(saved.Term, saved.Vote, saved.Repair); err != nil {
				t.Fatal(err)
			}
			allowed = []Meta{saved, sealed}
			if err := m.Seal(sealed.Sealed); err != nil {
				t.Fatal(err)
			}
		}
		if points < 1000*13 {
			t.Fatalf("sector %d: only %d crash points", sector, points)
		}
	}
}

func TestMetaCrashDuringSaveAndSeal(t *testing.T) {
	for _, sector := range []int64{disk.SectorSize, 4096} {
		for _, shred := range []float64{0, 1} {
			sim := newSim(2)
			sim.Sector, sim.ShredRate = sector, shred
			var done, doing Meta
			points := 0
			fsys := hookFS{Sim: sim, after: func(op string) {
				points++
				where := fmt.Sprintf("sector %d shred %v, done %+v, doing %+v, after %s", sector, shred, done, doing, op)
				lost := fork(sim, uint64(points))
				lost.Crash()
				_, st, err := CreateMeta(lost, dir, ident)
				if err != nil || (st != done && st != doing) {
					t.Fatalf("%s: after power loss %+v, %v", where, st, err)
				}

				proc := fork(sim, uint64(points))
				m, seen, err := CreateMeta(proc, dir, ident)
				if err != nil || (seen != done && seen != doing) {
					t.Fatalf("%s: after process crash %+v, %v", where, seen, err)
				}
				if err := m.Settle(); err != nil {
					t.Fatalf("%s: %v", where, err)
				}
				_ = m.Close()
				proc.Crash()
				if _, st, err = OpenMeta(proc, dir); err != nil || st != seen {
					t.Fatalf("%s: process settled %+v, after power loss %+v, %v", where, seen, st, err)
				}
			}}
			m, _ := mustSettle(t, fsys)
			for term := uint64(1); term <= 30; term++ {
				doing = Meta{Term: term, Vote: term % 4, Sealed: done.Sealed, Repair: term%5 == 0}
				if err := m.Save(doing.Term, doing.Vote, doing.Repair); err != nil {
					t.Fatal(err)
				}
				done = doing
				for range term % 3 {
					doing.Sealed += 7
					if err := m.Seal(doing.Sealed); err != nil {
						t.Fatal(err)
					}
					done = doing
				}
			}
			if points < 150 {
				t.Fatalf("sector %d shred %v: only %d crash points", sector, shred, points)
			}
		}
	}
}

func TestMetaSurvivesFailedSyncAndRestart(t *testing.T) {
	dropped := 0
	for seed := range uint64(30) {
		sim := newSim(seed)
		m, _ := mustSettle(t, sim)
		_ = m.Save(5, 1, false)
		sim.SyncErrRate = 1
		if err := m.Save(6, 2, false); !errors.Is(err, syscall.EIO) {
			t.Fatalf("save = %v", err)
		}
		sim.SyncErrRate = 0
		_ = m.Close()

		c := fork(sim, seed)
		c.Crash()
		if _, st, _ := OpenMeta(c, dir); st.Term == 5 {
			dropped++
		}

		m, seen := mustSettle(t, sim)
		_ = m.Close()
		sim.Crash()
		if _, st := mustOpenMeta(t, sim); st != seen {
			t.Fatalf("seed %d: restart settled %+v, after power loss %+v", seed, seen, st)
		}
	}
	if dropped == 0 {
		t.Fatal("failed sync never lost the write")
	}
}

type metaModel struct {
	kept  Meta
	prev  Meta
	tried []Meta
	shred bool
	stale int
}

func (mm *metaModel) allows(st Meta, intact bool) bool {
	if st == mm.kept || slices.Contains(mm.tried, st) {
		return true
	}
	if st == mm.prev && !intact && mm.shred {
		mm.stale++
		return true
	}
	return false
}

func TestMetaFaultsAndRestarts(t *testing.T) {
	seeds := uint64(400)
	if testing.Short() {
		seeds = 60
	}
	checks, stale := 0, 0
	for seed := range seeds {
		rng := rand.New(rand.NewPCG(seed, 11))
		sim := newSim(seed + 20000)
		sim.Sector = []int64{disk.SectorSize, 4096}[seed%2]
		sim.JournalRate = []float64{0, 0.5}[seed/2%2]
		sim.ShredRate = float64(seed / 4 % 2)
		mm := &metaModel{shred: sim.ShredRate > 0}
		step := 0
		fail := func(tag string, st Meta, intact bool) {
			t.Helper()
			t.Fatalf("seed %d step %d %s: got %+v, intact %v; kept %+v, slot before it %+v, tried %+v",
				seed, step, tag, st, intact, mm.kept, mm.prev, mm.tried)
		}
		verify := func(c *disk.Sim, tag string) {
			checks++
			for try := 0; ; try++ {
				if try == 6 {
					quiet(c)
				}
				m, st, err := CreateMeta(c, dir, ident)
				if err == nil {
					if !mm.allows(st, m.Intact()) {
						fail(tag, st, m.Intact())
					}
					err = m.Settle()
					_ = m.Close()
				}
				if errors.Is(err, syscall.EIO) {
					if rng.IntN(2) == 0 {
						c.Crash()
					}
					continue
				}
				if err != nil {
					t.Fatalf("seed %d step %d %s: %v", seed, step, tag, err)
				}
				quiet(c).Crash()
				if _, again, err := CreateMeta(c, dir, ident); err != nil || again != st {
					t.Fatalf("seed %d step %d %s: settled %+v, after power loss %+v, %v", seed, step, tag, st, again, err)
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
		restart := func() *MetaFile {
			for {
				m, st, err := CreateMeta(fsys, dir, ident)
				if err == nil {
					if !mm.allows(st, m.Intact()) {
						fail("restart", st, m.Intact())
					}
					sealed := st
					sealed.Sealed = rng.Uint64N(1000)
					if err = m.Settle(); err == nil {
						mm.tried = append(mm.tried, sealed)
						err = m.Seal(sealed.Sealed)
					}
					if err == nil {
						mm.prev, mm.kept, mm.tried = st, sealed, nil
						return m
					}
					_ = m.Close()
				}
				if !errors.Is(err, syscall.EIO) {
					t.Fatalf("seed %d step %d: restart: %v", seed, step, err)
				}
				if rng.IntN(2) == 0 {
					sim.Crash()
				}
			}
		}
		sim.WriteErrRate, sim.SyncErrRate, sim.MetaErrRate = 0.03, 0.06, 0.03
		m := restart()
		for step = range 40 {
			st, ok := mm.kept, false
			switch op := rng.IntN(5); {
			case op < 2:
				st.Term, st.Vote, st.Repair = st.Term+1+uint64(step), rng.Uint64N(3), rng.IntN(4) == 0
				mm.tried = append(mm.tried, st)
				if ok = m.Save(st.Term, st.Vote, st.Repair) == nil; ok {
					mm.prev, mm.kept, mm.tried = st, st, nil
				}
			case op < 4:
				st.Sealed = rng.Uint64N(1000)
				mm.tried = append(mm.tried, st)
				if ok = m.Seal(st.Sealed) == nil; ok {
					mm.prev, mm.kept, mm.tried = mm.kept, st, nil
				}
			}
			if !ok {
				_ = m.Close()
				if rng.IntN(2) == 0 {
					sim.Crash()
				}
				m = restart()
			}
		}
		_ = m.Close()
		stale += mm.stale
	}
	if !testing.Short() && stale == 0 {
		t.Fatal("a power loss never shredded the slot that Settle wrote back")
	}
	t.Logf("%d checked recoveries, %d of them fell back to the older slot", checks, stale)
}

func TestMetaLayoutIsPinned(t *testing.T) {
	var slot [MetaSlotLen]byte
	encodeMeta(slot[:], 5, Identity{Cluster: 0x0102030405060708, Node: 2, Nodes: 3}, Meta{Term: 9, Vote: 3, Sealed: 300, Repair: true})
	const want = "0500000000000000" + "0900000000000000" + "2c01000000000000" + "0807060504030201" + "03020301" + "253777e6"
	if got := fmt.Sprintf("%x", slot); got != want {
		t.Fatalf("slot\n%s\nwant\n%s", got, want)
	}
}

func TestMetaHoldsItsIdentity(t *testing.T) {
	held := func(sim *disk.Sim) (ids [2]Identity) {
		t.Helper()
		raw := contents(t, sim, metaPath)
		for k := range ids {
			_, ids[k], _, _ = decodeMeta(raw[k*MetaStride : k*MetaStride+MetaSlotLen])
		}
		return ids
	}
	sim := newSim(1)
	m, _ := mustSettle(t, sim)
	if got := held(sim); got != [2]Identity{ident, ident} {
		t.Fatalf("fresh meta holds %v, want %v twice", got, ident)
	}
	if err := m.Save(4, 1, true); err != nil {
		t.Fatal(err)
	}
	if err := m.Seal(9); err != nil {
		t.Fatal(err)
	}
	if err := m.Settle(); err != nil {
		t.Fatal(err)
	}
	newest := newestSlot(m)
	_ = m.Close()
	sim.Crash()
	if got := held(sim); got != [2]Identity{ident, ident} {
		t.Fatalf("after Save, Seal and Settle: %v, want %v twice", got, ident)
	}

	if err := sim.Corrupt(metaPath, newest+metaOffNode); err != nil {
		t.Fatal(err)
	}
	m, st, err := OpenMeta(sim, dir)
	if err != nil {
		t.Fatal(err)
	}
	if m.Intact() || m.Identity() != ident || st != (Meta{Term: 4, Vote: 1, Repair: true}) {
		t.Fatalf("one slot broken: %v, %+v, intact %v", m.Identity(), st, m.Intact())
	}
	if err := m.Seal(10); err != nil {
		t.Fatal(err)
	}
	_ = m.Close()
	sim.Crash()
	if got := held(sim); got != [2]Identity{ident, ident} {
		t.Fatalf("after the broken slot was written again: %v, want %v twice", got, ident)
	}
}

func TestOpenMetaDoesNotCreate(t *testing.T) {
	sim := disk.NewSim(rand.New(rand.NewPCG(1, 1)))
	_, _, err := OpenMeta(sim, dir)
	if !errors.Is(err, ErrNoMeta) || err.Error() != "wal: no meta: /data/wal does not exist" {
		t.Fatalf("no directory: %v", err)
	}
	if _, err := sim.List("/data"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("after OpenMeta on a missing directory, listing its parent gives %v", err)
	}

	if err := sim.MkdirAll(dir); err != nil {
		t.Fatal(err)
	}
	_, _, err = OpenMeta(sim, dir)
	if !errors.Is(err, ErrNoMeta) || err.Error() != "wal: no meta: /data/wal/meta" {
		t.Fatalf("empty directory: %v", err)
	}
	if names, err := sim.List(dir); err != nil || len(names) != 0 {
		t.Fatalf("empty directory after OpenMeta holds %v, %v", names, err)
	}
	if d := sim.Durable(); len(d) != 0 {
		t.Fatalf("durable files after OpenMeta: %v", d)
	}
	lock, err := sim.Lock(dir)
	if err != nil {
		t.Fatalf("lock after a refused OpenMeta: %v", err)
	}
	_ = lock.Close()
}

func TestCreateMetaOpensWhatIsThere(t *testing.T) {
	sim := newSim(1)
	other := Identity{Cluster: 9, Node: 1, Nodes: 5}
	m, _, err := CreateMeta(sim, dir, other)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Settle(); err != nil {
		t.Fatal(err)
	}
	if err := m.Save(3, 4, false); err != nil {
		t.Fatal(err)
	}
	if err := m.Seal(12); err != nil {
		t.Fatal(err)
	}
	_ = m.Close()
	before := files(t, sim)

	log := &slotLog{Sim: sim}
	m, st, err := CreateMeta(log, dir, ident)
	if err != nil {
		t.Fatal(err)
	}
	_ = m.Close()
	if st != (Meta{Term: 3, Vote: 4, Sealed: 12}) || m.Identity() != other {
		t.Fatalf("CreateMeta as %v on the meta of %v: %+v, %v", ident, other, st, m.Identity())
	}
	if offs := log.take(); len(offs) != 0 {
		t.Fatalf("CreateMeta wrote at %v", offs)
	}
	if after := files(t, sim); !maps.EqualFunc(before, after, bytes.Equal) {
		t.Fatal("CreateMeta changed the files of a directory with meta")
	}
}

func TestCreateMetaRefusesWhatIsNoIdentity(t *testing.T) {
	for _, id := range []Identity{
		{Cluster: 0, Node: 1, Nodes: 3},
		{Cluster: 7, Node: 0, Nodes: 3},
		{Cluster: 7, Node: 4, Nodes: 3},
		{Cluster: 7, Node: 1, Nodes: 256},
	} {
		sim := disk.NewSim(rand.New(rand.NewPCG(1, 1)))
		_, _, err := CreateMeta(sim, dir, id)
		if want := fmt.Sprintf("wal: %v is not an identity", id); err == nil || err.Error() != want {
			t.Fatalf("CreateMeta(%v) = %v, want %q", id, err, want)
		}
		if _, err := sim.List("/data"); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("CreateMeta(%v) made a directory: %v", id, err)
		}
	}
}

func TestMetaOfAnEarlierLayoutIsAnotherVersion(t *testing.T) {
	for _, repair := range []bool{false, true} {
		var raw [metaLen]byte
		for seq := range uint64(2) {
			slot := raw[seq*MetaStride:]
			binary.LittleEndian.PutUint64(slot, seq)
			binary.LittleEndian.PutUint64(slot[8:], 3)
			binary.LittleEndian.PutUint64(slot[16:], 2)
			binary.LittleEndian.PutUint64(slot[24:], 40)
			if repair {
				binary.LittleEndian.PutUint32(slot[32:], 1)
			}
			binary.LittleEndian.PutUint32(slot[36:], crc32.Checksum(slot[:36], castagnoli))
		}
		sim := newSim(1)
		if err := sim.MkdirAll(dir); err != nil {
			t.Fatal(err)
		}
		f, err := sim.Create(metaPath)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = f.WriteAt(raw[:], 0)
		_ = f.Sync()
		_ = f.Close()

		_, _, err = OpenMeta(sim, dir)
		want := "wal: unknown format version: /data/wal/meta holds no identity, an earlier build wrote it"
		if !errors.Is(err, ErrVersion) || err.Error() != want {
			t.Fatalf("repair %v: %v, want %q", repair, err, want)
		}
		if _, _, err := CreateMeta(sim, dir, ident); !errors.Is(err, ErrVersion) {
			t.Fatalf("repair %v: CreateMeta on the old layout: %v", repair, err)
		}
	}
}

func TestMetaSlotsOfTwoNodesAreCorrupt(t *testing.T) {
	sim := newSim(1)
	m, _ := mustSettle(t, sim)
	_ = m.Close()
	var slot [MetaSlotLen]byte
	encodeMeta(slot[:], 1, Identity{Cluster: 7, Node: 3, Nodes: 3}, Meta{})
	f, err := sim.Open(metaPath)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.WriteAt(slot[:], MetaStride)
	_ = f.Sync()
	_ = f.Close()

	var ce *CorruptError
	if _, _, err := OpenMeta(sim, dir); !errors.As(err, &ce) || ce.File != metaPath || ce.Offset != MetaStride {
		t.Fatalf("two valid slots of two nodes: %v", err)
	}
}

func TestSaveRefusesAVoteThatDoesNotFit(t *testing.T) {
	sim := newSim(1)
	m, _ := mustSettle(t, sim)
	if err := m.Save(1, 2, false); err != nil {
		t.Fatal(err)
	}
	before := contents(t, sim, metaPath)
	err := m.Save(1, 256, false)
	if err == nil || err.Error() != "wal: meta stopped after error: wal: vote for node 256 does not fit a slot" {
		t.Fatalf("Save(1, 256, false) = %v", err)
	}
	if err := m.Seal(5); err == nil {
		t.Fatal("Seal after the refused vote went through")
	}
	_ = m.Close()
	if !bytes.Equal(contents(t, sim, metaPath), before) {
		t.Fatal("the refused vote changed meta")
	}
	sim.Crash()
	if _, st := mustOpenMeta(t, sim); st != (Meta{Term: 1, Vote: 2}) {
		t.Fatalf("after the refused vote: %+v", st)
	}
}
