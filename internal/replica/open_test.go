package replica

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"

	"github.com/Demonisreal/dmn-ledger/internal/client"
	"github.com/Demonisreal/dmn-ledger/internal/disk"
	"github.com/Demonisreal/dmn-ledger/internal/raft"
	"github.com/Demonisreal/dmn-ledger/internal/wal"
)

func small(cfg *Config) {
	cfg.SnapshotEvery, cfg.SegmentSize = 4, 512
}

func snapFile(s raft.Snapshot) string {
	return fmt.Sprintf("/n/snap/snap-%020d-%020d.snap", s.Index, s.Term)
}

func names(files []disk.DurableFile) []string {
	out := make([]string, len(files))
	for i, f := range files {
		out[i] = f.Name
	}
	return out
}

func TestBrokenNewestSnapshotFallsBackToTheOlder(t *testing.T) {
	tn := newTestNet(t, 3, small)
	tn.elect(0)
	c := tn.register(0, 1)
	n := tn.nodes[2]
	var older, newest raft.Snapshot
	for i := uint64(1); older.Index == 0 || n.Status().Last == newest.Index; i++ {
		ok(t, c.do(client.CreateAccounts, accounts(i)))
		tn.tick(tn.all()...)
		if n.snap != newest {
			older, newest = newest, n.snap
		}
	}
	last := n.Status().Last
	tn.crash(2)
	tn.flip(2, snapFile(newest), 40)
	tn.open(2)
	n = tn.nodes[2]
	if st := n.Status(); st.Repair || st.Applied != older.Index || st.Last != last {
		t.Fatalf("status %+v, want a voter at snapshot %d with a log up to %d", st, older.Index, last)
	}
	if got := names(stored(tn.disks[2], ".snap")); !slices.Equal(got, []string{snapFile(older)}) {
		t.Fatalf("snapshots %v, want only %s", got, snapFile(older))
	}
	tn.converge(2)
	if n.snap.Index <= newest.Index {
		t.Fatalf("n3 took no snapshot after %d: %+v", newest.Index, n.snap)
	}
	if got := names(stored(tn.disks[2], ".snap")); !slices.Equal(got, []string{snapFile(older), snapFile(n.snap)}) {
		t.Fatalf("snapshots %v, want %s kept next to %s", got, snapFile(older), snapFile(n.snap))
	}
}

func installed(t *testing.T) (*testNet, raft.Snapshot, raft.Snapshot) {
	t.Helper()
	tn := newTestNet(t, 3, small)
	tn.elect(0)
	c := tn.register(0, 1)
	n := tn.nodes[2]
	for i := uint64(1); n.snap.Index == 0 || n.snap.Index != n.Status().Last; i++ {
		ok(t, c.do(client.CreateAccounts, accounts(i)))
		for range 3 {
			tn.tick(tn.all()...)
		}
	}
	own := n.snap
	tn.cut[3] = true
	for i := range uint64(30) {
		ok(t, c.do(client.CreateAccounts, accounts(i+100)))
	}
	tn.cut[3] = false
	tn.drop = func(m raft.Message) bool { return m.To == 3 && m.Type == raft.MsgApp && len(m.Entries) > 0 }
	for range 100 {
		tn.tick(tn.all()...)
	}
	if n.snap.Index <= own.Index || n.Status().Last != n.snap.Index {
		t.Fatalf("n3 %+v with snapshot %+v, want an installed snapshot and nothing after it", n.Status(), n.snap)
	}
	return tn, own, n.snap
}

func TestBrokenInstalledSnapshotIsDamage(t *testing.T) {
	tn, own, got := installed(t)
	tn.crash(2)
	tn.flip(2, snapFile(got), 40)
	tn.open(2)
	if st := tn.nodes[2].Status(); !st.Repair || st.Applied != own.Index {
		t.Fatalf("n3 acknowledged %d, restarts at %d and does not know: %+v", got.Index, own.Index, st)
	}
	tn.drop = nil
	tn.converge(2)
}

func TestBrokenSlotAndBrokenInstalledSnapshotIsDamage(t *testing.T) {
	tn, _, got := installed(t)
	tn.crash(2)
	slots := []int64{0, 4096}
	for _, off := range slots {
		d := tn.disks[2].Clone(rand.New(rand.NewPCG(1, 1)))
		if err := errors.Join(d.Corrupt("/n/meta", off+30), d.Corrupt(snapFile(got), 40)); err != nil {
			t.Fatal(err)
		}
		st, err := tn.probe(2, d)
		if err != nil || !st.Repair {
			t.Fatalf("slot at %d and the installed snapshot %d broken: %+v, %v, want repair", off, got.Index, st, err)
		}
	}
}

func TestAllSnapshotsBroken(t *testing.T) {
	for _, tc := range []struct {
		name    string
		segment int64
		repair  bool
	}{
		{"log from the start", 0, false},
		{"compacted log", 512, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tn := newTestNet(t, 3, func(cfg *Config) { cfg.SnapshotEvery, cfg.SegmentSize = 4, tc.segment })
			tn.elect(0)
			c := tn.register(0, 1)
			for i := range uint64(14) {
				ok(t, c.do(client.CreateAccounts, accounts(i+1)))
				tn.tick(tn.all()...)
			}
			snaps := stored(tn.disks[2], ".snap")
			first, _ := tn.nodes[2].log.FirstIndex()
			last := tn.nodes[2].Status().Last
			if len(snaps) != 2 || first > 1 != tc.repair {
				t.Fatalf("%d snapshots, log from %d", len(snaps), first)
			}
			tn.crash(2)
			for _, f := range snaps {
				tn.flip(2, f.Name, f.Size/2)
			}
			tn.open(2)
			want := Status{Last: last}
			if tc.repair {
				want = Status{Repair: true}
			}
			if st := tn.nodes[2].Status(); st.Repair != want.Repair || st.Last != want.Last || st.Applied != 0 {
				t.Fatalf("status %+v, want repair %t with a log up to %d", st, want.Repair, want.Last)
			}
			if left := stored(tn.disks[2], ".snap"); len(left) != 0 {
				t.Fatalf("broken snapshots still on disk: %v", names(left))
			}
			tn.converge(2)
		})
	}
}

func TestSingleNodeRefusesToStartDamaged(t *testing.T) {
	for _, tc := range []struct {
		name  string
		spoil func(tn *testNet)
	}{
		{"broken frame up to the seal", func(tn *testNet) {
			logs := stored(tn.disks[0], ".log")
			tn.flip(0, logs[len(logs)-1].Name, logs[len(logs)-1].Size-10)
		}},
		{"broken snapshots over a compacted log", func(tn *testNet) {
			for _, f := range stored(tn.disks[0], ".snap") {
				tn.flip(0, f.Name, f.Size/2)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tn := newTestNet(t, 1, small)
			tn.elect(0)
			c := tn.register(0, 1)
			for i := range uint64(14) {
				ok(t, c.do(client.CreateAccounts, accounts(i+1)))
			}
			tn.crash(0)
			tc.spoil(tn)
			before := tn.disks[0].Durable()
			for range 2 {
				if _, err := tn.probe(0, tn.disks[0]); !errors.Is(err, wal.ErrCorrupt) {
					t.Fatalf("open of a damaged single node: %v, want ErrCorrupt", err)
				}
				tn.disks[0].Crash()
				if after := tn.disks[0].Durable(); !slices.Equal(after, before) {
					t.Fatalf("files %v after the refused start, %v before", after, before)
				}
			}
		})
	}
}

func TestCrashWhileEmptyingALongerLogOnStart(t *testing.T) {
	tn := stoppedTestNet(t, 3, small)
	rng := rand.New(rand.NewPCG(3, 3))
	var before, frozen *disk.Sim
	back := false
	tn.tap = func(i int, fsys disk.FS) disk.FS {
		if i != 0 {
			return fsys
		}
		return tapFS{fsys, func(op, name string) {
			if !back || frozen != nil {
				return
			}
			if before != nil && op == "write" && strings.HasPrefix(name, "/n/meta") {
				frozen = before
			}
			if before != nil || op == "rename" && strings.HasSuffix(name, ".snap") {
				before = tn.disks[0].Clone(rng)
			}
		}}
	}
	for i := range tn.nodes {
		tn.open(i)
	}
	tn.elect(0)
	c := tn.register(0, 1)
	for i := range uint64(6) {
		ok(t, c.do(client.CreateAccounts, accounts(i+1)))
		tn.tick(tn.all()...)
	}
	tn.cut[1] = true
	for i := range uint64(12) {
		tn.send(0, c.request(client.CreateAccounts, accounts(i+50)))
	}
	longer := tn.nodes[0].Status().Last
	c.node = tn.choose(1, 2)
	for i := range uint64(9) {
		ok(t, c.do(client.CreateAccounts, accounts(i+100)))
		tn.tick(1, 2)
	}
	tn.cut[1], back = false, true
	for range 100 {
		tn.tick(tn.all()...)
	}
	s := tn.nodes[0].snap
	if frozen == nil || s.Index >= longer {
		t.Fatalf("n1 installed no snapshot below the end of its log at %d: %+v", longer, s)
	}
	frozen.Crash()
	if st, err := tn.probe(0, frozen.Clone(rng)); err != nil || st.Repair || st.Last != s.Index || st.Applied != s.Index {
		t.Fatalf("start between the snapshot and its seal: %+v, %v, want a voter at %d", st, err, s.Index)
	}
	points, removed := 0, false
	d := frozen.Clone(rng)
	_, err := tn.probe(0, tapFS{d, func(op, name string) {
		points++
		removed = removed || op == "remove" && strings.HasSuffix(name, ".log")
		lost := d.Clone(rng)
		lost.Crash()
		if st, err := tn.probe(0, lost); err != nil || st.Repair || st.Last < s.Index {
			t.Fatalf("power loss after %s %s of the start: %+v, %v, want a voter at %d", op, name, st, err, s.Index)
		}
	}})
	if err != nil || !removed || points < 6 {
		t.Fatalf("start: %v, %d disk accesses, log removed: %t", err, points, removed)
	}
}

func TestCrashWhileEmptyingADamagedLog(t *testing.T) {
	tn, own, got := installed(t)
	tn.drop = nil
	for range 3 {
		tn.tick(tn.all()...)
	}
	if first, _ := tn.nodes[2].log.FirstIndex(); first != got.Index+1 || tn.nodes[2].snap != got {
		t.Fatalf("n3 log starts at %d with snapshot %+v, want entries right after snapshot %d", first, tn.nodes[2].snap, got.Index)
	}
	tn.crash(2)
	tn.flip(2, snapFile(got), 40)
	rng := rand.New(rand.NewPCG(4, 4))
	d := tn.disks[2].Clone(rng)
	points := 0
	st, err := tn.probe(2, tapFS{d, func(op, name string) {
		points++
		lost := d.Clone(rng)
		lost.Crash()
		if st, err := tn.probe(2, lost); err != nil || !st.Repair {
			t.Fatalf("power loss after %s %s of the start: %+v, %v, want repair", op, name, st, err)
		}
	}})
	if err != nil || !st.Repair || st.Last != own.Index || points < 8 {
		t.Fatalf("start with the installed snapshot broken: %+v, %v after %d disk accesses, want repair at %d", st, err, points, own.Index)
	}
}

func TestCrashWhileFlaggingWithABrokenSlot(t *testing.T) {
	tn, _ := written(t, 5)
	tn.crash(2)
	logs := stored(tn.disks[2], ".log")
	tail := logs[len(logs)-1]
	rng := rand.New(rand.NewPCG(5, 5))
	for _, slot := range []int64{0, 4096} {
		d := tn.disks[2].Clone(rng)
		if err := errors.Join(d.Corrupt("/n/meta", slot+30), d.Corrupt(tail.Name, tail.Size-10)); err != nil {
			t.Fatal(err)
		}
		points := 0
		st, err := tn.probe(2, tapFS{d, func(op, name string) {
			points++
			lost := d.Clone(rng)
			lost.Crash()
			if st, err := tn.probe(2, lost); err != nil || !st.Repair {
				t.Fatalf("slot at %d broken, power loss after %s %s of the start: %+v, %v, want repair", slot, op, name, st, err)
			}
		}})
		if err != nil || !st.Repair || points < 4 {
			t.Fatalf("slot at %d and a sealed frame broken: %+v, %v after %d disk accesses, want repair", slot, st, err, points)
		}
	}
}

func TestStartWritesMetaAfterTheLog(t *testing.T) {
	tn := stoppedTestNet(t, 3, nil)
	var stopped *disk.Sim
	armed := false
	tn.tap = func(i int, fsys disk.FS) disk.FS {
		if i != 2 {
			return fsys
		}
		return tapFS{fsys, func(op, name string) {
			if armed && stopped == nil && op == "write" && strings.HasPrefix(name, "/n/wal/") {
				stopped = tn.disks[2].Clone(rand.New(rand.NewPCG(1, 1)))
			}
		}}
	}
	for i := range tn.nodes {
		tn.open(i)
	}
	tn.elect(0)
	c := tn.register(0, 1)
	ok(t, c.do(client.CreateAccounts, accounts(1)))
	tn.tick(tn.all()...)
	armed = true
	ok(t, c.do(client.CreateAccounts, accounts(2)))
	if stopped == nil {
		t.Fatal("n3 wrote no entry")
	}
	rng := rand.New(rand.NewPCG(2, 2))
	for range 40 {
		d := stopped.Clone(rng)
		settled := false
		_, err := tn.probe(2, tapFS{d, func(op, name string) {
			if settled || op != "write" || !strings.HasPrefix(name, "/n/meta") {
				return
			}
			settled = true
			for _, slot := range []int64{0, 4096} {
				lost := d.Clone(rng)
				lost.Crash()
				if err := lost.Garble("/n/meta", slot); err != nil {
					t.Fatal(err)
				}
				if st, err := tn.probe(2, lost); err != nil || st.Repair {
					t.Fatalf("power loss tears the slot at %d in the first write to meta: %+v, %v, want a voter", slot, st, err)
				}
			}
		}})
		if err != nil || !settled {
			t.Fatalf("start after a process crash: %v, meta written: %t", err, settled)
		}
	}
}

func TestStartSealsWhatTheLogHolds(t *testing.T) {
	tn := stoppedTestNet(t, 3, nil)
	var frozen *disk.Sim
	armed := false
	tn.tap = func(i int, fsys disk.FS) disk.FS {
		if i != 2 {
			return fsys
		}
		return tapFS{fsys, func(op, name string) {
			if armed && frozen == nil && op == "sync" && strings.HasPrefix(name, "/n/wal/") {
				frozen = tn.disks[2].Clone(rand.New(rand.NewPCG(1, 1)))
			}
		}}
	}
	for i := range tn.nodes {
		tn.open(i)
	}
	tn.elect(0)
	c := tn.register(0, 1)
	ok(t, c.do(client.CreateAccounts, accounts(1)))
	tn.tick(tn.all()...)
	armed = true
	ok(t, c.do(client.CreateAccounts, accounts(2)))
	tn.tick(tn.all()...)
	last := tn.nodes[2].Status().Last
	tn.stop(2)
	frozen.Crash()
	tn.disks[2], tn.held[2] = frozen, receipt{}
	tn.open(2)
	for range 3 {
		tn.tick(tn.all()...)
	}
	if st := tn.nodes[2].Status(); st.Last != last || tn.held[2].acked != last {
		t.Fatalf("n3 after a power loss between the sync and the seal: %+v, acknowledged %d, want %d", st, tn.held[2].acked, last)
	}
	tn.spoilTail(2)
}
