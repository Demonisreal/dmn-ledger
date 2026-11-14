package replica

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/Demonisreal/dmn-ledger/internal/client"
	"github.com/Demonisreal/dmn-ledger/internal/disk"
	"github.com/Demonisreal/dmn-ledger/internal/raft"
	"github.com/Demonisreal/dmn-ledger/internal/wal"
)

func tour(tn *testNet) {
	t := tn.t
	t.Helper()
	for i := range tn.nodes {
		tn.open(i)
	}
	tn.elect(0)
	c := tn.register(0, 1)
	next := uint64(0)
	write := func(count int) {
		t.Helper()
		for range count {
			next++
			ok(t, c.do(client.CreateAccounts, accounts(next)))
		}
		tn.tick(tn.all()...)
	}
	strand := func(count int) uint64 {
		tn.cut[uint64(c.node)+1] = true
		for range count {
			next++
			tn.send(c.node, c.request(client.CreateAccounts, accounts(next)))
		}
		return tn.nodes[c.node].Status().Last
	}
	rejoin := func(i int) {
		t.Helper()
		tn.cut[uint64(i)+1] = false
		lead := tn.nodes[c.node]
		for range 100 {
			tn.tick(tn.all()...)
			if tn.nodes[i].Digest() == lead.Digest() {
				return
			}
		}
		t.Fatalf("n%d did not catch up: %+v, leader %+v", i+1, tn.nodes[i].Status(), lead.Status())
	}
	others := func(i int) []int {
		return []int{(i + 1) % 3, (i + 2) % 3}
	}

	write(6)
	tn.crash(2)
	tn.open(2)
	write(2)

	old := c.node
	stranded := strand(3)
	c.node = tn.choose(others(old)...)
	write(2)
	installs := tn.snaps
	rejoin(old)
	if term, err := tn.nodes[old].EntryTerm(stranded); err != nil || term != tn.nodes[c.node].Status().Term || tn.snaps != installs {
		t.Fatalf("n%d did not cut its log at a conflict: term %d at %d, %v, %d snapshots sent", old+1, term, stranded, err, tn.snaps-installs)
	}

	tn.stop(old)
	tn.open(old)
	write(3)

	old = c.node
	stranded = strand(12)
	c.node = tn.choose(others(old)...)
	write(9)
	rejoin(old)
	if s := tn.nodes[old].snap; tn.snaps == installs || s.Index >= stranded {
		t.Fatalf("n%d installed no snapshot below the end of its log: snapshot %+v, log ended at %d", old+1, s, stranded)
	}
	installs = tn.snaps

	lag := others(c.node)[0]
	tn.cut[uint64(lag)+1] = true
	write(12)
	rejoin(lag)
	if tn.snaps == installs {
		t.Fatalf("n%d caught up without a snapshot", lag+1)
	}
	tn.crash(lag)
	tn.open(lag)
	write(3)

	lag = others(c.node)[1]
	tn.cut[uint64(lag)+1] = true
	tn.crash(c.node)
	tn.open(c.node)
	c.node = tn.choose(others(lag)...)
	write(2)
	batch := false
	tn.drop = func(m raft.Message) bool {
		batch = batch || m.To == uint64(lag)+1 && len(m.Entries) > 1 && m.Term > tn.nodes[lag].Status().Term
		return false
	}
	rejoin(lag)
	if !batch {
		t.Fatalf("n%d did not get the new term and several entries in one message: %+v", lag+1, tn.nodes[lag].Status())
	}
	tn.spoilTail(lag)
	tn.drop = func(m raft.Message) bool { return m.To == uint64(lag)+1 && len(m.Entries) > 0 }
	write(2)
	tn.crash(lag)
	tn.open(lag)
	if st := tn.nodes[lag].Status(); !st.Repair {
		t.Fatalf("n%d left repair without the entries it lost: %+v", lag+1, st)
	}
	tn.drop = nil
	tn.converge(lag)
	tn.rotted[lag] = false
	write(3)
	for range 10 {
		tn.tick(tn.all()...)
	}
	for i, n := range tn.nodes {
		if st := n.Status(); st.Repair || n.Digest() != tn.nodes[c.node].Digest() {
			t.Fatalf("n%d at the end: %+v", i+1, st)
		}
	}
}

func rot(t *testing.T, d *disk.Sim, rng *rand.Rand) string {
	t.Helper()
	logs, snaps := stored(d, ".log"), stored(d, ".snap")
	if len(logs) == 0 {
		return "nothing"
	}
	f := logs[len(logs)-1]
	off := f.Size - 1 - rng.Int64N(min(f.Size, 200))
	switch k := rng.IntN(4); {
	case k == 2:
		f = logs[rng.IntN(len(logs))]
		off = rng.Int64N(f.Size)
	case k == 3 && len(snaps) > 0:
		f = snaps[rng.IntN(len(snaps))]
		off = rng.Int64N(f.Size)
	}
	spoil, unit := d.Corrupt, "bit"
	if rng.IntN(4) == 0 {
		spoil, unit = d.Garble, "sector"
	}
	if err := spoil(f.Name, off); err != nil {
		t.Fatal(err)
	}
	return fmt.Sprintf("%s at %d of %s", unit, off, f.Name)
}

func TestCrashAtEveryDiskAccess(t *testing.T) {
	for _, tc := range []struct {
		name  string
		rot   bool
		shred float64
	}{
		{"power loss", false, 0},
		{"power loss and rot", true, 0},
		{"shredded sectors", false, 1},
		{"shredded sectors and rot", true, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tn := stoppedTestNet(t, 3, small)
			rng := rand.New(rand.NewPCG(7, 7))
			damaged := tc.rot || tc.shred > 0
			points, opens, repairs := 0, 0, 0
			lose := func(d *disk.Sim) *disk.Sim {
				c := d.Clone(rand.New(rand.NewPCG(rng.Uint64(), 1)))
				c.ShredRate = tc.shred
				c.Crash()
				return c
			}
			var check func(i int, d *disk.Sim, what string, nested bool)
			check = func(i int, d *disk.Sim, what string, nested bool) {
				var fsys disk.FS = d
				if nested {
					fsys = tapFS{d, func(op, name string) {
						check(i, lose(d), fmt.Sprintf("%s, then power loss after %s %s of the next start", what, op, name), false)
					}}
				}
				st, err := tn.probe(i, fsys)
				opens++
				switch {
				case err != nil:
					t.Fatalf("n%d, %s: open: %v", i+1, what, err)
				case st.Repair && !damaged && !tn.rotted[i]:
					t.Fatalf("n%d, %s: in repair without a damaged disk: %+v", i+1, what, st)
				case st.Repair:
					repairs++
				}
				if err := tn.held[i].kept(st); err != nil {
					t.Fatalf("n%d, %s: %v", i+1, what, err)
				}
			}
			tn.tap = func(i int, fsys disk.FS) disk.FS {
				return tapFS{fsys, func(op, name string) {
					points++
					if testing.Short() && points%8 != 0 {
						return
					}
					what := fmt.Sprintf("access %d, power loss after %s %s", points, op, name)
					lost := lose(tn.disks[i])
					if tc.rot {
						what += ", " + rot(t, lost, rng)
					}
					check(i, lost, what, true)
					if damaged {
						return
					}
					stopped := tn.disks[i].Clone(rand.New(rand.NewPCG(rng.Uint64(), 2)))
					what = fmt.Sprintf("access %d, process crash after %s %s", points, op, name)
					check(i, stopped, what, true)
					check(i, lose(stopped), what+", then power loss", false)
				}}
			}
			tour(tn)
			if repairs == 0 {
				t.Fatalf("none of %d starts was in repair, the run passes through one", opens)
			}
			t.Logf("%d disk accesses, %d starts checked, %d of them in repair", points, opens, repairs)
		})
	}
}

type crashPoint struct {
	node int
	disk *disk.Sim
	held receipt
}

func FuzzOpenDamaged(f *testing.F) {
	const spotLen = 6
	tn := stoppedTestNet(f, 3, small)
	var points []crashPoint
	accesses := uint64(0)
	tn.tap = func(i int, fsys disk.FS) disk.FS {
		return tapFS{fsys, func(string, string) {
			accesses++
			if accesses%4 != 0 {
				return
			}
			lost := tn.disks[i].Clone(rand.New(rand.NewPCG(accesses, 5)))
			lost.Crash()
			points = append(points, crashPoint{i, lost, tn.held[i]})
		}}
	}
	tour(tn)
	rng := rand.New(rand.NewPCG(11, 11))
	for range 200 {
		spots := make([]byte, spotLen*rng.IntN(4))
		for i := range spots {
			spots[i] = byte(rng.Uint32())
		}
		f.Add(uint16(rng.Uint32()), spots)
	}
	f.Fuzz(func(t *testing.T, point uint16, spots []byte) {
		p := points[int(point)%len(points)]
		d := p.disk.Clone(rand.New(rand.NewPCG(uint64(point), 6)))
		files := stored(d, "")
		var slots [2]bool
		for ; len(spots) >= spotLen && len(files) > 0; spots = spots[spotLen:] {
			file := files[int(spots[0])%len(files)]
			off := int64(binary.LittleEndian.Uint32(spots[2:])) % file.Size
			spoil, reach := d.Corrupt, int64(wal.MetaSlotLen)
			if spots[1]&1 == 1 {
				spoil, reach = d.Garble, d.Sector
			}
			if file.Name == metaFile && off%wal.MetaStride < reach {
				slots[off/wal.MetaStride] = true
			}
			if err := spoil(file.Name, off); err != nil {
				t.Fatal(err)
			}
		}
		st, err := tn.probe(p.node, d)
		if err != nil {
			if !slots[0] || !slots[1] || !errors.Is(err, wal.ErrCorrupt) {
				t.Fatalf("n%d does not open: %v", p.node+1, err)
			}
			return
		}
		if err := p.held.kept(st); err != nil {
			t.Fatalf("n%d: %v", p.node+1, err)
		}
	})
}
