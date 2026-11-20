package replica

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/Demonisreal/dmn-ledger/internal/client"
	"github.com/Demonisreal/dmn-ledger/internal/disk"
	"github.com/Demonisreal/dmn-ledger/internal/raft"
	"github.com/Demonisreal/dmn-ledger/internal/state"
	"github.com/Demonisreal/dmn-ledger/internal/wal"
)

var node2 = wal.Identity{Cluster: cluster, Node: 2, Nodes: 3}

func metaBytes(t *testing.T, d *disk.Sim) []byte {
	t.Helper()
	f, err := d.Open(metaFile)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	raw := make([]byte, wal.MetaStride+wal.MetaSlotLen)
	if _, err := f.ReadAt(raw, 0); err != nil {
		t.Fatal(err)
	}
	return raw
}

func initialised(t *testing.T, seed uint64, id wal.Identity) *disk.Sim {
	t.Helper()
	d := disk.NewSim(rand.New(rand.NewPCG(seed, 9)))
	if err := Init(d, "/n", id); err != nil {
		t.Fatal(err)
	}
	return d
}

func TestOpenRefusesADirectoryWithoutMeta(t *testing.T) {
	tn := newTestNet(t, 3, nil)
	tn.elect(0)
	c := tn.register(0, 1)
	ok(t, c.do(client.CreateAccounts, accounts(1, 2)))
	tn.tick(tn.all()...)
	tn.stop(1)
	used := tn.disks[1]
	if err := errors.Join(used.Remove(metaFile), used.SyncDir("/n")); err != nil {
		t.Fatal(err)
	}
	if len(stored(used, ".log")) == 0 {
		t.Fatal("n2 wrote no segment")
	}
	missing := disk.NewSim(rand.New(rand.NewPCG(1, 9)))
	empty := disk.NewSim(rand.New(rand.NewPCG(2, 9)))
	if err := empty.MkdirAll("/n"); err != nil {
		t.Fatal(err)
	}
	tn.cfgs[1].Create = false
	for _, tc := range []struct {
		name string
		d    *disk.Sim
	}{
		{"missing", missing},
		{"empty", empty},
		{"with wal and snap", used},
	} {
		before := tc.d.Durable()
		if _, err := tn.probe(1, tc.d); !errors.Is(err, wal.ErrNoMeta) {
			t.Errorf("%s: open = %v, want wal.ErrNoMeta", tc.name, err)
		}
		if after := tc.d.Durable(); !slices.Equal(after, before) {
			t.Errorf("%s: durable files %v, before the start %v", tc.name, after, before)
		}
	}
	if _, err := missing.List("/n"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("a refused start made the missing directory: %v", err)
	}
}

func TestOpenRefusesTheDirectoryOfAnotherNode(t *testing.T) {
	for _, tc := range []struct {
		name string
		tune func(*Config)
		own  string
	}{
		{"another node", func(cfg *Config) { cfg.ID = 3 }, "node 3 of 3 in cluster 7"},
		{"another number of nodes", func(cfg *Config) { cfg.Peers = []uint64{1, 2, 3, 4, 5} }, "node 2 of 5 in cluster 7"},
		{"another cluster", func(cfg *Config) { cfg.Cluster = 9 }, "node 2 of 3 in cluster 9"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tn := stoppedTestNet(t, 3, nil)
			tn.disks[1] = initialised(t, 1, node2)
			tn.cfgs[1].Create = false
			before := metaBytes(t, tn.disks[1])
			cfg := tn.cfgs[1]
			tc.tune(&tn.cfgs[1])
			_, err := tn.probe(1, tn.disks[1])
			want := "replica: the directory belongs to another node or cluster: /n holds node 2 of 3 in cluster 7, not " + tc.own
			if !errors.Is(err, ErrIdentity) || err.Error() != want {
				t.Fatalf("open = %v, want %q", err, want)
			}
			for _, sub := range []string{"/n/wal", "/n/snap"} {
				if _, err := tn.disks[1].List(sub); !errors.Is(err, fs.ErrNotExist) {
					t.Fatalf("a refused start made %s: %v", sub, err)
				}
			}
			if !bytes.Equal(metaBytes(t, tn.disks[1]), before) {
				t.Fatal("a refused start changed meta")
			}
			tn.cfgs[1] = cfg
			if _, err := tn.probe(1, tn.disks[1]); err != nil {
				t.Fatalf("open as the owner after the refusal: %v", err)
			}
		})
	}
}

func TestOpenRefusesAForeignSlotNextToABrokenOne(t *testing.T) {
	tn := stoppedTestNet(t, 3, nil)
	tn.cfgs[1].Create = false
	d := initialised(t, 1, node2)
	foreign := metaBytes(t, initialised(t, 2, wal.Identity{Cluster: cluster, Node: 3, Nodes: 3}))
	f, err := d.Open(metaFile)
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.WriteAt(foreign[wal.MetaStride:], wal.MetaStride)
	if err := errors.Join(err, f.Sync(), f.Close()); err != nil {
		t.Fatal(err)
	}
	if _, err := tn.probe(1, d); !errors.Is(err, wal.ErrCorrupt) {
		t.Fatalf("two valid slots of two nodes: %v, want wal.ErrCorrupt", err)
	}
	if err := d.Corrupt(metaFile, 9); err != nil {
		t.Fatal(err)
	}
	_, err = tn.probe(1, d)
	want := "replica: the directory belongs to another node or cluster: /n holds node 3 of 3 in cluster 7, not node 2 of 3 in cluster 7"
	if !errors.Is(err, ErrIdentity) || err.Error() != want {
		t.Fatalf("own slot broken, the other one of n3: %v, want %q", err, want)
	}
}

func TestOpenWithCreateStillCompares(t *testing.T) {
	tn := stoppedTestNet(t, 3, nil)
	tn.disks[2] = initialised(t, 1, node2)
	before := metaBytes(t, tn.disks[2])
	if _, err := tn.probe(2, tn.disks[2]); !errors.Is(err, ErrIdentity) {
		t.Fatalf("n3 with Create on the directory of n2: %v, want ErrIdentity", err)
	}
	if !bytes.Equal(metaBytes(t, tn.disks[2]), before) {
		t.Fatal("a refused start with Create changed meta")
	}
}

type loss struct {
	tn          *testNet
	index, term uint64
	halted      error
}

func lostDisk(t *testing.T) *loss {
	t.Helper()
	tn := newTestNet(t, 3, nil)
	l := &loss{tn: tn}
	tn.halt = func(i int, err error) {
		l.halted = fmt.Errorf("n%d: %w", i+1, err)
	}
	tn.elect(0)
	c := tn.register(0, 7)
	ok(t, c.do(client.CreateAccounts, accounts(1, 2)))
	for range 20 {
		tn.tick(tn.all()...)
	}
	tn.cut[3] = true
	ok(t, c.do(client.CreateTransfers, transfer(10, 1, 2, 500)))
	l.index = tn.nodes[0].Status().Commit
	var err error
	if l.term, err = tn.nodes[0].EntryTerm(l.index); err != nil {
		t.Fatal(err)
	}
	tn.stop(1)
	tn.disks[1] = disk.NewSim(rand.New(rand.NewPCG(99, 9)))
	tn.cfgs[1].Create = false
	return l
}

func TestNodeOnAnEmptyDirectoryIsRefusedAndTheEntryStays(t *testing.T) {
	l := lostDisk(t)
	tn := l.tn
	if n, err := Open(tn.config(1)); err == nil {
		tn.nodes[1] = n
	}
	tn.cut[1] = true
	tn.cut[3] = false
	for range 400 {
		lead := -1
		for _, i := range []int{1, 2} {
			if n := tn.nodes[i]; n != nil && n.Status().Role == raft.Leader && n.Status().Commit >= n.Status().Last {
				lead = i
			}
		}
		if lead >= 0 {
			r := ok(t, tn.register(lead, 8).do(client.LookupTransfers, ids(10)))
			if len(r.Body) != state.TransferSize {
				t.Errorf("n%d leads term %d without n1, and the lookup of transfer 10 returns %d bytes", lead+1, tn.nodes[lead].Status().Term, len(r.Body))
			}
			break
		}
		tn.tick(1, 2)
	}

	tn.cut[1] = false
	for range 100 {
		tn.tick(tn.all()...)
	}
	if l.halted != nil {
		t.Fatal(l.halted)
	}
	if now, err := tn.nodes[0].EntryTerm(l.index); err != nil || now != l.term {
		t.Fatalf("entry %d of n1 has term %d (%v), it was committed in term %d", l.index, now, err, l.term)
	}
}

func TestNodeInitialisedAgainLosesACommittedEntry(t *testing.T) {
	l := lostDisk(t)
	tn := l.tn
	if err := Init(tn.disks[1], "/n", node2); err != nil {
		t.Fatal(err)
	}
	tn.open(1)
	if err := tn.held[1].kept(tn.nodes[1].Status()); err == nil {
		t.Fatal("the book of n2 holds on a directory made again")
	}
	tn.cut[1] = true
	tn.cut[3] = false
	if lead := tn.choose(1, 2); lead != 2 {
		t.Fatalf("n%d leads without n1, want n3", lead+1)
	}
	if r := ok(t, tn.register(2, 8).do(client.LookupTransfers, ids(10))); len(r.Body) != 0 {
		t.Fatalf("n3 finds transfer 10 (%d bytes): the limit this test holds has moved", len(r.Body))
	}
	tn.cut[1] = false
	for range 100 {
		tn.tick(tn.all()...)
	}
	if !errors.Is(l.halted, raft.ErrSafety) {
		t.Fatalf("n1 after it hears n3: %v, want raft.ErrSafety", l.halted)
	}
}
