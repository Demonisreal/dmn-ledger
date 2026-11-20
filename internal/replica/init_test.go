package replica

import (
	"errors"
	"io/fs"
	"math/rand/v2"
	"slices"
	"syscall"
	"testing"

	"github.com/Demonisreal/dmn-ledger/internal/disk"
	"github.com/Demonisreal/dmn-ledger/internal/wal"
)

func TestInitRefusesADirectoryANodeHasUsed(t *testing.T) {
	for _, tc := range []struct {
		name  string
		left  func(*disk.Sim) error
		is    error
		words string
	}{
		{"meta", func(d *disk.Sim) error { return Init(d, "/n", node2) }, ErrInitialised, "replica: the directory is initialised: /n"},
		{"wal", func(d *disk.Sim) error { return d.MkdirAll("/n/wal") }, ErrNotEmpty, "replica: the directory is not empty: /n holds wal"},
		{"snap", func(d *disk.Sim) error { return d.MkdirAll("/n/snap") }, ErrNotEmpty, "replica: the directory is not empty: /n holds snap"},
		{"a file of its own", func(d *disk.Sim) error { return leave(d, "/n/notes") }, ErrNotEmpty, "replica: the directory is not empty: /n holds notes"},
		{"lock", func(d *disk.Sim) error { return leave(d, "/n/"+disk.LockName) }, nil, ""},
		{"meta.tmp", func(d *disk.Sim) error { return leave(d, metaFile+".tmp") }, nil, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := disk.NewSim(rand.New(rand.NewPCG(1, 9)))
			if err := errors.Join(d.MkdirAll("/n"), tc.left(d)); err != nil {
				t.Fatal(err)
			}
			before := d.Durable()
			err := Init(d, "/n", node2)
			if tc.is == nil {
				if err != nil {
					t.Fatalf("init next to a leftover %s: %v", tc.name, err)
				}
				return
			}
			if !errors.Is(err, tc.is) || err.Error() != tc.words {
				t.Fatalf("init = %v, want %q", err, tc.words)
			}
			if after := d.Durable(); !slices.Equal(after, before) {
				t.Fatalf("a refused init left %v, before it %v", after, before)
			}
		})
	}
}

func TestInitDoesNotTakeADirectoryItCannotRead(t *testing.T) {
	for _, sub := range []string{"wal", "snap"} {
		t.Run(sub, func(t *testing.T) {
			d := disk.NewSim(rand.New(rand.NewPCG(1, 9)))
			if err := d.MkdirAll("/n/" + sub); err != nil {
				t.Fatal(err)
			}
			d.FailAfter(2)
			if err := Init(d, "/n", node2); !errors.Is(err, syscall.EIO) {
				t.Fatalf("init with /n/%s unreadable: %v, want EIO", sub, err)
			}
			d.FailAfter(0)
			if names, err := d.List("/n"); err != nil || len(names) != 0 {
				t.Fatalf("init with /n/%s unreadable left %v, %v", sub, names, err)
			}
		})
	}
}

type blindList struct {
	*disk.Sim
}

func (blindList) List(dir string) ([]string, error) {
	return nil, fs.ErrNotExist
}

func TestInitThatLostTheRaceDoesNotTakeTheDirectory(t *testing.T) {
	d := disk.NewSim(rand.New(rand.NewPCG(1, 9)))
	if err := Init(d, "/n", node2); err != nil {
		t.Fatal(err)
	}
	before := d.Durable()
	node3 := wal.Identity{Cluster: cluster, Node: 3, Nodes: 3}
	err := Init(blindList{d}, "/n", node3)
	if !errors.Is(err, ErrInitialised) || err.Error() != "replica: the directory is initialised: /n" {
		t.Fatalf("second init after the check = %v, want ErrInitialised", err)
	}
	if after := d.Durable(); !slices.Equal(after, before) {
		t.Fatalf("the second init left %v, before it %v", after, before)
	}
	m, _, err := wal.OpenMeta(d, "/n")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = m.Close() }()
	if m.Identity() != node2 {
		t.Fatalf("the directory holds %s, want %s", m.Identity(), node2)
	}
}

func leave(d *disk.Sim, name string) error {
	f, err := d.Create(name)
	if err != nil {
		return err
	}
	return errors.Join(f.Sync(), f.Close(), d.SyncDir("/n"))
}

func TestInitIsOnDiskWhenItReturns(t *testing.T) {
	tn := stoppedTestNet(t, 3, nil)
	tn.cfgs[1].Create = false
	tn.disks[1] = initialised(t, 1, node2)
	tn.disks[1].Crash()
	n, err := Open(tn.config(1))
	if err != nil {
		t.Fatalf("open after init and a power loss: %v", err)
	}
	defer func() { _ = n.Close() }()
	if st := n.Status(); st.Term != 0 || st.Vote != 0 || st.Last != 0 || st.Repair || n.meta.Sealed() != 0 {
		t.Fatalf("fresh directory opens with %+v, seal %d", st, n.meta.Sealed())
	}
}

func TestInitCrashAtEveryDiskAccess(t *testing.T) {
	tn := stoppedTestNet(t, 3, nil)
	tn.cfgs[1].Create = false
	open := func(d *disk.Sim) error {
		cfg := tn.config(1)
		cfg.FS = d
		n, err := Open(cfg)
		if err != nil {
			return err
		}
		return n.Close()
	}
	for _, journal := range []float64{0, 0.5, 1} {
		for _, shred := range []float64{0, 1} {
			accesses := 0
			for k := uint64(1); ; k++ {
				d := disk.NewSim(rand.New(rand.NewPCG(k, 3)))
				d.JournalRate, d.ShredRate = journal, shred
				d.FailAfter(k)
				if Init(d, "/n", node2) == nil {
					accesses = int(k) - 1
					break
				}
				d.FailAfter(0)
				d.Crash()
				err := open(d)
				switch {
				case errors.Is(err, wal.ErrNoMeta):
					if err := Init(d, "/n", node2); err != nil {
						t.Fatalf("journal %v shred %v access %d: init after a refused start: %v", journal, shred, k, err)
					}
				case err != nil:
					t.Fatalf("journal %v shred %v access %d: open after the power loss: %v", journal, shred, k, err)
				default:
					if err := Init(d, "/n", node2); !errors.Is(err, ErrInitialised) {
						t.Fatalf("journal %v shred %v access %d: init on a directory that opens: %v", journal, shred, k, err)
					}
				}
				if err := open(d); err != nil {
					t.Fatalf("journal %v shred %v access %d: open after the second init: %v", journal, shred, k, err)
				}
				d.Crash()
				if err := open(d); err != nil {
					t.Fatalf("journal %v shred %v access %d: open after one more power loss: %v", journal, shred, k, err)
				}
			}
			if accesses != 9 {
				t.Fatalf("journal %v shred %v: init makes %d disk accesses that can fail, want 9", journal, shred, accesses)
			}
		}
	}
}

func TestInitOnALockedDirectoryFails(t *testing.T) {
	d := disk.NewSim(rand.New(rand.NewPCG(1, 9)))
	if err := d.MkdirAll("/n"); err != nil {
		t.Fatal(err)
	}
	lock, err := d.Lock("/n")
	if err != nil {
		t.Fatal(err)
	}
	if err := Init(d, "/n", node2); !errors.Is(err, disk.ErrLocked) {
		t.Fatalf("init on a locked directory: %v, want disk.ErrLocked", err)
	}
	if names, err := d.List("/n"); err != nil || len(names) != 0 {
		t.Fatalf("a locked init left %v, %v", names, err)
	}
	_ = lock.Close()
	if err := Init(d, "/n", node2); err != nil {
		t.Fatalf("init after the lock is gone: %v", err)
	}
}
