package check

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Demonisreal/dmn-ledger/internal/disk"
	"github.com/Demonisreal/dmn-ledger/internal/raft"
	"github.com/Demonisreal/dmn-ledger/internal/replica"
	"github.com/Demonisreal/dmn-ledger/internal/sim"
	"github.com/Demonisreal/dmn-ledger/internal/wal"
)

var errHalt = errors.New("halt")

func halted(t *testing.T, seed uint64, at func(c *sim.Cluster) uint64) (*watch, uint64) {
	t.Helper()
	var id uint64
	c, err := sim.NewCluster(sim.ClusterConfig{Seed: seed, Profile: sim.ProfileRot, Nodes: 3, Check: func(c *sim.Cluster) error {
		if id = at(c); id != 0 {
			return errHalt
		}
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Run(); !errors.Is(err, errHalt) {
		t.Fatalf("seed %d never got there: %v", seed, err)
	}
	w := newWatch(Config{Seed: seed, Profile: sim.ProfileRot})
	w.sample = 1
	w.c = c
	w.terms = make([]uint64, c.Nodes())
	return w, id
}

func repairing(c *sim.Cluster) uint64 {
	var sick uint64
	for id := uint64(1); id <= c.Nodes(); id++ {
		n := c.Node(id)
		switch {
		case n == nil:
			return 0
		case n.Status().Repair:
			sick = id
		}
	}
	return sick
}

func damagedAndDown(c *sim.Cluster) uint64 {
	for id := uint64(1); id <= c.Nodes(); id++ {
		if c.Node(id) == nil && c.World().Rotted(id) {
			return id
		}
	}
	return 0
}

func downUnharmed(c *sim.Cluster) uint64 {
	for id := uint64(1); id <= c.Nodes(); id++ {
		if c.Node(id) == nil && !c.World().Rotted(id) && c.World().Now() > c.World().Quiet()/4 {
			return id
		}
	}
	return 0
}

func unharmed(t *testing.T, w *watch) uint64 {
	t.Helper()
	for id := uint64(1); id <= w.c.Nodes(); id++ {
		if w.c.Node(id) != nil && !w.c.World().Rotted(id) {
			return id
		}
	}
	t.Fatal("no running node with an undamaged disk")
	return 0
}

func spoilLogTail(t *testing.T, d *disk.Sim) {
	t.Helper()
	var tail disk.DurableFile
	for _, f := range d.Durable() {
		if strings.HasPrefix(filepath.Base(f.Name), wal.SegmentPrefix) && f.Size > 0 {
			tail = f
		}
	}
	if tail.Name == "" {
		t.Fatal("no log segment on the disk")
	}
	if err := d.Corrupt(tail.Name, tail.Size-1); err != nil {
		t.Fatal(err)
	}
}

func spoilBothSlots(t *testing.T, d *disk.Sim) {
	t.Helper()
	for slot := range int64(2) {
		if err := d.Corrupt(filepath.Join("/ledger", wal.MetaName), slot*wal.MetaStride+3); err != nil {
			t.Fatal(err)
		}
	}
}

func syncDir(t *testing.T, w *watch, id uint64) {
	t.Helper()
	fsys := &watchedFS{FS: w.c.World().Disk(id), w: w, id: id}
	if err := fsys.SyncDir("/ledger"); err != nil {
		t.Fatal(err)
	}
}

func TestNodeInRepairSaysNothingOfAVoter(t *testing.T) {
	cases := []struct {
		m      raft.Message
		voters bool
	}{
		{raft.Message{Type: raft.MsgPreVote}, true},
		{raft.Message{Type: raft.MsgVote}, true},
		{raft.Message{Type: raft.MsgPreVoteResp}, true},
		{raft.Message{Type: raft.MsgVoteResp}, true},
		{raft.Message{Type: raft.MsgApp}, true},
		{raft.Message{Type: raft.MsgSnap}, true},
		{raft.Message{Type: raft.MsgPreVoteResp, Reject: true}, false},
		{raft.Message{Type: raft.MsgVoteResp, Reject: true}, false},
		{raft.Message{Type: raft.MsgAppResp, Index: 1}, false},
		{raft.Message{Type: raft.MsgAppResp, Reject: true}, false},
		{raft.Message{Type: raft.MsgRepairProbe}, false},
		{raft.Message{Type: raft.MsgRepairInfo}, false},
	}
	for _, tc := range cases {
		name := tc.m.Type.String()
		if tc.m.Reject {
			name += " reject"
		}
		t.Run(name, func(t *testing.T) {
			w, sick := halted(t, 3, repairing)
			m := tc.m
			m.To, m.Term = sick%3+1, w.c.Node(sick).Status().Term
			reason := ""
			if tc.voters {
				reason = "while in repair"
			}
			wantViolation(t, w.said(sick, []raft.Message{m}), "S10", reason)

			w, _ = halted(t, 3, repairing)
			if err := w.said(unharmed(t, w), []raft.Message{m}); err != nil {
				t.Fatalf("a node that is not in repair: %v", err)
			}
		})
	}
}

func TestFailedTellsDamageFromOtherStops(t *testing.T) {
	corrupt := fmt.Errorf("replica: %w", wal.ErrCorrupt)
	other := errors.New("replica: snapshot file 7/1 holds 6/1")

	w := &watch{cfg: Config{Seed: 3, Profile: sim.ProfileBeyond}}
	wantViolation(t, w.failed(1, corrupt), "F1", "without a disk error")

	w, sick := halted(t, 3, repairing)
	wantViolation(t, w.failed(sick, corrupt), "F1", "")
	wantViolation(t, w.failed(sick, other), "F1", "without a disk error")
	wantViolation(t, w.failed(unharmed(t, w), corrupt), "F1", "without a disk error")
	w.cfg.Profile = sim.ProfileBeyond
	wantViolation(t, w.failed(unharmed(t, w), corrupt), "F1", "without a disk error")

	w, down := halted(t, 3, damagedAndDown)
	wantViolation(t, w.failed(down, corrupt), "F1", "does not start on its damaged disk")
	w.cfg.Profile = sim.ProfileBeyond
	wantViolation(t, w.failed(down, corrupt), "F1", "")
	wantViolation(t, w.failed(down, other), "F1", "without a disk error")

	w, down = halted(t, 2, downUnharmed)
	w.cfg.Profile = sim.ProfileBeyond
	wantViolation(t, w.failed(down, corrupt), "F1", "without a disk error")
}

type unsyncedLog struct {
	disk.FS
}

func (u unsyncedLog) Create(name string) (disk.File, error) {
	f, err := u.FS.Create(name)
	if err != nil || filepath.Base(filepath.Dir(name)) != "wal" {
		return f, err
	}
	return unsyncedFile{f}, nil
}

func (u unsyncedLog) Open(name string) (disk.File, error) {
	f, err := u.FS.Open(name)
	if err != nil || filepath.Base(filepath.Dir(name)) != "wal" {
		return f, err
	}
	return unsyncedFile{f}, nil
}

func TestRunningNodeIsInRepairOnlyAfterDamage(t *testing.T) {
	w, _ := halted(t, 3, repairing)
	if err := w.check(w.c); err != nil {
		t.Fatalf("in repair on a disk the plan damaged: %v", err)
	}

	w = newWatch(Config{Seed: 0, Profile: sim.ProfileCrash})
	c, err := sim.NewCluster(sim.ClusterConfig{
		Seed:    0,
		Profile: sim.ProfileCrash,
		Nodes:   3,
		Disk:    func(_ uint64, fsys disk.FS) disk.FS { return unsyncedLog{fsys} },
		Check:   w.check,
	})
	if err != nil {
		t.Fatal(err)
	}
	w.c = c
	w.terms = make([]uint64, c.Nodes())
	wantViolation(t, c.Run(), "F2", "in repair, the plan never damaged its disk")
}

func TestPowerLossLeavesRepairOnlyAfterDamage(t *testing.T) {
	ack := func(st replica.Status, more uint64) raft.Message {
		return raft.Message{Type: raft.MsgAppResp, To: st.Lead, Index: st.Last + more, Term: st.Term}
	}
	t.Run("undamaged disk", func(t *testing.T) {
		for _, tc := range []struct {
			profile sim.Profile
			reason  string
		}{
			{sim.ProfileCalm, "leaves the node in repair, the plan never damaged its disk"},
			{sim.ProfileRot, "leaves the node in repair, the plan never damaged its disk"},
			{sim.ProfileBeyond, ""},
		} {
			w, _, follower := calmWatch(t)
			w.cfg.Profile = tc.profile
			spoilLogTail(t, w.c.World().Disk(follower.ID))
			wantViolation(t, w.keeps(follower.ID, ack(follower, 0)), "F2", tc.reason)
			if err := w.said(follower.ID, []raft.Message{ack(follower, 0)}); err != nil {
				t.Fatal(err)
			}
			syncDir(t, w, follower.ID)
			wantViolation(t, w.stuck, "F2", tc.reason)
		}
	})
	t.Run("damaged disk", func(t *testing.T) {
		w, sick := halted(t, 3, repairing)
		st := w.c.Node(sick).Status()
		wantViolation(t, w.keeps(sick, ack(st, 3)), "S5", "")
		if err := w.said(sick, []raft.Message{ack(st, 3)}); err != nil {
			t.Fatal(err)
		}
		syncDir(t, w, sick)
		wantViolation(t, w.stuck, "S5", "")

		well := unharmed(t, w)
		st = w.c.Node(well).Status()
		wantViolation(t, w.keeps(well, ack(st, 3)), "S5", "holds no repair flag")
		if err := w.said(well, []raft.Message{ack(st, 3)}); err != nil {
			t.Fatal(err)
		}
		syncDir(t, w, well)
		wantViolation(t, w.stuck, "S5", "and no repair flag")
	})
	t.Run("repair keeps term and vote", func(t *testing.T) {
		w, sick := halted(t, 3, repairing)
		st := w.c.Node(sick).Status()
		wantViolation(t, w.keeps(sick, raft.Message{Type: raft.MsgAppResp, Index: 1, Term: st.Term + 1}), "S5", "holds term")
		w.receipts[sick] = receipt{vote: sick, voteTerm: st.Term + 1}
		syncDir(t, w, sick)
		wantViolation(t, w.stuck, "S5", "promised in term")
	})
}

func TestUnreadableDiskIsAllowedOnlyBeyondTheModel(t *testing.T) {
	for _, tc := range []struct {
		name    string
		profile sim.Profile
		node    func(t *testing.T, w *watch, sick uint64) uint64
		reason  string
	}{
		{"rot", sim.ProfileRot, func(_ *testing.T, _ *watch, sick uint64) uint64 { return sick }, "does not open"},
		{"beyond", sim.ProfileBeyond, func(_ *testing.T, _ *watch, sick uint64) uint64 { return sick }, ""},
		{"beyond, undamaged disk", sim.ProfileBeyond, func(t *testing.T, w *watch, _ uint64) uint64 { return unharmed(t, w) }, "does not open"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w, sick := halted(t, 3, repairing)
			w.cfg.Profile = tc.profile
			id := tc.node(t, w, sick)
			st := w.c.Node(id).Status()
			spoilBothSlots(t, w.c.World().Disk(id))
			wantViolation(t, w.keeps(id, raft.Message{Type: raft.MsgAppResp, Index: 1, Term: st.Term}), "S5", tc.reason)
			syncDir(t, w, id)
			wantViolation(t, w.stuck, "S5", tc.reason)
		})
	}
}

func TestBookIsCheckedAtTheStartAndWhenRepairEnds(t *testing.T) {
	cases := []struct {
		name   string
		book   func(st replica.Status) receipt
		reason string
	}{
		{"all kept", func(st replica.Status) receipt {
			return receipt{vote: st.Vote, voteTerm: st.Term, term: st.Term, reach: st.Last}
		}, ""},
		{"acknowledged past the end", func(st replica.Status) receipt {
			return receipt{term: st.Term, reach: st.Last + 1}
		}, "leaves a log that ends at"},
		{"acknowledged past the end in an older term", func(st replica.Status) receipt {
			return receipt{term: st.Term - 1, reach: st.Last + 1}
		}, "leaves a log that ends at"},
		{"acknowledged past the end before a later leader sent entries", func(st replica.Status) receipt {
			return receipt{term: st.Term - 1, reach: st.Last + 1, fed: st.Term}
		}, ""},
		{"another vote", func(st replica.Status) receipt {
			return receipt{vote: st.Vote%3 + 1, voteTerm: st.Term}
		}, "voted for"},
		{"a later term", func(st replica.Status) receipt {
			return receipt{term: st.Term + 1}
		}, "promised in term"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w, _ := finished(t)
			w.over, w.terms = false, make([]uint64, 3)
			n := w.c.Node(2)
			w.receipts[2] = tc.book(n.Status())
			wantViolation(t, w.check(w.c), "S5", strings.ReplaceAll(tc.reason, "leaves", "the restart leaves"))

			w = checked(t)
			n = w.c.Node(2)
			w.receipts[2] = tc.book(n.Status())
			if err := w.check(w.c); err != nil {
				t.Fatalf("a node that neither started nor left repair: %v", err)
			}
			w.repairs[n] = true
			wantViolation(t, w.check(w.c), "S5", strings.ReplaceAll(tc.reason, "leaves", "the end of the repair leaves"))
			if err := w.check(w.c); err != nil {
				t.Fatalf("the check after the repair ended: %v", err)
			}
		})
	}
	t.Run("in repair", func(t *testing.T) {
		w, sick := halted(t, 3, repairing)
		st := w.c.Node(sick).Status()
		w.receipts[sick] = receipt{term: st.Term, reach: st.Last + 1}
		if err := w.check(w.c); err != nil {
			t.Fatalf("a start in repair with a shorter log: %v", err)
		}
		w.receipts[sick] = receipt{vote: st.Vote%3 + 1, voteTerm: st.Term}
		if err := w.check(w.c); err != nil {
			t.Fatalf("a node that stays in repair: %v", err)
		}
		w, sick = halted(t, 3, repairing)
		w.receipts[sick] = receipt{vote: st.Vote%3 + 1, voteTerm: st.Term}
		wantViolation(t, w.check(w.c), "S5", "voted for")
	})
}

func TestRepairEndIsSeenInARun(t *testing.T) {
	w := newWatch(Config{Seed: 3, Profile: sim.ProfileRot})
	forged := 0
	c, err := sim.NewCluster(sim.ClusterConfig{Seed: 3, Profile: sim.ProfileRot, Nodes: 3, Check: func(c *sim.Cluster) error {
		for id := uint64(1); id <= c.Nodes(); id++ {
			if n := c.Node(id); n != nil && n.Status().Repair {
				st := n.Status()
				w.receipts[id] = receipt{term: st.Term, reach: st.Last + 1000}
				forged++
			}
		}
		return w.check(c)
	}})
	if err != nil {
		t.Fatal(err)
	}
	w.c = c
	w.terms = make([]uint64, c.Nodes())
	err = c.Run()
	if forged == 0 {
		t.Fatal("no node was in repair")
	}
	wantViolation(t, err, "S5", "the end of the repair leaves a log that ends at")
}

type tallyFS struct {
	disk.FS
	sync func()
}

type tallyFile struct {
	disk.File
	fs *tallyFS
}

func (f *tallyFS) Create(name string) (disk.File, error) {
	file, err := f.FS.Create(name)
	if err != nil {
		return nil, err
	}
	return tallyFile{file, f}, nil
}

func (f *tallyFS) Open(name string) (disk.File, error) {
	file, err := f.FS.Open(name)
	if err != nil {
		return nil, err
	}
	return tallyFile{file, f}, nil
}

func (f tallyFile) Sync() error {
	defer f.fs.sync()
	return f.File.Sync()
}

func TestSyncsDuringTheStartAreSampled(t *testing.T) {
	w := newWatch(Config{Seed: 1, Profile: sim.ProfileCrash})
	w.sample = 1
	watched := w.disk()
	before, during := 0, 0
	_, err := w.run(func(id uint64, fsys disk.FS) disk.FS {
		return &tallyFS{FS: watched(id, fsys), sync: func() {
			if w.c != nil && w.c.Node(id) == nil && w.syncs > before {
				during++
			}
			before = w.syncs
		}}
	})
	if err != nil {
		t.Fatal(err)
	}
	if during == 0 {
		t.Fatal("no sync of a starting node opened a clone")
	}

	w, down := halted(t, 2, downUnharmed)
	d := w.c.World().Disk(down).Clone(w.rng)
	d.Crash()
	n, err := w.c.Open(down, d)
	if err != nil {
		t.Fatal(err)
	}
	st := n.Status()
	if err := n.Close(); err != nil {
		t.Fatal(err)
	}
	w.receipts[down] = receipt{term: st.Term, reach: st.Last + 1}
	syncDir(t, w, down)
	wantViolation(t, w.stuck, "S5", "a power loss during the start leaves a log that ends at")
}

func TestRunEndsWithoutRepair(t *testing.T) {
	w, sick := halted(t, 3, repairing)
	w.over = true
	err := w.end()
	wantViolation(t, err, "L4", "still in repair at the end")
	if v, _ := errors.AsType[*Violation](err); v.Node != sick {
		t.Fatalf("L4 names n%d, n%d is in repair", v.Node, sick)
	}
}

func TestBeyondSkipsTheEnd(t *testing.T) {
	const seed = 8
	w := newWatch(Config{Seed: seed, Profile: sim.ProfileBeyond})
	if _, err := w.run(w.disk()); err != nil {
		t.Fatal(err)
	}
	if w.over {
		t.Fatal("the end was checked")
	}
	w.over = true
	if err := w.end(); err == nil {
		t.Fatalf("seed %d ends like a run inside the fault model, the test needs another seed", seed)
	}
}
