package replica

import (
	"bytes"
	"errors"
	"math/rand/v2"
	"testing"

	"github.com/Demonisreal/dmn-ledger/internal/audit"
	"github.com/Demonisreal/dmn-ledger/internal/client"
	"github.com/Demonisreal/dmn-ledger/internal/raft"
)

func lagging(t *testing.T) (*testNet, uint64, envelope) {
	t.Helper()
	tn := newTestNet(t, 3, func(cfg *Config) { cfg.SnapshotEvery, cfg.SegmentSize = 4, 512 })
	tn.elect(0)
	c := tn.register(0, 1)
	for i := range uint64(6) {
		ok(t, c.do(client.CreateAccounts, accounts(i+1)))
	}
	tn.tick(tn.all()...)
	acked := tn.nodes[2].Status().Last
	tn.cut[3] = true
	for i := range uint64(30) {
		ok(t, c.do(client.CreateAccounts, accounts(i+100)))
	}
	for range 200 {
		for _, i := range []int{0, 1} {
			tn.clocks[i] += tickNs
			tn.nodes[i].Tick()
			tn.run(i)
		}
		for _, e := range tn.queue {
			if e.m.To == 3 && e.m.Type == raft.MsgSnap {
				tn.deliver()
				return tn, acked, e
			}
		}
		tn.deliver()
	}
	t.Fatal("the leader sent no snapshot to n3")
	return nil, 0, envelope{}
}

func TestCrashDuringSnapshotInstallKeepsWhatWasAcknowledged(t *testing.T) {
	tn, acked, e := lagging(t)
	term := tn.nodes[2].Status().Term
	tn.crash(2)
	base := tn.disks[2]
	for k := uint64(1); k <= 200; k++ {
		tn.disks[2] = base.Clone(rand.New(rand.NewPCG(k, 1)))
		tn.open(2)
		tn.nodes[2].Receive(e.m, bytes.NewReader(e.snap))
		tn.disks[2].FailAfter(k)
		_, err := tn.nodes[2].Process()
		tn.disks[2].FailAfter(0)
		tn.crash(2)
		tn.open(2)
		if st := tn.nodes[2].Status(); st.Term < term || st.Last < acked {
			t.Fatalf("crash at access %d of the install: term %d, log ends at %d, acknowledged %d in term %d", k, st.Term, st.Last, acked, term)
		}
		if err == nil {
			if k < 5 {
				t.Fatalf("install finished within %d disk accesses", k)
			}
			return
		}
	}
	t.Fatal("the install still failed after 200 disk accesses")
}

func TestLogReachesBackToTheOlderSnapshot(t *testing.T) {
	tn := newTestNet(t, 1, func(cfg *Config) { cfg.SnapshotEvery, cfg.SegmentSize = 4, 512 })
	tn.elect(0)
	c := tn.register(0, 1)
	var older, newest uint64
	compacted := false
	for i := range uint64(60) {
		ok(t, c.do(client.CreateAccounts, accounts(i+1)))
		n := tn.nodes[0]
		if n.snap.Index != newest {
			older, newest = newest, n.snap.Index
		}
		first, _ := n.log.FirstIndex()
		if first > older+1 {
			t.Fatalf("log starts at %d, the older snapshot is at %d", first, older)
		}
		compacted = compacted || first > 1
	}
	if !compacted {
		t.Fatal("the log was never compacted")
	}
}

func TestSnapshotHeadMustMatchRaft(t *testing.T) {
	tn, _, e := lagging(t)
	n := tn.nodes[2]
	m := e.m
	m.Index++
	n.Receive(m, bytes.NewReader(e.snap))
	if _, err := n.Process(); !errors.Is(err, errBadSnapshot) {
		t.Fatalf("snapshot %d/%d for raft's %d/%d: %v, want errBadSnapshot", e.m.Index, e.m.LogTerm, m.Index, m.LogTerm, err)
	}
}

func TestSnapshotHoldsMaxSessions(t *testing.T) {
	tn := newTestNet(t, 1, func(cfg *Config) { cfg.SnapshotEvery = 4 })
	tn.elect(0)
	c := tn.register(0, 1)
	for i := uint64(1); tn.nodes[0].snap.Index == 0; i++ {
		ok(t, c.do(client.CreateAccounts, accounts(i)))
	}
	tn.stop(0)
	tn.cfgs[0].MaxSessions = 17
	if _, err := tn.probe(0, tn.disks[0]); !errors.Is(err, ErrMaxSessions) {
		t.Fatalf("start with MaxSessions 17 over a snapshot of 16: %v, want ErrMaxSessions", err)
	}
	cfg := tn.config(0)
	if err := Recount(cfg, audit.Head{}); !errors.Is(err, ErrMaxSessions) {
		t.Fatalf("recount with MaxSessions 17: %v, want ErrMaxSessions", err)
	}
	tn.cfgs[0].MaxSessions = 16
	tn.open(0)
	tn.elect(0)
	ok(t, c.do(client.CreateAccounts, accounts(100)))
}

func TestInstalledSnapshotHoldsMaxSessions(t *testing.T) {
	tn, _, e := lagging(t)
	n := tn.nodes[2]
	n.cfg.MaxSessions++
	n.Receive(e.m, bytes.NewReader(e.snap))
	if _, err := n.Process(); !errors.Is(err, ErrMaxSessions) {
		t.Fatalf("install of a snapshot with MaxSessions 16 on a node with %d: %v, want ErrMaxSessions", n.cfg.MaxSessions, err)
	}
}

func FuzzLoadSnapshot(f *testing.F) {
	tn := newTestNet(f, 1, func(cfg *Config) { cfg.SnapshotEvery = 1 << 40 })
	tn.elect(0)
	for i := range uint64(3) {
		c := tn.register(0, i+1)
		ok(f, c.do(client.CreateAccounts, accounts(10*i+1, 10*i+2)))
		ok(f, c.do(client.CreateTransfers, transfer(100+i, 10*i+1, 10*i+2, 5)))
	}
	var snap bytes.Buffer
	if err := tn.nodes[0].writeSnapshot(&snap); err != nil {
		f.Fatal(err)
	}
	f.Add(snap.Bytes())
	f.Add(snap.Bytes()[:snapHeadLen+sessionLen+3])
	f.Fuzz(func(t *testing.T, b []byte) {
		n := &Node{cfg: tn.cfgs[0]}
		if err := n.load(bytes.NewReader(b)); err != nil {
			return
		}
		if err := n.CheckInvariants(); err != nil {
			t.Fatal(err)
		}
		var again bytes.Buffer
		if err := n.writeSnapshot(&again); err != nil {
			t.Fatal(err)
		}
		m := &Node{cfg: tn.cfgs[0]}
		if err := m.load(&again); err != nil {
			t.Fatalf("a written snapshot does not load: %v", err)
		}
		if m.Digest() != n.Digest() {
			t.Fatal("digest changed on the second load")
		}
	})
}
