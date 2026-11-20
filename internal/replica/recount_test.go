package replica

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"strings"
	"testing"

	"github.com/Demonisreal/dmn-ledger/internal/audit"
	"github.com/Demonisreal/dmn-ledger/internal/client"
	"github.com/Demonisreal/dmn-ledger/internal/disk"
	"github.com/Demonisreal/dmn-ledger/internal/raft"
	"github.com/Demonisreal/dmn-ledger/internal/state"
	"github.com/Demonisreal/dmn-ledger/internal/u128"
	"github.com/Demonisreal/dmn-ledger/internal/wal"
)

type counted struct {
	tn            *testNet
	c             *tester
	older, newest raft.Snapshot
	head          audit.Head
}

func countedNet(t *testing.T) *counted {
	t.Helper()
	tn := newTestNet(t, 3, small)
	tn.elect(0)
	c := tn.register(0, 1)
	ok(t, c.do(client.CreateAccounts, accounts(1, 2)))
	ct := &counted{tn: tn, c: c}
	n := tn.nodes[0]
	for i := uint64(10); ct.older.Index == 0 || n.Status().Last < ct.newest.Index+3; i++ {
		ok(t, c.do(client.CreateTransfers, transfer(i, 1, 2, i)))
		if n.snap != ct.newest {
			ct.older, ct.newest = ct.newest, n.snap
		}
	}
	for range 4 {
		tn.tick(tn.all()...)
	}
	ct.head = n.Status().Head
	for i, n := range tn.nodes {
		if h := n.Status().Head; h != ct.head {
			t.Fatalf("n%d at head %d, n1 at %d", i+1, h.Seq, ct.head.Seq)
		}
	}
	return ct
}

func (ct *counted) copy(i int) *disk.Sim {
	d := ct.tn.disks[i].Clone(rand.New(rand.NewPCG(uint64(i), 7)))
	d.Crash()
	return d
}

func (ct *counted) config(fsys disk.FS) Config {
	cfg := ct.tn.config(0)
	cfg.FS = fsys
	return cfg
}

func (ct *counted) recount(fsys disk.FS, want audit.Head) error {
	_, err := Recount(ct.config(fsys), want)
	return err
}

func (ct *counted) firstRun(t *testing.T, fsys disk.FS) *Node {
	t.Helper()
	cfg := ct.config(fsys)
	cfg.ID, cfg.Peers, cfg.SnapshotEvery = 1, []uint64{1}, math.MaxUint64
	cfg.Create, cfg.recount = false, true
	n, _, err := recount(cfg, ct.head, audit.Head{})
	if err != nil {
		t.Fatalf("first run: %v", err)
	}
	t.Cleanup(func() { _ = n.Close() })
	return n
}

func editEntry(t *testing.T, fsys disk.FS, index uint64, edit func(body []byte)) {
	t.Helper()
	w, err := wal.Open(fsys, "/n/wal", wal.Options{Nonces: rand.NewChaCha8([32]byte{9})})
	if err != nil {
		t.Fatal(err)
	}
	last, _ := w.LastIndex()
	ents, err := w.Entries(index, last+1, math.MaxInt64)
	if err != nil {
		t.Fatal(err)
	}
	for i := range ents {
		ents[i].Body = bytes.Clone(ents[i].Body)
	}
	edit(ents[0].Body)
	if err := errors.Join(w.TruncateAfter(index-1), w.Append(ents...), w.Sync(), w.Close()); err != nil {
		t.Fatal(err)
	}
}

func moreAmount(body []byte) {
	rec := body[requestHead : requestHead+state.TransferSize]
	tr := state.DecodeTransfer(rec)
	tr.Amount = tr.Amount.AddWrap(u128.From64(1))
	copy(rec, state.AppendTransfer(nil, tr))
}

func TestRecountMatchesTheLiveHead(t *testing.T) {
	ct := countedNet(t)
	for i := range ct.tn.nodes {
		d := ct.copy(i)
		if snaps := stored(d, ".snap"); len(snaps) != 2 {
			t.Fatalf("n%d holds %d snapshots, want two", i+1, len(snaps))
		}
		runs, err := Recount(ct.config(d.Clone(rand.New(rand.NewPCG(1, 1)))), ct.head)
		if err != nil {
			t.Fatalf("recount of n%d: %v", i+1, err)
		}
		if len(runs) != 2 || runs[0].Snapshot <= runs[1].Snapshot {
			t.Fatalf("recount of n%d: runs %+v, want one from each snapshot, the newest first", i+1, runs)
		}
		for _, run := range runs {
			if run.Links != ct.head.Seq || run.Committed != runs[0].Committed || run.Committed >= run.Links {
				t.Fatalf("recount of n%d: run %+v, the head is link %d", i+1, run, ct.head.Seq)
			}
		}
		ahead := audit.Head{Seq: ct.head.Seq + 1, Hash: ct.head.Hash}
		if err := ct.recount(d.Clone(rand.New(rand.NewPCG(1, 2))), ahead); !errors.Is(err, ErrBehind) {
			t.Fatalf("recount of n%d against a later head: %v, want ErrBehind", i+1, err)
		}
		other := ct.head
		other.Hash[0]++
		if err := ct.recount(d.Clone(rand.New(rand.NewPCG(1, 3))), other); !errors.Is(err, ErrOtherLog) {
			t.Fatalf("recount of n%d against another head after the snapshots: %v, want ErrOtherLog", i+1, err)
		}
		first, _ := ct.tn.nodes[0].sm.HeadAt(1)
		first.Hash[0]++
		if err := ct.recount(d, first); !errors.Is(err, ErrOtherHead) {
			t.Fatalf("recount of n%d against another head in a snapshot: %v, want ErrOtherHead", i+1, err)
		}
	}
}

func TestRecountFindsChangedLogEntry(t *testing.T) {
	ct := countedNet(t)
	for _, tc := range []struct {
		name   string
		index  uint64
		before bool
		want   error
	}{
		{"after the newest snapshot", ct.newest.Index + 1, false, ErrOtherLog},
		{"between the snapshots", ct.newest.Index, true, ErrOtherHead},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := ct.copy(0)
			editEntry(t, d, tc.index, func(body []byte) {
				if client.Op(body[32]) != client.CreateTransfers {
					t.Fatalf("entry %d holds %s", tc.index, client.Op(body[32]))
				}
				moreAmount(body)
			})
			if tc.before {
				if n := ct.firstRun(t, d.Clone(rand.New(rand.NewPCG(2, 1)))); n.snap != ct.newest {
					t.Fatalf("first run from snapshot %d, want %d", n.snap.Index, ct.newest.Index)
				}
			}
			err := ct.recount(d, ct.head)
			if !errors.Is(err, tc.want) {
				t.Fatalf("recount: %v, want %v", err, tc.want)
			}
			from := ct.newest.Index
			if tc.before {
				from = ct.older.Index
			}
			if !strings.Contains(err.Error(), fmt.Sprintf("from snapshot %d ", from)) {
				t.Fatalf("recount: %v, want the run from snapshot %d", err, from)
			}
		})
	}
}

func TestRecountFindsChangedSnapshot(t *testing.T) {
	ct := countedNet(t)
	d := ct.copy(0)
	var snap []byte
	err := wal.ReadSnapshot(d, "/n/snap", wal.SnapshotMeta(ct.newest), func(r io.Reader) error {
		var err error
		snap, err = io.ReadAll(r)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	rec := state.AppendTransfer(nil, ct.tn.nodes[0].sm.LookupTransfers([]u128.Uint128{u128.From64(10)}, nil)[0])
	at := bytes.Index(snap, rec)
	if at < 0 {
		t.Fatal("transfer 10 is not in the newest snapshot")
	}
	tr := state.DecodeTransfer(rec)
	tr.Code++
	copy(snap[at:], state.AppendTransfer(nil, tr))
	err = wal.SaveSnapshot(d, "/n/snap", wal.SnapshotMeta(ct.newest), func(w io.Writer) error {
		_, err := w.Write(snap)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	err = ct.recount(d, ct.head)
	if !errors.Is(err, state.ErrBadSnapshot) || !strings.Contains(err.Error(), "records give") {
		t.Fatalf("recount of a snapshot with another code: %v, want a bad snapshot whose records give another root", err)
	}
}

func TestRecountIgnoresTheUncommittedTail(t *testing.T) {
	ct := countedNet(t)
	tn := ct.tn
	tn.cut[2], tn.cut[3] = true, true
	tn.send(0, ct.c.request(client.CreateAccounts, accounts(50)))
	if r := tn.take(); len(r) != 0 {
		t.Fatalf("answered without a majority: %+v", r)
	}
	d := ct.copy(0)
	if err := ct.recount(d.Clone(rand.New(rand.NewPCG(3, 1))), ct.head); err != nil {
		t.Fatalf("recount with an uncommitted tail: %v", err)
	}
	if n := ct.firstRun(t, d); n.sm.Head().Seq <= ct.head.Seq {
		t.Fatalf("the copy holds %d links, the live head %d: no tail was counted", n.sm.Head().Seq, ct.head.Seq)
	}
}

func TestRecountOfADeposedLeaderIsNotAnotherHead(t *testing.T) {
	ct := countedNet(t)
	tn := ct.tn
	tn.cut[1] = true
	tn.send(0, ct.c.request(client.CreateAccounts, accounts(50)))
	if r := tn.take(); len(r) != 0 {
		t.Fatalf("answered without a majority: %+v", r)
	}
	d := ct.copy(0)

	lead := tn.choose(1, 2)
	c := tn.register(lead, 2)
	ok(t, c.do(client.CreateAccounts, accounts(60)))
	head := tn.nodes[lead].Status().Head
	if head.Seq != ct.head.Seq+1 {
		t.Fatalf("the cluster is at link %d, want %d", head.Seq, ct.head.Seq+1)
	}

	runs, err := Recount(ct.config(d.Clone(rand.New(rand.NewPCG(4, 1)))), ct.head)
	if err != nil {
		t.Fatalf("recount against the head before the split: %v", err)
	}
	if runs[0].Links != head.Seq {
		t.Fatalf("the copy of n1 holds %d links, want %d with the one never committed", runs[0].Links, head.Seq)
	}
	if err := ct.recount(d, head); !errors.Is(err, ErrOtherLog) {
		t.Fatalf("recount of a deposed leader against the head of the cluster: %v, want ErrOtherLog", err)
	}
}

func TestRecountOfAnIdleFollowerAfterInstall(t *testing.T) {
	tn, _, e := lagging(t)
	n := tn.nodes[2]
	n.Receive(e.m, bytes.NewReader(e.snap))
	if _, err := n.Process(); err != nil {
		t.Fatal(err)
	}
	head := n.Status().Head
	d := tn.disks[2].Clone(rand.New(rand.NewPCG(5, 5)))
	d.Crash()
	if snaps := stored(d, ".snap"); len(snaps) != 2 {
		t.Fatalf("n3 holds %d snapshots, want its own and the installed one", len(snaps))
	}
	cfg := tn.config(2)
	cfg.FS = d
	runs, err := Recount(cfg, head)
	if err != nil {
		t.Fatalf("recount right after an installed snapshot: %v", err)
	}
	if len(runs) != 1 || runs[0].Snapshot != e.m.Index || runs[0].Links != head.Seq {
		t.Fatalf("runs %+v, want one from the installed snapshot %d up to link %d", runs, e.m.Index, head.Seq)
	}
}

func TestRecountReportsABrokenSnapshot(t *testing.T) {
	ct := countedNet(t)
	for _, tc := range []struct {
		name string
		snap raft.Snapshot
	}{
		{"newest", ct.newest},
		{"older", ct.older},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := ct.copy(0)
			if err := d.Corrupt(snapFile(tc.snap), 100); err != nil {
				t.Fatal(err)
			}
			if err := ct.recount(d, ct.head); !errors.Is(err, wal.ErrCorrupt) {
				t.Fatalf("recount with a broken %s snapshot: %v, want ErrCorrupt", tc.name, err)
			}
		})
	}
}

func TestRecountOfADamagedDirFails(t *testing.T) {
	ct := countedNet(t)
	d := ct.copy(0)
	logs := stored(d, ".log")
	tail := logs[len(logs)-1]
	if err := d.Corrupt(tail.Name, tail.Size-10); err != nil {
		t.Fatal(err)
	}
	if err := ct.recount(d, ct.head); !errors.Is(err, wal.ErrCorrupt) {
		t.Fatalf("recount of a damaged log: %v, want ErrCorrupt", err)
	}
}

func TestRecountOpensTheDirectoryOfAnyNode(t *testing.T) {
	ct := countedNet(t)
	runs, err := Recount(ct.config(ct.copy(1)), ct.head)
	if err != nil {
		t.Fatalf("recount of the copy of n2 as a cluster of one: %v", err)
	}
	if runs[0].Links != ct.head.Seq {
		t.Fatalf("runs %+v, the head is link %d", runs, ct.head.Seq)
	}
}

func TestRecountRefusesAnotherCluster(t *testing.T) {
	ct := countedNet(t)
	cfg := ct.config(ct.copy(1))
	cfg.Cluster = 9
	_, err := Recount(cfg, ct.head)
	want := "replica: the directory belongs to another node or cluster: /n holds node 2 of 3 in cluster 7, not node 1 of 1 in cluster 9"
	if !errors.Is(err, ErrIdentity) || err.Error() != want {
		t.Fatalf("recount in cluster 9: %v, want %q", err, want)
	}
}

func TestRecountDoesNotCreate(t *testing.T) {
	tn := stoppedTestNet(t, 3, nil)
	if _, err := Recount(tn.config(1), audit.Head{}); !errors.Is(err, wal.ErrNoMeta) {
		t.Fatalf("recount of an empty copy: %v, want wal.ErrNoMeta", err)
	}
	if files := tn.disks[1].Durable(); len(files) != 0 {
		t.Fatalf("recount of an empty copy left %v", files)
	}
}
