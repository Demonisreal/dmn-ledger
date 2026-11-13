package replica

import (
	"strings"
	"testing"

	"github.com/Demonisreal/dmn-ledger/internal/client"
	"github.com/Demonisreal/dmn-ledger/internal/disk"
)

func BenchmarkProcessTransfer(b *testing.B) {
	tn := newTestNet(b, 1, func(cfg *Config) { cfg.SnapshotEvery = 1 << 40 })
	tn.elect(0)
	c := tn.register(0, 1)
	ok(b, c.do(client.CreateAccounts, accounts(1, 2)))
	n := tn.nodes[0]
	id := uint64(100)
	b.ReportAllocs()
	for b.Loop() {
		id++
		tn.clocks[0] += tickNs
		n.Tick()
		n.Request(c.request(client.CreateTransfers, transfer(id, 1, 2, 1)))
		out, err := n.Process()
		if err != nil {
			b.Fatal(err)
		}
		if len(out.Replies) != 1 || out.Replies[0].Status != client.OK {
			b.Fatalf("replies %+v", out.Replies)
		}
	}
}

func BenchmarkProcessTransferDisk(b *testing.B) {
	dir := b.TempDir()
	tn := stoppedTestNet(b, 1, func(cfg *Config) { cfg.FS, cfg.Dir, cfg.SnapshotEvery = disk.OS{}, dir, 1<<40 })
	syncs := 0
	tn.tap = func(_ int, fsys disk.FS) disk.FS {
		return tapFS{fsys, func(op, _ string) {
			if strings.HasPrefix(op, "sync") {
				syncs++
			}
		}}
	}
	tn.open(0)
	tn.elect(0)
	c := tn.register(0, 1)
	ok(b, c.do(client.CreateAccounts, accounts(1, 2)))
	n := tn.nodes[0]
	id := uint64(100)
	syncs = 0
	b.ReportAllocs()
	for b.Loop() {
		id++
		tn.clocks[0] += tickNs
		n.Tick()
		n.Request(c.request(client.CreateTransfers, transfer(id, 1, 2, 1)))
		out, err := n.Process()
		if err != nil {
			b.Fatal(err)
		}
		if len(out.Replies) != 1 || out.Replies[0].Status != client.OK {
			b.Fatalf("replies %+v", out.Replies)
		}
	}
	b.ReportMetric(float64(syncs)/float64(b.N), "fsync/op")
	if err := n.Close(); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkProcessIdleTick(b *testing.B) {
	tn := newTestNet(b, 1, nil)
	tn.elect(0)
	n := tn.nodes[0]
	b.ReportAllocs()
	for b.Loop() {
		tn.clocks[0] += tickNs
		n.Tick()
		if _, err := n.Process(); err != nil {
			b.Fatal(err)
		}
	}
}
