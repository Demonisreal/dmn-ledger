package replica

import (
	"testing"

	"github.com/Demonisreal/dmn-ledger/internal/client"
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
