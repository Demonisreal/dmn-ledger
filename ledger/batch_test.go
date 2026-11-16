package ledger

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/Demonisreal/dmn-ledger/internal/disk"
	"github.com/Demonisreal/dmn-ledger/internal/server"
)

func servedNode(t *testing.T) string {
	t.Helper()
	clients, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	peering, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var d net.Dialer
	srv, err := server.Open(server.Config{
		ID:            1,
		Cluster:       cluster,
		Peers:         []string{peering.Addr().String()},
		Dir:           t.TempDir(),
		FS:            disk.OS{},
		Tick:          time.Millisecond,
		SnapshotEvery: 100000,
		MaxSessions:   64,
		MaxClients:    16,
		Key:           proofKey,
		Clients:       clients,
		Peering:       peering,
		Dial: func(ctx context.Context, addr string) (net.Conn, error) {
			return d.DialContext(ctx, "tcp", addr)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	halted := make(chan error, 1)
	go func() { halted <- srv.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-halted; err != nil {
			t.Error(err)
		}
	})
	return clients.Addr().String()
}

func TestBundledCallsShareTheBatchReceipt(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), patience)
	defer cancel()
	c, err := Dial(ctx, Config{Cluster: cluster, Nodes: []string{servedNode(t)}})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.CreateAccounts(ctx, []Account{{ID: From64(1), Ledger: 1, Code: 1}, {ID: From64(2), Ledger: 1, Code: 1}}); err != nil {
		t.Fatal(err)
	}
	id := uint64(100)
	for range 50 {
		booked := make([]Result, 4)
		var wg sync.WaitGroup
		for k := range booked {
			id++
			tid := id
			wg.Go(func() {
				res, err := c.CreateTransfers(ctx, []Transfer{transfer(tid, 0)})
				if err != nil || len(res.Failures) > 0 {
					t.Errorf("transfer %d: %+v, %v", tid, res, err)
				}
				booked[k] = res
			})
		}
		wg.Wait()
		if t.Failed() {
			return
		}
		for _, behind := range booked {
			if behind.Batch == behind.Timestamp {
				continue
			}
			first, err := c.ProveBatch(ctx, behind.Batch)
			if err != nil || first == nil {
				t.Fatalf("batch at %d: %x, %v", behind.Batch, first, err)
			}
			p, err := VerifyProof(first, cluster, keysOf(proofKey))
			if err != nil || p.Kind != ProofBatch || p.Time != behind.Batch {
				t.Fatalf("receipt of the batch at %d: %+v, %v", behind.Batch, p, err)
			}
			for _, res := range booked {
				if res.Batch != behind.Batch {
					continue
				}
				again, err := c.ProveBatch(ctx, res.Batch)
				if err != nil || string(again) != string(first) {
					t.Fatalf("call at %d in the batch at %d got another receipt, %v", res.Timestamp, res.Batch, err)
				}
			}
			if none, err := c.ProveBatch(ctx, behind.Timestamp); err != nil || none != nil {
				t.Fatalf("a batch receipt for the timestamp %d of a bundled call: %d bytes, %v", behind.Timestamp, len(none), err)
			}
			return
		}
	}
	t.Fatal("no call was bundled behind another in 50 rounds of four")
}
