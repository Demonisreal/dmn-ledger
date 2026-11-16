//go:build linux

package main

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Demonisreal/dmn-ledger/ledger"
)

func TestNodeWithAnotherMaxSessionsStaysOut(t *testing.T) {
	begin := time.Now()
	addrs := freeAddrs(t, 6)
	peers, clients := addrs[:3], addrs[3:]
	nodes := make([]*proc, 3)
	for i := range nodes {
		var extra []string
		if i == 2 {
			extra = append(extra, "LEDGER_MAX_SESSIONS=17")
		}
		nodes[i] = launch(t, nodeEnv(i+1, peers, clients[i], t.TempDir(), extra...)...)
	}
	ctx, cancel := context.WithDeadline(context.Background(), begin.Add(10*time.Second))
	defer cancel()

	var mu sync.Mutex
	var seen []string
	watching, stop := context.WithCancel(ctx)
	var wg sync.WaitGroup
	wg.Go(func() {
		for watching.Err() == nil {
			probe, done := context.WithTimeout(watching, 500*time.Millisecond)
			st, err := ledger.Status(probe, 1, clients[2])
			done()
			if err == nil && (st.Role == ledger.Leader || st.Leader != 0 || st.Commit != 0) {
				mu.Lock()
				seen = append(seen, st.Role.String())
				mu.Unlock()
			}
			time.Sleep(20 * time.Millisecond)
		}
	})

	cl, err := ledger.Dial(ctx, ledger.Config{Cluster: 1, Nodes: clients})
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	if res, err := cl.CreateAccounts(ctx, []ledger.Account{{ID: ledger.From64(1), Ledger: 1, Code: 10}, {ID: ledger.From64(2), Ledger: 1, Code: 10}}); err != nil || len(res.Failures) > 0 {
		t.Fatalf("create accounts: %+v, %v", res, err)
	}
	if res, err := cl.CreateTransfers(ctx, []ledger.Transfer{{ID: ledger.From64(10), DebitAccountID: ledger.From64(1), CreditAccountID: ledger.From64(2), Amount: ledger.From64(5), Ledger: 1, Code: 1}}); err != nil || len(res.Failures) > 0 {
		t.Fatalf("transfer: %+v, %v", res, err)
	}

	const refused = "peer runs with another MaxSessions: node 3 has 17, node "
	for !strings.Contains(nodes[0].logs(t), refused) || !strings.Contains(nodes[1].logs(t), refused) || !strings.Contains(nodes[2].logs(t), "another MaxSessions") {
		if ctx.Err() != nil {
			t.Fatalf("not every node logged the refusal:\nnode 1:\n%s\nnode 2:\n%s\nnode 3:\n%s", nodes[0].logs(t), nodes[1].logs(t), nodes[2].logs(t))
		}
		time.Sleep(20 * time.Millisecond)
	}

	var out string
	var code int
	for {
		out, code, err = ledgerctl(ctx, clients, "status")
		if err != nil {
			t.Fatalf("ledgerctl status: %v\n%s", err, out)
		}
		if strings.Contains(out, "nodes agree at index") || ctx.Err() != nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if code != 0 || len(lines) != 5 || !strings.HasPrefix(lines[4], "nodes agree at index ") {
		t.Fatalf("exit %d, status:\n%s", code, out)
	}
	third := strings.Fields(lines[3])
	if len(third) != 12 || third[2] == "leader" || third[4] != "0" || third[5] != "0" || third[7] != "0:0000000000000000" {
		t.Fatalf("node 3 made progress:\n%s", out)
	}
	stop()
	wg.Wait()
	if len(seen) > 0 {
		t.Fatalf("node 3 showed a leader, led or committed: %v", seen)
	}
	if took := time.Since(begin); took > 10*time.Second {
		t.Fatalf("took %v, at most 10s", took)
	}
}
