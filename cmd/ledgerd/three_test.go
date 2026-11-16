//go:build linux

package main

import (
	"bytes"
	"context"
	"errors"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/Demonisreal/dmn-ledger/ledger"
)

func ledgerctl(ctx context.Context, nodes []string, args ...string) (string, int, error) {
	cmd := exec.CommandContext(ctx, filepath.Join(bin, "ledgerctl"), args...) //nolint:gosec // G204: the binary TestMain built
	cmd.Env = []string{"LEDGER_CLUSTER=1", "LEDGER_NODES=" + strings.Join(nodes, ",")}
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	if exit, ok := errors.AsType[*exec.ExitError](err); ok {
		return out.String(), exit.ExitCode(), nil
	}
	return out.String(), 0, err
}

func TestThreeProcesses(t *testing.T) {
	begin := time.Now()
	addrs := freeAddrs(t, 6)
	peers, clients := addrs[:3], addrs[3:]
	nodes := make([]*proc, 3)
	for i := range nodes {
		nodes[i] = launch(t, nodeEnv(i+1, peers, clients[i], t.TempDir())...)
	}
	ctx, cancel := context.WithDeadline(context.Background(), begin.Add(10*time.Second))
	defer cancel()

	cl, err := ledger.Dial(ctx, ledger.Config{Cluster: 1, Nodes: clients})
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	res, err := cl.CreateAccounts(ctx, []ledger.Account{
		{ID: ledger.From64(1), Ledger: 1, Code: 10},
		{ID: ledger.From64(2), Ledger: 1, Code: 10},
	})
	if err != nil || len(res.Failures) > 0 {
		t.Fatalf("create accounts: %+v, %v", res, err)
	}
	pay := func(amount uint64) ledger.Result {
		t.Helper()
		res, err := cl.CreateTransfers(ctx, []ledger.Transfer{{
			ID:              ledger.NewID(),
			DebitAccountID:  ledger.From64(1),
			CreditAccountID: ledger.From64(2),
			Amount:          ledger.From64(amount),
			Ledger:          1,
			Code:            1,
		}})
		if err != nil || len(res.Failures) > 0 {
			t.Fatalf("pay %d: %+v, %v", amount, res, err)
		}
		return res
	}
	before := pay(500)

	leader := -1
	var killed *proc
	for i, p := range nodes {
		if st, err := ledger.Status(ctx, 1, clients[i]); err == nil && st.Role == ledger.Leader {
			leader, killed = i, p
		}
	}
	if killed == nil {
		t.Fatal("no node says it leads after a transfer went through")
	}
	killed.signal(t, syscall.SIGKILL)
	if code := killed.exit(t); code != -1 {
		t.Fatalf("killed leader exited with %d", code)
	}

	after := pay(250)
	if after.Timestamp <= before.Timestamp {
		t.Fatalf("transfer after the kill at %d, before it at %d", after.Timestamp, before.Timestamp)
	}
	accounts, err := cl.LookupAccounts(ctx, []ledger.Uint128{ledger.From64(1), ledger.From64(2)})
	if err != nil || len(accounts) != 2 || accounts[0].DebitsPosted != ledger.From64(750) || accounts[1].CreditsPosted != ledger.From64(750) {
		t.Fatalf("accounts %+v, %v, want 750 posted from 1 to 2", accounts, err)
	}

	var out string
	var code int
	for {
		out, code, err = ledgerctl(ctx, clients, "status")
		if err != nil {
			t.Fatalf("ledgerctl status: %v\n%s", err, out)
		}
		if strings.Contains(out, "nodes agree at index") {
			break
		}
		if ctx.Err() != nil {
			t.Fatalf("the survivors never agreed:\n%s", out)
		}
		time.Sleep(20 * time.Millisecond)
	}
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if code != 1 || len(lines) != 5 || !strings.HasPrefix(lines[0], "node  address") {
		t.Fatalf("exit %d, output:\n%s", code, out)
	}
	leaders := 0
	for i, line := range lines[1:4] {
		cells := strings.Fields(line)
		switch {
		case cells[0] != strconv.Itoa(i+1) || cells[1] != clients[i]:
			t.Fatalf("row %d is %q", i+1, line)
		case i == leader:
			if !strings.HasPrefix(cells[2], "unreachable:") {
				t.Fatalf("the killed node shows %q", line)
			}
		case len(cells) != 11 || cells[7] != "no":
			t.Fatalf("survivor %d shows %q", i+1, line)
		case cells[2] == "leader":
			leaders++
		}
	}
	if leaders != 1 {
		t.Fatalf("%d leaders among the survivors:\n%s", leaders, out)
	}
	for i, p := range nodes {
		if i != leader && strings.Contains(p.logs(t), "halted") {
			t.Fatalf("node %d halted:\n%s", i+1, p.logs(t))
		}
	}
	if took := time.Since(begin); took > 10*time.Second {
		t.Fatalf("took %v, at most 10s", took)
	}
}
