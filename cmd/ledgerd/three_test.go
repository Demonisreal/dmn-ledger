//go:build linux

package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/Demonisreal/dmn-ledger/internal/audit"
	"github.com/Demonisreal/dmn-ledger/internal/disk"
	"github.com/Demonisreal/dmn-ledger/internal/notary"
	"github.com/Demonisreal/dmn-ledger/internal/state"
	"github.com/Demonisreal/dmn-ledger/internal/u128"
	"github.com/Demonisreal/dmn-ledger/internal/wal"
	"github.com/Demonisreal/dmn-ledger/ledger"
)

func ledgerctl(ctx context.Context, nodes []string, args ...string) (string, int, error) {
	out, _, code, err := ledgerctlErr(ctx, nodes, args...)
	return out, code, err
}

func ledgerctlErr(ctx context.Context, nodes []string, args ...string) (string, string, int, error) {
	cmd := exec.CommandContext(ctx, filepath.Join(bin, "ledgerctl"), args...) //nolint:gosec // G204: the binary TestMain built
	cmd.Env = []string{"LEDGER_CLUSTER=1", "LEDGER_NODES=" + strings.Join(nodes, ",")}
	var out, errs bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errs
	err := cmd.Run()
	if exit, ok := errors.AsType[*exec.ExitError](err); ok {
		return out.String(), errs.String(), exit.ExitCode(), nil
	}
	return out.String(), errs.String(), 0, err
}

func changeTransferInSnapshot(t *testing.T, dir string, tr ledger.Transfer) {
	t.Helper()
	rec := state.AppendTransfer(nil, state.Transfer{
		ID:              u128.Uint128(tr.ID),
		DebitAccountID:  u128.Uint128(tr.DebitAccountID),
		CreditAccountID: u128.Uint128(tr.CreditAccountID),
		Amount:          u128.Uint128(tr.Amount),
		PendingID:       u128.Uint128(tr.PendingID),
		Timeout:         tr.Timeout,
		Ledger:          tr.Ledger,
		Code:            tr.Code,
		Flags:           state.TransferFlags(tr.Flags),
		Timestamp:       tr.Timestamp,
	})
	snaps := filepath.Join(dir, "snap")
	var snap []byte
	meta, _, err := wal.LoadSnapshot(disk.OS{}, snaps, func(r io.Reader) error {
		var err error
		snap, err = io.ReadAll(r)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	at := bytes.Index(snap, rec)
	if at < 0 {
		t.Fatalf("transfer %s is not in snapshot %d of %s", tr.ID, meta.Index, dir)
	}
	changed := state.DecodeTransfer(rec)
	changed.Code++
	copy(snap[at:], state.AppendTransfer(nil, changed))
	err = wal.SaveSnapshot(disk.OS{}, snaps, meta, func(w io.Writer) error {
		_, err := w.Write(snap)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestThreeProcesses(t *testing.T) {
	begin := time.Now()
	addrs := freeAddrs(t, 6)
	peers, clients := addrs[:3], addrs[3:]
	nodes := make([]*proc, 3)
	dirs := make([]string, 3)
	files := t.TempDir()
	for i := range nodes {
		keyFile := filepath.Join(files, "node"+strconv.Itoa(i+1)+".key")
		if err := notary.WriteKey(keyFile, ed25519.NewKeyFromSeed(bytes.Repeat([]byte{byte(i + 1)}, ed25519.SeedSize))); err != nil {
			t.Fatal(err)
		}
		dirs[i] = t.TempDir()
		nodes[i] = launch(t, nodeEnv(i+1, peers, clients[i], dirs[i], "LEDGER_KEY_FILE="+keyFile, "LEDGER_SNAPSHOT_EVERY=4")...)
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
		case len(cells) != 12 || cells[8] != "no":
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

	ctx, cancel = context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	at := func(name string) string { return filepath.Join(files, name) }
	expect := func(code int, args ...string) string {
		t.Helper()
		out, errs, got, err := ledgerctlErr(ctx, clients, args...)
		if err != nil || got != code {
			t.Fatalf("ledgerctl %s: exit %d, %v, want exit %d\nstdout:\n%s\nstderr:\n%s", strings.Join(args, " "), got, err, code, out, errs)
		}
		return out
	}
	book := func(id uint64) {
		t.Helper()
		res, err := cl.CreateTransfers(ctx, []ledger.Transfer{{ID: ledger.From64(id), DebitAccountID: ledger.From64(1), CreditAccountID: ledger.From64(2), Amount: ledger.From64(id), Ledger: 1, Code: 1}})
		if err != nil || len(res.Failures) > 0 {
			t.Fatalf("transfer %d: %+v, %v", id, res, err)
		}
	}
	book(77)
	expect(0, "proof", "transfer", "77", "-o", at("77.proof"))
	keys := expect(1, "status", "-keys")
	if lines := strings.Split(strings.TrimSpace(keys), "\n"); len(lines) != 2 {
		t.Fatalf("status -keys with one node down printed:\n%s", keys)
	}
	if err := os.WriteFile(at("keys"), []byte(keys), 0o600); err != nil {
		t.Fatal(err)
	}
	if out := expect(0, "verify", "proof", at("77.proof"), "-keys", at("keys")); !strings.HasPrefix(out, "file="+at("77.proof")+" kind=transfer id=77 ") {
		t.Fatalf("verify proof printed %q", out)
	}
	proof, err := os.ReadFile(at("77.proof"))
	if err != nil {
		t.Fatal(err)
	}
	proof[len(proof)/3] ^= 0x04
	if err := os.WriteFile(at("77.flipped"), proof, 0o600); err != nil { //nolint:gosec // G703: a file in the directory of this test
		t.Fatal(err)
	}
	expect(1, "verify", "proof", at("77.flipped"), "-keys", at("keys"))

	expect(0, "head", "-o", at("head.1"))
	for id := uint64(78); id < 82; id++ {
		book(id)
	}
	expect(0, "head", "-o", at("head.2"))
	if out := expect(0, "verify", "chain", at("head.1"), at("head.2"), "-keys", at("keys")); !strings.HasPrefix(out, "connected: ") {
		t.Fatalf("verify chain printed %q", out)
	}
	expect(0, "verify", "chain", at("77.proof"), at("head.2"), "-keys", at("keys"))

	head, err := os.ReadFile(at("head.2"))
	if err != nil {
		t.Fatal(err)
	}
	_, signed, _, err := audit.Decode(head)
	if err != nil {
		t.Fatalf("head.2: %v", err)
	}
	transfers, err := cl.LookupTransfers(ctx, []ledger.Uint128{ledger.From64(77)})
	if err != nil || len(transfers) != 1 {
		t.Fatalf("lookup of transfer 77: %+v, %v", transfers, err)
	}
	stopped := (leader + 1) % 3
	awaitStatus(t, clients[stopped], func(st ledger.NodeStatus) bool { return st.HeadSeq >= signed.Seq })
	if out := expect(3, "verify", "dir", dirs[stopped], "-head", at("head.2"), "-keys", at("keys")); !strings.Contains(out, "is locked by a running node") {
		t.Fatalf("verify dir of a running node printed %q", out)
	}
	nodes[stopped].signal(t, syscall.SIGTERM)
	if code := nodes[stopped].exit(t); code != 0 {
		t.Fatalf("node %d: exit %d after SIGTERM", stopped+1, code)
	}
	if out := expect(0, "verify", "dir", dirs[stopped], "-head", at("head.2"), "-keys", at("keys")); !strings.Contains(out, "the directory leads to head ") {
		t.Fatalf("verify dir printed %q", out)
	}
	changed := filepath.Join(t.TempDir(), "changed")
	if err := os.CopyFS(changed, os.DirFS(dirs[stopped])); err != nil {
		t.Fatal(err)
	}
	changeTransferInSnapshot(t, changed, transfers[0])
	if out := expect(1, "verify", "dir", changed, "-head", at("head.2"), "-keys", at("keys")); !strings.Contains(out, "records give") {
		t.Fatalf("verify dir of a changed snapshot printed %q", out)
	}

	forged := audit.Head{Seq: 1, Hash: [32]byte{0xf0}}
	signer := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{byte(stopped + 1)}, ed25519.SeedSize))
	if err := os.WriteFile(at("forged"), append(audit.AppendHead(nil, forged), notary.Sign(signer, 1, forged)...), 0o600); err != nil {
		t.Fatal(err)
	}
	if out := expect(1, "verify", "dir", dirs[stopped], "-head", at("forged"), "-keys", at("keys")); !strings.HasPrefix(out, "DIFFERS: replica: the directory leads to another head: link 1 from snapshot ") {
		t.Fatalf("verify dir against another head at link 1 printed %q", out)
	}
}
