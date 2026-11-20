//go:build linux

package history

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Demonisreal/dmn-ledger/internal/check"
	"github.com/Demonisreal/dmn-ledger/internal/client"
	"github.com/Demonisreal/dmn-ledger/internal/local"
	"github.com/Demonisreal/dmn-ledger/internal/local/localtest"
	"github.com/Demonisreal/dmn-ledger/internal/state"
	"github.com/Demonisreal/dmn-ledger/internal/u128"
	"github.com/Demonisreal/dmn-ledger/ledger"
)

var bin string

func TestMain(m *testing.M) {
	dir, remove := localtest.Build("", "../../cmd/ledgerd")
	bin = filepath.Join(dir, "ledgerd")
	code := m.Run()
	remove()
	os.Exit(code)
}

func transferBody(id, debit, credit, amount uint64) []byte {
	return state.AppendTransfer(nil, state.Transfer{
		ID:              u128.From64(id),
		DebitAccountID:  u128.From64(debit),
		CreditAccountID: u128.From64(credit),
		Amount:          u128.From64(amount),
		Ledger:          1,
		Code:            1,
	})
}

func TestIssueGivesWhatTheReplayerExpects(t *testing.T) {
	const cluster = 3
	c, err := local.Start(local.Config{Bin: bin, Dir: t.TempDir(), Cluster: cluster, Seed: 5000})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	cl, err := ledger.Dial(ctx, ledger.Config{Cluster: cluster, Nodes: c.Addrs()})
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	rec := NewRecorder(Options{Window: 16, Backlog: 4, Since: since()}, check.NewReplayer())
	accounts := state.AppendAccount(state.AppendAccount(nil, state.Account{ID: u128.From64(1), Ledger: 1, Code: 1}), state.Account{ID: u128.From64(2), Ledger: 1, Code: 1})
	steps := []struct {
		op   client.Op
		body []byte
	}{
		{client.CreateAccounts, accounts},
		{client.CreateTransfers, append(transferBody(10, 1, 2, 5), transferBody(11, 1, 99, 1)...)},
		{client.LookupAccounts, ids(1, 2)},
		{client.LookupTransfers, ids(10, 11)},
	}
	var replies []client.Reply
	for _, s := range steps {
		tr, ok := rec.Begin(ctx, 1, func() (client.Op, []byte) { return s.op, s.body })
		if !ok {
			t.Fatal("Begin gave up")
		}
		reply, err := Issue(ctx, cl, tr.Call.Op, tr.Call.Body)
		if err != nil {
			t.Fatalf("%s: %v", s.op, err)
		}
		rec.End(tr, reply)
		replies = append(replies, reply)
	}
	fails := client.DecodeFailures(replies[1].Body)
	switch {
	case replies[0].Time == 0 || replies[1].Time <= replies[0].Time:
		t.Fatalf("writes at %d and %d", replies[0].Time, replies[1].Time)
	case len(fails) != 1 || fails[0].Index != 1 || fails[0].Reason != state.CreditAccountNotFound:
		t.Fatalf("transfers failed with %v, want the second for its credit account", fails)
	case len(replies[2].Body) != 2*state.AccountSize || len(replies[3].Body) != state.TransferSize:
		t.Fatalf("lookups gave %d and %d bytes, want two accounts and one transfer", len(replies[2].Body), len(replies[3].Body))
	}
	if err := rec.Hold(ctx, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	rec.Close()
	if err := rec.Judge(ctx); err != nil {
		t.Fatalf("the replayer refuses what Issue gave: %v", err)
	}
	if calls, windows := rec.Judged(); calls != len(steps) || windows != 1 {
		t.Fatalf("judged %d calls in %d windows, want %d in 1", calls, windows, len(steps))
	}
}
