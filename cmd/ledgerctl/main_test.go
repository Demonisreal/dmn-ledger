package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"fmt"
	"net"
	"os"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Demonisreal/dmn-ledger/internal/disk"
	"github.com/Demonisreal/dmn-ledger/internal/replica"
	"github.com/Demonisreal/dmn-ledger/internal/server"
	"github.com/Demonisreal/dmn-ledger/internal/wal"
	"github.com/Demonisreal/dmn-ledger/ledger"
)

func dataDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if err := replica.Init(disk.OS{}, dir, wal.Identity{Cluster: 1, Node: 1, Nodes: 1}); err != nil {
		t.Fatal(err)
	}
	return dir
}

func singleNode(t *testing.T) string {
	t.Helper()
	addr, _ := serveNode(t, nil, dataDir(t))
	return addr
}

func serveNode(t *testing.T, key ed25519.PrivateKey, dir string) (string, func()) {
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
		Cluster:       1,
		Peers:         []string{peering.Addr().String()},
		Dir:           dir,
		FS:            disk.OS{},
		Tick:          time.Millisecond,
		SnapshotEvery: 100000,
		MaxSessions:   64,
		MaxClients:    16,
		Key:           key,
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
	var once sync.Once
	stop := func() {
		once.Do(func() {
			cancel()
			if err := <-halted; err != nil {
				t.Error(err)
			}
		})
	}
	t.Cleanup(stop)
	return clients.Addr().String(), stop
}

func deadAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

type step struct {
	args []string
	code int
	out  string
}

func (s step) check(t *testing.T) {
	t.Helper()
	var out, errs bytes.Buffer
	code := run(s.args, &out, &errs)
	if code != s.code || !regexp.MustCompile(s.out).MatchString(out.String()+errs.String()) {
		t.Fatalf("%v: exit %d, stdout %q, stderr %q, want exit %d and %q", s.args, code, out.String(), errs.String(), s.code, s.out)
	}
}

func args(line string) []string {
	return strings.Fields(line)
}

func TestExitCodes(t *testing.T) {
	addr := singleNode(t)
	t.Setenv("LEDGER_CLUSTER", "1")
	t.Setenv("LEDGER_NODES", addr)
	for _, s := range []step{
		{args("account create -id 1 -ledger 1 -code 10"), 0, `^ok 1 at 20\d\d-\d\d-\d\dT[0-9:.]{18}Z\n$`},
		{args("account create -id 1 -ledger 1 -code 10"), 1, `^failed 1: exists\n$`},
		{args("account create -id 0x2 -ledger 1 -code 10 -limit debits"), 0, `^ok 2 at `},
		{args("account create -ledger 1 -code 10 -limit credits"), 0, `^ok [1-9]\d* at `},
		{args("account create -id 3 -ledger 0 -code 10"), 1, `^failed 3: ledger_must_not_be_zero\n$`},
		{args("transfer create -id 10 -debit 1 -credit 2 -amount 100 -ledger 1 -code 1"), 0, `^ok 10 at `},
		{args("transfer create -id 11 -debit 1 -credit 2 -amount 50 -ledger 1 -code 1 -pending -timeout 3600"), 0, `^ok 11 at `},
		{args("transfer create -id 14 -debit 2 -credit 1 -amount 1000 -ledger 1 -code 1"), 1, `^failed 14: exceeds_credits\n$`},
		{args("transfer post -id 12 -pending-id 11 -amount 20"), 0, `^ok 12 at `},
		{args("transfer void -id 13 -pending-id 11"), 1, `^failed 13: pending_transfer_already_posted\n$`},
		{args("transfer post -pending-id 11"), 1, `^failed [1-9]\d*: pending_transfer_already_posted\n$`},
		{args("transfer get 10 11 0xc"), 0, `^id=10 debit_account_id=1 credit_account_id=2 amount=100 pending_id=0 timeout=0 ledger=1 code=1 flags=0 timestamp=\S+\n` +
			`id=11 debit_account_id=1 credit_account_id=2 amount=50 pending_id=0 timeout=3600 ledger=1 code=1 flags=2 timestamp=\S+\n` +
			`id=12 debit_account_id=1 credit_account_id=2 amount=20 pending_id=11 timeout=0 ledger=1 code=1 flags=4 timestamp=\S+\n$`},
		{args("transfer get 10 99"), 1, `^id=10 .*\nnot found 99\n$`},
		{args("account get 1 2"), 0, `^id=1 ledger=1 code=10 flags=0 debits_pending=0 debits_posted=120 credits_pending=0 credits_posted=0 timestamp=\S+\n` +
			`id=2 ledger=1 code=10 flags=1 debits_pending=0 debits_posted=0 credits_pending=0 credits_posted=120 timestamp=\S+\n$`},
		{args("account get 0x63 1"), 1, `^id=1 .*\nnot found 99\n$`},
		{args("status"), 0, `^node  address  .*\n1\s+127\.0\.0\.1:\d+\s+leader\s+1\s+\d+\s+\d+\s+[0-9a-f]{16}\s+11:[0-9a-f]{16}\s+no\s+20\S+\s+[+-]\d+ms\s+-\nno two nodes at the same index\n$`},
	} {
		s.check(t)
	}
	t.Setenv("LEDGER_NODES", addr+","+deadAddr(t))
	step{args("status"), 1, `\n2     127\.0\.0\.1:\d+  unreachable: dial tcp .*connection refused\n`}.check(t)
	step{args("account get 1"), 0, `^id=1 `}.check(t)
}

func TestNoAnswer(t *testing.T) {
	patience = 300 * time.Millisecond
	t.Cleanup(func() { patience = 10 * time.Second })
	t.Setenv("LEDGER_CLUSTER", "1")
	t.Setenv("LEDGER_NODES", deadAddr(t))
	for _, s := range []step{
		{args("account create -id 1 -ledger 1 -code 10"), 3, `ledgerctl: 1: context deadline exceeded\n$`},
		{args("transfer create -id 5 -debit 1 -credit 2 -amount 1 -ledger 1 -code 1"), 3, `ledgerctl: 5: context deadline exceeded\n$`},
		{args("account get 1"), 3, `ledgerctl: account get: context deadline exceeded\n$`},
		{args("transfer get 1"), 3, `ledgerctl: transfer get: context deadline exceeded\n$`},
		{args("status"), 3, `unreachable: dial tcp .*connection refused\nno two nodes at the same index\n$`},
	} {
		s.check(t)
	}
}

func TestBatchTimeOnlyWhenItDiffers(t *testing.T) {
	for _, tc := range []struct {
		booked ledger.Result
		want   string
	}{
		{ledger.Result{Timestamp: 1794808307000000005, Batch: 1794808307000000005}, "ok 7 at 2026-11-16T05:51:47.000000005Z\n"},
		{ledger.Result{Timestamp: 1794808307000000005, Batch: 1794808307000000002}, "ok 7 at 2026-11-16T05:51:47.000000005Z in the batch of 2026-11-16T05:51:47.000000002Z\n"},
	} {
		var out bytes.Buffer
		c := &ctl{stdout: &out}
		if code := c.written(ledger.From64(7), tc.booked, nil); code != 0 || out.String() != tc.want {
			t.Errorf("%+v: exit %d, %q, want %q", tc.booked, code, out.String(), tc.want)
		}
	}
}

func TestErrorsAfterSending(t *testing.T) {
	for _, tc := range []struct {
		err  error
		code int
	}{
		{fmt.Errorf("%w: %w", ledger.ErrUncertain, context.DeadlineExceeded), 3},
		{fmt.Errorf("%w: %w", ledger.ErrUncertain, ledger.ErrEvicted), 3},
		{ledger.ErrEvicted, 3},
		{context.DeadlineExceeded, 3},
		{ledger.ErrRejected, 1},
	} {
		var errs bytes.Buffer
		c := &ctl{stderr: &errs}
		if code := c.fail("7", tc.err); code != tc.code || errs.String() != "ledgerctl: 7: "+tc.err.Error()+"\n" {
			t.Errorf("%v: exit %d, stderr %q, want exit %d", tc.err, code, errs.String(), tc.code)
		}
	}
}

func TestUsage(t *testing.T) {
	t.Setenv("LEDGER_CLUSTER", "1")
	t.Setenv("LEDGER_NODES", "127.0.0.1:1")
	for _, s := range []step{
		{nil, 2, `usage:`},
		{args("account"), 2, `usage:`},
		{args("account delete 1"), 2, `usage:`},
		{args("ledger status"), 2, `usage:`},
		{args("status now"), 2, `ledgerctl status: unexpected arguments \[now\]`},
		{args("account create -ledger x -code 1"), 2, `invalid value "x" for flag -ledger: want a decimal number up to 4294967295`},
		{args("account create -ledger 4294967296 -code 1"), 2, `invalid value "4294967296" for flag -ledger`},
		{args("account create -ledger 1 -code 65536"), 2, `invalid value "65536" for flag -code: want a decimal number up to 65535`},
		{args("account create -ledger 1 -code 0x10"), 2, `invalid value "0x10" for flag -code`},
		{args("account create -id 0xg -ledger 1 -code 1"), 2, `invalid value "0xg" for flag -id`},
		{args("account create -id 340282366920938463463374607431768211456 -ledger 1 -code 1"), 2, `value out of range`},
		{args("account create -ledger 1 -code 1 -limit both"), 2, `-limit "both", want debits or credits`},
		{args("account create -ledger 1 -code 1 extra"), 2, `unexpected arguments \[extra\]`},
		{args("transfer create -timeout -1"), 2, `invalid value "-1" for flag -timeout`},
		{args("transfer void -id 1 -pending-id 2 -amount 3"), 2, `flag provided but not defined: -amount`},
		{args("account get"), 2, `ledgerctl account get: want at least one ID`},
		{args("transfer get 1 two"), 2, `ledgerctl transfer get: ledger: parse "two": invalid syntax`},
		{args("-h"), 0, `usage:`},
		{args("account create -h"), 0, `-limit`},
	} {
		s.check(t)
	}
	for _, env := range []struct{ cluster, nodes, want string }{
		{"", "127.0.0.1:1", `LEDGER_CLUSTER="": want a number from 1 to 2\^64-1`},
		{"0", "127.0.0.1:1", `LEDGER_CLUSTER="0"`},
		{"0x10000000000000000", "127.0.0.1:1", `LEDGER_CLUSTER="0x10000000000000000"`},
		{"1", "", `LEDGER_NODES="": want addresses separated by commas`},
		{"1", "127.0.0.1:1,,127.0.0.1:3", `LEDGER_NODES="127.0.0.1:1,,127.0.0.1:3"`},
	} {
		t.Setenv("LEDGER_CLUSTER", env.cluster)
		t.Setenv("LEDGER_NODES", env.nodes)
		step{args("status"), 2, env.want}.check(t)
		step{args("account get 1"), 2, env.want}.check(t)
	}
}

func TestMissingEnvironment(t *testing.T) {
	t.Setenv("LEDGER_CLUSTER", "1")
	t.Setenv("LEDGER_NODES", "127.0.0.1:1")
	for _, name := range []string{"LEDGER_CLUSTER", "LEDGER_NODES"} {
		t.Run(name, func(t *testing.T) {
			unset(t, name)
			step{args("status"), 2, "^ledgerctl: " + name + " is missing\n$"}.check(t)
		})
	}
}

func unset(t *testing.T, name string) {
	t.Setenv(name, "")
	_ = os.Unsetenv(name)
}
