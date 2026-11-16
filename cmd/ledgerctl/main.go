package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Demonisreal/dmn-ledger/ledger"
)

const usage = `usage:
  ledgerctl status
  ledgerctl account create [-id N] -ledger N -code N [-limit debits|credits]
  ledgerctl account get ID...
  ledgerctl transfer create [-id N] -debit N -credit N -amount N -ledger N -code N [-pending [-timeout SEC]]
  ledgerctl transfer post [-id N] -pending-id N [-amount N]
  ledgerctl transfer void [-id N] -pending-id N
  ledgerctl transfer get ID...
IDs and amounts are decimal or 0x. LEDGER_CLUSTER names the cluster, LEDGER_NODES the client
addresses of its nodes in the order of their IDs, separated by commas.`

var patience = 10 * time.Second

type ctl struct {
	cluster uint64
	nodes   []string
	stdout  io.Writer
	stderr  io.Writer
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	c := &ctl{stdout: stdout, stderr: stderr}
	commands := map[string]func([]string) int{
		"status":          c.status,
		"account create":  c.createAccount,
		"account get":     c.getAccounts,
		"transfer create": c.createTransfer,
		"transfer post":   func(args []string) int { return c.settle("transfer post", ledger.PostPending, args) },
		"transfer void":   func(args []string) int { return c.settle("transfer void", ledger.VoidPending, args) },
		"transfer get":    c.getTransfers,
	}
	if len(args) > 0 && (args[0] == "-h" || args[0] == "-help" || args[0] == "help") {
		fmt.Fprintln(stderr, usage)
		return 0
	}
	name := strings.Join(args[:min(2, len(args))], " ")
	if len(args) > 0 && args[0] == "status" {
		name = "status"
	}
	cmd, ok := commands[name]
	if !ok {
		fmt.Fprintln(stderr, usage)
		return 2
	}
	return cmd(args[len(strings.Fields(name)):])
}

func (c *ctl) env() error {
	s, ok := os.LookupEnv("LEDGER_CLUSTER")
	if !ok {
		return errors.New("LEDGER_CLUSTER is missing")
	}
	u, err := ledger.Parse(s)
	if err != nil || u.Hi != 0 || u.Lo == 0 {
		return fmt.Errorf("LEDGER_CLUSTER=%q: want a number from 1 to 2^64-1, decimal or 0x", s)
	}
	c.cluster = u.Lo
	s, ok = os.LookupEnv("LEDGER_NODES")
	if !ok {
		return errors.New("LEDGER_NODES is missing")
	}
	for addr := range strings.SplitSeq(s, ",") {
		if addr == "" {
			return fmt.Errorf("LEDGER_NODES=%q: want addresses separated by commas", s)
		}
		c.nodes = append(c.nodes, addr)
	}
	return nil
}

func (c *ctl) flags(name string) *flag.FlagSet {
	fs := flag.NewFlagSet("ledgerctl "+name, flag.ContinueOnError)
	fs.SetOutput(c.stderr)
	return fs
}

func (c *ctl) parse(fs *flag.FlagSet, args []string, positional bool) (int, bool) {
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0, false
		}
		return 2, false
	}
	switch {
	case positional && fs.NArg() == 0:
		fmt.Fprintf(c.stderr, "%s: want at least one ID\n", fs.Name())
		return 2, false
	case !positional && fs.NArg() > 0:
		fmt.Fprintf(c.stderr, "%s: unexpected arguments %v\n", fs.Name(), fs.Args())
		return 2, false
	}
	if err := c.env(); err != nil {
		fmt.Fprintln(c.stderr, "ledgerctl:", err)
		return 2, false
	}
	return 0, true
}

func (c *ctl) call(subject string, do func(context.Context, *ledger.Client) int) int {
	ctx, cancel := context.WithTimeout(context.Background(), patience)
	defer cancel()
	cl, err := ledger.Dial(ctx, ledger.Config{Cluster: c.cluster, Nodes: c.nodes})
	if err != nil {
		return c.fail(subject, err)
	}
	defer cl.Close()
	return do(ctx, cl)
}

func (c *ctl) fail(subject string, err error) int {
	fmt.Fprintf(c.stderr, "ledgerctl: %s: %v\n", subject, err)
	if errors.Is(err, ledger.ErrRejected) {
		return 1
	}
	return 3
}

func (c *ctl) written(id ledger.Uint128, booked ledger.Result, err error) int {
	switch {
	case err != nil:
		return c.fail(id.String(), err)
	case len(booked.Failures) > 0:
		fmt.Fprintf(c.stdout, "failed %s: %s\n", id, booked.Failures[0].Reason)
		return 1
	}
	fmt.Fprintf(c.stdout, "ok %s at %s\n", id, stamp(booked.Timestamp))
	return 0
}

func ids(fs *flag.FlagSet) ([]ledger.Uint128, error) {
	var list []ledger.Uint128
	for _, s := range fs.Args() {
		id, err := ledger.Parse(s)
		if err != nil {
			return nil, err
		}
		list = append(list, id)
	}
	return list, nil
}

func number128(p *ledger.Uint128) func(string) error {
	return func(s string) (err error) {
		*p, err = ledger.Parse(s)
		return err
	}
}

func number[T uint16 | uint32](p *T) func(string) error {
	return func(s string) error {
		n, err := strconv.ParseUint(s, 10, 64)
		if err != nil || uint64(T(n)) != n {
			return fmt.Errorf("want a decimal number up to %d", ^T(0))
		}
		*p = T(n)
		return nil
	}
}

func given(fs *flag.FlagSet, name string) bool {
	seen := false
	fs.Visit(func(f *flag.Flag) { seen = seen || f.Name == name })
	return seen
}

func stamp(ns uint64) string {
	if ns == 0 {
		return "-"
	}
	return time.Unix(0, int64(ns)).UTC().Format("2006-01-02T15:04:05.000000000Z") //nolint:gosec // G115: ledger time stays below 2^63
}
