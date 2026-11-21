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
  ledgerctl status [-keys]
  ledgerctl account create [-id N] -ledger N -code N [-limit debits|credits]
  ledgerctl account get ID...
  ledgerctl transfer create [-id N] -debit N -credit N -amount N -ledger N -code N [-pending [-timeout SEC]]
  ledgerctl transfer post [-id N] -pending-id N [-amount N]
  ledgerctl transfer void [-id N] -pending-id N
  ledgerctl transfer get ID...
  ledgerctl key new FILE
  ledgerctl key show FILE
  ledgerctl proof account|transfer|expiry ID [-o FILE]
  ledgerctl proof batch TIME [-o FILE]
  ledgerctl head [-o FILE]
  ledgerctl verify proof FILE... -keys FILE [-unsigned]
  ledgerctl verify chain OLDER NEWER -keys FILE [-links FILE]
  ledgerctl verify dir DIR -head FILE -keys FILE
  ledgerctl init -id N -nodes N DIR
IDs and amounts are decimal or 0x, TIME is the timestamp of the first record of a batch, in
nanoseconds or as printed by create. LEDGER_CLUSTER names the cluster, LEDGER_NODES the client
addresses of its nodes in the order of their IDs, separated by commas; verify dir reads
LEDGER_MAX_SESSIONS like ledgerd. The keys file holds one public key per line as 64 hex digits,
as status -keys prints them.

A proof that verifies shows this much: the holder of a listed key declared that the chain of this
cluster has this head at this link number, and SHA-256 puts the record under that head. It does
not show that a majority agreed, that the head is the newest, a balance, what became of a transfer
later, or that anything is missing. verify dir recounts an in-memory copy of the data directory of
a stopped node up to the link number of a head and leaves the directory as it was.

verify chain connects two heads over the links between them. "connected" holds wherever the links
came from: SHA-256 shows that the newer head continues the older one. "CONFLICT" is printed only
for two heads with the same number and different hashes; that needs no links and shows that their
signers vouch for two histories. Links that do not connect show nothing about the heads, because
nobody vouches for links: from a file given with -links they are "invalid", from the cluster
there is "no verdict".

init makes DIR the data directory of node -id in a cluster of -nodes nodes. ledgerd starts on no
other directory. It is for a node that has never run: the votes and acknowledgements of a node are
in its directory and nowhere else, and a node that lost it cannot come back under its ID.`

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
		"key new":         c.newKey,
		"key show":        c.showKey,
		"proof account":   c.prove(ledger.ProofAccount),
		"proof transfer":  c.prove(ledger.ProofTransfer),
		"proof expiry":    c.prove(ledger.ProofExpiry),
		"proof batch":     c.prove(ledger.ProofBatch),
		"head":            c.prove(ledger.ProofHead),
		"verify proof":    c.verifyProofs,
		"verify chain":    c.verifyChain,
		"verify dir":      c.verifyDir,
		"init":            c.initDir,
	}
	if len(args) > 0 && (args[0] == "-h" || args[0] == "-help" || args[0] == "help") {
		fmt.Fprintln(stderr, usage)
		return 0
	}
	name := strings.Join(args[:min(2, len(args))], " ")
	if len(args) > 0 && (args[0] == "status" || args[0] == "head" || args[0] == "init") {
		name = args[0]
	}
	cmd, ok := commands[name]
	if !ok {
		fmt.Fprintln(stderr, usage)
		return 2
	}
	return cmd(args[len(strings.Fields(name)):])
}

func (c *ctl) env() error {
	if err := c.clusterEnv(); err != nil {
		return err
	}
	return c.nodesEnv()
}

func (c *ctl) clusterEnv() error {
	s, ok := os.LookupEnv("LEDGER_CLUSTER")
	if !ok {
		return errors.New("LEDGER_CLUSTER is missing")
	}
	u, err := ledger.Parse(s)
	if err != nil || u.Hi != 0 || u.Lo == 0 {
		return fmt.Errorf("LEDGER_CLUSTER=%q: want a number from 1 to 2^64-1, decimal or 0x", s)
	}
	c.cluster = u.Lo
	return nil
}

func (c *ctl) nodesEnv() error {
	s, ok := os.LookupEnv("LEDGER_NODES")
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

func (c *ctl) positional(fs *flag.FlagSet, args []string) ([]string, int, bool) {
	var pos []string
	for {
		if err := fs.Parse(args); err != nil {
			if errors.Is(err, flag.ErrHelp) {
				return nil, 0, false
			}
			return nil, 2, false
		}
		if fs.NArg() == 0 {
			return pos, 0, true
		}
		pos = append(pos, fs.Arg(0))
		args = fs.Args()[1:]
	}
}

func (c *ctl) misuse(fs *flag.FlagSet, format string, args ...any) int {
	fmt.Fprintf(c.stderr, "%s: %s\n", fs.Name(), fmt.Sprintf(format, args...))
	return 2
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
	if booked.Batch != booked.Timestamp {
		fmt.Fprintf(c.stdout, "ok %s at %s in the batch of %s\n", id, stamp(booked.Timestamp), stamp(booked.Batch))
		return 0
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
