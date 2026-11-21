package main

import (
	"errors"
	"fmt"
	"path/filepath"

	"github.com/Demonisreal/dmn-ledger/internal/disk"
	"github.com/Demonisreal/dmn-ledger/internal/replica"
	"github.com/Demonisreal/dmn-ledger/internal/wal"
)

func (c *ctl) initDir(args []string) int {
	flags := c.flags("init")
	id := flags.Uint64("id", 0, "the ID of the node, its position in LEDGER_PEERS")
	nodes := flags.Uint64("nodes", 0, "the number of nodes in the cluster: 1, 3 or 5")
	dirs, code, ok := c.positional(flags, args)
	if !ok {
		return code
	}
	switch {
	case len(dirs) != 1 || dirs[0] == "":
		return c.misuse(flags, "want one directory")
	case *nodes != 1 && *nodes != 3 && *nodes != 5:
		return c.misuse(flags, "-nodes: want 1, 3 or 5")
	case *id < 1 || *id > *nodes:
		return c.misuse(flags, "-id: want a number from 1 to %d", *nodes)
	}
	if err := c.clusterEnv(); err != nil {
		fmt.Fprintln(c.stderr, "ledgerctl:", err)
		return 2
	}
	dir, err := filepath.Abs(dirs[0])
	if err != nil {
		fmt.Fprintln(c.stderr, "ledgerctl: init:", err)
		return 1
	}
	ident := wal.Identity{Cluster: c.cluster, Node: *id, Nodes: *nodes}
	err = replica.Init(disk.OS{}, dir, ident)
	switch {
	case errors.Is(err, replica.ErrInitialised):
		fmt.Fprintf(c.stderr, "ledgerctl: init: %s is already initialised, nothing was changed\n", dir)
		return 2
	case errors.Is(err, replica.ErrNotEmpty):
		fmt.Fprintf(c.stderr, "ledgerctl: init: init takes only a directory that no node has used: %v\n", err)
		return 2
	case errors.Is(err, disk.ErrLocked):
		fmt.Fprintf(c.stderr, "ledgerctl: init: %s is locked by a running node\n", dir)
		return 2
	case err != nil:
		fmt.Fprintln(c.stderr, "ledgerctl: init:", err)
		return 1
	}
	fmt.Fprintf(c.stdout, "initialised %s as %s\n", dir, ident)
	fmt.Fprintf(c.stdout, "start it only if node %d has never run in cluster %d: a node that ran has voted and acknowledged entries, and this directory knows none of that\n", *id, c.cluster)
	return 0
}
