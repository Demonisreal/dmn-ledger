//go:build linux

package local

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/Demonisreal/dmn-ledger/internal/disk"
	"github.com/Demonisreal/dmn-ledger/internal/faultnet"
	"github.com/Demonisreal/dmn-ledger/internal/notary"
	"github.com/Demonisreal/dmn-ledger/internal/replica"
	"github.com/Demonisreal/dmn-ledger/internal/wal"
	"github.com/Demonisreal/dmn-ledger/ledger"
)

const nodes = 3

var ErrRunning = errors.New("local: node is running")

type Config struct {
	Bin     string
	Dir     string
	Cluster uint64
	Env     []string
	Keys    []ed25519.PrivateKey
	Clients bool
	Seed    uint64
	Stderr  func(node int) io.Writer
	Exited  func(node int, err error)
}

type Cluster struct {
	cfg    Config
	nodes  []*Node
	addrs  []string
	doors  []string
	keys   [][]byte
	routes []Route
	links  map[Route]*faultnet.Link
	logs   []*os.File
	closed bool
}

func Start(cfg Config) (*Cluster, error) {
	ports, err := freePorts(cfg.Seed, 2*nodes)
	if err != nil {
		return nil, err
	}
	c := &Cluster{cfg: cfg, routes: routes(cfg.Clients), links: map[Route]*faultnet.Link{}}
	if err := c.boot(ports[:nodes], ports[nodes:]); err != nil {
		return nil, errors.Join(err, c.Close())
	}
	return c, nil
}

func (c *Cluster) boot(peers, addrs []string) error {
	c.addrs, c.doors = addrs, addrs
	for i, r := range c.routes {
		target := addrs[r.To-1]
		if r.From != 0 {
			target = peers[r.To-1]
		}
		l, err := faultnet.Listen(target, rand.New(rand.NewPCG(c.cfg.Seed, 100+uint64(i)))) //nolint:gosec // G404: the faults of a link replay from the seed
		if err != nil {
			return err
		}
		c.links[r] = l
	}
	if c.cfg.Clients {
		c.doors = nil
		for id := 1; id <= nodes; id++ {
			c.doors = append(c.doors, c.links[Route{0, id}].Addr())
		}
	}
	if c.cfg.Stderr == nil {
		for id := 1; id <= nodes; id++ {
			logf, err := os.OpenFile(filepath.Join(c.cfg.Dir, fmt.Sprintf("n%d.log", id)), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
			if err != nil {
				return err
			}
			c.logs = append(c.logs, logf)
		}
		c.cfg.Stderr = func(node int) io.Writer { return c.logs[node-1] }
	}
	for id := 1; id <= nodes; id++ {
		list := make([]string, nodes)
		for to := 1; to <= nodes; to++ {
			list[to-1] = peers[to-1]
			if to != id {
				list[to-1] = c.links[Route{id, to}].Addr()
			}
		}
		n := &Node{c: c, id: id, dir: filepath.Join(c.cfg.Dir, fmt.Sprintf("n%d", id))}
		n.env = []string{
			"LEDGER_ID=" + strconv.Itoa(id),
			"LEDGER_CLUSTER=" + strconv.FormatUint(c.cfg.Cluster, 10),
			"LEDGER_PEERS=" + strings.Join(list, ","),
			"LEDGER_CLIENT_ADDR=" + addrs[id-1],
			"LEDGER_DIR=" + n.dir,
		}
		if c.cfg.Keys != nil {
			key := c.cfg.Keys[id-1]
			keyFile := filepath.Join(c.cfg.Dir, fmt.Sprintf("n%d.key", id))
			if err := notary.WriteKey(keyFile, key); err != nil {
				return err
			}
			c.keys = append(c.keys, key.Public().(ed25519.PublicKey))
			n.env = append(n.env, "LEDGER_KEY_FILE="+keyFile)
		}
		n.env = append(n.env, c.cfg.Env...)
		if err := replica.Init(disk.OS{}, n.dir, wal.Identity{Cluster: c.cfg.Cluster, Node: uint64(id), Nodes: nodes}); err != nil {
			return err
		}
		c.nodes = append(c.nodes, n)
		if err := n.Start(); err != nil {
			return err
		}
	}
	return nil
}

func freePorts(seed uint64, n int) ([]string, error) {
	var addrs []string
	for port := 20000 + int(seed%10000); len(addrs) < n && port < 32768; port++ {
		addr := "127.0.0.1:" + strconv.Itoa(port)
		if l, err := net.Listen("tcp", addr); err == nil {
			_ = l.Close()
			addrs = append(addrs, addr)
		}
	}
	if len(addrs) < n {
		return nil, fmt.Errorf("local: found %d free ports below 32768, want %d", len(addrs), n)
	}
	return addrs, nil
}

func (c *Cluster) Node(id int) *Node {
	return c.nodes[id-1]
}

func (c *Cluster) Addrs() []string {
	return c.addrs
}

func (c *Cluster) Doors() []string {
	return c.doors
}

func (c *Cluster) Keys() [][]byte {
	return c.keys
}

func (c *Cluster) Link(r Route) *faultnet.Link {
	return c.links[r]
}

func (c *Cluster) Statuses(ctx context.Context) []*ledger.NodeStatus {
	sts := make([]*ledger.NodeStatus, len(c.addrs))
	var wg sync.WaitGroup
	for i, addr := range c.addrs {
		wg.Go(func() {
			if st, err := ledger.Status(ctx, c.cfg.Cluster, addr); err == nil {
				sts[i] = &st
			}
		})
	}
	wg.Wait()
	return sts
}

func (c *Cluster) Leader(ctx context.Context) int {
	id, term := 0, uint64(0)
	for _, st := range c.Statuses(ctx) {
		if st != nil && st.Role == ledger.Leader && st.Term >= term {
			id, term = int(st.ID), st.Term //nolint:gosec // G115: a node ID from 1 to 3
		}
	}
	return id
}

func (c *Cluster) Close() error {
	if c.closed {
		return nil
	}
	c.closed = true
	for _, n := range c.nodes {
		n.Stop()
	}
	var errs []error
	for _, l := range c.links {
		errs = append(errs, l.Close())
	}
	for _, logf := range c.logs {
		errs = append(errs, logf.Close())
	}
	return errors.Join(errs...)
}
