package ledgerrun

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/Demonisreal/dmn-ledger/bench/load"
	"github.com/Demonisreal/dmn-ledger/bench/raw"
	"github.com/Demonisreal/dmn-ledger/ledger"
)

var (
	ErrNode       = errors.New("ledgerrun: node failed")
	ErrNoLeader   = errors.New("ledgerrun: no leader")
	ErrNoFollower = errors.New("ledgerrun: no follower")
	ErrFilesystem = errors.New("ledgerrun: data directory not on ext4")
	ErrLeftover   = errors.New("ledgerrun: run directory not empty")
	ErrRefused    = errors.New("ledgerrun: refused")
	ErrStats      = errors.New("ledgerrun: bad stats line")
	ErrBooks      = errors.New("ledgerrun: books do not add up")
	ErrBudget     = errors.New("ledgerrun: stage does not fit the budget")
	ErrBehind     = errors.New("ledgerrun: a node fell behind")
	ErrDiverged   = errors.New("ledgerrun: nodes disagree at the same index")
)

const (
	cluster      = 1
	chunk        = 10_000
	endowed      = 1 << 63
	filler       = 1 << 62
	ext4         = 0xef53
	readyWithin  = 10 * time.Second
	levelWithin  = 5 * time.Second
	stopWithin   = 10 * time.Second
	sampleWithin = 500 * time.Millisecond
)

type Config struct {
	Bin      string
	Ctl      string
	Dir      string
	Nodes    int
	Lanes    int
	Pin      bool
	Env      []string
	Profiles string
	Budget   uint64
}

type Cluster struct {
	cfg      Config
	run      string
	env      map[string]string
	nodes    []*node
	sessions []*ledger.Client
	w        load.Workload
	budget   uint64
	written  atomic.Uint64
	filled   uint64
	mu       sync.Mutex
	books    load.Books
}

func Start(ctx context.Context, cfg Config) (*Cluster, error) {
	if cfg.Nodes != 1 && cfg.Nodes != 3 || cfg.Lanes < 1 {
		return nil, fmt.Errorf("ledgerrun: %d nodes and %d lanes, want 1 or 3 nodes and at least one lane", cfg.Nodes, cfg.Lanes)
	}
	dir, err := filepath.Abs(cfg.Dir)
	if err != nil {
		return nil, err
	}
	var fs syscall.Statfs_t
	if err := syscall.Statfs(dir, &fs); err != nil {
		return nil, err
	}
	if fs.Type != ext4 {
		return nil, fmt.Errorf("%w: %s has file system %#x", ErrFilesystem, dir, fs.Type)
	}
	limit := "4500MiB"
	if cfg.Nodes == 3 {
		limit = "1500MiB"
	}
	c := &Cluster{cfg: cfg, run: filepath.Join(dir, "run"), budget: cmp.Or(cfg.Budget, Budget(cfg.Nodes))}
	if left, _ := os.ReadDir(c.run); len(left) > 0 {
		return nil, fmt.Errorf("%w: %s holds %d entries", ErrLeftover, c.run, len(left))
	}
	c.env = map[string]string{
		"LEDGER_CLUSTER":        strconv.Itoa(cluster),
		"LEDGER_SNAPSHOT_EVERY": "4294967296",
		"LEDGER_SWEEP":          "on",
		"LEDGER_MAX_CLIENTS":    "128",
		"GOMEMLIMIT":            limit,
	}
	for _, kv := range cfg.Env {
		name, given, _ := strings.Cut(kv, "=")
		c.env[name] = given
	}
	if err := c.boot(ctx); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *Cluster) boot(ctx context.Context) error {
	if err := os.MkdirAll(c.run, 0o750); err != nil {
		return err
	}
	err := c.launch(ctx)
	if err != nil {
		err = errors.Join(err, c.halt(), os.RemoveAll(c.run))
	}
	return err
}

func (c *Cluster) launch(ctx context.Context) error {
	ports, err := freePorts(2 * c.cfg.Nodes)
	if err != nil {
		return err
	}
	peers, clients := ports[:c.cfg.Nodes], ports[c.cfg.Nodes:]
	for i := range c.cfg.Nodes {
		n := &node{id: i + 1, addr: clients[i]}
		n.log = filepath.Join(c.run, n.name()+".log")
		n.dir = filepath.Join(c.run, n.name())
		env := []string{
			"LEDGER_ID=" + strconv.Itoa(n.id),
			"LEDGER_PEERS=" + strings.Join(peers, ","),
			"LEDGER_CLIENT_ADDR=" + n.addr,
			"LEDGER_DIR=" + n.dir,
		}
		if c.cfg.Profiles != "" {
			env = append(env, "LEDGER_PROFILE="+filepath.Join(c.cfg.Profiles, n.name()+".prof"))
		}
		for name, given := range c.env {
			env = append(env, name+"="+given)
		}
		slices.Sort(env)
		n.env = env
		if c.cfg.Pin {
			n.cores = cpus(i)
		}
		cmd := exec.CommandContext(ctx, c.cfg.Ctl, "init", "-id", strconv.Itoa(n.id), "-nodes", strconv.Itoa(c.cfg.Nodes), n.dir) //nolint:gosec // G204: the ledgerctl that Build made
		cmd.Env = []string{"LEDGER_CLUSTER=" + c.env["LEDGER_CLUSTER"]}
		if msg, err := cmd.CombinedOutput(); err != nil {
			return fmt.Errorf("init %s: %w: %s", n.name(), err, bytes.TrimSpace(msg))
		}
		if err := n.start(c.cfg.Bin); err != nil {
			return fmt.Errorf("start %s: %w", n.name(), err)
		}
		c.nodes = append(c.nodes, n)
	}
	return c.ready(ctx)
}

func cpus(node int) string {
	return fmt.Sprintf("%d,%d", 2*node, 2*node+1)
}

func freePorts(n int) ([]string, error) {
	addrs := make([]string, n)
	for i := range addrs {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return nil, err
		}
		defer l.Close()
		addrs[i] = l.Addr().String()
	}
	return addrs, nil
}

func (c *Cluster) ready(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, readyWithin)
	defer cancel()
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	nodes := c.running()
	for {
		sts, err := c.statuses(ctx)
		if err == nil && agreed(sts) {
			for i, n := range nodes {
				n.status = sts[i]
			}
			return nil
		}
		for _, n := range nodes {
			select {
			case <-n.exited:
				return n.failure("ended while starting")
			default:
			}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%w that all %d nodes agree on: %w", ErrNoLeader, len(nodes), errors.Join(context.Cause(ctx), err))
		case <-tick.C:
		}
	}
}

func agreed(sts []ledger.NodeStatus) bool {
	lead, term := sts[0].Leader, sts[0].Term
	if lead == 0 {
		return false
	}
	for _, st := range sts {
		if st.Leader != lead || st.Term != term || st.ID == lead && st.Role != ledger.Leader {
			return false
		}
	}
	return true
}

func (c *Cluster) statuses(ctx context.Context) ([]ledger.NodeStatus, error) {
	ctx, cancel := context.WithTimeout(ctx, sampleWithin)
	defer cancel()
	nodes := c.running()
	sts := make([]ledger.NodeStatus, len(nodes))
	errs := make([]error, len(nodes))
	var wg sync.WaitGroup
	for i, n := range nodes {
		wg.Go(func() { sts[i], errs[i] = ledger.Status(ctx, cluster, n.addr) })
	}
	wg.Wait()
	return sts, errors.Join(errs...)
}

func (c *Cluster) Follower(ctx context.Context) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, readyWithin)
	defer cancel()
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		sts, err := c.statuses(ctx)
		var id uint64
		for _, st := range sts {
			if st.ID != 0 && st.Role == ledger.Follower && st.Leader != 0 && st.Leader != st.ID {
				id = max(id, st.ID)
			}
		}
		if id > 0 {
			return int(id), nil //nolint:gosec // G115: a node id from 1 to 3
		}
		select {
		case <-ctx.Done():
			return 0, fmt.Errorf("%w among %d nodes: %w", ErrNoFollower, len(sts), errors.Join(context.Cause(ctx), err))
		case <-tick.C:
		}
	}
}

func (c *Cluster) Setup(ctx context.Context, w load.Workload) error {
	c.w = w
	c.mu.Lock()
	c.books = load.Books{}
	c.mu.Unlock()
	addrs := make([]string, len(c.nodes))
	for i, n := range c.nodes {
		addrs[i] = n.addr
	}
	for range c.cfg.Lanes {
		s, err := ledger.Dial(ctx, ledger.Config{Cluster: cluster, Nodes: addrs})
		if err != nil {
			return fmt.Errorf("dial a session: %w", err)
		}
		c.sessions = append(c.sessions, s)
	}
	bank := w.Bank()
	accounts := make([]ledger.Account, 0, chunk)
	for id := uint64(1); id <= bank; id++ {
		a := ledger.Account{ID: ledger.From64(id), Ledger: load.Ledger, Code: load.Code, Flags: ledger.DebitsMustNotExceedCredits}
		if id == bank {
			a.Flags = 0
		}
		accounts = append(accounts, a)
		if len(accounts) < chunk && id < bank {
			continue
		}
		if err := refused(c.sessions[0].CreateAccounts(ctx, accounts)); err != nil {
			return fmt.Errorf("create accounts: %w", err)
		}
		accounts = accounts[:0]
	}
	transfers := make([]ledger.Transfer, 0, chunk)
	for id := uint64(1); id < bank; id++ {
		transfers = append(transfers, ledger.Transfer{
			ID:              ledger.Uint128{Hi: endowed, Lo: id},
			DebitAccountID:  ledger.From64(bank),
			CreditAccountID: ledger.From64(id),
			Amount:          ledger.From64(load.Endowment),
			Ledger:          load.Ledger,
			Code:            load.Code,
		})
		if len(transfers) < chunk && id < bank-1 {
			continue
		}
		c.written.Add(uint64(len(transfers)))
		if err := refused(c.sessions[0].CreateTransfers(ctx, transfers)); err != nil {
			return fmt.Errorf("endow accounts: %w", err)
		}
		transfers = transfers[:0]
	}
	return nil
}

func (c *Cluster) Fill(ctx context.Context, n uint64) error {
	accounts := uint64(c.w.Accounts) //nolint:gosec // G115: a count of accounts, never negative
	transfers := make([]ledger.Transfer, 0, chunk)
	ops := make([]load.Op, 0, chunk)
	for left := n; left > 0; left -= uint64(len(transfers)) {
		transfers, ops = transfers[:0], ops[:0]
		for range min(left, chunk) {
			c.filled++
			op := load.Op{Kind: load.Single, ID: ledger.Uint128{Hi: filler, Lo: c.filled}, Debit: 1 + c.filled%accounts, Credit: 1 + (c.filled+1)%accounts, Amount: 1}
			ops = append(ops, op)
			transfers = append(transfers, transfer(op))
		}
		c.written.Add(uint64(len(transfers)))
		res, err := c.sessions[0].CreateTransfers(ctx, transfers)
		c.mu.Lock()
		for _, op := range ops {
			c.books.Count(op, load.Outcome{}, err)
		}
		c.mu.Unlock()
		if err := refused(res, err); err != nil {
			return fmt.Errorf("fill the ledger: %w", err)
		}
	}
	return nil
}

func refused(res ledger.Result, err error) error {
	if err != nil || len(res.Failures) == 0 {
		return err
	}
	f := res.Failures[0]
	return fmt.Errorf("%w: %s at %d, %d failures", ErrRefused, f.Reason, f.Index, len(res.Failures))
}

func (c *Cluster) Do(ctx context.Context, lane int, op load.Op) (load.Outcome, error) {
	c.written.Add(1)
	res, err := c.sessions[lane].CreateTransfers(ctx, []ledger.Transfer{transfer(op)})
	var out load.Outcome
	switch {
	case err != nil:
	case len(res.Failures) == 0:
	case res.Failures[0].Reason == ledger.Exists:
		out.Existed = true
	default:
		return out, fmt.Errorf("%w: %s %v: %s", ErrRefused, op.Kind, op.ID, res.Failures[0].Reason)
	}
	c.mu.Lock()
	c.books.Count(op, out, err)
	c.mu.Unlock()
	return out, err
}

func transfer(op load.Op) ledger.Transfer {
	tr := ledger.Transfer{
		ID:              op.ID,
		DebitAccountID:  ledger.From64(op.Debit),
		CreditAccountID: ledger.From64(op.Credit),
		Amount:          ledger.From64(op.Amount),
		PendingID:       op.Pending,
		Ledger:          load.Ledger,
		Code:            load.Code,
	}
	switch op.Kind {
	case load.Reserve:
		tr.Flags, tr.Timeout = ledger.Pending, load.ReserveTimeout
	case load.Post:
		tr.Flags = ledger.PostPending
	case load.Void:
		tr.Flags = ledger.VoidPending
	}
	return tr
}

func (c *Cluster) Sample(ctx context.Context) (load.Inside, error) {
	ctx, cancel := context.WithTimeout(ctx, sampleWithin)
	defer cancel()
	nodes := c.running()
	parts := make([]load.Part, len(nodes))
	errs := make([]error, len(nodes))
	var wg sync.WaitGroup
	for i, n := range nodes {
		wg.Go(func() { parts[i], errs[i] = n.sample(ctx) })
	}
	wg.Wait()
	var inside load.Inside
	for i := range nodes {
		if errs[i] == nil {
			inside = append(inside, parts[i])
		}
	}
	return inside, errors.Join(errs...)
}

func (c *Cluster) running() []*node {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.DeleteFunc(slices.Clone(c.nodes), func(n *node) bool { return n.stopped })
}

func (c *Cluster) halted(n *node) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return n.stopped
}

func (c *Cluster) mark(n *node, stopped bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	n.stopped = stopped
}

func (c *Cluster) node(id int) (*node, error) {
	if id < 1 || id > len(c.nodes) {
		return nil, fmt.Errorf("ledgerrun: no node %d of %d", id, len(c.nodes))
	}
	return c.nodes[id-1], nil
}

func (c *Cluster) Stop(id int) error {
	n, err := c.node(id)
	if err != nil || c.halted(n) {
		return err
	}
	c.mark(n, true)
	if err := n.stop(); err != nil {
		return err
	}
	fi, err := os.Stat(n.log)
	if err != nil {
		return err
	}
	n.logged = fi.Size()
	return nil
}

func (c *Cluster) Received(id int) (uint64, error) {
	n, err := c.node(id)
	if err != nil {
		return 0, err
	}
	return received(n.log, n.logged)
}

func (c *Cluster) Restart(ctx context.Context, id int) error {
	n, err := c.node(id)
	if err != nil || !c.halted(n) {
		return err
	}
	if err := n.start(c.cfg.Bin); err != nil {
		return fmt.Errorf("start %s: %w", n.name(), err)
	}
	if err := n.ready(ctx); err != nil {
		return err
	}
	c.mark(n, false)
	return nil
}

func (c *Cluster) Pids() []int {
	var pids []int
	for _, n := range c.running() {
		pids = append(pids, n.cmd.Process.Pid)
	}
	return pids
}

func (c *Cluster) Describe() raw.Target {
	env := make(map[string]string, len(c.env))
	for name, given := range c.env {
		if filepath.IsAbs(given) {
			given = filepath.Base(given)
		}
		env[name] = given
	}
	var cores []string
	for i := range c.cfg.Nodes {
		if c.cfg.Pin {
			cores = append(cores, cpus(i))
		}
	}
	return raw.Target{Kind: "ledger", Nodes: c.cfg.Nodes, Lanes: c.cfg.Lanes, Cores: strings.Join(cores, " "), Env: env}
}

func (c *Cluster) Close() error {
	c.hangUp()
	return errors.Join(c.halt(), os.RemoveAll(c.run))
}

func (c *Cluster) hangUp() {
	for _, s := range c.sessions {
		_ = s.Close()
	}
	c.sessions = nil
}

func (c *Cluster) halt() error {
	nodes := c.running()
	errs := make([]error, len(nodes))
	var wg sync.WaitGroup
	for i, n := range nodes {
		wg.Go(func() { errs[i] = n.stop() })
	}
	wg.Wait()
	c.nodes = nil
	return errors.Join(errs...)
}
