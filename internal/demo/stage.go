//go:build linux

package demo

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	mrand "math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/Demonisreal/dmn-ledger/internal/check"
	"github.com/Demonisreal/dmn-ledger/internal/client"
	"github.com/Demonisreal/dmn-ledger/internal/history"
	"github.com/Demonisreal/dmn-ledger/internal/local"
	"github.com/Demonisreal/dmn-ledger/internal/sim"
	"github.com/Demonisreal/dmn-ledger/ledger"
)

const (
	attempts     = 5
	electWithin  = 10 * time.Second
	dialWithin   = 10 * time.Second
	judgedWithin = 30 * time.Second
	loopAlive    = 60 * time.Second
	sightAlive   = 10 * time.Second
)

var nodeEnv = []string{"LEDGER_MAX_CLIENTS=16", "GOMEMLIMIT=64MiB"}

type Config struct {
	Dir     string
	Ledgerd string
	Log     *log.Logger
}

type pace struct {
	holds              [Flip + 1]time.Duration
	light, full        time.Duration
	rest, stand, retry time.Duration
	gap, calm, per     time.Duration
	pause              time.Duration
	limit              int
}

var standard = pace{
	holds: [Flip + 1]time.Duration{Kill: 2 * time.Second, Freeze: 5 * time.Second, Cut: 6 * time.Second},
	light: 5 * time.Second,
	full:  60 * time.Second,
	rest:  30 * time.Second,
	stand: 30 * time.Second,
	retry: time.Second,
	gap:   10 * time.Second,
	calm:  4 * time.Second,
	per:   30 * time.Second,
	pause: time.Second,
	limit: 20_000,
}

type Stage struct {
	cfg    Config
	pace   pace
	issue  func(ctx context.Context, cl *ledger.Client, op client.Op, body []byte) (client.Reply, error)
	during func(Kind)
	wishes chan wish

	mu       sync.Mutex
	st       State
	journal  journal
	cur      *run
	sight    sight
	acting   bool
	began    time.Time
	ended    time.Time
	visits   []visit
	watched  time.Time
	resting  bool
	clusters int
	earlier  int
	beats    struct{ loop, sight time.Time }
}

func New(cfg Config) *Stage {
	now := time.Now()
	s := &Stage{cfg: cfg, pace: standard, issue: history.Issue, wishes: make(chan wish, 1), watched: now}
	s.st = State{Started: now.UnixMilli(), Phase: "setup", Load: "held", Totals: Totals{Findings: []Finding{}}, Recent: []Booking{}}
	s.beats.loop, s.beats.sight = now, now
	return s
}

type run struct {
	n      int
	id     uint64
	why    string
	began  time.Time
	keys   []ed25519.PrivateKey
	seed   uint64
	rng    *mrand.Rand
	ctx    context.Context
	cancel context.CancelFunc
	c      *local.Cluster
	rec    *history.Recorder
	judge  context.CancelFunc
	judged chan struct{}
	load   []*ledger.Client
	teller *ledger.Client
	wg     sync.WaitGroup

	ready    bool
	found    *Finding
	broke    error
	sent     []sent
	marks    []mark
	provable uint64
	latest   uint64
	calls    map[int]sim.Call
	sunk     int
	sealed   []sealed
	ruled    int
	upto     uint64
	awake    chan struct{}
	limit    chan struct{}
	ears     [3]ear
	fresh    chan struct{}
	pids     [3]int
	up       [3]time.Time
	frozen   [3]bool
	cut      map[local.Route]bool
	notes    [3]Note
	flipping int
	asked    [3]time.Time
}

func (r *run) since() uint64 {
	return uint64(time.Since(r.began)) //nolint:gosec // G115: a cluster lives for minutes
}

func (s *Stage) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	wg.Go(func() { s.watch(ctx) })
	defer func() {
		cancel()
		wg.Wait()
	}()
	why := "start"
	for {
		r, err := s.raise(ctx, why)
		if r == nil {
			return err
		}
		why = s.stage(ctx, r)
		s.quiet(r)
		if s.finding(r) != nil {
			why = "finding"
			s.stand(ctx, r)
		}
		s.lower(r)
		if ctx.Err() != nil {
			return nil
		}
	}
}

func (s *Stage) raise(ctx context.Context, why string) (*run, error) {
	wait := s.pace.retry
	for try := 1; ; try++ {
		r, err := s.setup(ctx, why)
		if err == nil {
			return r, nil
		}
		if ctx.Err() != nil {
			return nil, nil
		}
		s.report("demo", fmt.Sprintf(saySetupFailed, try, attempts, err))
		s.change(func(st *State) { st.Cluster.Failed = err.Error() })
		if try == attempts {
			return nil, fmt.Errorf("demo: gave up after %d attempts to set up a cluster: %w", try, err)
		}
		if !sleep(ctx, wait) {
			return nil, nil
		}
		wait *= 2
	}
}

func (s *Stage) setup(ctx context.Context, why string) (*run, error) {
	s.beat()
	dir := filepath.Join(s.cfg.Dir, "run")
	if err := os.RemoveAll(dir); err != nil {
		return nil, err
	}
	if err := os.Mkdir(dir, 0o700); err != nil {
		return nil, err
	}
	r, err := s.prepare(ctx, dir, why)
	if err == nil {
		err = s.boot(r)
		if err != nil {
			s.quiet(r)
			s.lower(r)
		}
	}
	if err != nil {
		_ = os.RemoveAll(dir)
		if broke := s.broke(r); broke != nil {
			return nil, broke
		}
		return nil, err
	}
	return r, nil
}

func (s *Stage) broke(r *run) error {
	if r == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return r.broke
}

func (s *Stage) prepare(ctx context.Context, dir, why string) (*run, error) {
	var seeds [3]uint64
	for i := range seeds {
		for seeds[i] == 0 {
			var b [8]byte
			_, _ = rand.Read(b[:])
			seeds[i] = binary.LittleEndian.Uint64(b[:])
		}
	}
	r := &run{
		why:    why,
		began:  time.Now(),
		id:     seeds[0],
		seed:   seeds[2],
		rng:    mrand.New(mrand.NewPCG(seeds[2], 0)), //nolint:gosec // G404: the load and the flipped bit need no secret
		judged: make(chan struct{}),
		calls:  map[int]sim.Call{},
		awake:  make(chan struct{}),
		limit:  make(chan struct{}, 1),
		fresh:  make(chan struct{}),
		cut:    map[local.Route]bool{},
	}
	for range 3 {
		_, key, err := ed25519.GenerateKey(nil)
		if err != nil {
			return nil, err
		}
		r.keys = append(r.keys, key)
	}
	r.ctx, r.cancel = context.WithCancel(ctx)
	r.rec = history.NewRecorder(history.Options{Window: window, Backlog: backlog, Since: r.since, Sink: func(calls []sim.Call) error {
		s.sink(r, calls)
		return nil
	}}, check.NewReplayer())
	r.judge = s.startJudge(ctx, r)
	s.mu.Lock()
	s.clusters++
	r.n = s.clusters
	s.mu.Unlock()
	c, err := local.Start(local.Config{
		Bin:     s.cfg.Ledgerd,
		Dir:     dir,
		Cluster: r.id,
		Env:     nodeEnv,
		Keys:    r.keys,
		Seed:    seeds[1],
		Stderr:  func(id int) io.Writer { return &tap{s: s, r: r, id: id} },
		Exited:  func(id int, err error) { s.exited(r, id, err) },
	})
	if err != nil {
		s.quiet(r)
		return nil, err
	}
	r.c = c
	now := time.Now()
	s.mu.Lock()
	for id := 1; id <= 3; id++ {
		r.pids[id-1], r.up[id-1] = c.Node(id).Pid(), now
	}
	if !s.resting {
		close(r.awake)
	}
	s.cur, s.sight = r, sight{}
	s.st.Phase, s.st.Load, s.st.Finding, s.st.Action = "setup", "held", nil, nil
	s.st.Cluster = Cluster{N: r.n, ID: fmt.Sprintf("%016x", r.id), Since: now.UnixMilli(), Why: why, Limit: s.pace.limit, Failed: s.st.Cluster.Failed}
	s.st.Books = Books{}
	s.st.Recent = []Booking{}
	s.st.Rev++
	s.draw(r)
	s.mu.Unlock()
	return r, nil
}

func (s *Stage) boot(r *run) error {
	if err := s.elect(r); err != nil {
		return err
	}
	s.beat()
	if err := s.dial(r); err != nil {
		return err
	}
	s.beat()
	s.mu.Lock()
	pids := r.pids
	s.mu.Unlock()
	s.report("demo", fmt.Sprintf(saySetup, r.n, s.reason(r.why), r.id, r.id, pids[0], pids[1], pids[2]))
	if err := s.fund(r); err != nil {
		return err
	}
	s.say("demo", fmt.Sprintf(sayFunded, money(fmt.Sprint(funding))))
	if !s.count(r, true, 0) {
		return errors.New("demo: the first count failed")
	}
	s.mu.Lock()
	r.ready = true
	s.st.Phase, s.st.Load = "steady", "running"
	if s.resting {
		s.st.Load = "resting"
	}
	s.st.Cluster.Failed = ""
	s.st.Totals.Clusters++
	s.st.Rev++
	s.mu.Unlock()
	for k, cl := range r.load {
		rng := mrand.New(mrand.NewPCG(r.seed, uint64(k+1))) //nolint:gosec // G404: the load needs no secret
		r.wg.Go(func() { s.drive(r, k+1, cl, rng) })
	}
	return nil
}

func (s *Stage) reason(why string) string {
	switch why {
	case "limit":
		return fmt.Sprintf(whyLimit, num(s.pace.limit))
	case "finding":
		return whyFinding
	}
	return whyStart
}

func (s *Stage) elect(r *run) error {
	ctx, cancel := context.WithTimeout(r.ctx, electWithin)
	defer cancel()
	for {
		sts := r.c.Statuses(ctx)
		if agreed(sts) {
			return nil
		}
		if !sleep(ctx, agreeEvery) {
			return fmt.Errorf("demo: no leader all three agree on within %v", electWithin)
		}
	}
}

func agreed(sts []*ledger.NodeStatus) bool {
	for _, st := range sts {
		if st == nil || st.Leader == 0 || st.Leader != sts[0].Leader || st.Term != sts[0].Term || st.ID == st.Leader && st.Role != ledger.Leader {
			return false
		}
	}
	return true
}

func (s *Stage) startJudge(ctx context.Context, r *run) context.CancelFunc {
	ctx, cancel := context.WithCancel(ctx)
	go func() {
		defer close(r.judged)
		err := r.rec.Judge(ctx)
		if v, ok := errors.AsType[*check.Violation](err); ok {
			s.fail(r, v.Check, v.Reason)
		} else if err != nil && ctx.Err() == nil {
			s.fail(r, "replay", fmt.Sprintf(failJudge, err))
		}
	}()
	return cancel
}

func (s *Stage) dial(r *run) error {
	ctx, cancel := context.WithTimeout(r.ctx, dialWithin)
	defer cancel()
	for range clients + 1 {
		cl, err := ledger.Dial(ctx, ledger.Config{Cluster: r.id, Nodes: r.c.Addrs()})
		if err != nil {
			return err
		}
		r.load = append(r.load, cl)
	}
	r.teller, r.load = r.load[clients], r.load[:clients]
	return nil
}

func (s *Stage) stage(ctx context.Context, r *run) string {
	light := time.NewTicker(s.pace.light)
	defer light.Stop()
	full := time.NewTicker(s.pace.full)
	defer full.Stop()
	beat := time.NewTicker(time.Second)
	defer beat.Stop()
	for {
		s.beat()
		select {
		case <-r.ctx.Done():
			return ""
		case w := <-s.wishes:
			if r.ctx.Err() == nil {
				s.act(r, w)
			}
		case <-r.limit:
			s.change(func(st *State) { st.Phase = "setup" })
			s.say("check", fmt.Sprintf(sayLimit, num(s.pace.limit)))
			if s.count(r, true, 0) {
				return "limit"
			}
		case <-light.C:
			if !s.rests() {
				s.count(r, false, 0)
			}
		case <-full.C:
			if !s.rests() {
				s.count(r, true, 0)
			}
		case <-beat.C:
			if s.tires(r) {
				s.say("demo", sayRest)
				s.count(r, true, 0)
			}
		}
		if ctx.Err() != nil {
			return ""
		}
	}
}

func (s *Stage) rests() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.resting
}

func (s *Stage) tires(r *run) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.resting || time.Since(s.watched) < s.pace.rest {
		return false
	}
	s.resting = true
	r.awake = make(chan struct{})
	s.st.Load = "resting"
	s.st.Rest = Rest{Since: time.Now().UnixMilli()}
	s.st.Rev++
	return true
}

func (s *Stage) quiet(r *run) {
	r.cancel()
	r.wg.Wait()
	r.rec.Close()
	tm := time.NewTimer(judgedWithin)
	defer tm.Stop()
	select {
	case <-r.judged:
	case <-tm.C:
	}
	r.judge()
	<-r.judged
}

func (s *Stage) lower(r *run) {
	for _, cl := range r.load {
		_ = cl.Close()
	}
	if r.teller != nil {
		_ = r.teller.Close()
	}
	_ = r.c.Close()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.earlier = s.st.Totals.Windows
	r.pids = [3]int{}
	s.draw(r)
	if s.acting {
		select {
		case <-s.wishes:
		default:
		}
		s.acting, s.ended = false, time.Now()
	}
	s.st.Phase = "setup"
	s.st.Rev++
}

func (s *Stage) beat() {
	s.mu.Lock()
	s.beats.loop = time.Now()
	s.mu.Unlock()
}

func (s *Stage) change(f func(st *State)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	f(&s.st)
	s.st.Rev++
}

func (s *Stage) say(src, text string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.journal.add(time.Now(), src, text)
}

func (s *Stage) report(src, text string) {
	s.say(src, text)
	s.cfg.Log.Print(text)
}

func (s *Stage) State() State {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.st
	now := time.Now()
	st.Now = now.UnixMilli()
	if until := s.cooled(); now.Before(until) {
		st.Next = until.UnixMilli()
	}
	if st.Action != nil {
		a := *st.Action
		st.Action = &a
	}
	if st.Finding != nil {
		f := *st.Finding
		st.Finding = &f
	}
	st.Recent = slices.Clone(st.Recent)
	st.Totals.Findings = slices.Clone(st.Totals.Findings)
	return st
}

func (s *Stage) Lines(after uint64) []Line {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.journal.after(after)
}

func (s *Stage) Watching(streams int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if streams != s.st.Viewers {
		s.st.Viewers = streams
		s.st.Rev++
	}
	if streams == 0 {
		return
	}
	s.watched = time.Now()
	if !s.resting {
		return
	}
	s.resting = false
	s.st.Rest.Until = s.watched.UnixMilli()
	if s.st.Load == "resting" {
		s.st.Load = "running"
	}
	s.st.Rev++
	if r := s.cur; r != nil {
		close(r.awake)
	}
}

func (s *Stage) Alive() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return time.Since(s.beats.loop) < loopAlive && time.Since(s.beats.sight) < sightAlive
}

func sleep(ctx context.Context, d time.Duration) bool {
	tm := time.NewTimer(d)
	defer tm.Stop()
	select {
	case <-tm.C:
		return true
	case <-ctx.Done():
		return false
	}
}
