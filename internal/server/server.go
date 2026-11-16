package server

import (
	"context"
	"crypto/ed25519"
	crand "crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"net"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/Demonisreal/dmn-ledger/internal/audit"
	"github.com/Demonisreal/dmn-ledger/internal/client"
	"github.com/Demonisreal/dmn-ledger/internal/disk"
	"github.com/Demonisreal/dmn-ledger/internal/notary"
	"github.com/Demonisreal/dmn-ledger/internal/raft"
	"github.com/Demonisreal/dmn-ledger/internal/replica"
	"github.com/Demonisreal/dmn-ledger/internal/u128"
	"github.com/Demonisreal/dmn-ledger/internal/wire"
)

const (
	heartbeatTicks = 5
	electionTicks  = 50
	snapshotTicks  = 500
	maxAppendBytes = 1 << 20

	eventQueue  = 256
	maxSnapshot = 8 << 30

	recvDir = "recv"
	snapDir = "snap"
)

type Config struct {
	ID            uint64
	Cluster       uint64
	Peers         []string
	Dir           string
	FS            disk.FS
	Tick          time.Duration
	SnapshotEvery uint64
	SegmentSize   int64
	MaxSessions   int
	MaxClients    int
	Sweep         bool
	Key           ed25519.PrivateKey
	Clients       net.Listener
	Peering       net.Listener
	Dial          func(ctx context.Context, addr string) (net.Conn, error)
	Log           *log.Logger
}

type Server struct {
	cfg      Config
	node     *replica.Node
	ticks    ticks
	ticked   uint64
	events   chan event
	fatal    chan error
	slots    chan struct{}
	hellos   chan struct{}
	peers    map[uint64]*peer
	conns    map[u128.Uint128]*clientConn
	spools   []*spool
	asked    []*clientConn
	spoolMax int64
	sweeper  sweeper
	notes    chan note
	role     raft.Role
	term     uint64
	lead     uint64
	repair   bool
	opened   replica.Status
	stamper  [32]byte
	counters counters
	wg       sync.WaitGroup
}

type eventKind uint8

const (
	gotMessage eventKind = iota
	gotSnapshot
	gotRequest
	gotStatus
	connClosed
)

type event struct {
	kind  eventKind
	msg   raft.Message
	spool *spool
	req   client.Request
	conn  *clientConn
}

func Open(cfg Config) (*Server, error) {
	switch {
	case cfg.Cluster == 0:
		return nil, errors.New("server: cluster id must not be 0")
	case cfg.Tick <= 0:
		return nil, errors.New("server: tick must be positive")
	case cfg.MaxClients < 1:
		return nil, errors.New("server: MaxClients must be at least 1")
	case cfg.Key != nil && len(cfg.Key) != ed25519.PrivateKeySize:
		return nil, fmt.Errorf("server: a signing key has %d bytes, not %d", ed25519.PrivateKeySize, len(cfg.Key))
	}
	var seed [16]byte
	if _, err := crand.Read(seed[:]); err != nil {
		return nil, err
	}
	ids := make([]uint64, len(cfg.Peers))
	for i := range ids {
		ids[i] = uint64(i) + 1
	}
	node, err := replica.Open(replica.Config{
		ID:             cfg.ID,
		Peers:          ids,
		FS:             cfg.FS,
		Dir:            cfg.Dir,
		Clock:          wallClock,
		Rand:           rand.New(rand.NewPCG(binary.BigEndian.Uint64(seed[:]), binary.BigEndian.Uint64(seed[8:]))), //nolint:gosec // G404: a PCG seeded from crypto/rand, as the driver contract asks
		MaxSessions:    cfg.MaxSessions,
		SnapshotEvery:  cfg.SnapshotEvery,
		SegmentSize:    cfg.SegmentSize,
		ClockStep:      uint64(cfg.Tick + cfg.Tick/4), //nolint:gosec // G115: the tick is positive
		ElectionTicks:  electionTicks,
		HeartbeatTicks: heartbeatTicks,
		SnapshotTicks:  snapshotTicks,
		MaxAppendBytes: maxAppendBytes,
		MaxUncommitted: 2 * uint64(cfg.MaxClients),
	})
	if err != nil {
		return nil, err
	}
	if err := clearSpools(cfg.FS, filepath.Join(cfg.Dir, recvDir)); err != nil {
		return nil, errors.Join(err, node.Close())
	}
	s := &Server{
		cfg:      cfg,
		node:     node,
		ticks:    ticks{every: cfg.Tick, most: electionTicks - 2*heartbeatTicks},
		events:   make(chan event, eventQueue),
		fatal:    make(chan error, 1),
		slots:    make(chan struct{}, cfg.MaxClients),
		hellos:   make(chan struct{}, hellosOfPeer*(len(cfg.Peers)-1)),
		peers:    make(map[uint64]*peer),
		conns:    make(map[u128.Uint128]*clientConn),
		spoolMax: maxSnapshot,
		sweeper:  sweeper{on: cfg.Sweep, self: cfg.ID},
		notes:    make(chan note, logQueue),
	}
	s.opened = node.Status()
	if cfg.Key != nil {
		s.stamper = [32]byte(cfg.Key.Public().(ed25519.PublicKey))
	}
	s.role, s.term, s.lead, s.repair = s.opened.Role, s.opened.Term, s.opened.Lead, s.opened.Repair
	for i, addr := range cfg.Peers {
		if id := uint64(i) + 1; id != cfg.ID {
			s.peers[id] = &peer{id: id, addr: addr, out: make(chan []byte, peerQueue)}
		}
	}
	return s, nil
}

func (s *Server) Opened() (term uint64, repair bool) {
	return s.opened.Term, s.opened.Repair
}

func wallClock() uint64 {
	return uint64(time.Now().UnixNano()) //nolint:gosec // G115: the wall clock is after 1970
}

func clearSpools(fsys disk.FS, dir string) error {
	if err := fsys.MkdirAll(dir); err != nil {
		return err
	}
	names, err := fsys.List(dir)
	if err != nil {
		return err
	}
	for _, name := range names {
		if err := fsys.Remove(filepath.Join(dir, name)); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	quiet, logged := make(chan struct{}), make(chan struct{})
	if s.cfg.Log != nil {
		go func() {
			defer close(logged)
			s.writeLog(quiet)
		}()
	} else {
		close(logged)
	}
	s.wg.Go(func() { s.acceptClients(ctx) })
	s.wg.Go(func() { s.acceptPeers(ctx) })
	for _, p := range s.peers {
		s.wg.Go(func() { s.writePeer(ctx, p) })
	}
	err := s.loop(ctx)
	cancel()
	_ = s.cfg.Clients.Close()
	_ = s.cfg.Peering.Close()
	s.wg.Wait()
	for len(s.events) > 0 {
		if ev := <-s.events; ev.spool != nil {
			s.discard(ev.spool)
		}
	}
	close(quiet)
	select {
	case <-logged:
	case <-time.After(logGrace):
	}
	return errors.Join(err, s.node.Close())
}

func (s *Server) loop(ctx context.Context) error {
	start := time.Now()
	timer := time.NewTimer(s.cfg.Tick)
	defer timer.Stop()
	for {
		timer.Reset(s.ticks.wake(time.Since(start)))
		idle := time.Now()
		select {
		case <-ctx.Done():
			return nil
		case err := <-s.fatal:
			return err
		case ev := <-s.events:
			s.feed(ev)
		case <-timer.C:
		}
		s.counters.waited(time.Since(idle))
		for range len(s.events) {
			s.feed(<-s.events)
		}
		owed := s.ticks.take(time.Since(start))
		for range owed {
			s.node.Tick()
		}
		s.ticked += uint64(owed) //nolint:gosec // G115: take never returns less than 0
		s.sweep(owed)
		began := time.Now()
		out, err := s.node.Process()
		s.counters.processed(time.Since(began))
		s.release()
		if err != nil {
			return err
		}
		if err := s.send(ctx, out.Messages); err != nil {
			return err
		}
		if err := s.answer(out.Replies); err != nil {
			return err
		}
		s.answerStatus()
		s.watch()
	}
}

func (s *Server) feed(ev event) {
	switch ev.kind {
	case gotMessage:
		s.node.Receive(ev.msg, nil)
	case gotSnapshot:
		s.spools = append(s.spools, ev.spool)
		s.node.Receive(ev.msg, io.NewSectionReader(ev.spool.file, 0, ev.spool.size))
	case gotRequest:
		s.conns[ev.req.Client] = ev.conn
		s.node.Request(ev.req)
	case gotStatus:
		s.asked = append(s.asked, ev.conn)
	case connClosed:
		if s.conns[ev.req.Client] == ev.conn {
			delete(s.conns, ev.req.Client)
		}
	}
}

func (s *Server) status() wire.Status {
	st := s.node.Status()
	return wire.Status{
		ID:         st.ID,
		Role:       st.Role,
		Repair:     st.Repair,
		Term:       st.Term,
		Lead:       st.Lead,
		Commit:     st.Commit,
		Applied:    st.Applied,
		Last:       st.Last,
		ApplyHash:  st.ApplyHash,
		Sessions:   uint32(st.Sessions), //nolint:gosec // G115: MaxSessions is an int the operator sets, far below 2^32
		Time:       st.Time,
		Clock:      wallClock(),
		NextExpiry: st.NextExpiry,
		Head:       st.Head,
		Key:        s.stamper,
	}
}

func (s *Server) release() {
	for _, sp := range s.spools {
		s.discard(sp)
	}
	clear(s.spools)
	s.spools = s.spools[:0]
}

func (s *Server) send(ctx context.Context, msgs []raft.Message) error {
	for _, m := range msgs {
		frame, err := wire.AppendMessage(nil, s.cfg.Cluster, m)
		if err != nil {
			return err
		}
		p := s.peers[m.To]
		if m.Type != raft.MsgSnap {
			p.push(frame)
			continue
		}
		if p.sending.CompareAndSwap(false, true) {
			s.wg.Go(func() {
				defer p.sending.Store(false)
				s.sendSnapshot(ctx, p, m, frame)
			})
		}
	}
	return nil
}

func (s *Server) answer(replies []client.Reply) error {
	for _, r := range replies {
		if s.sweeper.owns(r.Client) {
			s.sweeper.step(r)
			continue
		}
		c, ok := s.conns[r.Client]
		if !ok {
			continue
		}
		if s.cfg.Key != nil && r.Status == client.OK && (r.Op == client.Prove || r.Op == client.Head) && len(r.Body) > 0 {
			_, h, rest, err := audit.Decode(r.Body)
			if err == nil && rest != nil {
				err = errors.New("it already carries a stamp")
			}
			if err != nil {
				return fmt.Errorf("server: the node answered %s #%d with a receipt it cannot stamp: %w", r.Op, r.Number, err)
			}
			r.Body = append(slices.Clip(r.Body), notary.Sign(s.cfg.Key, s.cfg.Cluster, h)...)
		}
		s.reply(c, wire.AppendReply(nil, s.cfg.Cluster, r))
	}
	return nil
}

func (s *Server) answerStatus() {
	if len(s.asked) == 0 {
		return
	}
	frame := wire.AppendStatus(nil, s.cfg.Cluster, s.status())
	for _, c := range s.asked {
		s.reply(c, frame)
	}
	clear(s.asked)
	s.asked = s.asked[:0]
}

func (s *Server) stop(err error) {
	select {
	case s.fatal <- err:
	default:
	}
}

func closeOnStop(ctx context.Context, conn net.Conn) func() {
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	return func() {
		stop()
		_ = conn.Close()
	}
}

func keepAccepting(ctx context.Context, err error) bool {
	return !errors.Is(err, net.ErrClosed) && sleep(ctx, acceptPause)
}
