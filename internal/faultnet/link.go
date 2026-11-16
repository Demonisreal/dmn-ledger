package faultnet

import (
	"encoding/binary"
	"io"
	"math/rand/v2"
	"net"
	"sync"
	"time"

	"github.com/Demonisreal/dmn-ledger/internal/wire"
)

const (
	offType   = 5
	offSize   = 16
	helloSize = 17
	maxHeld   = 4 << 20
)

type Fault struct {
	Cut       bool
	Blackhole bool
	Stall     bool
	Delay     time.Duration
	Jitter    time.Duration
	Loss      float64
	Flip      float64
}

type Link struct {
	target string
	ln     net.Listener
	wg     sync.WaitGroup

	mu      sync.Mutex
	rng     *rand.Rand
	fault   Fault
	changed chan struct{}
	conns   map[*conn]struct{}
	closed  bool
}

type conn struct {
	src, dst net.Conn
	done     chan struct{}
	once     sync.Once

	queue []frame
	held  int
	eof   bool
	holed bool
}

type frame struct {
	raw   []byte
	due   time.Time
	lossy bool
}

func Listen(target string, rng *rand.Rand) (*Link, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	l := &Link{
		target:  target,
		ln:      ln,
		rng:     rng,
		changed: make(chan struct{}),
		conns:   make(map[*conn]struct{}),
	}
	l.wg.Go(l.accept)
	return l, nil
}

func (l *Link) Addr() string {
	return l.ln.Addr().String()
}

func (l *Link) Set(f Fault) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.fault = f
	for c := range l.conns {
		if f.Cut || c.holed && !f.Blackhole {
			c.close()
		}
	}
	l.notify()
}

func (l *Link) Close() error {
	l.mu.Lock()
	l.closed = true
	for c := range l.conns {
		c.close()
	}
	l.mu.Unlock()
	err := l.ln.Close()
	l.wg.Wait()
	return err
}

func (l *Link) notify() {
	close(l.changed)
	l.changed = make(chan struct{})
}

func (l *Link) await(c *conn, ready func() bool) bool {
	for !ready() {
		changed := l.changed
		l.mu.Unlock()
		select {
		case <-changed:
		case <-c.done:
			l.mu.Lock()
			return false
		}
		l.mu.Lock()
	}
	return true
}

func (l *Link) accept() {
	for {
		src, err := l.ln.Accept()
		if err != nil {
			return
		}
		l.wg.Go(func() { l.serve(src) })
	}
}

func (l *Link) cut() bool {
	return l.closed || l.fault.Cut
}

func (l *Link) serve(src net.Conn) {
	l.mu.Lock()
	cut := l.cut()
	l.mu.Unlock()
	if cut {
		_ = src.Close()
		return
	}
	dst, err := net.DialTimeout("tcp", l.target, time.Second)
	if err != nil {
		_ = src.Close()
		return
	}
	c := &conn{src: src, dst: dst, done: make(chan struct{})}
	defer c.close()

	l.mu.Lock()
	cut = l.cut()
	if !cut {
		l.conns[c] = struct{}{}
	}
	l.mu.Unlock()
	if cut {
		return
	}
	defer func() {
		l.mu.Lock()
		delete(l.conns, c)
		l.mu.Unlock()
	}()

	l.wg.Go(func() {
		_, _ = io.Copy(c.src, c.dst)
		c.close()
	})
	l.wg.Go(func() { l.read(c) })
	l.write(c)
}

func (l *Link) read(c *conn) {
	defer func() {
		l.mu.Lock()
		c.eof = true
		l.notify()
		l.mu.Unlock()
	}()
	spare := false
	for {
		l.mu.Lock()
		ok := l.await(c, func() bool { return !l.fault.Stall })
		l.mu.Unlock()
		if !ok {
			return
		}
		var head [wire.HeaderSize]byte
		if _, err := io.ReadFull(c.src, head[:]); err != nil {
			return
		}
		size := binary.BigEndian.Uint32(head[offSize:])
		if size > wire.MaxPeerBody {
			return
		}
		n := wire.HeaderSize + int(size)

		l.mu.Lock()
		ok = l.await(c, func() bool { return c.held == 0 || c.held+n <= maxHeld })
		c.held += n
		l.mu.Unlock()
		if !ok {
			return
		}
		raw := make([]byte, n)
		copy(raw, head[:])
		if _, err := io.ReadFull(c.src, raw[wire.HeaderSize:]); err != nil {
			return
		}
		t := wire.Type(raw[offType])
		if t == wire.TypeHello && size == helloSize && raw[n-1] == wire.KindSnapshot {
			spare = true
		}

		l.mu.Lock()
		due := time.Now().Add(l.fault.Delay)
		if l.fault.Jitter > 0 {
			due = due.Add(time.Duration(l.rng.Int64N(int64(l.fault.Jitter))))
		}
		lossy := !spare && t > wire.TypeMessage && t < wire.TypeSnapshotChunk
		c.queue = append(c.queue, frame{raw: raw, due: due, lossy: lossy})
		l.notify()
		l.mu.Unlock()
	}
}

func (l *Link) write(c *conn) {
	for {
		l.mu.Lock()
		ok := l.await(c, func() bool { return len(c.queue) > 0 || c.eof })
		if !ok || len(c.queue) == 0 {
			l.mu.Unlock()
			return
		}
		fr := c.queue[0]
		l.mu.Unlock()

		if wait := time.Until(fr.due); wait > 0 {
			select {
			case <-time.After(wait):
			case <-c.done:
				return
			}
		}

		l.mu.Lock()
		if !l.await(c, func() bool { return !l.fault.Stall }) {
			l.mu.Unlock()
			return
		}
		c.queue[0] = frame{}
		c.queue = c.queue[1:]
		drop := false
		switch {
		case l.fault.Blackhole:
			drop = true
			c.holed = true
		case fr.lossy && l.rng.Float64() < l.fault.Loss:
			drop = true
		case l.rng.Float64() < l.fault.Flip:
			bit := l.rng.IntN(len(fr.raw) * 8)
			fr.raw[bit>>3] ^= 1 << (bit & 7)
		}
		l.mu.Unlock()

		if !drop {
			if _, err := c.dst.Write(fr.raw); err != nil {
				return
			}
		}
		l.mu.Lock()
		c.held -= len(fr.raw)
		l.notify()
		l.mu.Unlock()
	}
}

func (c *conn) close() {
	c.once.Do(func() {
		close(c.done)
		_ = c.src.Close()
		_ = c.dst.Close()
	})
}
