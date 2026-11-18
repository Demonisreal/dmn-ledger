package history

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/Demonisreal/dmn-ledger/internal/check"
	"github.com/Demonisreal/dmn-ledger/internal/client"
	"github.com/Demonisreal/dmn-ledger/internal/sim"
)

var ErrOpen = errors.New("history: calls still open")

type Options struct {
	Window  int
	Backlog int
	Since   func() uint64
	Sink    func(calls []sim.Call) error
}

type Turn struct {
	N    int
	Call sim.Call
}

type Recorder struct {
	o Options
	r *check.Replayer

	mu            sync.Mutex
	cond          *sync.Cond
	calls         []sim.Call
	first         int
	started       int
	open          int
	held          bool
	closed        bool
	pending       [][]sim.Call
	sinkErr       error
	judgedCalls   int
	judgedWindows int
}

func NewRecorder(o Options, r *check.Replayer) *Recorder {
	rec := &Recorder{o: o, r: r}
	rec.cond = sync.NewCond(&rec.mu)
	return rec
}

func (rec *Recorder) wake() {
	rec.mu.Lock()
	rec.cond.Broadcast()
	rec.mu.Unlock()
}

func (rec *Recorder) Begin(ctx context.Context, client uint64, pick func() (client.Op, []byte)) (Turn, bool) {
	stop := context.AfterFunc(ctx, rec.wake)
	defer stop()
	rec.mu.Lock()
	defer rec.mu.Unlock()
	for ctx.Err() == nil && !rec.closed && (rec.held || len(rec.pending) >= rec.o.Backlog || rec.started == rec.o.Window && rec.open > 0) {
		rec.cond.Wait()
	}
	if ctx.Err() != nil || rec.closed {
		return Turn{}, false
	}
	if rec.started == rec.o.Window {
		rec.flush()
	}
	rec.started++
	op, body := pick()
	t := Turn{N: rec.first + len(rec.calls), Call: sim.Call{Client: client, Op: op, Body: body, Start: rec.o.Since()}}
	rec.calls = append(rec.calls, t.Call)
	rec.open++
	return t, true
}

func (rec *Recorder) End(t Turn, reply client.Reply) {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	call := &rec.calls[t.N-rec.first]
	call.End, call.Reply = rec.o.Since(), reply
	rec.open--
	rec.cond.Broadcast()
}

func (rec *Recorder) Hold(ctx context.Context, within time.Duration) error {
	ctx, cancel := context.WithTimeoutCause(ctx, within, ErrOpen)
	defer cancel()
	stop := context.AfterFunc(ctx, rec.wake)
	defer stop()
	rec.mu.Lock()
	defer rec.mu.Unlock()
	rec.held = true
	for rec.open > 0 {
		if ctx.Err() != nil {
			return context.Cause(ctx)
		}
		rec.cond.Wait()
	}
	rec.flush()
	return nil
}

func (rec *Recorder) Resume() {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	rec.held = false
	rec.cond.Broadcast()
}

func (rec *Recorder) Push(calls []sim.Call) {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	rec.seal(calls)
}

func (rec *Recorder) Close() {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	rec.closed = true
	rec.cond.Broadcast()
}

func (rec *Recorder) flush() {
	rec.seal(rec.calls)
	rec.calls, rec.started = nil, 0
}

func (rec *Recorder) seal(calls []sim.Call) {
	rec.first += len(calls)
	rec.cond.Broadcast()
	if len(calls) == 0 {
		return
	}
	rec.pending = append(rec.pending, calls)
	if rec.o.Sink != nil && rec.sinkErr == nil {
		rec.sinkErr = rec.o.Sink(calls)
	}
}

func (rec *Recorder) Judge(ctx context.Context) error {
	stop := context.AfterFunc(ctx, rec.wake)
	defer stop()
	for {
		calls, err := rec.next(ctx)
		if calls == nil {
			return err
		}
		if err := rec.r.Window(calls); err != nil {
			return err
		}
		rec.mu.Lock()
		rec.judgedCalls += len(calls)
		rec.judgedWindows++
		rec.mu.Unlock()
	}
}

func (rec *Recorder) next(ctx context.Context) ([]sim.Call, error) {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	for len(rec.pending) == 0 && !rec.closed && rec.sinkErr == nil && ctx.Err() == nil {
		rec.cond.Wait()
	}
	switch {
	case rec.sinkErr != nil:
		return nil, rec.sinkErr
	case ctx.Err() != nil:
		return nil, ctx.Err()
	case len(rec.pending) == 0:
		return nil, nil
	}
	calls := rec.pending[0]
	rec.pending = rec.pending[1:]
	rec.cond.Broadcast()
	return calls, nil
}

func (rec *Recorder) Judged() (calls, windows int) {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	return rec.judgedCalls, rec.judgedWindows
}
