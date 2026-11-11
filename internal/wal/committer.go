package wal

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
)

var (
	ErrStopped = errors.New("wal: committer stopped")
	ErrStarted = errors.New("wal: committer already started")
)

type CommitterOptions struct {
	MaxEntries int
	MaxBytes   int
	Queue      int
}

type Committer struct {
	w       *WAL
	opts    CommitterOptions
	subs    chan submission
	done    chan struct{}
	started atomic.Bool
	err     error
}

type submission struct {
	ents []Entry
	errc chan error
}

func NewCommitter(w *WAL, opts CommitterOptions) (*Committer, error) {
	if opts.MaxEntries < 1 || opts.MaxBytes < 1 || opts.Queue < 1 {
		return nil, fmt.Errorf("wal: committer limits must be positive: %+v", opts)
	}
	return &Committer{
		w:    w,
		opts: opts,
		subs: make(chan submission, opts.Queue),
		done: make(chan struct{}),
	}, nil
}

func (c *Committer) Submit(ctx context.Context, ents ...Entry) (<-chan error, error) {
	select {
	case <-c.done:
		return nil, c.err
	default:
	}
	sub := submission{ents: ents, errc: make(chan error, 1)}
	select {
	case c.subs <- sub:
	case <-c.done:
		return nil, c.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case <-c.done:
		c.drain()
	default:
	}
	return sub.errc, nil
}

func (c *Committer) Run(ctx context.Context) error {
	if !c.started.CompareAndSwap(false, true) {
		return ErrStarted
	}
	err := c.loop(ctx)
	c.err = ErrStopped
	if err != nil {
		c.err = err
	}
	close(c.done)
	c.drain()
	return err
}

func (c *Committer) drain() {
	for {
		select {
		case sub := <-c.subs:
			sub.errc <- c.err
		default:
			return
		}
	}
}

func (c *Committer) loop(ctx context.Context) error {
	var round []submission
	var next submission
	held := false
	for {
		if !held {
			if ctx.Err() != nil {
				return nil
			}
			select {
			case <-ctx.Done():
				return nil
			case next = <-c.subs:
			}
		}
		held = false
		round = append(round[:0], next)
		n, size := len(next.ents), entriesSize(next.ents)
	collect:
		for n < c.opts.MaxEntries && size < c.opts.MaxBytes {
			select {
			case sub := <-c.subs:
				sn, ss := len(sub.ents), entriesSize(sub.ents)
				if n+sn > c.opts.MaxEntries || size+ss > c.opts.MaxBytes {
					next, held = sub, true
					break collect
				}
				round = append(round, sub)
				n, size = n+sn, size+ss
			default:
				break collect
			}
		}
		err := c.persist(round)
		clear(round)
		if err != nil {
			if held {
				next.errc <- err
			}
			return err
		}
	}
}

func (c *Committer) persist(round []submission) error {
	var err error
	for _, sub := range round {
		if err = c.w.Append(sub.ents...); err != nil {
			break
		}
	}
	if err == nil {
		err = c.w.Sync()
	}
	for _, sub := range round {
		sub.errc <- err
	}
	return err
}

func entriesSize(ents []Entry) int {
	size := 0
	for _, e := range ents {
		size += frameSize(e)
	}
	return size
}
