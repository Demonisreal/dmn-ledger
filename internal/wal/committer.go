package wal

import (
	"cmp"
	"context"
	"errors"
)

var ErrStopped = errors.New("wal: committer stopped")

type Committer struct {
	w          *WAL
	maxEntries int
	maxBytes   int
	subs       chan submission
	done       chan struct{}
	err        error
}

type submission struct {
	ents []Entry
	errc chan error
	err  error
}

func NewCommitter(w *WAL, maxEntries, maxBytes int) *Committer {
	return &Committer{
		w:          w,
		maxEntries: maxEntries,
		maxBytes:   maxBytes,
		subs:       make(chan submission, maxEntries),
		done:       make(chan struct{}),
	}
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
	err := c.loop(ctx)
	c.err = cmp.Or(err, ErrStopped)
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
		for n < c.maxEntries && size < c.maxBytes {
			select {
			case sub := <-c.subs:
				sn, ss := len(sub.ents), entriesSize(sub.ents)
				if n+sn > c.maxEntries || size+ss > c.maxBytes {
					next, held = sub, true
					break collect
				}
				round = append(round, sub)
				n, size = n+sn, size+ss
			default:
				break collect
			}
		}
		if err := c.persist(round); err != nil {
			if held {
				next.errc <- err
			}
			return err
		}
	}
}

func (c *Committer) persist(round []submission) error {
	for i := range round {
		round[i].err = c.w.Append(round[i].ents...)
	}
	err := c.w.Sync()
	for _, sub := range round {
		sub.errc <- cmp.Or(sub.err, err)
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
