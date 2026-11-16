package ledger

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/Demonisreal/dmn-ledger/internal/client"
)

func (c *Client) run(ctx context.Context) {
	c.tick = time.NewTicker(tickEvery)
	defer c.tick.Stop()
	for {
		select {
		case <-ctx.Done():
			c.shut()
			return
		case cl := <-c.calls:
			c.enqueue(cl)
		case cl := <-c.withdraw:
			c.abandon(cl)
		case a := <-c.replies:
			if a.conn.node == c.last {
				c.heard = true
			}
			c.sess.Step(a.reply)
		case l := <-c.losses:
			c.forget(l)
		case <-c.tick.C:
			c.sess.Tick()
		}
		c.settle(ctx)
	}
}

func (c *Client) enqueue(cl *call) {
	if c.evicted {
		cl.answer(outcome{err: ErrEvicted})
		return
	}
	c.queue = append(c.queue, cl)
}

func (c *Client) abandon(cl *call) {
	if k := slices.Index(c.queue, cl); k >= 0 {
		c.queue = slices.Delete(c.queue, k, k+1)
		cl.answer(outcome{err: cl.ctx.Err()})
		return
	}
	if slices.Contains(c.batch, cl) {
		cl.answer(outcome{err: cl.unsure(cl.ctx.Err())})
	}
}

func (c *Client) settle(ctx context.Context) {
	if r, ok := c.sess.Result(); ok {
		c.finish(r)
	}
	if c.batch == nil && !c.evicted {
		c.launch()
	}
	for out := c.sess.Outbox(); len(out) > 0; out = c.sess.Outbox() {
		for _, m := range out {
			c.send(ctx, m)
		}
	}
	if c.batch == nil {
		c.tick.Stop()
	}
}

func (c *Client) launch() {
	var batch []*call
	taken, size := 0, 0
	for _, cl := range c.queue {
		if err := cl.ctx.Err(); err != nil {
			cl.answer(outcome{err: err})
			taken++
			continue
		}
		if len(batch) > 0 && (cl.op != batch[0].op || size+len(cl.body) > client.MaxBody) {
			break
		}
		if len(batch) > 0 {
			last := batch[len(batch)-1]
			cl.off = last.off + last.records
		}
		batch = append(batch, cl)
		taken++
		size += len(cl.body)
		if cl.linked || cl.op.Lookup() {
			break
		}
	}
	c.queue = slices.Delete(c.queue, 0, taken)
	if len(batch) == 0 {
		return
	}
	body := batch[0].body
	if len(batch) > 1 {
		body = make([]byte, 0, size)
		for _, cl := range batch {
			body = append(body, cl.body...)
		}
	}
	if err := c.sess.Submit(batch[0].op, body); err != nil {
		panic(err)
	}
	c.batch = batch
	c.tick.Reset(tickEvery)
}

func (c *Client) finish(r client.Reply) {
	batch := c.batch
	c.batch = nil
	switch op := batch[0].op; {
	case r.Status == client.OK && (op == client.Register || op.Lookup()):
		batch[0].answer(outcome{body: r.Body})
	case r.Status == client.OK:
		c.split(batch, r)
	case r.Status == client.Malformed:
		for _, cl := range batch {
			cl.answer(outcome{err: ErrRejected})
		}
	case r.Status == client.SessionUnknown:
		c.evicted = true
		for _, cl := range batch {
			cl.answer(outcome{err: cl.unsure(ErrEvicted)})
		}
		for _, cl := range c.queue {
			cl.answer(outcome{err: ErrEvicted})
		}
		c.queue = nil
	default:
		for _, cl := range batch {
			cl.answer(outcome{err: cl.unsure(fmt.Errorf("ledger: unexpected reply %v", r.Status))})
		}
	}
}

func (c *Client) split(batch []*call, r client.Reply) {
	results := make([]Result, len(batch))
	for k, cl := range batch {
		results[k].Timestamp = r.Time + uint64(cl.off) //nolint:gosec // G115: an offset into a batch below MaxBody
	}
	for _, f := range client.DecodeFailures(r.Body) {
		k := sort.Search(len(batch), func(k int) bool { return batch[k].off+batch[k].records > f.Index })
		if k == len(batch) {
			continue
		}
		results[k].Failures = append(results[k].Failures, Failure{Index: f.Index - batch[k].off, Reason: Reason(f.Reason)})
	}
	for k, cl := range batch {
		cl.answer(outcome{written: results[k]})
	}
}

func (c *Client) shut() {
	for _, cl := range c.batch {
		cl.answer(outcome{err: cl.unsure(ErrClosed)})
	}
	for _, cl := range c.queue {
		cl.answer(outcome{err: ErrClosed})
	}
	c.batch, c.queue = nil, nil
}
