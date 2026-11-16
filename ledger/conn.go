package ledger

import (
	"context"
	"net"
	"time"

	"github.com/Demonisreal/dmn-ledger/internal/client"
	"github.com/Demonisreal/dmn-ledger/internal/wire"
)

type conn struct {
	node   uint64
	out    chan []byte
	cancel context.CancelFunc
}

type arrival struct {
	conn  *conn
	reply client.Reply
}

type loss struct {
	conn    *conn
	refused bool
}

func (c *Client) send(ctx context.Context, m client.Message) {
	if c.last != 0 && c.last != m.To && !c.heard {
		c.hangUp(c.last)
	}
	c.last, c.heard = m.To, false
	if c.conns[m.To-1] == nil {
		if time.Now().Before(c.down[m.To-1]) {
			c.sess.Unreachable(m.To)
			return
		}
		c.conns[m.To-1] = c.connect(ctx, m.To)
	}
	select {
	case c.conns[m.To-1].out <- wire.AppendRequest(nil, c.cfg.Cluster, m.Request):
	default:
		c.hangUp(m.To)
		c.sess.Unreachable(m.To)
	}
}

func (c *Client) forget(l loss) {
	node := l.conn.node
	if c.conns[node-1] != l.conn {
		return
	}
	c.hangUp(node)
	if l.refused {
		c.down[node-1] = time.Now().Add(redialAfter)
	}
	c.sess.Unreachable(node)
}

func (c *Client) hangUp(node uint64) {
	if cn := c.conns[node-1]; cn != nil {
		cn.cancel()
		c.conns[node-1] = nil
	}
}

func (c *Client) connect(ctx context.Context, node uint64) *conn {
	ctx, cancel := context.WithCancel(ctx)
	cn := &conn{node: node, out: make(chan []byte, 4), cancel: cancel}
	c.wg.Go(func() { c.serve(ctx, cn) })
	return cn
}

func (c *Client) serve(ctx context.Context, cn *conn) {
	dctx, cancel := context.WithTimeout(ctx, dialTimeout)
	nc, err := c.dial(dctx, c.cfg.Nodes[cn.node-1])
	cancel()
	if err != nil {
		c.report(ctx, loss{conn: cn, refused: true})
		return
	}
	context.AfterFunc(ctx, func() { _ = nc.Close() })
	c.wg.Go(func() { c.read(ctx, cn, nc) })
	for {
		select {
		case frame := <-cn.out:
			if _, err := nc.Write(frame); err != nil {
				_ = nc.Close()
				return
			}
		case <-ctx.Done():
			return
		}
	}
}

func (c *Client) read(ctx context.Context, cn *conn, nc net.Conn) {
	for {
		f, _, err := wire.ReadFrame(nc, nil, c.cfg.Cluster, wire.MaxClientBody)
		if err != nil {
			break
		}
		r, err := f.Reply()
		if err != nil || r.Client != c.id {
			break
		}
		select {
		case c.replies <- arrival{conn: cn, reply: r}:
		case <-ctx.Done():
			return
		}
	}
	c.report(ctx, loss{conn: cn})
}

func (c *Client) report(ctx context.Context, l loss) {
	select {
	case c.losses <- l:
	case <-ctx.Done():
	}
}
