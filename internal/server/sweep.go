package server

import (
	crand "crypto/rand"

	"github.com/Demonisreal/dmn-ledger/internal/client"
	"github.com/Demonisreal/dmn-ledger/internal/raft"
	"github.com/Demonisreal/dmn-ledger/internal/u128"
)

type sweeper struct {
	on   bool
	self uint64
	id   u128.Uint128
	sess *client.Session
	busy bool
	last uint64
}

func (s *Server) sweep(owed int64) {
	w := &s.sweeper
	if !w.on {
		return
	}
	st := s.node.Status()
	if st.Role != raft.Leader {
		return
	}
	if w.sess != nil {
		for range owed {
			w.sess.Tick()
		}
	}
	if !w.busy && st.NextExpiry != 0 && st.NextExpiry <= wallClock() && s.ticked-w.last >= electionTicks {
		w.busy, w.last = true, s.ticked
		if w.sess == nil {
			var b [client.IDSize]byte
			_, _ = crand.Read(b[:])
			w.id = u128.FromBytes(b[:])
			w.sess = client.New(w.id, []uint64{w.self}, electionTicks)
		} else {
			_ = w.sess.Submit(client.CreateTransfers, nil)
		}
	}
	if w.sess == nil {
		return
	}
	if out := w.sess.Outbox(); len(out) > 0 {
		s.node.Request(out[len(out)-1].Request)
	}
}

func (w *sweeper) owns(id u128.Uint128) bool {
	return w.sess != nil && id == w.id
}

func (w *sweeper) step(r client.Reply) {
	w.sess.Step(r)
	done, ok := w.sess.Result()
	switch {
	case !ok:
	case done.Op == client.Register && done.Status == client.OK:
		_ = w.sess.Submit(client.CreateTransfers, nil)
	case done.Status == client.SessionUnknown:
		w.sess, w.busy = nil, false
	default:
		w.busy = false
	}
}
