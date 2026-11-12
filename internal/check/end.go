package check

import (
	"github.com/Demonisreal/dmn-ledger/internal/client"
	"github.com/Demonisreal/dmn-ledger/internal/raft"
	"github.com/Demonisreal/dmn-ledger/internal/replica"
	"github.com/Demonisreal/dmn-ledger/internal/u128"
)

func (w *watch) end() error {
	nodes := make([]*replica.Node, w.c.Nodes())
	for i := range nodes {
		id := uint64(i) + 1
		n := w.c.Node(id)
		if n == nil {
			return w.fail("S7", id, "down at the end")
		}
		if err := n.CheckInvariants(); err != nil {
			return w.fail("S6", id, "at the end: %v", err)
		}
		nodes[i] = n
	}
	head, digest := nodes[0].Status(), nodes[0].Digest()
	leaders := 0
	for i, n := range nodes[1:] {
		st, d := n.Status(), n.Digest()
		if st.Applied != head.Applied || d != digest {
			return w.fail("S7", uint64(i)+2, "applied %d digest %x, n1 applied %d digest %x", st.Applied, d[:4], head.Applied, digest[:4])
		}
		if st.Commit != head.Commit {
			return w.fail("L2", uint64(i)+2, "commit %d, n1 commit %d", st.Commit, head.Commit)
		}
	}
	for _, n := range nodes {
		if n.Status().Role == raft.Leader {
			leaders++
		}
	}
	if leaders != 1 {
		return w.fail("L2", 0, "%d leaders at the end", leaders)
	}
	if err := w.answered(); err != nil {
		return err
	}
	if err := w.owners(nodes); err != nil {
		return err
	}
	return w.replay(nodes[0])
}

func (w *watch) answered() error {
	quiet := w.c.World().Quiet()
	for i, call := range w.c.Calls() {
		switch {
		case call.End == 0:
			return w.fail("L1", 0, "c%d call %d (%s) sent at %d never answered", call.Client-w.c.Nodes(), i, call.Op, call.Start)
		case call.End > quiet && call.End-max(quiet, call.Start) > answerBound:
			return w.fail("L1", 0, "c%d call %d (%s) answered %d ns into the quiet phase", call.Client-w.c.Nodes(), i, call.Op, call.End-max(quiet, call.Start))
		}
	}
	return nil
}

func (w *watch) owners(nodes []*replica.Node) error {
	numbers := make(map[u128.Uint128]uint64)
	for i, call := range w.c.Calls() {
		r, who := call.Reply, call.Client-w.c.Nodes()
		numbers[call.ID]++
		if r.Client != call.ID || r.Number != numbers[call.ID] || r.Op != call.Op {
			return w.fail("E1", 0, "c%d call %d (%s #%d of %v) got the reply to %s #%d of %v", who, i, call.Op, numbers[call.ID], call.ID, r.Op, r.Number, r.Client)
		}
		if r.Status != client.SessionUnknown {
			continue
		}
		for k, n := range nodes {
			if since, ok := n.Session(r.Client); ok && since == call.Session {
				return w.fail("E2", uint64(k)+1, "c%d call %d got SessionUnknown, the node still holds session %d", who, i, since)
			}
		}
	}
	if w.cfg.Profile != ProfileEvict {
		return nil
	}
	for k, n := range nodes {
		if s := n.Status().Sessions; s > evictSessions {
			return w.fail("E2", uint64(k)+1, "%d sessions, the limit is %d", s, evictSessions)
		}
	}
	return nil
}
