package replica

import (
	"encoding/binary"
	"math"

	"github.com/Demonisreal/dmn-ledger/internal/audit"
	"github.com/Demonisreal/dmn-ledger/internal/client"
	"github.com/Demonisreal/dmn-ledger/internal/state"
	"github.com/Demonisreal/dmn-ledger/internal/u128"
	"github.com/Demonisreal/dmn-ledger/internal/wal"
)

const (
	kindRegister uint8 = 1
	kindRequest  uint8 = 2

	requestHead = client.IDSize + 8 + 8 + 1 + 8

	headSize  = 2 + 8 + audit.HashSize
	proofSize = 2 + state.TransferSize + 4 + 1 + audit.MaxHops*audit.HashSize + 8 + audit.HashSize + audit.LinkSize
)

type session struct {
	since  uint64
	last   uint64
	access uint64
	reply  client.Reply
}

func appendRequest(req client.Request, ts uint64) []byte {
	b := make([]byte, 0, requestHead+len(req.Body))
	b = req.Client.AppendBytes(b)
	b = binary.BigEndian.AppendUint64(b, req.Session)
	b = binary.BigEndian.AppendUint64(b, req.Number)
	b = append(b, byte(req.Op))
	b = binary.BigEndian.AppendUint64(b, ts)
	return append(b, req.Body...)
}

func wellFormed(req client.Request) bool {
	if len(req.Body) > client.MaxBody {
		return false
	}
	switch req.Op {
	case client.Register:
		return len(req.Body) == 0
	case client.CreateAccounts:
		return len(req.Body)%state.AccountSize == 0
	case client.CreateTransfers:
		return len(req.Body)%state.TransferSize == 0
	case client.LookupAccounts:
		return replyFits(req.Body, state.AccountSize)
	case client.LookupTransfers:
		return replyFits(req.Body, state.TransferSize)
	case client.Prove:
		return len(req.Body) == 1+8 && req.Body[0] == audit.KindBatch ||
			len(req.Body) == 1+client.IDSize && req.Body[0] >= audit.KindAccount && req.Body[0] <= audit.KindExpiry
	case client.Head:
		return len(req.Body) == 0
	case client.Links:
		return len(req.Body) == 16 && binary.BigEndian.Uint64(req.Body) <= binary.BigEndian.Uint64(req.Body[8:])
	}
	return false
}

func replyFits(ids []byte, size int) bool {
	return len(ids)%client.IDSize == 0 && len(ids)/client.IDSize*size <= client.MaxBody
}

func replyBound(req client.Request) int {
	switch req.Op {
	case client.LookupAccounts:
		return len(req.Body) / client.IDSize * state.AccountSize
	case client.LookupTransfers:
		return len(req.Body) / client.IDSize * state.TransferSize
	case client.Prove:
		return proofSize
	case client.Links:
		return int(min(binary.BigEndian.Uint64(req.Body[8:])-binary.BigEndian.Uint64(req.Body), client.MaxLinks)) * audit.LinkSize
	}
	return headSize
}

func (n *Node) apply(e wal.Entry) {
	var head [8 + 8 + 1]byte
	binary.BigEndian.PutUint64(head[:], e.Index)
	binary.BigEndian.PutUint64(head[8:], e.Term)
	head[16] = byte(e.Kind)
	n.hasher.Reset()
	n.hasher.Write(n.hash[:])
	n.hasher.Write(head[:])
	n.hasher.Write(e.Body)
	n.hasher.Sum(n.hash[:0])
	n.applied, n.appliedTerm = e.Index, e.Term

	var r client.Reply
	switch uint8(e.Kind) {
	case kindRegister:
		r = n.register(e.Index, e.Body)
	case kindRequest:
		r = n.request(e.Index, e.Body)
	}
	if len(n.waiting) > 0 && n.waiting[0].index == e.Index {
		delete(n.proposed, keyOf(n.waiting[0].req))
		n.waiting = n.waiting[1:]
		r.Leader = n.cfg.ID
		n.replies = append(n.replies, r)
	}
}

func (n *Node) register(index uint64, body []byte) client.Reply {
	if len(body) != client.IDSize {
		return client.Reply{}
	}
	id := u128.FromBytes(body)
	s := n.sessions[id]
	if s == nil {
		if len(n.sessions) >= n.cfg.MaxSessions {
			n.evict()
		}
		s = &session{since: index, access: index}
		n.sessions[id] = s
	}
	return client.Reply{Client: id, Op: client.Register, Status: client.OK, Body: binary.BigEndian.AppendUint64(nil, s.since)}
}

func (n *Node) evict() {
	var victim u128.Uint128
	oldest := uint64(math.MaxUint64)
	for id, s := range n.sessions {
		if s.access < oldest {
			victim, oldest = id, s.access
		}
	}
	delete(n.sessions, victim)
}

func (n *Node) request(index uint64, body []byte) client.Reply {
	if len(body) < requestHead {
		return client.Reply{}
	}
	r := client.Reply{
		Client: u128.FromBytes(body),
		Number: binary.BigEndian.Uint64(body[24:]),
		Op:     client.Op(body[32]),
	}
	since := binary.BigEndian.Uint64(body[16:])
	s := n.sessions[r.Client]
	switch {
	case s == nil || s.since != since:
		r.Status = client.SessionUnknown
	case fresh(s, r.Number):
		r = n.execute(r, n.entryTime(body), body[requestHead:])
		s.last, s.access, s.reply = r.Number, index, r
	case r.Number == s.last && r.Number > 0:
		r = s.reply
	default:
		r.Status = client.Stale
	}
	return r
}

func (n *Node) execute(r client.Reply, ts uint64, recs []byte) client.Reply {
	var op state.Op
	switch r.Op {
	case client.CreateAccounts:
		op = state.CreateAccounts
	case client.CreateTransfers:
		op = state.CreateTransfers
	default:
		r.Status = client.Malformed
		return r
	}
	at := state.StartTime(ts, n.sm.Last())
	fails, err := n.sm.Apply(op, ts, recs, n.fails[:0])
	n.fails = fails
	if err != nil {
		r.Status = client.Malformed
		return r
	}
	if n.appliedTerm != n.leading {
		n.anchor, n.anchorTick = n.sm.Last(), n.ticks
	}
	r.Status, r.Time, r.Body = client.OK, at, client.AppendFailures(r.Body, fails)
	return r
}

func (n *Node) lookup(req client.Request) (client.Reply, int) {
	var body []byte
	var leaves uint32
	switch req.Op {
	case client.LookupAccounts:
		for _, a := range n.sm.LookupAccounts(client.DecodeIDs(req.Body), nil) {
			body = state.AppendAccount(body, a)
		}
	case client.LookupTransfers:
		for _, tr := range n.sm.LookupTransfers(client.DecodeIDs(req.Body), nil) {
			body = state.AppendTransfer(body, tr)
		}
	case client.Prove:
		body, leaves = n.prove(req.Body[0], req.Body[1:])
	case client.Head:
		body = audit.AppendHead(nil, n.sm.Head())
	case client.Links:
		from, to := binary.BigEndian.Uint64(req.Body), binary.BigEndian.Uint64(req.Body[8:])
		for _, l := range n.sm.Links(from, from+min(to-from, client.MaxLinks), nil) {
			body = audit.AppendLink(body, l)
		}
	}
	return client.Reply{
		Client: req.Client,
		Number: req.Number,
		Op:     req.Op,
		Status: client.OK,
		Leader: n.cfg.ID,
		Time:   n.sm.Last(),
		Body:   body,
	}, len(body) + int(leaves)*state.TransferSize
}

func (n *Node) prove(kind byte, key []byte) ([]byte, uint32) {
	var proof audit.Receipt
	var found bool
	switch kind {
	case audit.KindBatch:
		proof, found = n.sm.ProveBatch(binary.BigEndian.Uint64(key))
	case audit.KindAccount:
		proof, found = n.sm.ProveAccount(u128.FromBytes(key))
	case audit.KindTransfer:
		proof, found = n.sm.ProveTransfer(u128.FromBytes(key))
	case audit.KindExpiry:
		proof, found = n.sm.ProveExpiry(u128.FromBytes(key))
	}
	if !found {
		return nil, 0
	}
	return audit.AppendReceipt(nil, proof), proof.Link.Leaves
}
