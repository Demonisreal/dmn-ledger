package ledger

import (
	"cmp"
	"context"

	"github.com/Demonisreal/dmn-ledger/internal/raft"
	"github.com/Demonisreal/dmn-ledger/internal/wire"
)

type Role uint8

const (
	Follower Role = iota
	PreCandidate
	Candidate
	Leader
)

func (r Role) String() string {
	return raft.Role(r).String()
}

type NodeStatus struct {
	ID         uint64
	Role       Role
	Repair     bool
	Term       uint64
	Leader     uint64
	Commit     uint64
	Applied    uint64
	Last       uint64
	ApplyHash  [32]byte
	Sessions   uint32
	Time       uint64
	Clock      uint64
	NextExpiry uint64
	HeadSeq    uint64
	Head       [32]byte
	Key        []byte
}

func Status(ctx context.Context, cluster uint64, addr string) (NodeStatus, error) {
	return status(ctx, cluster, addr, dialTCP)
}

func status(ctx context.Context, cluster uint64, addr string, dial dialFunc) (NodeStatus, error) {
	nc, err := dial(ctx, addr)
	if err != nil {
		return NodeStatus{}, err
	}
	defer nc.Close()
	stop := context.AfterFunc(ctx, func() { _ = nc.Close() })
	defer stop()
	if _, err := nc.Write(wire.AppendStatusRequest(nil, cluster)); err != nil {
		return NodeStatus{}, cmp.Or(ctx.Err(), err)
	}
	f, _, err := wire.ReadFrame(nc, nil, cluster, wire.MaxClientBody)
	if err != nil {
		return NodeStatus{}, cmp.Or(ctx.Err(), err)
	}
	st, err := f.Status()
	if err != nil {
		return NodeStatus{}, err
	}
	var key []byte
	if st.Key != ([32]byte{}) {
		key = st.Key[:]
	}
	return NodeStatus{
		ID:         st.ID,
		Role:       Role(st.Role),
		Repair:     st.Repair,
		Term:       st.Term,
		Leader:     st.Lead,
		Commit:     st.Commit,
		Applied:    st.Applied,
		Last:       st.Last,
		ApplyHash:  st.ApplyHash,
		Sessions:   st.Sessions,
		Time:       st.Time,
		Clock:      st.Clock,
		NextExpiry: st.NextExpiry,
		HeadSeq:    st.Head.Seq,
		Head:       st.Head.Hash,
		Key:        key,
	}, nil
}
