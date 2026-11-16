package wire

import (
	"crypto/sha256"
	"encoding/binary"

	"github.com/Demonisreal/dmn-ledger/internal/audit"
	"github.com/Demonisreal/dmn-ledger/internal/client"
	"github.com/Demonisreal/dmn-ledger/internal/raft"
	"github.com/Demonisreal/dmn-ledger/internal/u128"
)

const (
	requestHead = client.IDSize + 8 + 8 + 1
	replyHead   = client.IDSize + 8 + 1 + 1 + 8 + 8
	statusSize  = 182

	flagRepair = 1
)

type Status struct {
	ID         uint64
	Role       raft.Role
	Repair     bool
	Term       uint64
	Lead       uint64
	Commit     uint64
	Applied    uint64
	Last       uint64
	ApplyHash  [sha256.Size]byte
	Sessions   uint32
	Time       uint64
	Clock      uint64
	NextExpiry uint64
	Head       audit.Head
	Key        [32]byte
}

func AppendRequest(dst []byte, cluster uint64, req client.Request) []byte {
	start := len(dst)
	dst = appendHeader(dst, cluster, TypeRequest)
	dst = req.Client.AppendBytes(dst)
	dst = binary.BigEndian.AppendUint64(dst, req.Session)
	dst = binary.BigEndian.AppendUint64(dst, req.Number)
	dst = append(dst, byte(req.Op))
	dst = append(dst, req.Body...)
	return finish(dst, start)
}

func (f Frame) Request() (client.Request, error) {
	b := f.Body
	if f.Type != TypeRequest || len(b) < requestHead || len(b) > requestHead+client.MaxBody {
		return client.Request{}, f.mismatch("request")
	}
	return client.Request{
		Client:  u128.FromBytes(b),
		Session: binary.BigEndian.Uint64(b[16:]),
		Number:  binary.BigEndian.Uint64(b[24:]),
		Op:      client.Op(b[32]),
		Body:    trailing(b[requestHead:]),
	}, nil
}

func AppendReply(dst []byte, cluster uint64, r client.Reply) []byte {
	start := len(dst)
	dst = appendHeader(dst, cluster, TypeReply)
	dst = r.Client.AppendBytes(dst)
	dst = binary.BigEndian.AppendUint64(dst, r.Number)
	dst = append(dst, byte(r.Op), byte(r.Status))
	dst = binary.BigEndian.AppendUint64(dst, r.Leader)
	dst = binary.BigEndian.AppendUint64(dst, r.Time)
	dst = append(dst, r.Body...)
	return finish(dst, start)
}

func (f Frame) Reply() (client.Reply, error) {
	b := f.Body
	if f.Type != TypeReply || len(b) < replyHead || len(b) > replyHead+client.MaxBody {
		return client.Reply{}, f.mismatch("reply")
	}
	return client.Reply{
		Client: u128.FromBytes(b),
		Number: binary.BigEndian.Uint64(b[16:]),
		Op:     client.Op(b[24]),
		Status: client.Status(b[25]),
		Leader: binary.BigEndian.Uint64(b[26:]),
		Time:   binary.BigEndian.Uint64(b[34:]),
		Body:   trailing(b[replyHead:]),
	}, nil
}

func AppendStatusRequest(dst []byte, cluster uint64) []byte {
	return finish(appendHeader(dst, cluster, TypeStatusRequest), len(dst))
}

func (f Frame) StatusRequest() error {
	if f.Type != TypeStatusRequest || len(f.Body) != 0 {
		return f.mismatch("status request")
	}
	return nil
}

func AppendStatus(dst []byte, cluster uint64, st Status) []byte {
	start := len(dst)
	dst = appendHeader(dst, cluster, TypeStatus)
	dst = binary.BigEndian.AppendUint64(dst, st.ID)
	dst = append(dst, byte(st.Role), bit(st.Repair))
	dst = binary.BigEndian.AppendUint64(dst, st.Term)
	dst = binary.BigEndian.AppendUint64(dst, st.Lead)
	dst = binary.BigEndian.AppendUint64(dst, st.Commit)
	dst = binary.BigEndian.AppendUint64(dst, st.Applied)
	dst = binary.BigEndian.AppendUint64(dst, st.Last)
	dst = append(dst, st.ApplyHash[:]...)
	dst = binary.BigEndian.AppendUint32(dst, st.Sessions)
	dst = binary.BigEndian.AppendUint64(dst, st.Time)
	dst = binary.BigEndian.AppendUint64(dst, st.Clock)
	dst = binary.BigEndian.AppendUint64(dst, st.NextExpiry)
	dst = binary.BigEndian.AppendUint64(dst, st.Head.Seq)
	dst = append(dst, st.Head.Hash[:]...)
	dst = append(dst, st.Key[:]...)
	return finish(dst, start)
}

func (f Frame) Status() (Status, error) {
	b := f.Body
	if f.Type != TypeStatus || len(b) != statusSize || b[8] > byte(raft.Leader) || b[9]&^flagRepair != 0 {
		return Status{}, f.mismatch("status")
	}
	return Status{
		ID:         binary.BigEndian.Uint64(b),
		Role:       raft.Role(b[8]),
		Repair:     b[9] == flagRepair,
		Term:       binary.BigEndian.Uint64(b[10:]),
		Lead:       binary.BigEndian.Uint64(b[18:]),
		Commit:     binary.BigEndian.Uint64(b[26:]),
		Applied:    binary.BigEndian.Uint64(b[34:]),
		Last:       binary.BigEndian.Uint64(b[42:]),
		ApplyHash:  [sha256.Size]byte(b[50:]),
		Sessions:   binary.BigEndian.Uint32(b[82:]),
		Time:       binary.BigEndian.Uint64(b[86:]),
		Clock:      binary.BigEndian.Uint64(b[94:]),
		NextExpiry: binary.BigEndian.Uint64(b[102:]),
		Head:       audit.Head{Seq: binary.BigEndian.Uint64(b[110:]), Hash: audit.Hash(b[118:])},
		Key:        [32]byte(b[150:]),
	}, nil
}

func bit(set bool) byte {
	if set {
		return 1
	}
	return 0
}
