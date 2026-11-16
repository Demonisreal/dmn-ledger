package ledger

import (
	"context"
	"errors"
	"net"
	"reflect"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Demonisreal/dmn-ledger/internal/client"
	"github.com/Demonisreal/dmn-ledger/internal/raft"
	"github.com/Demonisreal/dmn-ledger/internal/wire"
)

func TestStatus(t *testing.T) {
	sample := wire.Status{
		ID:         3,
		Role:       raft.Leader,
		Repair:     true,
		Term:       5,
		Lead:       2,
		Commit:     4271,
		Applied:    4270,
		Last:       4272,
		Sessions:   17,
		Time:       1_763_112_307_473_359_071,
		Clock:      1_763_112_307_474_000_000,
		NextExpiry: 1_763_112_317_000_000_000,
	}
	sample.Head.Seq = 4269
	for i := range sample.ApplyHash {
		sample.ApplyHash[i] = byte(i + 1)
		sample.Head.Hash[i] = byte(i + 0x41)
		sample.Key[i] = byte(i + 0x81)
	}
	want := NodeStatus{
		ID: 3, Role: Leader, Repair: true, Term: 5, Leader: 2, Commit: 4271, Applied: 4270, Last: 4272,
		ApplyHash: sample.ApplyHash, Sessions: 17,
		Time: sample.Time, Clock: sample.Clock, NextExpiry: sample.NextExpiry,
		HeadSeq: 4269, Head: sample.Head.Hash, Key: sample.Key[:],
	}
	node := func(answer func(nc net.Conn)) dialFunc {
		return func(_ context.Context, addr string) (net.Conn, error) {
			if addr != "n3:7101" {
				return nil, errRefused
			}
			mine, theirs := net.Pipe()
			go func() {
				defer theirs.Close()
				frame, _, err := wire.ReadFrame(theirs, nil, cluster, wire.MaxClientBody)
				if err != nil || frame.StatusRequest() != nil {
					t.Errorf("node got %v, %v, want a status request", frame.Type, err)
					return
				}
				answer(theirs)
			}()
			return mine, nil
		}
	}
	synctest.Test(t, func(t *testing.T) {
		st, err := status(context.Background(), cluster, "n3:7101", node(func(nc net.Conn) {
			_, _ = nc.Write(wire.AppendStatus(nil, cluster, sample))
		}))
		if err != nil || !reflect.DeepEqual(st, want) {
			t.Fatalf("status %+v, %v, want %+v", st, err, want)
		}
		keyless := sample
		keyless.Key = [32]byte{}
		st, err = status(context.Background(), cluster, "n3:7101", node(func(nc net.Conn) {
			_, _ = nc.Write(wire.AppendStatus(nil, cluster, keyless))
		}))
		if err != nil || st.Key != nil || st.HeadSeq != 4269 {
			t.Fatalf("status of a node without a key: key %x, head %d, %v", st.Key, st.HeadSeq, err)
		}
		if st.Role.String() != "leader" {
			t.Fatalf("role %q", st.Role)
		}

		_, err = status(context.Background(), cluster, "n3:7101", node(func(nc net.Conn) {
			_, _ = nc.Write(wire.AppendReply(nil, cluster, client.Reply{Status: client.OK}))
		}))
		if !errors.Is(err, wire.ErrFormat) {
			t.Fatalf("status answered with a reply: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, err = status(ctx, cluster, "n3:7101", node(func(nc net.Conn) {
			_, _ = nc.Read(make([]byte, 1))
		}))
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("status of a silent node: %v, want DeadlineExceeded", err)
		}

		if _, err := status(context.Background(), cluster, "n1:7101", node(nil)); !errors.Is(err, errRefused) {
			t.Fatalf("status of an unreachable node: %v", err)
		}
	})
}
