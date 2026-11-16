package main

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Demonisreal/dmn-ledger/ledger"
)

func hash(b byte) [32]byte {
	var h [32]byte
	for i := range h {
		h[i] = b + byte(i)
	}
	return h
}

func TestStatusOutput(t *testing.T) {
	const at = 1794808307473359071
	node := func(role ledger.Role, applied uint64, h byte) ledger.NodeStatus {
		return ledger.NodeStatus{Role: role, Term: 2, Commit: 4271, Applied: applied, ApplyHash: hash(h), HeadSeq: applied - 3000, Head: hash(h + 0x10), Time: at}
	}
	for _, tc := range []struct {
		name   string
		probes []probe
		code   int
		want   string
	}{
		{"healthy", []probe{
			{addr: "127.0.0.1:7101", st: node(ledger.Follower, 4271, 0x36), clock: 1400 * time.Microsecond},
			{addr: "127.0.0.1:7102", st: node(ledger.Follower, 4271, 0x36), clock: -2 * time.Millisecond},
			{addr: "127.0.0.1:7103", st: node(ledger.Leader, 4271, 0x36), clock: -400 * time.Microsecond},
		}, 0, `node  address         role      term  commit  applied  apply hash        head                   repair  ledger time                     clock  next expiry
1     127.0.0.1:7101  follower  2     4271    4271     363738393a3b3c3d  1271:464748494a4b4c4d  no      2026-11-16T05:51:47.473359071Z  +1ms   -
2     127.0.0.1:7102  follower  2     4271    4271     363738393a3b3c3d  1271:464748494a4b4c4d  no      2026-11-16T05:51:47.473359071Z  -2ms   -
3     127.0.0.1:7103  leader    2     4271    4271     363738393a3b3c3d  1271:464748494a4b4c4d  no      2026-11-16T05:51:47.473359071Z  +0ms   -
nodes agree at index 4271
`},
		{"one down, one in repair", []probe{
			{addr: "127.0.0.1:7101", st: node(ledger.Leader, 4271, 0x36)},
			{addr: "127.0.0.1:7102", err: errors.New("dial tcp 127.0.0.1:7102: connect: connection refused")},
			{addr: "[::1]:7103", st: ledger.NodeStatus{Role: ledger.Follower, Repair: true, Term: 2, Commit: 4271, Applied: 4271, ApplyHash: hash(0x36), HeadSeq: 1271, Head: hash(0x46), Time: at, NextExpiry: at + 3600e9}, clock: 12 * time.Millisecond},
		}, 1, `node  address         role      term  commit  applied  apply hash        head                   repair  ledger time                     clock  next expiry
1     127.0.0.1:7101  leader    2     4271    4271     363738393a3b3c3d  1271:464748494a4b4c4d  no      2026-11-16T05:51:47.473359071Z  +0ms   -
2     127.0.0.1:7102  unreachable: dial tcp 127.0.0.1:7102: connect: connection refused
3     [::1]:7103      follower  2     4271    4271     363738393a3b3c3d  1271:464748494a4b4c4d  yes     2026-11-16T05:51:47.473359071Z  +12ms  2026-11-16T06:51:47.473359071Z
nodes agree at index 4271
`},
		{"differ", []probe{
			{addr: "10.0.0.1:7101", st: node(ledger.Leader, 4272, 0x36)},
			{addr: "10.0.0.2:7101", st: node(ledger.Follower, 4271, 0x36)},
			{addr: "10.0.0.3:7101", st: node(ledger.Follower, 4271, 0x99)},
			{addr: "10.0.0.4:7101", st: node(ledger.Follower, 4270, 0x01)},
			{addr: "10.0.0.5:7101", st: node(ledger.Follower, 4270, 0x01)},
		}, 1, `node  address        role      term  commit  applied  apply hash        head                   repair  ledger time                     clock  next expiry
1     10.0.0.1:7101  leader    2     4271    4272     363738393a3b3c3d  1272:464748494a4b4c4d  no      2026-11-16T05:51:47.473359071Z  +0ms   -
2     10.0.0.2:7101  follower  2     4271    4271     363738393a3b3c3d  1271:464748494a4b4c4d  no      2026-11-16T05:51:47.473359071Z  +0ms   -
3     10.0.0.3:7101  follower  2     4271    4271     999a9b9c9d9e9fa0  1271:a9aaabacadaeafb0  no      2026-11-16T05:51:47.473359071Z  +0ms   -
4     10.0.0.4:7101  follower  2     4271    4270     0102030405060708  1270:1112131415161718  no      2026-11-16T05:51:47.473359071Z  +0ms   -
5     10.0.0.5:7101  follower  2     4271    4270     0102030405060708  1270:1112131415161718  no      2026-11-16T05:51:47.473359071Z  +0ms   -
nodes DIFFER at index 4271
`},
		{"same apply hash, other head", []probe{
			{addr: "10.0.0.1:7101", st: node(ledger.Leader, 4271, 0x36)},
			{addr: "10.0.0.2:7101", st: ledger.NodeStatus{Role: ledger.Follower, Term: 2, Commit: 4271, Applied: 4271, ApplyHash: hash(0x36), HeadSeq: 1271, Head: hash(0x99), Time: at}},
		}, 1, `node  address        role      term  commit  applied  apply hash        head                   repair  ledger time                     clock  next expiry
1     10.0.0.1:7101  leader    2     4271    4271     363738393a3b3c3d  1271:464748494a4b4c4d  no      2026-11-16T05:51:47.473359071Z  +0ms   -
2     10.0.0.2:7101  follower  2     4271    4271     363738393a3b3c3d  1271:999a9b9c9d9e9fa0  no      2026-11-16T05:51:47.473359071Z  +0ms   -
nodes DIFFER at index 4271
`},
		{"no shared index", []probe{
			{addr: "127.0.0.1:7101", st: ledger.NodeStatus{Role: ledger.Leader, Term: 1, Commit: 3, Applied: 3, ApplyHash: hash(1)}},
			{addr: "127.0.0.1:7102", st: ledger.NodeStatus{Role: ledger.PreCandidate, Term: 1, Commit: 2, Applied: 2, ApplyHash: hash(2)}},
			{addr: "127.0.0.1:7103", st: ledger.NodeStatus{Role: ledger.Candidate, Term: 1, Commit: 1, Applied: 1, ApplyHash: hash(3)}},
		}, 0, `node  address         role          term  commit  applied  apply hash        head                repair  ledger time  clock  next expiry
1     127.0.0.1:7101  leader        1     3       3        0102030405060708  0:0000000000000000  no      -            +0ms   -
2     127.0.0.1:7102  precandidate  1     2       2        0203040506070809  0:0000000000000000  no      -            +0ms   -
3     127.0.0.1:7103  candidate     1     1       1        030405060708090a  0:0000000000000000  no      -            +0ms   -
no two nodes at the same index
`},
		{"single node in repair", []probe{
			{addr: "127.0.0.1:7101", st: ledger.NodeStatus{Role: ledger.Follower, Repair: true, Term: 4, Commit: 9, Applied: 9, ApplyHash: hash(7), Time: at}},
		}, 1, `node  address         role      term  commit  applied  apply hash        head                repair  ledger time                     clock  next expiry
1     127.0.0.1:7101  follower  4     9       9        0708090a0b0c0d0e  0:0000000000000000  yes     2026-11-16T05:51:47.473359071Z  +0ms   -
no two nodes at the same index
`},
		{"nobody answers", []probe{
			{addr: "127.0.0.1:7101", err: errors.New("context deadline exceeded")},
			{addr: "127.0.0.1:7102", err: errors.New("context deadline exceeded")},
			{addr: "127.0.0.1:7103", err: errors.New("context deadline exceeded")},
		}, 3, `node  address         role  term  commit  applied  apply hash  head  repair  ledger time  clock  next expiry
1     127.0.0.1:7101  unreachable: context deadline exceeded
2     127.0.0.1:7102  unreachable: context deadline exceeded
3     127.0.0.1:7103  unreachable: context deadline exceeded
no two nodes at the same index
`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer
			if code := report(&out, tc.probes); code != tc.code || out.String() != tc.want {
				t.Fatalf("exit %d, want %d, output:\n%s\nwant:\n%s", code, tc.code, out.String(), tc.want)
			}
		})
	}
}

func TestClockIsMeasuredAtHalfTheRoundTrip(t *testing.T) {
	sent := time.Unix(0, 1794808307000000000)
	for _, tc := range []struct {
		clock uint64
		back  time.Duration
		want  time.Duration
	}{
		{1794808307005000000, 10 * time.Millisecond, 0},
		{1794808307009000000, 10 * time.Millisecond, 4 * time.Millisecond},
		{1794808306998000000, 4 * time.Millisecond, -4 * time.Millisecond},
	} {
		if got := offset(tc.clock, sent, sent.Add(tc.back)); got != tc.want {
			t.Errorf("node clock %d, answer after %v: offset %v, want %v", tc.clock, tc.back, got, tc.want)
		}
	}
}

func TestStatusKeys(t *testing.T) {
	key := hash(0x50)
	probes := []probe{
		{addr: "127.0.0.1:7101", st: ledger.NodeStatus{Key: key[:]}},
		{addr: "127.0.0.1:7102", err: errors.New("connection refused")},
		{addr: "127.0.0.1:7103", st: ledger.NodeStatus{}},
	}
	var out, errs bytes.Buffer
	c := &ctl{stdout: &out, stderr: &errs}
	if code := c.keyList(probes); code != 1 || out.String() != "505152535455565758595a5b5c5d5e5f606162636465666768696a6b6c6d6e6f node 1\n" ||
		errs.String() != "node 2 at 127.0.0.1:7102: unreachable: connection refused\nnode 3 at 127.0.0.1:7103 has no key\n" {
		t.Fatalf("exit %d, stdout %q, stderr %q", code, out.String(), errs.String())
	}
	out.Reset()
	if code := c.keyList(probes[:1]); code != 0 || !strings.HasSuffix(out.String(), " node 1\n") {
		t.Fatalf("one node with a key: exit %d, %q", code, out.String())
	}
	if code := c.keyList(probes[1:2]); code != 3 {
		t.Fatalf("nobody answers: exit %d", code)
	}
}
