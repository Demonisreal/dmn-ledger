package raft

import (
	"errors"
	"math/rand/v2"
	"testing"
)

type fuzzInput struct {
	b []byte
}

func (in *fuzzInput) next() byte {
	if len(in.b) == 0 {
		return 0
	}
	x := in.b[0]
	in.b = in.b[1:]
	return x
}

func (in *fuzzInput) message() Message {
	m := Message{
		Type:    MsgType(in.next()%7 + 1),
		From:    uint64(in.next()%3 + 1),
		To:      1,
		Term:    uint64(in.next() % 8),
		Index:   uint64(in.next() % 12),
		LogTerm: uint64(in.next() % 8),
		Commit:  uint64(in.next() % 14),
		Seq:     uint64(in.next() % 4),
	}
	flags := in.next()
	m.Reject = flags&1 != 0
	t := m.LogTerm
	for k := range uint64(flags>>1) % 4 {
		t += uint64(in.next() % 2)
		m.Entries = append(m.Entries, Entry{Index: m.Index + 1 + k, Term: t, Kind: 1, Body: []byte{byte(k)}})
	}
	return m
}

func FuzzStep(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{3, 2, 3, 3, 2, 0, 0, 0, 4, 1, 4, 1, 3, 2, 2, 0, 1, 3})
	f.Add([]byte{4, 4, 1, 3, 3, 2, 5, 1, 2, 1, 0, 4, 6, 1, 3, 5, 2, 0, 1, 0})
	f.Add([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 0, 1, 3, 3, 2, 0, 0, 0, 3, 1, 3, 3, 3, 2, 0, 0, 0})
	f.Fuzz(func(t *testing.T, b []byte) {
		log := newMemLog(1, 1, 2)
		hs := HardState{Term: 2}
		cfg := Config{
			ID: 1, Peers: []uint64{1, 2, 3}, ElectionTicks: 3, HeartbeatTicks: 1, SnapshotTicks: 3,
			MaxAppendBytes: 2, MaxUncommitted: 8, Rand: rand.New(rand.NewPCG(1, 2)), Log: log, HardState: hs,
		}
		r, err := New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		in := &fuzzInput{b: b}
		var commit uint64
		for len(in.b) > 0 {
			switch op := in.next(); op % 8 {
			case 0:
				err = r.Tick()
			case 1:
				_, err = r.Propose(1, []byte{op})
				if errors.Is(err, ErrNotLeader) || errors.Is(err, ErrBusy) {
					err = nil
				}
			case 2:
				err = r.ReadIndex(uint64(op))
				if errors.Is(err, ErrNotLeader) || errors.Is(err, ErrNotReady) {
					err = nil
				}
			default:
				err = r.Step(in.message())
			}
			if errors.Is(err, ErrSafety) {
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !r.HasReady() {
				continue
			}
			rd := r.Ready()
			if rd.HardState != nil {
				if rd.HardState.Term < hs.Term || rd.HardState.Term == hs.Term && hs.Vote != 0 && rd.HardState.Vote != hs.Vote {
					t.Fatalf("hard state %+v after %+v", *rd.HardState, hs)
				}
				hs = *rd.HardState
			}
			if rd.Snapshot != nil {
				if rd.Snapshot.Index <= commit {
					t.Fatalf("snapshot %d at or below commit %d", rd.Snapshot.Index, commit)
				}
				log.install(*rd.Snapshot)
			}
			if len(rd.Entries) > 0 {
				first := rd.Entries[0].Index
				if first <= commit || first > log.LastIndex()+1 {
					t.Fatalf("entries from %d, commit %d, last %d", first, commit, log.LastIndex())
				}
				for k, e := range rd.Entries {
					if e.Index != first+uint64(k) {
						t.Fatalf("entries not contiguous: %+v", rd.Entries)
					}
				}
				log.append(rd.Entries)
			}
			if lt, _ := log.Term(log.LastIndex()); lt > hs.Term {
				t.Fatalf("last entry term %d above hard state term %d", lt, hs.Term)
			}
			if rd.Commit < commit || rd.Commit > log.LastIndex() {
				t.Fatalf("commit %d after %d, last %d", rd.Commit, commit, log.LastIndex())
			}
			commit = rd.Commit
			for _, rr := range rd.Reads {
				if rr.Index > commit {
					t.Fatalf("read at %d above commit %d", rr.Index, commit)
				}
			}
			r.Advance()
		}
		cfg.HardState = hs
		if _, err := New(cfg); err != nil {
			t.Fatalf("restart: %v", err)
		}
	})
}
