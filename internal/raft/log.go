package raft

import (
	"fmt"
	"slices"
)

func (r *Raft) lastIndex() uint64 {
	if n := len(r.ents); n > 0 {
		return r.ents[n-1].Index
	}
	if r.snap != nil {
		return r.snap.Index
	}
	return r.log.LastIndex()
}

func (r *Raft) termAt(i uint64) (uint64, bool, error) {
	if n := len(r.ents); n > 0 && i >= r.ents[0].Index {
		if i > r.ents[n-1].Index {
			return 0, false, nil
		}
		return r.ents[i-r.ents[0].Index].Term, true, nil
	}
	if r.snap != nil {
		if i != r.snap.Index {
			return 0, false, nil
		}
		return r.snap.Term, true, nil
	}
	s := r.log.Snapshot()
	switch {
	case i == s.Index:
		return s.Term, true, nil
	case i == 0:
		return 0, r.log.FirstIndex() == 1, nil
	case i < r.log.FirstIndex() || i > r.log.LastIndex():
		return 0, false, nil
	}
	t, err := r.log.Term(i)
	if err != nil {
		return 0, false, fmt.Errorf("raft: term at %d: %w", i, err)
	}
	return t, true, nil
}

func (r *Raft) lastTerm() (uint64, error) {
	last := r.lastIndex()
	t, ok, err := r.termAt(last)
	if err == nil && !ok {
		err = fmt.Errorf("raft: last index %d unreadable", last)
	}
	return t, err
}

func (r *Raft) entries(lo, hi uint64) ([]Entry, error) {
	var ents []Entry
	var size int64
	if len(r.ents) == 0 || lo < r.ents[0].Index {
		end := hi
		if len(r.ents) > 0 {
			end = min(hi, r.ents[0].Index)
		}
		stable, err := r.log.Entries(lo, end, r.maxAppendBytes)
		if err != nil {
			return nil, fmt.Errorf("raft: entries [%d, %d): %w", lo, end, err)
		}
		if uint64(len(stable)) < end-lo || end == hi {
			return stable, nil
		}
		for _, e := range stable {
			size += int64(len(e.Body))
		}
		ents, lo = stable, end
	}
	off := r.ents[0].Index
	unstable := r.ents[lo-off : hi-off]
	ents = slices.Grow(slices.Clip(ents), len(unstable))
	for _, e := range unstable {
		if len(ents) > 0 && size+int64(len(e.Body)) > r.maxAppendBytes {
			break
		}
		ents = append(ents, e)
		size += int64(len(e.Body))
	}
	return ents, nil
}

func (r *Raft) appendEnts(ents []Entry) {
	if len(r.ents) > 0 && ents[0].Index > r.ents[0].Index {
		r.ents = append(r.ents[:ents[0].Index-r.ents[0].Index], ents...)
		return
	}
	r.ents = append([]Entry(nil), ents...)
}
