//go:build !simbug

package raft

func upToDate(term, index, lastTerm, last uint64) bool {
	return term > lastTerm || term == lastTerm && index >= last
}

func (r *Raft) maybeCommit() {
	var buf [maxPeers]uint64
	match := buf[:0]
	for _, p := range r.prs {
		match = append(match, p.match)
	}
	if n := quorumValue(match, r.quorum()); n > r.commit && n >= r.termStart {
		r.commit = n
	}
}

func (r *Raft) merge(ents []Entry) error {
	for k, e := range ents {
		term, ok, err := r.termAt(e.Index)
		if err != nil {
			return err
		}
		if !ok || term != e.Term {
			r.appendEnts(ents[k:])
			return nil
		}
	}
	return nil
}

func (r *Raft) followCommit(commit, last uint64) {
	r.commit = max(r.commit, min(commit, last))
}

func (r *Raft) confirmedSeq() uint64 {
	var buf [maxPeers]uint64
	seqs := buf[:0]
	for _, p := range r.prs {
		seqs = append(seqs, p.seq)
	}
	return quorumValue(seqs, r.quorum())
}
