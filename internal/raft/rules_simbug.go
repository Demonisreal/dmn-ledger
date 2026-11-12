//go:build simbug

package raft

import "github.com/Demonisreal/dmn-ledger/internal/simbug"

func upToDate(term, index, lastTerm, last uint64) bool {
	return simbug.On("B7") || term > lastTerm || term == lastTerm && index >= last
}

func (r *Raft) maybeCommit() {
	var buf [maxPeers]uint64
	match := buf[:0]
	for _, p := range r.prs {
		match = append(match, p.match)
	}
	if n := quorumValue(match, r.quorum()); n > r.commit && (n >= r.termStart || simbug.On("B6")) {
		r.commit = n
	}
}

func (r *Raft) merge(ents []Entry) error {
	for k, e := range ents {
		term, ok, err := r.termAt(e.Index)
		if err != nil {
			return err
		}
		if !ok || term != e.Term && !simbug.On("B8") {
			r.appendEnts(ents[k:])
			return nil
		}
	}
	return nil
}

func (r *Raft) followCommit(commit, last uint64) {
	if simbug.On("B9") {
		last = r.lastIndex()
	}
	r.commit = max(r.commit, min(commit, last))
}

func (r *Raft) confirmedSeq() uint64 {
	if simbug.On("B10") {
		return r.seq
	}
	var buf [maxPeers]uint64
	seqs := buf[:0]
	for _, p := range r.prs {
		seqs = append(seqs, p.seq)
	}
	return quorumValue(seqs, r.quorum())
}

func (r *Raft) voter() bool {
	return !r.repair || simbug.On("B15")
}

func (r *Raft) repairGoal() uint64 {
	if simbug.On("B16") {
		return r.commit
	}
	return r.lastIndex()
}

func (r *Raft) ack(m Message, index uint64) {
	r.acked = max(r.acked, index)
	if simbug.On("B17") {
		r.heal()
	}
	r.send(Message{Type: MsgAppResp, To: m.From, Term: r.term, Index: index, Seq: m.Seq})
}
