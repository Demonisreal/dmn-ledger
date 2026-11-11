// Package raft is the consensus core: a deterministic state machine without I/O, clock or goroutines.
// The driver feeds it Tick, Step, Propose and ReadIndex and carries out each Ready before calling Advance.
// The driver contract, memory ownership and the rules for reads are in docs/ARCHITECTURE.md.
package raft
