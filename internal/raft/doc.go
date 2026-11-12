// Package raft is the consensus core: a deterministic state machine without I/O, clock or goroutines.
// The driver feeds it Tick, Step, Propose and ReadIndex and carries out each Ready before calling Advance.
// The driver contract, memory ownership, reads and the rules R1 to R24 are in docs/ARCHITECTURE.md.
package raft
