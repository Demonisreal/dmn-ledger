// Package replica is one ledger node: a single loop that drives the raft core, keeps the log, hard
// state and snapshots on a disk.FS, and applies committed entries to the state machine and the
// client sessions. The order of the steps and the session rules are in docs/ARCHITECTURE.md.
package replica
