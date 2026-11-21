// Package replica is one ledger node: a single loop that drives the raft core, keeps the log, the
// metadata with the seal and snapshots on a disk.FS, and applies committed entries to the state
// machine and the client sessions. Init makes the directory of a new node; Open refuses one that is
// not initialised or belongs to another node, decides whether the disk is damaged and, if it is,
// starts the node in repair. The order of the steps and the session rules are in
// docs/ARCHITECTURE.md.
package replica
