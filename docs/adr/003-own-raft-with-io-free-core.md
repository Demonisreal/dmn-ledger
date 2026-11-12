# ADR-003: Raft written for this project, with an I/O-free core

Status: accepted, 2026-11-11.

## Context

The ledger replicates one log over three or five nodes. Raft is the obvious protocol. The question
is whether to write it or to use a library such as etcd/raft.

A common argument for writing it yourself is that clock, network and disk must be replaceable in
a simulator. That argument does not hold: etcd/raft is I/O-free as well and could be driven by
the same simulator.

## Decision

`internal/raft` is written for this project. It is a state machine without clock, network, disk
or goroutines: the driver feeds it `Tick`, `Step`, `Propose` and `ReadIndex`, takes a `Ready`,
does the work and calls `Advance`. The vocabulary (`Ready`, `Advance`, `Step`, `HardState`,
`MsgApp`) follows etcd/raft on purpose, so that anyone who knows that library can read this one.

The reasons:

- The project exists to show that the protocol is understood, not only used. The core's rules R1
  to R18 are listed under "Rules" in [ARCHITECTURE.md](../ARCHITECTURE.md), and each has a table
  test in `internal/raft/rules_test.go`.
- The planned handling of disk corruption reaches into the core: a node that detected corruption
  must not vote until it has been repaired, and acknowledgements wait for a seal on disk. Neither
  is built yet. In a core written for the project both are small changes; with a library they
  would mean maintaining a fork.

## Consequences

- Pre-vote, check-quorum and ReadIndex are built in. Leases, leadership transfer, membership
  changes and follower reads are not.
- The core is small enough to read in one sitting. Five of its safety decisions can be switched
  off in a `simbug` build: the log comparison for votes, dropping a conflicting tail, the
  follower's commit bound, committing only entries of the leader's own term, and read
  confirmation by a majority. The rule tests of the core fail for each of them:
  `go test -tags simbug ./internal/raft -args -bug B6` runs them with B6 switched on and fails,
  and `TestRuleTestsCatchTheCoreBugs` checks all five. The simulator finds all five as well, but
  for other mistakes in the core it is weaker than the rule tests (ADR-008).
- Bugs in the core are this project's bugs. The answer to that risk is the rule tests, randomised
  schedules in memory and the full simulator, not trust in maturity.

## Rejected alternatives

- **etcd/raft.** The right choice for a production system: years of use, many users, well
  documented. Rejected only because of the two reasons above. If the planned repair mode were
  dropped, it would be the better option.
- **hashicorp/raft.** Owns its own goroutines, transport and storage. It cannot be stepped
  deterministically from a single thread without replacing most of it.
