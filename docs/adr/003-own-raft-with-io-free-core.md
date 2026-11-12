# ADR-003: Own Raft implementation with an I/O-free core

Status: accepted.

## Context

The ledger replicates one log over three or five nodes. Raft is the obvious protocol. The question
is whether to write it or to use a library such as etcd/raft.

An earlier draft argued that an own implementation was needed so that clock, network and disk could
be replaced in the simulator. That argument does not hold: etcd/raft is I/O-free as well and could
be driven by the same simulator.

## Decision

`internal/raft` is written for this project. It is a state machine without clock, network, disk
or goroutines: the driver feeds it `Tick`, `Step`, `Propose` and `ReadIndex`, takes a `Ready`,
does the work and calls `Advance`. The vocabulary (`Ready`, `Advance`, `Step`, `HardState`,
`MsgApp`) follows etcd/raft on purpose, so that anyone who knows that library can read this one.

The reasons are honest ones:

- The project exists to show that the protocol is understood, not only used. Every rule (R1 to R18
  in the phase document) has a table test, and the rules that matter for safety have a test that
  fails when the rule is removed.
- Disk repair reaches into the core: a node that detected disk corruption must not vote until it has
  been repaired, and acknowledgements wait for a seal on disk. Both are small changes in an own
  core and a fork of a library.

## Consequences

- Pre-vote, check-quorum and ReadIndex are built in. Leases, leadership transfer, membership
  changes and follower reads are not.
- The core is small enough to read in one sitting, and every safety rule can be switched off in a
  `simbug` build to show that the simulator notices (ADR-008).
- Bugs in the core are this project's bugs. The answer to that risk is the rule tests, randomised
  schedules in memory and the full simulator, not trust in maturity.

## Rejected alternatives

- **etcd/raft.** The right choice for a production system: years of use, many users, well
  documented. Rejected only because of the two reasons above. If the repair mode were dropped, it
  would be the better option.
- **hashicorp/raft.** Owns its own goroutines, transport and storage. It cannot be stepped
  deterministically from a single thread without replacing most of it.
