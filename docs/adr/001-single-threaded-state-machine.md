# ADR-001: One state machine, one thread, no locks

Status: accepted, 2026-11-11.

## Context

Every replica applies the same committed entries and must end up with the same balances, byte for
byte. Anything that lets two replicas order the same work differently, such as thread scheduling,
map iteration or a clock read during apply, makes them drift apart without any fault in the
network or on disk.

A ledger also has operations that touch more than one account at once: a transfer moves value
between two accounts, a post resolves a pending transfer, and linked transfers succeed or fail as
one chain.

## Decision

`internal/state` is a plain state machine that one thread owns. `Apply(op, ts, body, dst)` takes
one batch: an operation, the timestamp from the log entry and a body of fixed-size big-endian
records (96 bytes per account, 100 per transfer). It appends the failed events to `dst` and
returns it. A batch the machine cannot take (unknown op, a body that is not a whole number of
records, a timestamp at the limit) is rejected with `ErrMalformedBatch` before anything changes.
The rules and their order are in [ledger-rules.md](../ledger-rules.md).

- No locks, no goroutines, no clock. `depguard` keeps `sync`, `time` and `os` out of the package.
  The node loop calls `Apply` for each committed entry in log order.
- A chain is atomic through an undo log: one record per booked transfer with the four balances it
  touched and the pending transfer it resolved. A failing event rolls the chain back from the end.
- Transfers live in fixed blocks of 16 384 and are never copied when the ledger grows.
- `Digest` is SHA-256 over the snapshot bytes, which depend only on the state. Replicas compare it
  in the simulator, and the model test compares it with `internal/oracle`, a second implementation
  written plainly with `math/big` and maps.

Throughput comes from batches: one request carries up to 1 MiB of records, and one log entry and
one fsync cover all of them.

## Consequences

- The same entries give the same state on every replica, which is what the checkers S3 and S7
  compare. There is nothing to reason about in terms of interleavings.
- A node applies on one core, however many it has. Lookups are answered in the same loop, so a
  large lookup delays the writes behind it.
- `Apply` does not allocate in steady state. On an otherwise idle i5-12500H laptop, against a ledger
  with 1000 accounts and 100 000 stored transfers,
  `go test -run '^$' -bench Apply -cpu 1 -count 3 ./internal/state/` reports 130 to 160 ns per
  transfer for batches of 1, 128 and 8000, with 0 allocations per batch. Under load from other
  processes the same command gave up to three times as much. These are numbers for the state machine
  alone, without log, disk or network.
- The full invariant check (`CheckInvariants`) recomputes every balance from the transfers. It is
  too slow for the write path and runs only in tests, in the simulator and when a snapshot is read.

## Rejected alternatives

- **Applying in parallel with locks per account.** Two transfers on the same account would race,
  and the order they won would have to be the same on every replica. That order already exists:
  it is the log.
- **A storage engine such as SQLite or an LSM tree below the state machine.** Brings its own
  threads, caches and file format. Equal state would no longer mean equal bytes, and every replica
  would need a canonical export to compare digests.
- **An overlay per chain instead of an undo log.** Changes go to private copies of the touched
  records and are merged when the chain succeeds. Simpler to get right, and it is what the oracle
  does on purpose, so the two implementations do not share the idea. In the state machine it would
  allocate for every chain.
