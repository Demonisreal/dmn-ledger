# ADR-005: Time comes from the log

Status: accepted.

## Context

Transfers get timestamps, and pending transfers expire after a timeout. If every replica read its
own clock while applying, two replicas would expire different transfers and their balances would
drift apart. Clocks in a cluster disagree, jump and run at different speeds.

## Decision

Only the leader reads a clock, once, when it proposes a request. The timestamp travels in the log
entry, and every replica applies the entry with that timestamp. The state machine uses
`max(ts, last + 1)`, so time never goes backwards and every batch has its own instant.

The leader bounds its own clock by its tick count: `ts = min(clock, anchor + (ticks + 1) *
ClockStep)`, where the anchor is the newest ledger time it has applied. A clock that jumps ahead
moves ledger time by at most `ClockStep` per tick. A clock that falls behind only slows it down.

## Consequences

- A wrong clock can move deadlines, never balances or the order of transfers. Replicas cannot
  diverge because of time.
- The planted bug B12 (apply with the replica's own clock) is found by the end check S7 in the
  first seed: the replicas disagree on the digest.
- A leader that starts after a long pause stamps behind the real clock until its tick count
  catches up. Deadlines then expire late, never early.
- The simulator gives each node an offset, jumps forwards and backwards and different tick speeds
  (profile `clock`).

## Rejected alternatives

- **Clock of each replica at apply time.** Diverges, as B12 shows.
- **Hybrid logical clocks.** Solve ordering across nodes, which the log already provides.
- **A window around the median clock of the majority.** Would bound a leader whose clock is far
  off. Needs clocks in heartbeats; kept for the wire protocol.
