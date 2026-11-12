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

The leader bounds its own clock by its tick count: `ts = min(clock, last + (ticks since last + 1) *
ClockStep)`, where `last` is the newest ledger time it has applied, counted from the tick it was
applied in. A leader stamps only after applying an entry of its own term, so `last` is the newest
time of the cluster. A clock that jumps ahead moves ledger time by at most `ClockStep` per tick since
the last write. A clock that falls behind only slows it down. Stamps at or above 2^62 are refused
with `BadClock` before they reach the log.

## Consequences

- A wrong clock can move deadlines, never balances or the order of transfers. Replicas cannot
  diverge because of time.
- The planted bug B12 (apply with the replica's own clock) is found by the end check S7 in the
  first seed: the replicas disagree on the digest.
- A leader that restarts into a long pause without writes stamps behind the real clock until its
  tick count catches up. Deadlines then expire late.
- After a pause `T` without writes, a jump may put ledger time `(ClockStep/Tick - 1) * T` ahead of
  real time, and deadlines expire that much early.
- The very first write of a cluster takes the leader's clock as it is: one clock is trusted once.
  The window around the majority clock (below) would close this.
- The simulator gives each node an offset, jumps forwards and backwards and different tick speeds
  (profile `clock`).

## Rejected alternatives

- **Clock of each replica at apply time.** Diverges, as B12 shows.
- **Hybrid logical clocks.** Solve ordering across nodes, which the log already provides.
- **A window around the median clock of the majority.** Would bound a leader whose clock is far
  off. Needs clocks in heartbeats; kept for the wire protocol.
