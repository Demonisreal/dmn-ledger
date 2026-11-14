# ADR-005: Time comes from the log

Status: accepted, 2026-11-11.

## Context

Transfers get timestamps, and pending transfers expire after a timeout. If every replica read its
own clock while applying, two replicas would expire different transfers and their balances would
drift apart. Clocks in a cluster disagree, jump and run at different speeds.

## Decision

Only the leader reads a clock, once, when it proposes a request. The timestamp travels in the log
entry, and every replica applies the entry with that timestamp. The state machine uses
`max(ts, last + 1)`, so time never goes backwards and every batch has its own instant.

The leader bounds its own clock by its tick count. Every node keeps an anchor, a trusted time and
the tick it was taken in. A leader stamps the clock reading if it is at most
`anchor + (ticks since the anchor + 1) * ClockStep`, and the bound otherwise. A trusted reading
moves the anchor, but never above `anchor + ticks since the anchor * ClockStep`; a reading above
the bound does not move it. A node also moves the anchor when it applies a write it did not stamp
in its current term, and when it loads a snapshot. A leader stamps only after applying an entry of
its own term, so it starts from the newest time of the cluster. The details are under
"Timestamps" in [ARCHITECTURE.md](../ARCHITECTURE.md).

## Consequences

- A wrong clock can move deadlines, never balances or the order of transfers. Replicas cannot
  diverge because of time.
- The planted bug B12 (apply with the replica's own clock) is found by the end check S7 in the
  first seed: the replicas disagree on the digest.
- Ledger time never moves by more than one `ClockStep` plus `ClockStep` per tick since the anchor,
  plus one nanosecond per write, whatever the clock does; each new leader gets that one step once.
  With `ClockStep` at 1.25 ticks, as in the simulator, it runs at most 25 % faster than the leader's
  ticks. A deadline set while ledger time catches up can expire early by that factor: a 10 s timeout
  lasts at least 8 s, never zero. That is the price of a bound that only knows the leader's own
  ticks.
- Ticks without a write count towards the bound, so after a long idle time a jump of up to that
  length passes at once.
- An honest clock passes unchanged as long as the same node leads and its ticks keep pace. After a
  stall that swallowed ticks, or after a leader change, the bound is below the clock and is stamped
  instead. Counted with a counter added to `stamp`, which is not part of the repository, over 400
  seeds per profile, the bound was stamped for a quarter to half of the writes: about 28 % in `net`
  and `split`, 47 % in `leaders` and 51 % in `crash`, on average 0.9 to 1.6 s behind the leader's
  clock, and never in `calm`. The cause is mostly leader changes: the lag is the time since the
  last write the new leader had applied, and it shrinks at a quarter of real time.
- A clock that jumps back cannot move ledger time back. If writes arrive while it is behind, ledger
  time trails by as long as the clock was behind and catches up at a quarter of real time.
- `BadClock` (a reading of 2^62 or more) is only possible before a node has an anchor. Until then
  the leader's clock is taken as it is: one clock is trusted once. Afterwards no clock value stops
  the ledger from accepting writes.
- The simulator gives each node an offset, jumps forwards and backwards and different tick speeds
  (profile `clock`), and `internal/replica/clock_test.go` checks the bounds. No checker in the
  simulator observes time, so a stamp that is wrong but within the replicas' agreement only shows
  in those tests.

## Rejected alternatives

- **Clock of each replica at apply time.** Diverges, as B12 shows.
- **Hybrid logical clocks.** Solve ordering across nodes, which the log already provides.
- **Moving the anchor when entries are applied.** An earlier version of the bound counted from the
  last applied ledger time. It let a jump through once per write instead of once per tick, and an
  honest clock fell behind by the commit latency. Moving the anchor when the leader stamps fixes
  both.
- **Moving the anchor to every trusted reading.** A reading just below the bound would become the
  next anchor and free another full step, so a clock that creeps ahead by up to one `ClockStep`
  per write would not be slowed down at all. The anchor therefore never rises faster than the
  line it already describes.
- **A window around the median clock of the majority.** Would bound a leader whose clock is far
  off and remove the 25 % factor. Needs clocks in heartbeats; not built.
