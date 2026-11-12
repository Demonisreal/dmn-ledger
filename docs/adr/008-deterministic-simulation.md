# ADR-008: Deterministic simulation with the real node code

Status: accepted.

## Context

The bugs that matter in a replicated store appear under crashes in the middle of a write sequence,
lost and reordered messages and partitions, in combinations nobody writes a test for. Such a bug
seen once in a test run and never again is worth little.

## Decision

`internal/sim` runs a whole cluster in one goroutine on virtual time. One seed decides the fault
plan, network latencies, losses, disk behaviour and the workload. The nodes are the production
`replica.Node` with the real WAL on `disk.Sim`, the clients are the real session core of the SDK.
No package below the simulator reads a clock, starts a goroutine or uses global randomness;
`depguard` enforces that.

`internal/check` watches every run: one leader per term, one vote per node and term, the same apply
hash for each index on every node, terms that never fall, promises that survive a power cut on a
cloned disk, state invariants, equal digests at the end, an oracle replay of every confirmed write,
linearizability per window with porcupine, and progress after the faults have healed. `cmd/sim`
prints the first failing seed and the command that replays it with the full trace.

Twelve classic bugs can be planted with the build tag `simbug`, one at a time: three where the disk
lies, two in the order of disk and network, five in the consensus core and two in the sessions and
the clock. Without the tag the code paths do not exist.

## Consequences

- Every failure is a seed. `go run ./cmd/sim -seed N -trace` shows it again, event by event.
- 10 000 seeds take about a minute on four cores. CI runs 2000 fixed and 2000 new seeds on every
  pull request; longer runs happen at night outside CI.
- Ten of the twelve planted bugs are found, seven of them in the very first seed and the rest
  within 13 to 522 seeds. Two are not found in 1000 seeds, and the reason is known: a new leader's
  empty entry hides the Figure 8 commit bug, and check-quorum leaves almost no time for a stale
  read. The rule tests of the core catch both.
- The simulator checks, it does not prove. It knows disks only as well as `disk.Sim` models them,
  and a run is only as good as the faults its plan contains.
- porcupine's search can grow exponentially. It is bounded by the input, not by a timeout: few
  accounts, few clients, windows of at most 32 requests with nothing in flight at the borders.

## Rejected alternatives

- **Jepsen-style tests with real processes only.** Find real bugs, but are slow and cannot replay
  a failure. They come later as a second line.
- **Simulating only the consensus core.** Would have missed the disk bugs found in the WAL, which
  live in the code between the core and the disk.
- **A model checker such as TLA+.** Checks the design, not the code that runs. Worth doing for the
  repair protocol.
