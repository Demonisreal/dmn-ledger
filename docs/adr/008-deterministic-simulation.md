# ADR-008: Deterministic simulation with the real node code

Status: accepted, 2026-11-11.

## Context

The bugs that matter in a replicated store appear under crashes in the middle of a write sequence,
lost and reordered messages and partitions, in combinations nobody writes a test for. Such a bug
seen once in a test run and never again is worth little.

## Decision

`internal/sim` runs a whole cluster in one goroutine on virtual time. One seed decides the fault
plan, network latencies, losses, disk behaviour and the workload. The nodes are the production
`replica.Node` with the real WAL on `disk.Sim`, the clients are the real session core of the SDK.
`raft`, `replica`, `state`, `client`, `workload` and `sim` itself import no clock, network, file
system, `sync` or `crypto/rand`; `depguard` enforces that. `wal` is not under that rule: it gets the
disk and the source of its segment nonces passed in, and the simulator passes `disk.Sim` and a
seeded source. `TestWALNoncesAreSeeded` checks that a seed alone decides the segment bytes, and
the same seed always gives the same `TraceHash`.

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
- `go run ./cmd/sim -seeds 10000 -profile all` takes about 65 s on the 16 threads of an otherwise
  idle i5-12500H laptop, and about 95 s with `GOMAXPROCS=4`. CI runs 2000 fixed and 2000 new
  seeds of `all` on every pull request, plus short sweeps of `crash`, `evict`, `leaders`, `split`
  and five-node clusters; longer runs happen at night outside CI (`scripts/nightly-sim.sh`).
- All twelve planted bugs are found, six of them in the very first seed. B6 needs the profile
  `leaders` (first find at seed 365) and is not found in 10 000 seeds of `all` or `crash`: Figure
  8 needs a leader to die between counting an old entry and getting its own acknowledged. B10
  shows sooner in `split` (seed 563) than in `all` (seed 1149). The table is under "Planted bugs"
  in [ARCHITECTURE.md](../ARCHITECTURE.md).
- Finding the planted bugs does not mean finding every bug in the core. Two other mutations of
  the core passed 58 000 seeds of `all`, `crash`, `net`, `evict` and five-node clusters and at
  least 5 000 each of `leaders` and `split`, and were caught only by the rule tests.
- The simulator checks, it does not prove. It knows disks only as well as `disk.Sim` models them,
  and a run is only as good as the faults its plan contains.
- porcupine's search can grow exponentially. It is bounded by the input, not by a timeout: few
  accounts, few clients, windows of at most 32 requests with nothing in flight at the borders.

## Rejected alternatives

- **Jepsen-style tests with real processes only.** Find real bugs, but are slow and cannot replay
  a failure. Planned as a second line once there is a network server.
- **Simulating only the consensus core.** Would miss the bugs in the code between the core and
  the disk: the write order of hard state and entries, fsync and directory sync, torn writes. The
  planted bugs B1 to B5 are of that kind.
- **A model checker such as TLA+.** Checks the design, not the code that runs. Worth doing for the
  planned repair protocol after disk corruption.
