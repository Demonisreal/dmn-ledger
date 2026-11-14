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
cloned disk or a node that knows it lost them, repair only where the plan damaged a disk and no
vote from a node in repair, state invariants, equal digests at the end, an oracle replay of every
confirmed write, a replay of the committed log against every call and every node, linearizability
per window with porcupine, and progress after the faults have healed. `cmd/sim` prints the first
failing seed and the command that replays it with the full trace.

Eighteen bugs can be planted with the build tag `simbug`, one at a time: three where the disk
lies, four in the order of disk, seal and network, one in the order of a start on a damaged disk,
eight in the consensus core and two in the sessions and the clock. Without the tag the code paths
do not exist.

## Consequences

- Every failure is a seed. `go run ./cmd/sim -seed N -trace` shows it again, event by event.
- `go run ./cmd/sim -seeds 10000 -profile all` takes about 85 s on the 16 threads of an otherwise
  idle i5-12500H laptop, and about 115 s with `GOMAXPROCS=4`. The profile `rot` takes as long.
  CI runs 2000 fixed and 2000 new seeds of `all` on every pull request, 1000 fixed and 1000 new
  seeds of `rot`, 500 of `beyond`, plus short sweeps of `crash`, `evict`, `leaders`, `split` and
  five-node clusters. Longer runs happen at night outside CI (`scripts/nightly-sim.sh`): 200 000
  new seeds of `all`, 10 000 each of `rot` and `beyond`, and a search for the planted bug B16.
- Seventeen of the eighteen planted bugs are found within the seeds that `TestBugsAreFound`
  gives them, eight of them in the very first seed. B6 needs the profile `leaders` (first find at
  seed 403) and is not found in 10 000 seeds of `all` or `crash`: Figure 8 needs a leader to die
  between counting an old entry and getting its own acknowledged. B10 shows sooner in `split`
  (seed 563) than in `all` (seed 3820). B16, a wrong goal for a repair, shows in `rot` at seed
  2987 and is left to the nightly run and to the tests of the core, which catch it at once. The
  table is under "Planted bugs" in [ARCHITECTURE.md](../ARCHITECTURE.md).
- Finding the planted bugs does not mean finding every bug in the core. Three other mutations of
  the core, two in replication and reads and one in the repair rules, passed between 4 000 and
  20 000 seeds of the profiles made for them and were caught only by the tests of the core. The
  numbers are under "Planted bugs" in [ARCHITECTURE.md](../ARCHITECTURE.md).
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
  repair protocol after disk corruption (ADR-009), which rests on an argument and on tests.
