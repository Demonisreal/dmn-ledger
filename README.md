# dmn-ledger

A replicated double-entry ledger in Go: accounts and transfers, pending transfers that are posted,
voided or expire, and linked chains that succeed or fail as a whole. The ledger is kept on three or
five nodes with its own Raft implementation and its own write-ahead log. The account and transfer
model follows TigerBeetle's; the code is written independently in Go, and storage and replication
work differently.

The aim: no acknowledged transfer is lost or applied twice, and the books always balance, as long
as a majority of nodes is up. That has to hold when nodes crash in the middle of a write, when
messages are lost, delayed or reordered, when the network splits, when clocks are wrong and when
a disk returns broken data. Today this is checked, not proven: a deterministic simulator runs the
real node code against those faults and checks every run. There is no network server yet, so the
cluster runs only inside the simulator and the tests.

A node that finds a flipped bit or a garbled sector in data it had acknowledged stops voting
until the leader has filled it up again. It still counts for commits and reads, so a leader in
office keeps working with it. With three nodes, one damaged node and an unreachable leader stop
the cluster until the leader is back. Two damaged nodes of three can stop it for good. In both
cases it accepts no request, but it never answers one wrongly.

## Status

Works today:

- State machine with the ledger rules in [docs/ledger-rules.md](docs/ledger-rules.md), tested
  against a second, deliberately plain implementation.
- Write-ahead log with segments, checksums and torn-write handling, a metadata file with term,
  vote and a seal, and snapshots.
- Raft core without I/O: pre-vote, check-quorum, ReadIndex, snapshots for lagging followers.
- Repair after disk corruption. A seal on disk tells a torn write from a broken frame the node
  had acknowledged. A damaged node runs as a non-voter, is filled up by the leader and votes
  again, without anyone stepping in. Tested with a power loss at every disk access of a
  three-node run, with randomised schedules of the core and in the simulator. Read errors still
  stop a node, and the limits are in [ADR-009](docs/adr/009-disk-corruption-seal-and-repair.md).
- Node layer: one loop per node, client sessions for exactly-once, linearizable reads through
  ReadIndex, timestamps that come from the log.
- Simulator with network, disk and clock faults and disk corruption, checkers for safety,
  linearizability and progress, and eighteen planted bugs behind a build tag.

Not built yet:

- Network server, wire protocol, command-line client and the client SDK around the session core.
- Audit trail with proofs for single transfers.
- Benchmarks of a cluster on real disks and a real network, and a demo.
- Membership changes, leadership transfer and reads from followers.

## Try it

Run 1000 seeds of the simulator, each a full cluster with faults and a workload:

```
$ go run ./cmd/sim -seeds 1000
seeds 0 to 999 profile all: ok, widest window 32, 8.072s on 16 cores
```

Plant a bug, here B15 (a node whose disk has lost acknowledged entries still takes part in
elections), and let the simulator look for it in `rot`, the profile with disk corruption:

```
$ go run -tags simbug ./cmd/sim -bug B15 -seeds 100 -profile rot
seed 11 profile rot failed: seed 11 at 3806635025: S10 on n2: sent PreVote to n1 while in repair
reproduce: go run -tags simbug ./cmd/sim -bug B15 -seed 11 -profile rot -trace
exit status 1
```

Checker S10 saw node 2 ask for a pre-vote while it was in repair, 3.8 virtual seconds into the
run. The printed command replays exactly that run and writes every event, here about 6100 lines.
They show how the node got there:

```
$ go run -tags simbug ./cmd/sim -bug B15 -seed 11 -profile rot -trace > b15.txt
exit status 1
$ grep -n -e 'rot n2' -e 'start n2' b15.txt | tail -n 2
5577:3467786555 rot n2 /ledger/wal/wal-00000000000000000033.log 7396 bit
5852:3648150370 start n2
$ tail -n 3 b15.txt
3806635025 tick n2
seed 11 profile rot failed: seed 11 at 3806635025: S10 on n2: sent PreVote to n1 while in repair
reproduce: go run -tags simbug ./cmd/sim -bug B15 -seed 11 -profile rot -trace
```

At 3.47 s the fault plan cut the power of node 2 and flipped a bit in one of its WAL segments. The
node started again at 3.65 s, in repair. At 3.81 s its election timeout ran out, and with the
planted bug it asked for votes.

`go run ./cmd/sim -h` lists the flags and the fault profiles: `calm`, `net`, `crash`, `clock`,
`all`, `evict`, `leaders`, `split`, `rot` and `beyond`. Built with `go build -o sim ./cmd/sim`, it
exits with 0 when every seed passed, 1 for a finding, 2 for a usage error and 3 when a run failed
without a finding; `go run` prints the real code as "exit status N" and itself exits with 1.
`go test ./...` runs all tests, `go test -tags simbug ./...` also checks that the planted bugs are
found, seventeen of them by the simulator and one by the tests of the core.

## Requirements

Go 1.27.4 or newer, as set in `go.mod`. The only dependency is
[porcupine](https://github.com/anishathalye/porcupine) for the linearizability check.

## Documentation

- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md): layers, the contract between the Raft core and its
  driver, the core's rules, the node, the simulator, the checkers and the planted bugs.
- [docs/adr/](docs/adr/): the decisions and what they cost.
  [ADR-009](docs/adr/009-disk-corruption-seal-and-repair.md) covers disk corruption: what is
  detected, how a node is repaired and where that ends.
- [docs/ledger-rules.md](docs/ledger-rules.md): fields, check order, time, chains, invariants and
  the snapshot format of the state machine.

## License

All rights reserved. Prospective employers and clients may download, read and run the code to
evaluate the author. See [LICENSE](LICENSE).
