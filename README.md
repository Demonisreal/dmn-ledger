# dmn-ledger

A replicated double-entry ledger in Go: accounts and transfers, pending transfers that are posted,
voided or expire, and linked chains that succeed or fail as a whole. The ledger is kept on three or
five nodes with its own Raft implementation and its own write-ahead log. The account and transfer
model follows TigerBeetle's; the code is written independently in Go, and storage and replication
work differently.

The aim: no acknowledged transfer is lost or applied twice, and the books always balance, as long
as a majority of nodes is up. That has to hold when nodes crash in the middle of a write, when
messages are lost, delayed or reordered, when the network splits and when clocks are wrong. Today
this is checked, not proven: a deterministic simulator runs the real node code against those faults
and checks every run. There is no network server yet, so the cluster runs only inside the
simulator and the tests.

## Status

Works today:

- State machine with the ledger rules in [docs/ledger-rules.md](docs/ledger-rules.md), tested
  against a second, deliberately plain implementation.
- Write-ahead log with segments, checksums, torn-write handling, hard state and snapshots.
- Raft core without I/O: pre-vote, check-quorum, ReadIndex, snapshots for lagging followers.
- Node layer: one loop per node, client sessions for exactly-once, linearizable reads through
  ReadIndex, timestamps that come from the log.
- Simulator with network, disk and clock faults, checkers for safety, linearizability and
  progress, and twelve planted bugs behind a build tag.

Not built yet:

- Network server, wire protocol, command-line client and the client SDK around the session core.
- Repair after disk corruption. Checksums detect broken data; a node that finds it stops.
- Audit trail with proofs for single transfers.
- Benchmarks of a cluster on real disks and a real network, and a demo.
- Membership changes, leadership transfer and reads from followers.

## Try it

Run 1000 seeds of the simulator, each a full cluster with faults and a workload:

```
$ go run ./cmd/sim -seeds 1000
seeds 0 to 999 profile all: ok, widest window 32, 6.693s on 16 cores
```

Plant a bug, here B9 (a follower commits up to its own log end instead of the end of the leader's
message), and let the simulator look for it:

```
$ go run -tags simbug ./cmd/sim -bug B9 -seeds 200
seed 14 profile all failed: seed 14 at 3418693013: S3 on n4: applied 79 with hash 284cbbd8, seen 003aa367
reproduce: go run -tags simbug ./cmd/sim -bug B9 -seed 14 -profile all -trace
exit status 1
```

Checker S3 saw node 4 reach index 79 with an apply hash that differs from the one recorded for
that index before, 3.4 virtual seconds into the run. The printed command replays exactly that run
and writes every event, here about 8800 lines:

```
$ go run -tags simbug ./cmd/sim -bug B9 -seed 14 -profile all -trace > b9.txt
exit status 1
$ tail -n 4 b9.txt
3418452139 send n5->n3 App t5 i102/5 c102 s7 e103-103 due 3419625400
3418693013 wake n4
seed 14 profile all failed: seed 14 at 3418693013: S3 on n4: applied 79 with hash 284cbbd8, seen 003aa367
reproduce: go run -tags simbug ./cmd/sim -bug B9 -seed 14 -profile all -trace
```

`go run ./cmd/sim -h` lists the flags and the fault profiles: `calm`, `net`, `crash`, `clock`,
`all`, `evict`, `leaders` and `split`. Built with `go build -o sim ./cmd/sim`, it exits with 0 when
every seed passed, 1 for a finding, 2 for a usage error and 3 when a run failed without a finding;
`go run` prints the real code as "exit status N" and itself exits with 1. `go test ./...` runs all
tests, `go test -tags simbug ./...` also checks that the simulator finds the planted bugs.

## Requirements

Go 1.27.4 or newer, as set in `go.mod`. The only dependency is
[porcupine](https://github.com/anishathalye/porcupine) for the linearizability check.

## Documentation

- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md): layers, the contract between the Raft core and its
  driver, the core's rules, the node, the simulator, the checkers and the planted bugs.
- [docs/adr/](docs/adr/): the decisions and what they cost. ADR-009 is kept for the handling of
  disk corruption and will be written with it.
- [docs/ledger-rules.md](docs/ledger-rules.md): fields, check order, time, chains, invariants and
  the snapshot format of the state machine.

## License

All rights reserved. Prospective employers and clients may download, read and run the code to
evaluate the author. See [LICENSE](LICENSE).
