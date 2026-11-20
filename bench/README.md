# bench

The benchmarks of dmn-ledger: a module of its own that offers an open load to `ledgerd` and to
Postgres, writes one raw file per run and builds the report from those files. Why it measures the
way it does, and what the run of 2026-11-20 found, is in
[ADR-012](../docs/adr/012-benchmarks-open-load-one-machine-postgres.md). This file is for running
the plan yourself and for working out a figure of the report from its raw files.

## What a run needs

- **Linux with cgroup v2 and a systemd user session.** `scripts/bench.sh` starts the plan with
  `systemd-run --user --scope` under a memory limit, and `bench plan` refuses to start without
  one. It also needs `taskset`, `numfmt`, `git` and the Go version of `go.mod`.
- **A folder on ext4 below `/var/tmp`.** `BENCH_DIR`, by default `/var/tmp/dmn-ledger-bench`,
  holds the binaries, the data of the nodes (`run/`) and of Postgres (`pg/`). The script refuses a
  path outside `/var/tmp` and a folder with files it did not make; the plan refuses another file
  system than ext4, and a Postgres data directory on another device than `BENCH_DIR`. The disk
  whose flushes are counted is the one under `BENCH_DIR`.
- **Docker** for the runs against Postgres, port 55439 free on 127.0.0.1 and no container named
  `dmn-ledger-bench-pg`. The image is pulled by its digest on the first start.
- **Nothing else of the benchmark running.** The plan refuses to start while a `ledgerd` runs,
  while `run/` or `pg/` hold anything from an earlier run, and when the machine is not on mains
  power.
- **Memory.** The plan runs under `BENCH_MEMORY`, 6G for `all` and 4G for `smoke`, and the script
  wants that much and 1 GiB more available before it starts; the plan itself wants 4 GiB
  available. The Postgres container has 3 GiB without swap, and while Postgres runs the plan
  lowers its own limit so that both fit. Before every run it waits up to a minute for the memory
  it may need and stops with a message when it does not come.
- **16 hardware threads.** The targets run on cpus 0 to 5: node `n1` on 0 and 1, `n2` on 2 and 3,
  `n3` on 4 and 5, Postgres on all six. The plan runs on cpus 8 to 15 with `GOMAXPROCS` 8. Cpus 6
  and 7 stay free. For another CPU these numbers are in `ledgerrun/cluster.go` (`cpus`),
  `pg/server.go` (`Cores`), `cmd/bench/fly.go` (`targetCPUs`) and `scripts/bench.sh`
  (`taskset -c 8-15`); `machine.txt` names the cpus of the targets as text.

The machine should do little else while the plan runs. Seconds in which other processes use a
core or more are left out, and a run that has too few seconds left is repeated, so a busy desktop
makes the plan longer, not wrong.

## Commands

From the root of the repository:

| Command | What it does | Exit code |
|---|---|---|
| `scripts/bench.sh all` | the whole plan, then `bench report` and `bench check`; the results go to `docs/bench/<date>-<commit>` | 0 when `check` passes, 1 otherwise, 2 for a usage error |
| `scripts/bench.sh all -only REGEXP` | the same with flags for `bench plan`: a trial, its results go below `BENCH_DIR/trial/` | as above |
| `scripts/bench.sh smoke` | one pass with short windows, about 20 minutes, below `BENCH_DIR/smoke/`; `bench report` but no `bench check` | 0, or 1 when the plan or the report failed |
| `scripts/bench.sh clean` | stops and removes the container `dmn-ledger-bench-pg` and deletes `BENCH_DIR`, smoke runs and trials included | 0, 1 when it refuses the folder |

The plan of 2026-11-20 took about two hours and ten minutes, from the disk probe at its start,
13:34 UTC, to the one at its end, 15:44. A plan that stopped goes on where it stopped:
`scripts/bench.sh all` at the same commit finds the folder under `docs/bench` and skips every run
whose raw file is there. A second folder for the same commit makes the script refuse. `clean`
touches no other container and nothing under `docs/bench`.

`bench` itself, built with `cd bench && go build ./cmd/bench`:

| Command | What it does | Exit code |
|---|---|---|
| `bench selftest` | drives a target that answers at once at 200,000 arrivals a second and one that stalls for a second at 1,000 | 0, 1 when the generator adds 1 ms or more at p99, or when the stall does not show up as the wait of the arrivals due in it |
| `bench plan -dir DIR -out DIR [-repo DIR] [-smoke] [-only REGEXP]` | the plan; start it through the script | 0 when it finished, 1 when it stopped or could not clean up, 2 for a usage error |
| `bench report DIR` | writes `RESULTS.md` and the three curves from `DIR/raw` | 0, 1 on an error |
| `bench check DIR` | rebuilds the report, compares it with the folder and judges every group; one line per verdict, `ok` or `refused` | 0 when every verdict passes, 1 otherwise |

The tests: `cd bench && go test ./...` needs no Docker. `go test -tags pg ./pg ./cmd/bench` starts
the container, for `pg` in `BENCH_DIR/pg` and for `cmd/bench` below `TMPDIR`, which must be on ext4
as well, and checks the schema, the rules of the client and the cleanup against a real Postgres.

## The result folder

| File | Content |
|---|---|
| `raw/<run>.json` | one file per run, valid or not: the head (commit, machine, target, load, thresholds, disk probe), every second of the run and the verdict |
| `raw/<run>.n<k>.traces` | for the runs of m3, the CPU profile of each node as `go tool pprof -traces` prints it |
| `plan.json` | the groups the plan meant to make, with their number of passes; `bench check` compares the raw files with it |
| `plan.txt` | every run in the order it ran, with a summary or the reason it is not valid, and the invalid runs per measurement at the end |
| `machine.txt` | commit, CPU, memory, kernel, governor, file system, the thresholds of the disk speed, the disk probes at the start and the end, the heat counters, `pg_test_fsync` in the container and the self-test of the generator |
| `postgres.txt` | the version of Postgres and every setting that differs from the defaults |
| `probe-*.json`, `heat-*.json`, `selftest-*.json`, `pg-test-fsync-*.txt` | the sources of `machine.txt` |
| `RESULTS.md`, `m1-p50-32-lanes.svg`, `m1-p50-4-lanes.svg`, `m1-fsync-32-lanes.svg` | written by `bench report` |

`RESULTS.md` and the curves are never edited by hand: `bench check` rebuilds them from the raw
files and refuses the folder when one byte differs.

A run is named `<measurement><letter>-<target>x<lanes>-<load>-<rate>-<n>`, for example
`m1-l3x4-uniform-10k-128000-2r1`. The target is `l1` or `l3` for one or three nodes and `v1` to
`v4` for the variants of Postgres; `-b13` after the lanes marks a run with `LEDGER_BUG=B13`. `n` is
the pass, except in m5, where it counts the two runs of a pair, and `r1`, `r2` mark the repeats of
a run that was not valid.

## The measurements

| | Question | Runs |
|---|---|---|
| m1 | How many transfers a second does a target hold, and how does the wait grow with the rate? | a chain of stages per target and lanes, on `uniform-10k`, two passes |
| m2 | How long does a transfer wait at a fixed, moderate load, and what do the other loads change? | 2,000 a second on 32 lanes and 20,000 on 4, two passes |
| m3 | Where does the CPU time of a node go? | four runs built with `-tags pprof`, made once |
| m4 | How long does a node stop for a snapshot, and does that cost the lead? | five setups `a` to `e`, two passes |
| m5 | What does the fsync of `meta` cost? | one binary built with `-tags simbug`, without and with `LEDGER_BUG=B13`, A B A B |
| m7 | What does one missing follower do to three nodes? | three nodes with `n3` stopped after the setup, two passes |

There is no m6: a measurement of proofs was planned, but there is no load for them, and the
benchmarks of the chain do not reach `RESULTS.md`. m0 in `plan.txt` is the first Postgres run after
the start of the container, `v3x4` at 20,000 a second for 15 seconds, which warms the container
and is thrown away; it has no raw file.

**m1.** The stages are 1,000, 2,000, 4,000 and so on up to 512,000 arrivals a second. A chain runs
its stages one after the other on the same nodes or the same database and stops after the first
saturated one. The chains run in pairs, the ledger and the Postgres variant with the same lanes:
`l1x32` with `v1x32`, `l1x4` with `v3x4`, `l3x32` with `v2x32`, `l3x4` with `v4x4`, and the second
pass runs each pair in the other order.

**m2.** On 32 lanes at 2,000 a second: `uniform-10k` on `l1`, `v1`, `l3` and `v2`, `uniform-1m` and
`hot` on `l3`, `v1` and `v2`, `twophase` on `l3` and `v1`. On 4 lanes at 20,000 a second the same
with `v3` and `v4` in place of `v1` and `v2`. Two-phase transfers run under `READ COMMITTED` only.

**m3.** `l1x32` and `l3x32` at 2,000 a second, `l1x4` at 120,000 and `l3x4` at 50,000, all on
`uniform-10k`, with `LEDGER_PROFILE` set for each node.

**m4.** At 2,000 a second on 32 lanes, with `LEDGER_SNAPSHOT_EVERY=20000` on one node and `7000` on
three. Before the window the ledger is filled with transfers of 1 between neighbouring accounts:

| Setup | Nodes | Load | Filled with | Window |
|---|---|---|---|---|
| a | 1 | `uniform-10k` | 190,000 transfers | 35 s, 17 valid seconds needed |
| b | 1 | `uniform-1m` | 1,000,000 | the same |
| c | 1 | `uniform-1m` | 4,000,000 | the same |
| d | 3 | `uniform-1m` | 1,000,000 | the same |
| e | 3 | `uniform-1m` | 1,000,000 | 60 s, 30 needed; a follower is stopped 8 s after the start, 3 s into the window, and started again at 50 s |

**m5.** One node and three nodes, 32 lanes, `uniform-10k` at 2,000 a second, in the order without,
with, without, with, within a few minutes, in the first pass only.

**m7.** Three nodes on `uniform-10k`, 32 lanes at 2,000 a second and 4 lanes at 20,000. Node `n3` is
stopped after the setup and the run waits until one leader is left.

**Order.** The first pass runs m1, m2, m5, m3, m4 and m7, the second pass m1 and m2, then m4 and m7
once more, about an hour after their first pass. Then come the repeats: twice the runs that were
restless or had a checkpoint or a new WAL segment in their window, then a third and a fourth pass
of every chain of m1 and every group of m2 on `uniform-10k` that has no verdict yet, then twice
the repeats again. Between two runs the plan waits 10 seconds.

**Builds.** `ledgerd` is built from the repository three times: normally, with `-tags simbug` for
m5 only and with `-tags pprof` for m3 only. `bench check` refuses a raw file built with `simbug`
outside m5 and one built from a tree with changes.

**Nodes.** Each node gets `LEDGER_SNAPSHOT_EVERY=4294967296`, so no snapshot is taken outside
m4, `LEDGER_SWEEP=on`, `LEDGER_MAX_CLIENTS=128`, `GOMEMLIMIT=4500MiB` alone and `1500MiB` in a
cluster of three, the default tick of 10 ms, and runs under `taskset` on its two cpus.

## The rules in numbers

- **Open load.** Arrival `i` is due at `start + i/rate` and goes to lane `i mod lanes`. A
  dispatcher starts every arrival that is due without waiting for an answer. Latency is the time
  of the answer minus the time the arrival was due. At most 100,000 arrivals are open at once;
  one beyond that counts as never answered and the run as saturated.
- **Loads.** `uniform-10k` books between 10,000 accounts and `uniform-1m` between 1,000,000, both
  chosen evenly. `hot` has 10,000 accounts, and in 80 % of the transfers the credit side is one of
  the first 8. In `twophase` an arrival reserves and, once that is answered, posts (90 %) or
  voids (10 %): two transfers per arrival. Amounts are 1 to 100. One more account funds the
  others; every other account carries the flag that debits must not exceed credits. Both targets
  get the same stream of transfers from the same seed.
- **Timing.** A stage: 5 s warm-up, a window of 15 s that may grow to 30 s until it has 8 valid
  seconds at one disk speed, and up to 5 s to drain. The fixed load, m3, m5 and m7: 10 s warm-up,
  30 valid seconds in a window of up to 60 s, 5 s drain. `smoke`: 2 s warm-up, a window of 6 s
  and 3 s to drain for the stages, windows of 3 s for the rest.
- **Held and saturated.** A run is saturated when more arrivals are open at the end of its window
  than fall due in one second, when it reached the limit of 100,000 open arrivals, or when more
  than 1 % of the transfers due in the window were given up. Otherwise it held its rate.
- **At least.** The generator was the limit when it used more than 80 % of its cpus in the
  window, or when the median arrival started more than 1 ms late while the target had no backlog.
  A saturated run with such a limit says "at least" and counts as limited by the generator. A run
  that was not saturated and had such a limit is invalid, with the reason "generator at its
  limit".
- **Budget.** Three nodes may hold 2,000,000 transfers and one node 6,000,000, the transfers that
  fund the accounts included, so that the nodes stay inside their memory. A stage that would not
  fit is shortened, a quarter of the seconds that fit as warm-up and the rest as window, and then
  needs half of its window as valid seconds. Below a window of 6 s it is not run: the chain ends
  there and shows `budget` in the column "limit". When a stage fits the budget but not on top of
  what the nodes wrote in the stages before, they are started again with an empty data directory
  and set up anew. Postgres and m4 have no budget.
- **Repeats and stops.** A run that was restless, or a Postgres run with a checkpoint or a new WAL
  segment in its window, is repeated at the end, at most twice. The plan stops when the books of a
  run do not add up, and when more than a quarter of the planned runs of m1 or m2 are invalid and
  will not be repeated.

## Disk speed and valid seconds

Every second of a run reads the flushes of the disk and their time from `/proc/diskstats`. Under
20 flushes the second has no speed; with a mean flush time under 0.5 ms it is fast, over 1 ms
slow, and in between a transition. A run counts at the slow speed when it has enough valid slow
seconds in its window, else at the fast one when it has enough of those, and is restless when it
has neither and is not unpaced. The figures of a run come only from the valid window seconds of
its speed.

When more than half of the window seconds have fewer than 20 flushes, the run is unpaced: the
target was computing, not waiting for the disk. Every second of the window then counts at the
speed `unpaced` unless another reason leaves it out, and such runs are compared without asking
for an equally fast disk.

The column "left out" in the table of runs counts the window seconds outside the figures by
their reason:

| Reason | The second |
|---|---|
| other speed | was valid, but at the other disk speed than the run |
| transition | had a mean flush time between 0.5 and 1 ms |
| first second after a change | had another speed than the second before it |
| too few flushes | had fewer than 20 flushes |
| foreign cpu | had processes other than generator and target use a core or more |
| swapped out | had pages swapped out on the machine |
| stale status | had a node that did not answer for its status and counters in time, in this second or the one before |
| no sample | had no sample of the target, in this second or the one before |
| unread | had a file of the meter that could not be read |
| target idle | was in an unpaced run, with fewer than 20 flushes and less than half a core used by the target |

The column "reasons" names why a run is not valid:

| Reason | The run |
|---|---|
| restless | had not enough valid seconds at any speed |
| generator at its limit | had the generator as its limit without being saturated |
| target stalled | had a window second without a sample, with a stale status, or with too few flushes while the target used less than half a core; allowed in m4 and m7 |
| checkpoint in the window | was a Postgres run during which a checkpoint ran |
| new WAL segment in the window | was a Postgres run during which Postgres created a WAL segment |
| term changed | was a ledger run with a new term in the window or next to it; allowed in m4 and m7 |
| calls failed | had calls that failed without being saturated |
| verify failed | did not add up when the books were read back; the plan stops |
| nodes disagree after the run | had nodes that did not reach the same state afterwards |

## The criterion

The runs of a group are judged together. A group holds the runs with the same measurement,
target, lanes, environment of the nodes, build, load and rate; the pass and the letter of m4 do
not count. This follows `control/agree.go`:

1. The speed of the group is the best one among its valid runs: slow before fast before unpaced.
2. The valid runs at that speed are taken in the order of their run ID, so `-1`, `-1r1`, `-2`,
   `-3`, not in the order they ran.
3. The pairs are tried in the order (1, 2), (1, 3), (2, 3), (1, 4), (2, 4), (3, 4) and so on. The
   first pair whose mean flush times are within 10 % of each other decides; at the unpaced speed
   the first pair decides. Its throughput, its p50 and its fsyncs per transfer must each be within
   10 %, measured from the smaller value: then the group `agreed`, else it `missed`, with the first
   of the three that differs and both values.
4. Without such a pair the group is `undecided` with fewer than four runs. From four runs on, the
   pair with the nearest flush times is taken, and its p50 divided by the flush time and its
   fsyncs per transfer must be within 10 %: `agreed in flush units` or `missed`.

A group whose counted runs are all saturated, and the throughput under overload of a chain, are
judged by their throughput alone. A valid run limited by the generator passes it as "at least:
the generator was the limit". The throughput under overload of a chain takes the first saturated
stage of each pass; a chain in which no pass saturated is "at least: a pass held every stage", or
"every stage that fits the budget" when it ended at the budget.

`bench check` refuses the folder when a verdict fails. Required to agree are every group of m2 on
`uniform-10k`, which also fails with a single counted run, and the throughput under overload of
every chain of m1. Every other group is printed with its verdict and does not fail the check. A
verdict also fails for a raw file built from a tree with changes or with `simbug` outside m5, with
a head that lacks a field, with seconds, speeds or bands that do not follow from its own counts,
valid without verified books, valid with a new term or with stalls allowed outside m4 and m7, or
with transfers that existed before the run; for a group that `plan.json` does not name or a
planned pass without a run; and for a `RESULTS.md` or a curve that differs from what the raw files
give.

## The columns

| Column | Meaning |
|---|---|
| rate | arrivals offered per second; for `twophase` an arrival is two transfers |
| held | `held`, or `saturated at` the rate, with the number of runs when not all of them saturated |
| speed | the disk speed of the row: `slow`, `fast (no slow run)` or `unpaced` |
| flush ms | the mean flush time of the device over the counted seconds, from `/proc/diskstats` |
| runs | runs counted out of runs made; a run counts when it is valid and ran at the speed of its row |
| seconds | window seconds that entered the figures out of all window seconds of the counted runs, the highest p99 of a second left out, and the seconds in which the thermal counter of a target core rose |
| p50 ms, p99 ms, p99.9 ms | latency from the moment the arrival was due; "over N" means not answered by the end of the drain, N ms after the end of the window |
| p50 flushes | p50 divided by the mean flush time of the same run |
| throughput/s | transfers booked per counted second; for `twophase` both transfers of an arrival |
| target cpu | CPU time of the target divided by the hardware threads it was given, 2 for one node and 6 for three nodes and for Postgres, the mean over the window |
| fsync per transfer, all nodes | for the ledger the fsync calls of every node on the log, on `meta` and on any other file, from its counter line; for Postgres the syncs of its WAL (`pg_stat_io`, `object = 'wal'`, `context = 'normal'`); divided by the confirmed transfers |
| fsync per transfer, leader / one follower | the same for the leader and the mean of the followers, by the role in the last counted second |
| transfers per request | for the ledger, transfers per entry the leader applied, one client request each; for Postgres, per call of a function |
| retries per transfer | Postgres only: tries after a serialization failure or a deadlock, divided by the transfers booked |
| given up | Postgres only: the share of the transfers due that were given up after 20 tries |
| agreement | the verdict of the criterion with the two values that differ |
| last held stage | over the passes, the median of the highest stage held below the first saturated one; with an even number of passes the lower of the two middle values |
| throughput under overload | the throughput of the first saturated stage of each pass |
| limit | `target`, `generator`, `budget`, `none reached`, or `saturated, not valid` |
| snapshot MB, ms per 100 MB | the largest snapshot file of a node in the window, in units of 10⁶ bytes, and the longest round divided by it |
| longest round ms, worst second p99 ms | the slowest pass of the node loop and the highest p99 of any second in the window, valid or not |
| term changed | whether the term of a node changed in the window or in the second before or after it |
| log fsync, meta fsync, other fsync, computing in Process, reading, sending, answering, waiting for work | the time of the node loop in percent of the time the node was up, from its counters |
| p50 B/A, fsync per transfer B/A, throughput B/A | m5: the median of the runs with `LEDGER_BUG=B13` divided by that of the runs without; the fsyncs are per transfer, not per round |

The flush time is that of the device, not the time an fsync call takes, and the two kinds of call
do not cost the same: the disk probe in `machine.txt` times an fsync on a file that grows and an
`fdatasync` on a preallocated file, and on 2026-11-20 the first took about twice as long as the
second.

## Postgres

- **Image.** `postgres:18.7` pinned by its digest in `pg/image.txt`, started as
  `docker run -d --name dmn-ledger-bench-pg --network host --cpuset-cpus 0-5 --memory 3g
  --memory-swap 3g --shm-size 256m`, under the user that runs the plan, with its data in
  `BENCH_DIR/pg/data`.
- **Settings.** `shared_buffers=1GB`, so that every run fits into it; `max_wal_size=8GB`,
  `min_wal_size=4GB` and `checkpoint_timeout=15min`, so that no checkpoint falls into a window;
  `wal_sync_method=fdatasync`, the default on Linux, set explicitly; and `track_wal_io_timing=on`
  for the sync times. `fsync`, `synchronous_commit` and `full_page_writes` stay on, and every
  connection checks that they are. The isolation level is set per connection with
  `default_transaction_isolation`.
- **Schema.** `pg/schema.sql`: accounts with their balances and `CHECK` constraints for the
  limits, transfers with a `uuid` key, foreign keys to the accounts and no secondary index. A
  transfer is a call of a PL/pgSQL function that locks the rows of its accounts in the order of
  their IDs before it writes, and does nothing for an ID that exists.
- **Variants.**

  | Raw file | Name in the tables | A call carries | Connections | Isolation |
  |---|---|---|---|---|
  | `v1` | Postgres, one transaction per transfer, read committed | one transfer | a pool of 32 | `READ COMMITTED` |
  | `v2` | Postgres, one transaction per transfer, serializable | one transfer | a pool of 32 | `SERIALIZABLE` |
  | `v3` | Postgres, batched, read committed | what queued on its lane, at most 1,000 | one per lane | `READ COMMITTED` |
  | `v4` | Postgres, batched, serializable | the same | one per lane | `SERIALIZABLE` |

  A batch takes what queued on its lane while the previous call ran, like a session of the SDK,
  but at most 1,000 transfers: on 10,000 accounts a batch of the size the ledger takes would lock
  most of them in one transaction.
- **Retries.** On a serialization failure or a deadlock a call is tried again after a random
  pause of up to 1, 2, 4 and so on milliseconds, at most 50, up to 20 tries; then its transfers
  count as given up.
- **Each run.** Per load a template database is filled once and frozen with
  `VACUUM (FREEZE, ANALYZE)`. Each run copies it into a fresh database, takes a checkpoint, opens
  all its connections and loads the accounts and their key into the buffers with `pg_prewarm`.
  At the start of the container the plan makes 256 WAL segments and a checkpoint, so that a run
  finds its segments ready. A run in whose window a checkpoint ran or a WAL segment was created
  is invalid and repeated.

## Limits

What the measurement does not cover is in ADR-012, under "What the measurement does not cover".
Two things about the code that a reader of the tables should know:

- **A group of m4 does not know the letter.** The runs of `b` and `c`, one node with snapshots of
  311 and 614 MB, form one group, and so do `d` and `e`, three nodes without and with a stopped
  follower. On 2026-11-20 the first pair of each group came from the same setup, `b-1` with `b-2`
  and `d-1` with `d-2`; with other flush times the criterion could compare `b` with `c`.
  `RESULTS.md` lists each group of m4 with its runs and its verdict under the snapshot table.
- **The lanes of Postgres with one transaction per transfer.** `v1` and `v2` always have a pool of
  32 connections, and every transfer takes the next free one. They stand on 32 lanes in the tables
  next to the ledger, but the lane of a transfer plays no part for them.

Two files of the folder of 2026-11-20, `plan.txt` and `machine.txt`, were written by the plan
before its notes were made more precise, and they stay as they were written:

- `plan.txt` gives the snapshot sizes of m4 in MiB, 2²⁰ bytes, where `RESULTS.md` uses MB, 10⁶
  bytes: "largest snapshot file 296 MiB" for `m4b-l1x32-uniform-1m-2000-1` is 311 MB in the
  report. Plans from now on write MB.
- `plan.txt` says that invalid runs stay in `raw/`. The warm-up m0 has no raw file all the same;
  plans from now on say so.
- `machine.txt` gives the fast speed of the second disk probe as "fdatasync on a preallocated
  file 0s over 0 s, ratio 0.00". The probe had no second at that speed for that kind of write, and
  plans from now on write "no second at this speed".
