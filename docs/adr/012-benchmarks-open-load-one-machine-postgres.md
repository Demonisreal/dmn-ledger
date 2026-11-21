# ADR-012: Benchmarks: open load on one machine, with Postgres on the same disk

Status: accepted, 2026-11-20. The figures are those of one run of the plan, on 2026-11-20 at commit
`0298488`. `bench check` does not pass on that run: three required groups missed the 10 % criterion
(see "Where the criterion was missed"). The result folder and its raw files name the commit
`a6ee18b`, its hash before the history of the repository was cleaned up; the tree, `3c8fb2c`, is
the same.

## Context

Until now every number in an ADR was a micro-benchmark of one package. None of them said how long a
client of a cluster waits, how many transfers a node holds, what the seal or a snapshot costs on a
real disk, or how any of it compares with a database a reader already knows.

There is one machine to measure on: a laptop with an i5-12500H, 15.3 GiB of RAM and one NVMe disk
with ext4, next to a desktop session. What a benchmark on such a machine can say is narrow.

- Counts hold: how many fsync calls a transfer costs, how many transfers share a request.
- Ratios hold when both sides ran in turns, on the same disk, on the same evening: one node against
  three, the ledger against Postgres, a node with the seal against one without.
- Shapes hold: whether latency stays flat while the rate grows, and where a target stops keeping
  up.
- Milliseconds and transfers per second describe this laptop on this evening. They say little about
  a server.
- Anything beyond p99 describes the desktop session as much as the database.
- Three nodes on one machine share one disk and have no network between them. That measures what
  the protocol costs, not what a cluster on three machines does.

Three things make such numbers wrong without anybody lying. A load generator that sends the next
request only after the previous answer stops sending while the system stalls, so a stall shows up as
one slow request instead of as every request that was due in it (Schroeder, Wierman and
Harchol-Balter, "Open Versus Closed: A Cautionary Tale", NSDI 2006). A consumer disk changes its
speed on its own: on this one the flush time of the device moved between under 0.5 ms and over 1 ms
within single runs. And a comparison can be tilted by what the other system is given: a schema
nobody would write, a weaker durability setting, fewer cores.

## Decision

The benchmarks are a module of their own, `bench/`. One program, `bench`, runs a fixed plan, writes
one raw file per run and builds the report from those files.

1. **Measured is what a user gets.** The load goes through the public Go SDK to real `ledgerd`
   processes with real fsync, over TCP on loopback. `bench` imports the package `ledger` and nothing
   from `internal/`, so the bundling of the SDK is part of what is measured.
2. **Open load.** A run offers a fixed rate. Arrival `i` is due at `start + i/rate`, whether or not
   the target has answered anything. A dispatcher starts every arrival that is due and never waits
   for an answer. Latency is the time of the answer minus the time the arrival was due, not the time
   it was sent: a transfer that had to wait behind a stall counts with its whole wait. At most
   100,000 arrivals are open at once, and an arrival beyond that counts as never answered. Before a
   plan the generator runs against a target that answers at once and against one that stalls for a
   second; the result is in `machine.txt`.
3. **Lanes.** Arrival `i` goes to lane `i mod lanes`. For the ledger a lane is a session of the
   SDK, which keeps one request in flight and sends whatever queued behind it as the next request.
   The ledger is measured with 32 lanes, many clients that each send little, and with 4, few
   clients that bundle a lot. For Postgres a lane is a connection in the batched variants
   (decision 9). Both systems get the same stream of transfers from the same seed.
4. **Four loads.** `uniform-10k` books between 10,000 accounts chosen evenly and `uniform-1m`
   between a million. In `hot`, 8 of 10,000 accounts are the credit side of 80 % of the transfers.
   `twophase` reserves and, once that is answered, posts (90 %) or voids (10 %); one arrival is two
   transfers there. Every account except the one that funds the others carries the flag that debits
   must not exceed credits, and is funded so that the rule is checked on every transfer and never
   fails.
5. **Stages and a fixed load.** A stage offers one rate for a window of 15 seconds after 5 seconds
   of warm-up, and the rates double from 1,000 to 512,000 transfers a second. A stage is held when
   fewer arrivals are open at the end of its window than fall due in one second. Otherwise, or when
   more than 1 % of the transfers due were given up, it is saturated. The fixed load is 2,000 a
   second on 32 lanes and 20,000 a second on 4, with windows of 30 valid seconds. A run of three
   nodes writes at most 2 million transfers and a run of one node 6 million, so that the nodes stay
   inside their memory. The highest stages are shortened to fit, down to a window of 6 seconds, and
   a chain of stages ends where the next one would not fit. The generator runs on the same machine,
   on cpus 8 to 15; the targets have cpus 0 to 5. Where the generator was the limit, because it
   used more than 80 % of its cpus or started half of the arrivals more than 1 ms late, the result
   says "at least".
6. **The disk speed is part of every figure.** Each second of a run is sorted by the mean flush
   time of the device in that second, read from `/proc/diskstats`: fast below 0.5 ms, slow above
   1 ms, no verdict under 20 flushes. A second counts when its speed is clear and it is not the
   first after a change, when processes other than generator and target used less than one core,
   when nothing was swapped out and when every node reported its counters. A run counts at the slow
   speed when it has enough such seconds there, 30 for the fixed load and 8 for a stage, and
   otherwise at the fast one; its window is stretched up to twice its length to get them. A run
   with enough at neither speed is "restless" and is repeated at the end of the plan. The tables
   show the slow speed. It is where this disk stays under sustained load, and it is the less
   favourable one. Seconds in which the thermal counter of a target core rose stay in and are
   counted next to the figures.
7. **Median and spread, never the best run.** The stages, the fixed load, the snapshots and the
   runs with a missing follower are made in two passes about an hour apart, the targets in the
   opposite order the second time. The runs with and without the seal alternate within minutes,
   and the runs with a CPU profile are made once. A figure is the median over the counted runs,
   with the smallest and the largest value and the number of runs next to it. Invalid runs stay in
   `raw/` and are listed with their reasons.
8. **Two runs have to agree, and the first pair decides.** For each group of runs, the first two
   valid runs at the same disk speed whose mean flush times are within 10 % of each other form the
   pair. Their throughput, their p50 and their fsyncs per transfer must each be within 10 %.
   Without such a pair the group is undecided; from four runs on it is judged by p50 in flush times
   instead. A third run cannot overrule a pair that disagreed. p99 and p99.9 are reported and not
   judged. `bench check` fails when a fixed-load group on `uniform-10k` or the throughput under
   overload of a chain of stages does not agree. The rule is part of the measured commit and was
   not changed after the evening.
9. **Postgres gets what somebody who knows it would give it.** PostgreSQL 18.7 from the official
   image, pinned by its digest, on the host network, on the six hardware threads the three nodes
   share (a single node has two of them), with 3 GB of memory and its data on the same file system.
   `fsync`, `synchronous_commit` and `full_page_writes` are on and checked on every connection,
   `wal_sync_method` is `fdatasync`, `shared_buffers` is 1 GB so that a run fits into it, and
   `max_wal_size` and `checkpoint_timeout` keep checkpoints out of a window. The schema is how a
   ledger is built in Postgres: accounts with balances and `CHECK` constraints for the limits,
   transfers with a `uuid` key and foreign keys, no secondary index, because the ledger has none
   either. A transfer is one call of a PL/pgSQL function in its own transaction, which locks the
   account rows in the order of their IDs before it writes. There are four variants:

   | Name in the tables | A call carries | Connections | Isolation |
   |---|---|---|---|
   | Postgres, one transaction per transfer, read committed | one transfer | a pool of 32 | `READ COMMITTED` |
   | Postgres, one transaction per transfer, serializable | one transfer | a pool of 32 | `SERIALIZABLE` |
   | Postgres, batched, read committed | what queued on its lane, at most 1,000 transfers | one per lane, 4 | `READ COMMITTED` |
   | Postgres, batched, serializable | the same | one per lane, 4 | `SERIALIZABLE` |

   The batched variants bundle by the rule of the SDK: what queued while the previous call ran goes
   out as the next call. On a serialization failure or a deadlock the client tries again after a
   random pause, up to 20 tries, and then the transfer counts as given up. Two-phase transfers
   exist under `READ COMMITTED` only. The raw files name the variants `v1` to `v4` in this order.
   The settings that differ from the defaults are in `postgres.txt` next to the results.
10. **Both systems are measured between their pauses.** Snapshots are switched off in every
    measurement but the one that measures them, and a Postgres run with a checkpoint in its window
    is invalid.
11. **Raw data in the repository, and a report that can be rebuilt.** A result folder holds one
    JSON file per run with every second of it, valid or not, and `machine.txt`, `postgres.txt`,
    `plan.txt` and `plan.json`. `bench report` writes `RESULTS.md` and the curves from the raw
    files, the same bytes every time. `bench check` recomputes both, compares them with what is in
    the folder and applies decision 8; how to run the plan and how each column is made is in
    [bench/README.md](../../bench/README.md). Every figure below and in the README is copied from
    [RESULTS.md](../bench/2026-11-20-a6ee18b/RESULTS.md) of this run, with one exception that is
    marked: the disk probe is in `machine.txt` of the same folder.

What is equal on both sides and what is not:

| | Ledger | Postgres | What it does to the comparison |
|---|---|---|---|
| Load | the same stream, rate and number of lanes | | |
| Bundling on a lane | the SDK sends what queued on a session as one request | none with one transaction per transfer; the batched variants use the rule of the SDK | on 32 lanes the ledger bundles under load and Postgres with one transaction per transfer cannot |
| Hardware threads | 2 per node, so 2 or 6 | 6 | against one node Postgres has three times the CPU |
| Disk | one file system for both | | |
| Durability | an answer after the fsync of the log and of the seal | an answer after the WAL is flushed | confirmed means on disk for both |
| Transport | TCP on loopback | TCP on loopback | |
| Copies | 1 or 3 | 1, no replica | against three nodes Postgres writes one copy, the ledger three |
| State | all in memory | fits into `shared_buffers` | nothing here is about data larger than memory |
| Limit on debits | checked after every transfer | a `CHECK` per row, so once per account and batch when batched | favours batched Postgres |
| A transfer sent twice | session and transfer ID, the fields compared | primary key and `ON CONFLICT DO NOTHING` | Postgres does not compare the fields of a duplicate |
| Size of a bundle | up to 10,485 transfers per request | up to 1,000 per call | a batch of the ledger's size would lock most of the 10,000 accounts in one transaction; under overload Postgres ran at its 1,000 |
| Audit trail | a hash chain over batches, a Merkle tree per batch | none | the node hashes every transfer, Postgres does not |
| Queries | lookups by ID | SQL | not measured |

## The run of 2026-11-20

One run of the whole plan, on the laptop named above: governor `powersave`, turbo on, on mains, a
desktop session with an editor open next to it. The first run started at 13:36 and the last at
15:44. The plan made 218 runs. 212 are valid and 6 are not: five were restless, two had the
generator at its limit without counting as saturated, and one run is in both counts. Of the valid
runs 200 enter the figures. The other 12 settled at the fast disk speed and are left out, because
every row of the tables is at the slow speed, with a mean flush time of 1.59 ms (1.50–2.31) over
all counted runs. One more run has no raw file: the first Postgres run after the container
started is a warm-up that the plan throws away, and `plan.txt` names it.

Below, a figure is the median, with the smallest and the largest value of its runs in brackets. A
figure without brackets comes from a single run.

## Results

The fixed load on `uniform-10k`. "Flush times" is p50 divided by the mean flush time of the device
in the same run. "fsync per transfer" counts the sync calls of all nodes, or of Postgres on its WAL,
per confirmed transfer. "Runs" says what decision 8 made of the two runs.

| Target | Lanes | Transfers/s | p50 ms | p50 in flush times | p99 ms | fsync per transfer | Runs |
|---|---|---|---|---|---|---|---|
| one node | 32 | 2,000 | 10.5 (9.47–11.5) | 6.4 (6.0–6.8) | 24.3 (15.9–32.8) | 0.153 (0.142–0.164) | missed |
| three nodes | 32 | 2,000 | 58.4 (57.3–59.4) | 37.0 (36.4–37.5) | 99.3 (98.3–100) | 0.220 (0.217–0.224) | agreed |
| Postgres, one transaction per transfer, read committed | 32 | 2,000 | 3.07 (3.07–3.07) | 1.9 (1.9–1.9) | 4.61 (4.61–4.61) | 0.296 (0.294–0.298) | agreed |
| Postgres, one transaction per transfer, serializable | 32 | 2,000 | 3.17 (3.14–3.20) | 2.0 (1.9–2.0) | 4.99 (4.74–5.25) | 0.297 (0.282–0.311) | missed |
| one node | 4 | 20,000 | 18.2 (17.4–18.9) | 11.7 (11.2–12.1) | 30.0 (29.7–30.2) | 0.017 (0.016–0.017) | agreed |
| three nodes | 4 | 20,000 | 58.4 (53.2–63.5) | 36.8 (33.8–39.8) | 109 (106–111) | 0.023 (0.021–0.024) | missed |
| Postgres, batched, read committed | 4 | 20,000 | 9.86 (9.73–9.98) | 6.1 (6.0–6.2) | 33.5 (32.3–34.8) | 0.024 (0.024–0.024) | agreed |
| Postgres, batched, serializable | 4 | 20,000 | saturated | | | | agreed |

Batched Postgres under `SERIALIZABLE` did not hold 20,000 transfers a second: it confirmed 12,144 a
second (11,864–12,424) and gave up 7.4 % of the transfers (3.7 %–11.0 %).

1. **At these loads Postgres answers faster.** At 2,000 transfers a second its p50 is 3.07 ms
   against 10.5 ms for one node and 58.4 ms for three: one node takes 3.4 times as long and three
   nodes 19 times. At 20,000 a second on 4 lanes batched Postgres under `READ COMMITTED` is at
   9.86 ms, one node at 18.2 ms and three nodes at 58.4 ms. Postgres ran one copy and the three
   nodes three, all on the same disk.
2. **The same holds for nearly every stage Postgres held.** With one transaction per transfer its
   p50 stayed between 2.75 and 3.90 ms from 1,000 to 4,000 a second under both isolation levels,
   where one node on 32 lanes went from 9.98 ms (9.47–10.5) to 22.0 ms (22.0–22.0) and three nodes
   stayed between 48.6 and 55.3 ms. At 8,000 a second, the last stage it held, `SERIALIZABLE` was
   at 2.82 ms (2.75–3.52), and the two runs under `READ COMMITTED` gave 2.82 and 30.7 ms; one node
   was at 20.2 ms (17.9–22.5). Batched under `READ COMMITTED` it stayed between 3.84 and 6.72 ms up
   to 16,000 a second, where one node on 4 lanes was between 16.9 and 18.9 ms and three nodes
   between 42.5 and 51.7 ms. Only at 32,000 a second, the last stage batched Postgres held, is one
   node ahead: 17.4 ms (17.4–17.4) against 28.4 ms (27.6–30.7). Three nodes are behind Postgres at
   every stage Postgres held.
3. **Postgres stops earlier.** With one transaction per transfer the last stage held was 8,000 a
   second, and under overload it confirmed 8,824 a second (8,771–8,877) under `READ COMMITTED` and
   8,965 (7,838–8,997) under `SERIALIZABLE`. Batched under `READ COMMITTED` it held 32,000 and
   confirmed 44,917 (44,833–45,000) under overload, with every call at the limit of 1,000
   transfers. Batched under `SERIALIZABLE` it held 8,000; at 16,000 it confirmed 8,042 (8,000–8,083)
   and gave up 15.8 % (13.8 %–17.8 %). These are the limits of these variants on this load, not of
   Postgres. The pool has 32 connections, so at most 32 commits share a flush of the WAL. And at
   its limit batched Postgres used 16.6 % (16.4 %–16.8 %) of its six threads: four connections
   that each lock the accounts of 1,000 transfers out of 10,000 most likely wait for each other;
   lock waits were not sampled. With a million accounts it was faster at 20,000 a second
   (result 7), and no stages were run on that load.
4. **Where a node saturates was not found.** One node held 256,000 transfers a second on 32 lanes
   and on 4, at a p50 of 28.2 ms (25.1–31.2) and 23.6 ms (23.6–23.6), using 15.4 % (15.3 %–15.6 %)
   and 12.4 % (11.8 %–13.1 %) of its two hardware threads. At 512,000 the generator was the limit;
   the node confirmed at least 480,490 a second (454,775–506,205) on 32 lanes there. On 4 lanes the
   p50 of one node stays between 16.9 and 23.6 ms from 1,000 to 256,000 a second. On 32 lanes it is
   9.98 ms at 1,000 and 11.4 ms (10.8–12.0) at 2,000, and between 17.2 and 28.2 ms from 4,000 on,
   once a request carries more than one transfer.
5. **Three nodes did not saturate either, and the reason is the plan, not the cluster.** Their
   chain ends at 256,000 a second because the next stage would not fit the 2 million transfers a
   run of three nodes may write. On 4 lanes both passes held 256,000 at a p50 of 71.7 ms
   (67.6–75.8), from 8 of 12 window seconds. On 32 lanes one pass held it, at 115 ms from a single
   run with 5 valid seconds. The other pass was restless there and its repeat fell to the fast disk
   speed, so the table gives the last held stage as "at least 128,000 (128,000–256,000)", the
   lower of the two passes and the last stage both of them held.
6. **Grouping works, on both systems.** The fsyncs per transfer of one node on 32 lanes fall from
   0.314 (0.294–0.334) at 1,000 a second to 0.022 (0.022–0.023) at 16,000 and 0.001 (0.001–0.001)
   at 256,000. For three nodes, all nodes added up, they fall from 0.467 (0.431–0.503) to 0.030
   (0.028–0.033) and to 0.001 in the one run at 256,000. Postgres with one transaction per transfer
   under `READ COMMITTED` falls from 0.551 in one run at 1,000 to 0.072 (0.067–0.076) at 8,000: its
   WAL groups commits as well.
7. **More accounts change little.** With a million accounts instead of 10,000 the p50 of three
   nodes is 1.04 times that of `uniform-10k` on 32 lanes and 1.10 times on 4. For Postgres with one
   transaction per transfer it is 1.04 and 1.01 times. Batched Postgres under `READ COMMITTED` is
   faster with a million accounts, 0.60 times.
8. **Hot accounts do not slow the ledger down, and they cost Postgres its tail.** With 8 accounts
   on the credit side of 80 % of the transfers the p50 of three nodes is 0.87 times that of
   `uniform-10k` on 32 lanes and 1.04 times on 4. The 0.87 is no gain: on `uniform-10k` the same
   three nodes measured between 49.2 and 60.4 ms at this rate over the evening, and the hot runs
   lie at the lower end of that. Postgres with one transaction per transfer keeps its p50, 1.30
   times under `READ COMMITTED` and 1.10 times under `SERIALIZABLE`, and loses its p99: 27.4 ms
   (26.1–28.7), 5.94 times, and 90.1 ms (71.7–109), 18.05 times, with 0.854 retries per transfer
   (0.789–0.919). Batched under `READ COMMITTED` its p50 is 18.9 ms (18.9–18.9), 1.92 times.
9. **Two-phase transfers cost the ledger two commits.** At 2,000 reservations a second three nodes
   answer the reservation at a p50 of 59.4 ms (58.4–60.4) and have it posted or voided at 139 ms
   (139–139); Postgres with one transaction per transfer has it settled at 6.46 ms (6.40–6.53). At
   20,000 a second on 4 lanes three nodes held the load, settled at 176 ms (172–180) with 39,967
   transfers a second (39,944–39,991). Batched Postgres under `READ COMMITTED` did not: it confirmed
   34,067 transfers a second in the one run that counted.
10. **A missing follower makes three nodes slower, not faster.** With one follower stopped the p50
    at 2,000 a second is 73.7 ms (73.7–73.7) against 58.4 ms (57.3–59.4), a quarter more, and at
    20,000 a second 68.6 ms (67.6–69.6) against 58.4 ms (53.2–63.5). Every commit then needs the
    one follower that is left, where a healthy cluster takes the faster of two, and the leader
    builds an append for the absent node on every heartbeat: its share of time outside `Process`
    and outside waiting rises from 0.2 % to 2.3 % (2.2–2.3). The runs do not separate the two.
11. **The CPU is not where the time goes.** At 2,000 a second one node used 3.8 % of its two
    threads in the run with a profile, and the largest shares of that were wire and network
    (43.1 %), kernel (26.3 %) and runtime (12.3 %). At 120,000 a second on 4 lanes it used 6.8 %,
    led by applying (38.9 %), the SHA-256 of the audit chain (26.5 %) and the log (15.7 %).

## What the write path of the ledger costs

A round of a node is one append, one fsync of the log and one fsync of `meta` for the seal, and
nothing leaves the node before both (ADR-002, ADR-004, ADR-009). The counters of the node loop show
what that means at 2,000 transfers a second on one node: 50.2 % (50.2–50.3) of the time goes to the
fsync of the log, 48.9 % (48.9–48.9) to the fsync of `meta` and 0.7 % to computing in `Process`.
The node waits for its disk nearly all the time, and one round follows the other without a gap. At
120,000 a second on 4 lanes it is still 46.1 % and 43.6 %, with 10.2 % computing, in one run.

In the median a transfer on one node waits 6.4 flush times of the device and one on Postgres 1.9.
Three things add up to that.

- **Two syncs in sequence where Postgres needs one.** Without the fsync of `meta` the p50 of one
  node is 0.51 times what it is with it (see the seal, below).
- **Each sync of the node is the dearer kind.** The node calls fsync on a segment that grows as it
  is written, and on `meta`. Postgres calls `fdatasync` on a WAL segment that already exists. The
  disk probe of the plan writes 137 bytes and syncs, in a loop: at the slow speed an fsync on a
  growing file took 3.979 ms and an `fdatasync` on a preallocated file 1.74 ms at the start of the
  plan, and 3.592 ms and 1.846 ms at its end. These four figures are in `machine.txt`, not in
  `RESULTS.md`. The column "fsync per transfer" therefore compares calls of unequal weight: at
  2,000 a second one node makes 0.153 of them per transfer and Postgres 0.296, about half as many,
  and each of them costs about twice as much.
- **A transfer waits for the round in progress before its own,** and once a session always has a
  request in flight, also for the request ahead of it on its session. That is the step from 11.4 ms
  at 2,000 a second to 22.0 ms at 4,000 on 32 lanes. Postgres has no single writer: its 32
  backends commit side by side and share the flushes of the WAL.

With three nodes the same transfer waits 37.0 flush times (36.4–37.5), and its p50 is 5.6 times
that of one node, although the path has only twice the syncs: two on the leader, a round trip and
two on a follower (ADR-004). The leader of three spends 49.0 % (48.6–49.3) and 48.6 % (48.4–48.8)
of its time in the two fsyncs, a follower 30.2 % and 35.0 % with 34.3 % waiting for work. The rest
of the factor has three sources. The three nodes share one disk, so the six syncs of a round queue
on one device where three machines would run them side by side. The leader does not send while it
syncs and has at most one unacknowledged append per follower, so entries wait for the exchange
before them. And the one request in flight per session multiplies whatever a round trip costs. On
three machines the first of these falls away and a network takes its place. Neither was measured.

**The seal.** Its price was measured with one binary: built with the planted bugs, and run without
one and with B13, which leaves out the fsync of `meta` after the log (ADR-009). At 2,000 a second
on 32 lanes one node has a p50 of 10.9 ms (10.8–11.0) with the seal and 5.57 ms (5.50–5.63)
without, 0.51 times. Three nodes have 58.4 ms (56.3–60.4) and 26.1 ms (24.1–28.2), 0.45 times; the
two runs of three nodes without the seal missed the criterion. The seal doubles the wait at this
load. It does not change the number of sync calls per transfer, which is 0.99 and 1.08 times
without it: the node is bound by its disk either way, so without the seal a round is half as long,
and there are twice as many rounds with half the transfers each. The throughput ratio of 1.00 in
the table says nothing, because the load was fixed and held in both. What the seal costs a node
near its limit was not measured.

**Snapshots.** A node writes its whole state inside its loop (ADR-010). These runs force a snapshot
every 20,000 entries on one node and every 7,000 on three, at 2,000 transfers a second:

| Nodes | Accounts | Snapshot MB | Longest round ms, per run | ms per 100 MB, per run | Term changed |
|---|---|---|---|---|---|
| 1 | 10,000 | 34 | 67.4 and 85.0 | 198 and 250 | no |
| 1 | 1,000,000 | 311 | 542 and 628 | 174 and 202 | no |
| 1 | 1,000,000 | 614 | 1,198 and 1,273 | 195 and 207 | no |
| 3 | 1,000,000 | 308 to 315 | 761 to 1,129 | 242 to 366 | yes, in all four runs |

A single node stops for about 200 ms per 100 MB of snapshot, between 174 and 250 in six runs. It
has nobody to lose the lead to. With three nodes and a million accounts the longest round took 761
to 1,129 ms, the term changed in all four runs, and the worst second had a p99 of 1,606 to
2,359 ms. In the raw files the term rises at every snapshot of these runs, not once per run. The
election timeout is 0.5 to 1 s with the default tick (ADR-013). All three nodes reach the snapshot
interval at the same applied index, so three snapshots go to the one disk within a second or two,
and in two of the four runs the machine swapped pages out in those seconds. A snapshot of 308 to
315 MB is therefore a size at which it costs the lead on this machine, not the size at which that
begins: no smaller state was run with three nodes. Separate disks would not make that size safe,
since a single node already stops for 542 and 628 ms there. In two of the four runs a follower
was stopped and brought back. Both times it caught up from the log, so a follower that installs a
snapshot from the leader was not measured.

Both costs have a known remedy that is not built. Segments that are preallocated and synced with
`fdatasync` would make the sync of the log the cheaper kind; that changes the WAL and what the
crash tests pin down. A snapshot written outside the loop would take the pause away; ADR-010
rejects it for now, and ADR-013 says what it would touch. The figures here are what either would
be measured against.

## Where the criterion was missed

`bench check` refuses the result folder: 3 of its 110 verdicts fail. All three are groups of the
fixed load on `uniform-10k`, which decision 8 requires to agree. Their two runs had flush times
within 10 % of each other and still lay further apart:

| Group | One run | The other |
|---|---|---|
| one node, 32 lanes, 2,000 transfers a second | p50 11.52 ms | p50 9.472 ms |
| three nodes, 4 lanes, 20,000 transfers a second | p50 63.488 ms | p50 53.248 ms |
| Postgres, one transaction per transfer, serializable, 32 lanes, 2,000 transfers a second | 0.311 fsyncs per transfer | 0.282 fsyncs per transfer |

The first two are about a fifth apart, the third just over a tenth. Outside the required groups 26
more missed: 22 stages, two groups of the fixed load (`hot` on three nodes at 2,000 a second and
`uniform-1m` on three nodes at 20,000), three nodes without the seal, and the snapshot runs of one
node with a million accounts. `RESULTS.md` prints every verdict with the two values that
disagreed: in the column "agreement", and for the snapshot runs in a list under their table.

Of the 29 groups that missed, 25 belong to the ledger and 4 to Postgres. Among the groups that got
a verdict, the ledger agreed in 26 and missed in 25, and Postgres agreed in 31 and missed in 4.
Thirteen groups stayed undecided: the four runs with a CPU profile are made once, seven groups had
only one run at the slow speed, and in two the flush times of the two runs were more than 10 %
apart.

The pairing takes the flush time of the device as the measure of the disk. The wait of a transfer
on a node is made of fsync calls on files that grow, and those take longer than a flush of the
device and vary more: divided by the flush time, the p50 of one node at 2,000 a second still
spreads from 6.0 to 6.8, where Postgres stays at 1.9. So the sentence "two runs on an equally fast
disk were within 10 % of each other" holds only where the tables say `agreed`, and for the ledger
that is about half of its groups. Where they say `missed`, the spread printed next to the median
is the result.

The comparisons above do not hang on which of two runs is taken. At 2,000 a second the smallest
p50 of one node, 9.47 ms, is three times the largest of Postgres, 3.07 ms, and the smallest of
three nodes, 57.3 ms, is five times the largest of one node, 11.5 ms.

The criterion was not loosened afterwards, no group was taken out of it, and the plan was not run
again until it passed.

## When Postgres is the better choice

Outside any benchmark there is a short answer: nobody should run this ledger on data that matters
(ADR-013, "Not for production"). Read as a comparison of two designs, the measurement says this.

- **The load is in the range Postgres held, and latency matters.** Up to 8,000 transfers a second
  with one transaction per transfer, and up to 16,000 batched under `READ COMMITTED`, a single
  Postgres on this disk answered at a p50 of 2.75 to 6.72 ms. The exception is 8,000 a second
  under `READ COMMITTED`, where one run gave 2.82 ms and the other 30.7 ms. One node of the ledger
  needed 9.98 to 22.0 ms at the same rates and three nodes 42.5 to 63.0 ms.
- **One copy is enough, or replication is solved elsewhere.** One node against one Postgres is one
  copy against one copy, and at 2,000 a second one node takes 3.4 times as long. What a Postgres
  with a synchronous replica would give was not measured.
- **The data is asked questions.** The ledger answers lookups by ID and nothing else: no listing,
  no range by time, no sum, no balance at an earlier moment (ADR-013). In Postgres each of those is
  a query.
- **A transfer belongs in one transaction with other data.** An order and its payment commit
  together in Postgres. The ledger knows accounts and transfers, and its atomicity ends with the
  batch.
- **The data outgrows memory, or old data has to go.** The whole ledger is in the heap of every
  node and nothing is ever deleted. A node stops for about 200 ms per 100 MB to write a snapshot,
  and with three nodes and a million accounts the snapshots cost the lead on this machine.
- **Somebody has to run it.** Backup and restore, replacing a lost node, an upgrade without
  stopping, metrics: Postgres has them, and people who know them. The ledger has none (ADR-013).
- **The schema will change.** A new field on a transfer is a migration in Postgres and a new
  version of every format here.

## What speaks for the ledger

The first three were measured. The others are built and tested, not benchmarked.

- **Throughput through bundling.** On `uniform-10k` with 4 lanes, one copy each and the same disk,
  batched Postgres under `READ COMMITTED` held 32,000 transfers a second and confirmed 44,917
  (44,833–45,000) under overload, on six hardware threads. One node held 256,000 on two, using
  12.4 % (11.8 %–13.1 %) of them, and its limit was not reached. Three nodes, with three copies on
  that one disk, held 256,000 as well. The stages ran on 10,000 accounts only, where the batches
  of Postgres compete for the same rows (result 3); on a wider set of accounts its limit is
  likely higher and was not measured.
- **Latency that does not depend on what the transfers touch.** One thread applies and there are
  no locks (ADR-001). Hot accounts changed nothing for the ledger. With one transaction per
  transfer they multiplied the p99 of Postgres by 5.94 under `READ COMMITTED` and by 18.05 under
  `SERIALIZABLE`, and batched Postgres under `SERIALIZABLE` held 20,000 a second with none of the
  three loads it was given.
- **A flat curve.** From 4,000 to 256,000 transfers a second the p50 of one node stays between
  16.9 and 28.2 ms, with 32 lanes and with 4. Postgres is faster up to its last held stage and
  saturated at the next.
- **Replication without added parts.** Three copies, a leader change that clients follow on their
  own, and repair after disk corruption are in the binary (ADR-003, ADR-009). The Postgres of this
  comparison had one copy.
- **The rules of double entry in one place.** Balances, limits, linked chains and pending
  transfers with a timeout are the state machine, tested against a second, plain implementation.
  They are not constraints and functions that every application has to call correctly.
- **An audit trail.** A hash chain over all batches, and receipts that can be checked without the
  cluster (ADR-011). At 120,000 transfers a second its SHA-256 was 26.5 % of the CPU time of a
  node that used 6.8 % of its two threads.
- **Behaviour under faults that was checked.** The simulator (ADR-008) and the chaos test run this
  code under crashes, partitions and disk corruption. Nothing in this benchmark injected a fault.

## Consequences

- There are numbers now, each one traceable to a raw file. ADR-002, ADR-004, ADR-009, ADR-010,
  ADR-011 and ADR-013 said "not measured" where this run measured, and are amended.
- The result that speaks against the ledger, its latency at the loads Postgres holds, is in the
  README in the same table as the rest.
- The criterion can fail a whole evening, and it did. The folder is published with its verdict,
  and the sentence about agreement is restricted to the groups that agreed.
- The node got counters for the benchmarks to read: rounds, the slowest round, and fsyncs with
  their time by kind of file, printed on `SIGUSR1` (ADR-013, "No metrics endpoint"). A `ledgerd`
  built with the tag `pprof` writes a CPU profile; the default build refuses the variable for it.
- A plan takes an evening on which the machine does little else. A new run is a new folder next
  to this one, not a change to it.

## Rejected alternatives

- **Closed load,** where each client sends its next request after the previous answer. It is what
  most tools do, and it measures at the rate the system chooses for itself. A stall of one second
  costs one slow request per client instead of every transfer that was due.
- **`pgbench` with its built-in TPC-B script.** Another schema and another transaction than a
  ledger transfer, without the limit on debits and without idempotency. It would compare Postgres
  at its own benchmark with the ledger at another.
- **A tmpfs, or `fsync = off`.** Fast and repeatable, and it removes the one thing both systems
  spend their time on. Nearly every figure above is a count of syncs or a multiple of a flush.
- **The best of n runs.** It reports the luckiest disk of the evening. The median with its spread
  reports the evening.
- **A rented machine.** It has a better disk and steadier cores, it costs money per run, and no
  reader can repeat the run on it. The laptop is a worse machine with its limits written down.
- **Showing the fast disk speed.** The numbers would be several times better and true for the
  seconds in which the disk happened to be fast. Under sustained load it is not.
- **Loosening the criterion after the run, or running until it passes.** The 10 % are part of the
  commit that was measured. A criterion that is moved to where the results are decides nothing.
- **A comparison with other ledger databases.** Each of them would need the care Postgres got
  here, and the question a reader has is about the database he already runs.

## What the measurement does not cover

- **One laptop, one evening.** One run of the plan, with a desktop session and an editor next to
  it. Seconds in which other processes took a core or more are left out and counted in the table
  of runs. Whether another evening gives the same medians is not known.
- **One consumer disk.** On a disk with power-loss protection a flush costs far less, and both
  systems would be limited by something else. Nothing here says by what.
- **Three nodes on one disk, without a network.** Their syncs queue on one device and their
  messages take no time. The figures for three nodes are neither a lower nor an upper bound for a
  cluster on three machines.
- **Postgres without a replica,** in one configuration. Against three nodes it writes one copy. A
  synchronous replica, other settings, another function language and a second index were not
  tried.
- **Batched Postgres at 1,000 transfers per call, and 32 connections for the other two
  variants.** Under overload every call was at that limit. No other batch size and no larger pool
  was run on this evening.
- **Stages on one load.** The chains of stages ran on `uniform-10k` only. Where a target stops on
  a million accounts, with hot accounts or with two-phase transfers was not looked for.
- **The generator on the same machine.** It was the limit at 512,000 arrivals a second, so the
  rate at which a node saturates is not in the data.
- **Short windows and a small state.** A stage has 15 seconds and counts from 8 valid ones, the
  fixed load has 30, and the shortened stages at the top have as few as 5. No window is longer
  than a minute, and no run writes more than 2 million transfers on three nodes. Nothing here is
  about hours, about a ledger of many gigabytes or about a node close to its memory.
- **p99.9** is in the tables for completeness. With a desktop on the same machine, anything beyond
  p99 mostly describes the desktop.
- **Heat.** The thermal counter of a target core rose in many seconds. They stay in the figures
  and are counted in the column "seconds". The governor was `powersave`.
- **Reads.** Only writes were offered. Lookups and receipts under load were not measured. A
  measurement of the chain and of stamped receipts was planned and is not part of this plan,
  which has no load for receipts.
- **Faults.** No node was killed and no link was cut. The only failures in the plan are a follower
  that is stopped and the leader changes of the snapshot runs.
- **Catching up by snapshot.** The stopped follower caught up from the log both times.
- **What the seal costs under bundled load,** and what reading every entry back from the log
  costs: the CPU profile has one column for everything the log does.
