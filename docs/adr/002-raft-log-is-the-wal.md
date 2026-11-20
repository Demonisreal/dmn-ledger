# ADR-002: The Raft log is the WAL, one request is one entry

Status: accepted, 2026-11-11. Amended 2026-11-13: the metadata file and its seal (ADR-009).
Amended 2026-11-16: grouping in the server. Amended 2026-11-20: grouping and the cost of an fsync
measured on a real disk (ADR-012).

## Context

A replicated store needs a durable log for consensus, and its state needs to survive a crash. Many
systems keep two logs for that: the consensus log and the write-ahead log of the storage engine
below the state machine. Every write is then synced twice, and recovery has to bring two logs into
agreement.

The cost of a write is dominated by fsync. With `TMPDIR` on the NVMe disk (ext4) of an i5-12500H
laptop rather than on a tmpfs, `go test -run '^$' -bench AppendSync -count 3 ./internal/wal/`
appended and synced one 128-byte entry in 0.5 to 4 ms and 1024 entries in 0.8 to 5 ms, depending
on the session. Within a session a thousand entries cost about as much as one, at most a few times
as much. Whatever shares an fsync is almost free.

## Decision

There is one log, `internal/wal`. It is the Raft log, and nothing else records the ledger's
history: after a crash the state is the newest snapshot plus the committed entries after it,
applied again.

- Segment files `wal-<first index>.log` hold frames with length, CRC32C of header and body,
  index, term, kind, a sync mark and the body. Term, vote, the repair flag and the seal live in
  a file of their own with two slots (ADR-009), snapshots in their own directory.
- The core reads the log through its `Log` interface. The node appends, syncs, and applies
  committed entries by reading them back from the log, never from the memory they came from.
- One client request is one entry: kind 1 registers a session, kind 2 carries one batch of up to
  1 MiB of records with its timestamp, kind 0 is a new leader's empty entry.
- Grouping comes from the node loop. `Tick`, `Receive` and `Request` only change memory;
  `Process` turns everything that arrived since the last call into one `Ready`, one append, one
  fsync of the log and one of the seal. While a node waits for its disk, new input piles up and
  goes into the next round. There is no timer and no batch size to tune.

## Consequences

- One recovery path: the metadata, the newest valid snapshot, then the WAL, judged by the seal and
  checked against the snapshot.
- The WAL knows about replication: terms must not fall, `TruncateAfter` drops a conflicting tail,
  also in the middle of a segment, and `CompactBefore` removes whole segments after a snapshot.
- Groups grow with load and with disk latency. Under light load every request costs its own
  round, two fsyncs with the seal. In the server a round takes everything that arrived during the
  previous one: `TestRequestsDuringASyncShareTheNext` holds an fsync, sends 50 requests meanwhile
  and counts two log syncs for all 51. On a real disk, with the fsyncs of the log and of `meta`
  counted together (ADR-012): one node with 32 client sessions makes 0.314 fsyncs per transfer
  (0.294–0.334) at 1,000 transfers a second, 0.022 (0.022–0.023) at 16,000 and 0.001 at 256,000.
  Three nodes added up make 0.467 (0.431–0.503) at 1,000 a second and 0.030 (0.028–0.033) at
  16,000. At 2,000 a second the loop of one node spends 50.2 % of its time in the fsync of the log
  and 48.9 % in that of `meta`. One round follows the other without a gap, so a group is whatever
  arrived during two fsyncs.
- These fsyncs are the dear kind. A segment grows as it is written, and in the disk probe of that
  run (`machine.txt` in its folder) an fsync on a growing file took 3.979 ms where an `fdatasync`
  on a preallocated file took 1.74 ms. Postgres syncs its WAL the second way, which is part of why
  it answers faster than one node at a load both hold (ADR-012). Preallocated segments are not
  built.
- Applying reads each entry back from the WAL, with a second read through the page cache and a
  second CRC check. That is the price of applying only what is on disk. It is not measured on its
  own: the CPU profile of ADR-012 has one column for everything the log does, and it holds 11.6 %
  of the CPU time of one node at 2,000 transfers a second and 15.7 % at 120,000, of a node that
  uses 3.8 % and 6.8 % of its two hardware threads.
- A checksum alone cannot tell a bit flip in the last synced batch from a torn write. The seal in
  the metadata file can (ADR-009): a broken frame up to the seal is damage, a broken frame after
  it at the end of the log is cut off (`TestOpenJudgesDamageByTheSeal`).

## Rejected alternatives

- **A group committer in the WAL.** An early version had `wal.Committer`, a goroutine that
  collected submissions from many callers into one fsync. Once the node loop owned the WAL alone,
  it grouped by itself and the committer had no caller. It was removed, and with it the only
  goroutines in the package.
- **Batching on a timer**, for example 1 ms. Adds latency when the cluster is idle and a setting
  that has to be tuned per disk, for nothing the loop does not already do.
- **A separate log or storage engine for the state machine.** Two fsyncs per write and two logs to
  reconcile after a crash.
