# ADR-010: Snapshots of the whole state, blocking, two kept plus the log

Status: accepted, 2026-11-11. Amended 2026-11-13: a start falls back to the older snapshot
(ADR-009). Amended 2026-11-16: the snapshot as a stream, format 2. Amended 2026-11-20: the pause
measured on a real disk, and what it does to a leader (ADR-012).

## Context

The log grows with every request. A node that restarts must not replay all of it, and a follower
that fell far behind needs the state, not the entries the leader no longer has. No transfer is
ever deleted, so the ledger itself only grows as well.

## Decision

A node writes a snapshot of its whole state after `SnapshotEvery` applied entries, at the end of
`Process`, inside the node loop. Nothing else runs on the node while it writes.

- Content: a header with applied index and term, the apply hash, `MaxSessions` and the client
  sessions, then the canonical bytes of `state.WriteSnapshot`. The same state gives the same
  bytes. The state machine writes format 2: accounts and transfers in the order of their
  timestamps, then the links of the audit trail (ADR-011). The node's header is version 2 as well;
  it carries `MaxSessions`, and a node refuses a snapshot written with another value.
- `wal.SaveSnapshot` writes a temporary file with a CRC32C footer, syncs it, renames it, syncs the
  directory and keeps the two newest snapshots.
- Afterwards the WAL is compacted before the previous snapshot, by whole segments. Every entry
  after the older of the two kept snapshots stays in the log.
- A follower that gets a snapshot from the leader checks its header against what the core asked
  for, stores it the same way, loads it back from disk, seals the snapshot index (ADR-009) and
  only then empties its log.
- On start a node loads the newest snapshot whose checksum holds and deletes the broken ones above
  it.

## Consequences

- One format for restart, catch-up and comparison: `Digest()` is SHA-256 over the snapshot bytes.
- A crash while a snapshot is written leaves a temporary file, and the previous snapshot stays the
  newest.
- The node pauses for the whole write. On an otherwise idle i5-12500H laptop,
  `go test -run '^$' -bench Snapshot -count 3 ./internal/state/` serialised a ledger with 1000
  accounts and 100 000 transfers, about 10 MB, in 2.2 to 2.4 ms in memory and read it back with
  the full invariant check in 15 to 17 ms. Under load from other processes the same command gave
  two and a half to four times as much. Since format 2 the check on reading recomputes the audit
  trail as well, and reading the same ledger takes 39.5 to 40.6 ms (`GOMAXPROCS=4`); writing stays
  at 2.2 ms.
- On a real disk, fsync included, the longest round of a single node under 2,000 transfers a
  second was 67.4 and 85.0 ms with a snapshot of 34 MB, 542 and 628 ms with 311 MB and 1,198 and
  1,273 ms with 614 MB, two runs each: about 200 ms per 100 MB, between 174 and 250 (ADR-012).
  The pause grows with the ledger, because nothing is ever deleted.
- With three nodes the pause costs the lead. Three nodes on one machine and one disk, with a
  million accounts and snapshots of 308 to 315 MB, had a longest round of 761 to 1,129 ms, and
  the term changed in all four runs; the raw files show it rising at every snapshot. The election
  timeout is 0.5 to 1 s. All three nodes reach the snapshot interval at the same applied index,
  so on one disk their snapshots compete. Separate disks would not make this size safe: a single
  node already stops for 542 to 628 ms there. No smaller state was run with three nodes, so the
  size at which it begins is not known. A follower that installs a snapshot from the leader was
  not measured either: the follower that was stopped in two of the runs caught up from the log.
- The second snapshot and the log after it exist so that a broken newest snapshot can be replaced
  on the same node. If the log still reaches from the loaded snapshot to the seal, the node
  replays it and nothing else happens (`TestBrokenNewestSnapshotFallsBackToTheOlder`). With no
  snapshot left and a log that starts at 1, it replays from the start (`TestAllSnapshotsBroken`).
- The broken file is deleted because it would otherwise count as one of the two newest at the next
  snapshot and push out the healthy older one.
- Right after installing a snapshot from the leader, the log starts after that snapshot and does
  not reach back to the older one, until the node has taken a snapshot itself. If the installed
  snapshot breaks in that time, the gap cannot be closed on the node: it is damaged and repairs
  from the leader (ADR-009, `TestBrokenInstalledSnapshotIsDamage`). The same goes when every
  snapshot is broken and the log no longer starts at 1.
- The install seals the snapshot index so that this case is noticed. A seal still standing at the
  older snapshot would let the node fall back to it as if nothing had been acknowledged since.
- A snapshot whose checksum holds and whose content cannot be read is not a disk fault. The start
  fails.
- The simulator hands a snapshot to a follower as one payload. The server streams it on a
  connection of its own, so heartbeats keep flowing meanwhile: the `MsgSnap`, then the file in
  chunks of up to 256 KiB, then an end frame with the length and the CRC32C of all chunks. The
  receiver writes the chunks to a spool file and hands it to the node only after an end that
  matches, so a stream that was cut or has a gap never reaches `install`, which would stop the
  node on a snapshot it cannot load. The spool file keeps the node loop off the network, at the
  price of writing the snapshot twice on the receiver. Details under "Snapshots as a stream" in
  [ARCHITECTURE.md](../ARCHITECTURE.md).

## Rejected alternatives

- **Incremental or copy-on-write snapshots in the background.** No pause, but they need a second
  thread or persistent data structures in the state machine, which ADR-001 keeps single-threaded
  and plain. Worth it once the pause is measured and too long. It is measured now, and with three
  nodes and snapshots of 308 to 315 MB it is too long. It is still not built; ADR-013 says what
  it would touch.
- **One snapshot only.** Enough for restarts, since the temporary file protects the write. Leaves
  nothing to repair a corrupted snapshot from.
- **Never compacting the log.** A restart and every lagging follower would replay all of history.
