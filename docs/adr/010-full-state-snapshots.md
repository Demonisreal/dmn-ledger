# ADR-010: Snapshots of the whole state, blocking, two kept plus the log

Status: accepted, 2026-11-11. Amended 2026-11-13: a start falls back to the older snapshot
(ADR-009).

## Context

The log grows with every request. A node that restarts must not replay all of it, and a follower
that fell far behind needs the state, not the entries the leader no longer has. No transfer is
ever deleted, so the ledger itself only grows as well.

## Decision

A node writes a snapshot of its whole state after `SnapshotEvery` applied entries, at the end of
`Process`, inside the node loop. Nothing else runs on the node while it writes.

- Content: a header with applied index and term, the apply hash and the client sessions, then the
  canonical bytes of `state.WriteSnapshot`. The same state gives the same bytes.
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
  `go test -run '^$' -bench Snapshot -count 3 ./internal/state/` serialises a ledger with 1000
  accounts and 100 000 transfers, about 10 MB, in 2.2 to 2.4 ms in memory and reads it back with
  the full invariant check in 15 to 17 ms. Under load from other processes the same command gave
  two and a half to four times as much. The pause of a node writing to a real disk, fsync
  included, is not measured yet. It grows with the ledger, because nothing is ever deleted.
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
- A snapshot travels to a follower as one payload. There is no chunking.

## Rejected alternatives

- **Incremental or copy-on-write snapshots in the background.** No pause, but they need a second
  thread or persistent data structures in the state machine, which ADR-001 keeps single-threaded
  and plain. Worth it once the pause is measured and too long.
- **One snapshot only.** Enough for restarts, since the temporary file protects the write. Leaves
  nothing to repair a corrupted snapshot from.
- **Never compacting the log.** A restart and every lagging follower would replay all of history.
