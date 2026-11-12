# ADR-010: Snapshots of the whole state, blocking, two kept plus the log

Status: accepted, 2026-11-11.

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
  for, stores it the same way, loads it back from disk and only then empties its log.

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
- The second snapshot and the log behind it exist so that a broken newest snapshot can be replaced
  from the older one and the log on the same node. That repair is not built: `wal.LoadSnapshot`
  reads only the newest snapshot, and if its checksum fails, the node does not start.
- Right after installing a snapshot from the leader, the log starts after that snapshot and does
  not reach back to the older one, until the node has taken a snapshot itself.
- A snapshot travels to a follower as one payload. There is no chunking.

## Rejected alternatives

- **Incremental or copy-on-write snapshots in the background.** No pause, but they need a second
  thread or persistent data structures in the state machine, which ADR-001 keeps single-threaded
  and plain. Worth it once the pause is measured and too long.
- **One snapshot only.** Enough for restarts, since the temporary file protects the write. Leaves
  nothing to repair a corrupted snapshot from.
- **Never compacting the log.** A restart and every lagging follower would replay all of history.
