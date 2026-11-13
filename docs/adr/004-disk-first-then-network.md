# ADR-004: Disk first, then network

Status: accepted, 2026-11-11. Amended 2026-11-13: the seal is part of the order (ADR-009).

## Context

Raft is only safe if a node never forgets what it promised. A granted vote, a vote request and an
acknowledgement of entries are promises. If a node sends one and loses it in a crash, two leaders
can be elected in one term, or an entry counts as committed although too few disks hold it.

There are several ways to get the order right. etcd lets the leader send entries to followers
while its own fsync is still running (Raft dissertation, section 10.2.1) and has rules for each
kind of message.

## Decision

One rule: **nothing leaves a node before everything it relies on is on its disk.** The driver
handles every `Ready` in a fixed order:

1. store the hard state (term, vote and the repair flag),
2. install a snapshot, if there is one, and seal its index,
3. append and sync the entries, then seal the last index (ADR-009),
4. send the messages,
5. apply committed entries read back from the log and answer reads,
6. call `Advance`.

The leader counts its own log towards a majority only in `Advance`, after its own sync and seal.
When the term changes before a `Ready`, messages of the older term are dropped, so every message
in a `Ready` is covered by the state the same `Ready` stores.

## Consequences

- The rule is checked, not trusted. The simulator clones a node's disk at every 50th promise
  after a round and at every 20th sync inside a round or a start, cuts the power on the clone and
  checks that what the node promised is still there, or that the node knows it is not (checker
  S5).
- The planted bugs B4 (messages before the fsync) and B5 (entries before the hard state) are
  found, B4 in the first seed and B5 within 7 to 29 seeds from five starting points. B5 leaves a
  disk on which no node starts, which S5 reports. B4 leaves frames missing below the seal, so the
  node on the clone starts in repair on a disk that nobody damaged, which F2 reports.
- Commit latency is two fsyncs on the leader, for the log and for the seal, then a round trip,
  then two on the follower, one after the other. Sending in parallel would overlap the leader's
  with the rest. What that costs in a cluster is not measured yet; ADR-009 has the count of fsyncs
  per round and a first measurement of one node on a real disk.
- Batching needs no timer. Everything that arrives while a node waits for its disk shares the next
  fsync.

## Rejected alternatives

- **Sending in parallel with the leader's fsync.** Correct when done carefully and faster, but each
  message type then needs its own argument. The latency is named here instead of hidden.
- **Persisting asynchronously with a separate writer.** Needs locks or channels in the node loop
  and makes crash points depend on scheduling, which the simulator could not replay.
