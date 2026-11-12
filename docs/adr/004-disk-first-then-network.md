# ADR-004: Disk first, then network

Status: accepted.

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

1. store the hard state (term and vote),
2. install a snapshot, if there is one,
3. append and sync the entries,
4. send the messages,
5. apply committed entries read back from the log and answer reads,
6. call `Advance`.

The leader counts its own log towards a majority only in `Advance`, after its own sync. When the
term changes before a `Ready`, messages of the older term are dropped, so every message in a
`Ready` is covered by the state the same `Ready` stores.

## Consequences

- The rule is checked, not trusted. The simulator clones a node's disk at every 50th promise,
  cuts the power on the clone and checks that the promise is still there (checker S5).
- The planted bugs B4 (messages before the fsync) and B5 (entries before the hard state) are found
  by S5 in the first seed and by the restart check within a few hundred seeds.
- Commit latency is the leader's fsync, then a round trip, then the follower's fsync, one after the
  other. Sending in parallel would overlap the first fsync with the rest. The benchmarks measure what
  that costs.
- Batching needs no timer. Everything that arrives while a node waits for its disk shares the next
  fsync.

## Rejected alternatives

- **Sending in parallel with the leader's fsync.** Correct when done carefully and faster, but each
  message type then needs its own argument. The latency is named here instead of hidden.
- **Persisting asynchronously with a separate writer.** Needs locks or channels in the node loop
  and makes crash points depend on scheduling, which the simulator could not replay.
