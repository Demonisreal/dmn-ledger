# Architecture

Work in progress. This file grows with each phase; today it covers the layers and the contract
between the consensus core and the code that drives it.

## Layers

```
internal/server      goroutines, TCP, real clock, disk.OS
internal/replica     one loop per node: raft + WAL + state
  internal/raft      consensus core, no I/O
  internal/wal       log, hard state and snapshots on disk.FS
  internal/state     ledger state machine, deterministic
internal/client      request types and the session core of the SDK
internal/sim         deterministic simulator driving the same replica code
internal/workload    batch generator for the model test, the simulator and benchmarks
```

`raft`, `state`, `sim`, `replica`, `client` and `workload` use no clock, network, file system,
goroutines or global randomness. `depguard` in `.golangci.yml` enforces this for `time`, `net`, `os`, `sync` and
`crypto/rand`. Randomness is a `*rand.Rand` passed in, time is ticks.

## Consensus core (`internal/raft`)

The core is a state machine. The driver feeds it events and carries out what it asks for:

- inputs: `Tick()`, `Step(Message)`, `Propose(kind, body)`, `ReadIndex(tag)`
- output: `Ready()`, a batch of work, then `Advance()` once that work is done
- `Status()` for role, term, leader, commit index and last index

Pre-vote, check-quorum and ReadIndex are built in. There are no leases, no leadership transfer, no
membership changes and no reads from followers. Messages are Go values; the wire format comes
later.

### Driver contract

After any number of inputs, while `HasReady()` is true, the driver calls `Ready()` and then, in this
order:

1. If `HardState` is set, store it durably.
2. If `Snapshot` is set, store its payload durably, load the state from it and empty the log.
3. Append `Entries` durably: truncate the log after `Entries[0].Index-1`, append, sync.
4. Send `Messages`.
5. Read the entries `(applied, Commit]` back from the durable log, apply them, answer `Reads`.
6. Call `Advance()`.

Nothing leaves a node before everything it relies on is on disk. A granted vote, a vote request
and an acknowledgement are promises; a node that sends one and forgets it after a crash can elect
two leaders in one term or lose a committed entry. The leader counts its own log towards a majority
only in `Advance()`, after its own sync.

Sending in parallel with the leader's own sync (Raft dissertation, section 10.2.1) would save
latency and is deliberately not done.

### Validity of a Ready

- Between `Ready()` and `Advance()` the driver calls nothing else on the core.
- `Commit` is always set, not only when it changed. `HardState` is nil when term and vote are
  unchanged since the last `Advance()`.
- If the term changes before `Ready()`, messages of the older term are dropped, so every message
  in a `Ready` is covered by the state that the same `Ready` stores.
- If two `MsgSnap` arrive before one `Ready()`, the core asks for the higher one. The driver keeps
  snapshot payloads by index and term and installs the one named in `Ready.Snapshot`.

### Memory ownership

- `Ready.Entries`, `Ready.Messages` and `Ready.Reads` belong to the driver after `Advance()`. The
  core never writes to them again.
- `Message.Entries` may point into memory returned by `Log.Entries` or into the core's unstored
  entries. The network copies messages before delivery.
- `Step` keeps the entries of an accepted `MsgApp`, including their bodies. The caller must not
  change them afterwards. `Propose` keeps `body` without copying it.
- The core never writes to memory returned by `Log.Entries`. A `Log` must not reuse that memory
  either.

### The Log interface

`Log` is a read-only view of what is durable. The driver changes it in steps 2 and 3, and between
two rounds when it takes a snapshot of its own: `Snapshot()` moves forward and entries before an
older snapshot may disappear. The core keeps no copy of either, so a follower that needs a removed
entry simply gets a `MsgSnap`.

- `LastIndex() >= Snapshot().Index`. `FirstIndex()` is the first readable entry,
  `LastIndex()+1` when the log is empty. The log may reach back before the snapshot.
- `Term(i)` and `Entries(lo, hi, maxBytes)` are only called for readable indexes: the snapshot
  index, `[FirstIndex(), LastIndex()]`, and 0 when `FirstIndex() == 1`. Any error is a real read
  error.
- `Entries` returns `[lo, hi)`, at most `maxBytes` of bodies but always at least one entry.

### Errors and fail-stop

`Tick`, `Step`, `Propose` and `ReadIndex` return an error when the log cannot be read or when a
safety rule would be broken (`ErrSafety`: a second leader in one term, or a leader that contradicts
a committed entry). Both are fail-stop: the driver stops the node and restarts it from disk with
`New`. `ErrNotLeader`, `ErrBusy`, `ErrNotReady` and `ErrKind` are ordinary refusals.

Malformed messages (wrong recipient, unknown sender or type, entries with gaps or terms above the
message term) are dropped silently. Byzantine nodes are outside the fault model; this only keeps a
bad message from leaving a log that cannot be restarted.

### Concurrency

A `Raft` is not safe for concurrent use. One loop owns it. `Status()` is an input like any other
and is not called from a second goroutine.

### Reads

`ReadIndex(tag)` works only on a leader that has committed an entry of its own term. It records the
commit index and sends a heartbeat round; once a majority has answered in the leader's term, the
read appears in `Ready.Reads` as `(tag, index)`.

A read that reached `Ready.Reads` is valid even if the node lost leadership in the same round: its
linearization point is the moment a majority confirmed the leader, and every later leader was
elected after that. Reads still waiting when leadership is lost disappear without a message.

What the driver has to do:

- Answer a read from the state once `applied >= index`. Entries of a newer leader or an installed
  snapshot in `applied` are fine, all of it is committed.
- Never reuse a tag within a process. Unknown tags are ignored.
- Per round, first answer `Reads`, then use `Status()` to fail the reads that are still waiting
  with "not leader".

## Node (`internal/replica`)

A `Node` is one loop around the core. `Tick`, `Receive` and `Request` only change memory. `Process`
runs the driver contract above, round after round, until the core has nothing left, and returns the
messages and client replies. Everything that arrived since the previous `Process` shares one fsync;
that is the whole batching, there is no timer. Any I/O error, any error from the core and any
contradiction on disk stops the node: `Process` keeps returning the error, and only a new `Open`
continues.

### Files and start

A node keeps `<dir>/hardstate`, `<dir>/wal/` and `<dir>/snap/`. `Open` reads them in this order:

1. The hard state (term and vote).
2. The newest snapshot, if there is one: sessions, ledger state and the apply hash. `applied` is its
   index, otherwise 0.
3. The WAL, checked against the snapshot. If the log ends before the snapshot index, or holds
   another term there, it is emptied from the back (`TruncateAfter(0)`). If it starts after
   index + 1, `Open` fails.
4. The core, with a log view whose last index is `max(wal last, snapshot index)`.

The commit index is not stored. Entries after the snapshot are applied again once a leader reports
them as committed.

### Log entries

| Kind | Body (big endian) |
|---|---|
| 1, register | `client u128` |
| 2, request | `client u128`, `session u64`, `number u64`, `op u8`, `ts u64`, records |

One client request is one entry. Reads never reach the log.

### Requests on the leader

- A node that is not leader answers `NotLeader` with the leader it knows, 0 if none. A request over
  1 MiB, with an unknown op or with a body that is not a whole number of records is `Malformed` and
  writes nothing. A full core (`ErrBusy`) answers `Busy`.
- A write is proposed and remembered as (index, term). When an entry of that term is applied at that
  index, the reply goes out. When the node stops being leader, every waiting write gets
  `NotLeader`. The client asks again elsewhere, and its session makes sure nothing applies twice.
- Reads are collected; each `Process` issues at most one `ReadIndex` for all of them and answers
  them from the state once `applied` reaches the confirmed index. `ErrNotReady` answers `Busy`.
  Reads need no session and are not stored.

### Timestamps

The leader stamps each request entry with `ts = min(Clock(), anchor + (ticks since anchor + 1) *
ClockStep)`. The anchor is the newest ledger time the node has applied; it moves whenever that time
reaches the bound. The very first stamp of a cluster takes the clock as it is. The state machine
uses `max(ts, last + 1)`, so a clock that falls behind only slows ledger time down, and a clock that
jumps ahead moves ledger time by at most `ClockStep` per tick. Replicas never read a clock: time
comes from the log, so it cannot make them diverge. A leader that starts after a long pause without
writes stamps behind the clock until its tick count catches up; deadlines then expire late, never
early.

### Applying

Every node applies the same entries in the same way, and a broken body never panics:

- Each entry extends the apply hash: `hash = SHA-256(hash | index | term | kind | body)`.
- Register creates a session if the client is unknown. A full table (`MaxSessions`) first evicts
  the session with the oldest last access. The reply carries the session number, which is the index
  of the register entry that created it.
- Request: an unknown client or a wrong session number gives `SessionUnknown`. `number == last`
  gives the stored reply again, `number < last` gives `Stale`. Otherwise the batch goes to the state
  machine, and the reply (`Time = max(ts, last + 1)` and the failures) is stored in the session.
  A batch the state machine rejects is `Malformed`, stored the same way.

### Exactly once

The session is replicated state, so every node agrees on which request numbers have applied. A
retry with the same number gets the stored reply instead of a second effect. The session number
closes the remaining gap: a register request that arrives late, after its session was evicted,
creates a new session with a new number, and requests still carrying the old number get
`SessionUnknown` instead of applying again. Transfer IDs stay as a second layer for clients that
restart and resubmit.

### Snapshots

- Content: magic, version, applied index and term, apply hash, sessions sorted by client ID, then
  the bytes of `state.WriteSnapshot`. The same state gives the same bytes; `Digest()` is SHA-256 over
  them.
- A node takes one after `SnapshotEvery` applied entries, at the end of `Process`, and then compacts
  the WAL before the previous snapshot. The log always reaches back to the older of the two kept
  snapshots.
- Receiving: `Receive` keeps the payload of a `MsgSnap` by index and term. If the core asks to
  install it, the node checks the header against `Ready.Snapshot`, stores it with
  `wal.SaveSnapshot`, loads it back from disk and only then empties the log from the back. A crash in
  between leaves a log prefix that the start check empties. Payloads the core did not ask for are
  dropped after `Process`.
- Sending: the driver reads the bytes for a `MsgSnap` through `ReadSnapshot`. If that snapshot is
  gone, the message is dropped and the leader tries again later.

`Status()` reports ID, role, term, leader, commit, applied, last index, apply hash and the number of
sessions. `CheckInvariants()` runs the full state check.

## Client sessions (`internal/client`)

`Session` is the I/O-free core of the SDK. `Submit`, `Step` and `Tick` change memory, `Outbox`
returns the requests to send and `Result` the final reply of the request that just finished.

- A session starts with `Register` (number 0) and keeps the session number from its reply. After
  that one request is in flight at a time, and its number grows by one for every request, reads
  included, so a late reply never matches a later question.
- Replies with another number are dropped. `NotLeader` switches to the hinted node, or the next one,
  and resends at most once per tick. `Busy` waits two ticks. No reply within `retryTicks` moves to
  the next node with the same request and the same number.
- `SessionUnknown` ends the session with `ErrEvicted`. The outcome of the request in flight is then
  unknown; the session never registers again on its own, because that would break exactly once.
- Client IDs are random and new for every session. Reusing one would let delayed requests of the old
  session apply in the new one.
