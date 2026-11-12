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
internal/check       checkers that watch a simulated run, porcupine per window
internal/simbug      switch for the planted bugs, only in builds with -tags simbug
internal/workload    batch generator for the model test, the simulator and benchmarks
cmd/sim              runs seeds on all cores and prints the command that replays a failure
```

`raft`, `state`, `sim`, `replica`, `client` and `workload` use no clock, network, file system,
goroutines or global randomness. `depguard` in `.golangci.yml` enforces this for `time`, `net`, `os`, `sync` and
`crypto/rand`. Randomness is a `*rand.Rand` passed in, time is ticks. `check` may run seeds in
parallel but does not import `time` either: a seed's verdict never depends on the machine.

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
- A leader stamps writes only after it has applied an entry of its own term; before that a write
  gets `Busy`. A stamp outside the valid range (see Timestamps) gets `BadClock` and writes nothing.
- A write is proposed and remembered by its index. When the entry at that index is applied, the
  reply goes out. While a node stays leader, every entry after its own empty entry is one of its own
  proposals, so the term needs no check. When the node stops being leader, every waiting write gets
  `NotLeader`. The client asks again elsewhere, and its session makes sure nothing applies twice.
- Reads are collected; each `Process` issues at most one `ReadIndex` for all of them. A read
  appears in `Ready.Reads` with `Commit` at or above its index, and that commit is applied before
  the reads of the same round, so a confirmed read is answered at once. A tag the node already
  failed with `NotLeader` is skipped. `ErrNotReady` answers `Busy`. Reads need no session and are
  not stored.

### Timestamps

The leader stamps each request entry with `ts = min(Clock(), last + (ticks since last + 1) *
ClockStep)`, where `last` is the newest ledger time the node has applied and "ticks since last"
counts from the moment it applied it, on every node, also after a snapshot was loaded. Because the
leader stamps only after applying an entry of its own term, `last` is the newest time of the whole
cluster. The state machine uses `max(ts, last + 1)`, so a clock that falls behind only slows ledger
time down, and a clock that jumps ahead moves ledger time by at most `ClockStep` per tick since the
last write, plus one step. With `ClockStep` above the longest real tick, an honest clock passes the
bound unchanged.

Only the first write of a cluster (`last == 0`) takes the clock as it is: one clock is trusted once.
A stamp at or above 2^62 (the year 2116) is a broken clock and is refused with `BadClock` before it
reaches the log; this leaves the state machine room below its limit of 2^63, so no clock value can
stop the ledger from accepting writes. Replicas never read a clock: time comes from the log, so it
cannot make them diverge.

### Applying

Every node applies the same entries in the same way, and a broken body never panics:

- Each entry extends the apply hash: `hash = SHA-256(hash | index | term | kind | body)`.
- Register creates a session if the client is unknown. A full table (`MaxSessions`) first evicts
  the session with the oldest last access. The reply carries the session number, which is the index
  of the register entry that created it.
- Request: an unknown client or a wrong session number gives `SessionUnknown`. `number == last`
  gives the stored reply again, `number < last` gives `Stale`. Otherwise the batch goes to the state
  machine, and the reply is stored in the session: `Time` is the batch start
  `state.StartTime(ts, last) = max(ts, last + 1)`, the body lists the failures. A batch the state
  machine rejects is `Malformed`, stored the same way. A leader never proposes such a batch; the
  rule keeps every node deterministic on an entry it did not check itself.

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
  the WAL before the previous snapshot. The log reaches back to the older of the two kept
  snapshots, except after installing a snapshot from the leader: then the log is empty until the
  node takes its next own snapshot.
- Receiving: `Receive` keeps the payload of a `MsgSnap` by index and term. If the core asks to
  install it, the node checks the header against `Ready.Snapshot`, stores it with
  `wal.SaveSnapshot`, loads it back from disk and only then empties the log from the back. A crash in
  between leaves a log prefix that the start check empties. Payloads the core did not ask for are
  dropped after `Process`.
- Sending: the driver reads the bytes for a `MsgSnap` through `ReadSnapshot`. If that snapshot is
  gone, the message is dropped and the leader tries again later.

`Status()` reports ID, role, term, vote, leader, commit, applied, last index, apply hash and the
number of sessions. `CheckInvariants()` runs the full state check. `Digest()` hashes the snapshot
bytes, `LedgerDigest()` only the ledger state, which is what an independent model can recompute.

## Client sessions (`internal/client`)

`Session` is the I/O-free core of the SDK. `Submit`, `Step` and `Tick` change memory, `Outbox`
returns the requests to send and `Result` the final reply of the request that just finished.

- A session starts with `Register` (number 0) and keeps the session number from its reply. After
  that one request is in flight at a time, and its number grows by one for every request, reads
  included, so a late reply never matches a later question.
- `New` panics without nodes or with `retryTicks` below 1; both are programming errors.
- Replies with another number are dropped. `NotLeader` switches to the hinted node, or the next one,
  and resends at most once per tick. `Busy` and `BadClock` wait two ticks on the same node. No reply
  within `retryTicks` moves to the next node with the same request and the same number.
- `SessionUnknown` ends the session with `ErrEvicted`. The outcome of the request in flight is then
  unknown; the session never registers again on its own, because that would break exactly once.
- Client IDs are random and new for every session. Reusing one would let delayed requests of the old
  session apply in the new one.

## Simulator (`internal/sim`)

One goroutine runs a whole cluster on virtual time. A seed decides everything: the fault plan, the
latency and loss of every link, each disk's behaviour on power loss, the workload and the order of
events with equal time. The nodes are `replica.Node` on `disk.Sim`, the clients are
`client.Session`. A trace line is written for every event, and `TraceHash` is the SHA-256 over all
of them; the same seed gives the same hash.

- **Fault plan.** A storm phase of 2 to 6 seconds with up to twelve windows (partitions of several
  kinds, process crashes, power loss on one or all nodes, crashes after the k-th disk access,
  failing and slow fsync, EIO, clock jumps), then five quiet seconds in which everything heals and
  every node runs. Profiles `calm`, `net`, `crash`, `clock` and `all` choose which kinds appear.
- **Disk time.** A node is busy while its fsyncs run. Its messages leave afterwards, and inputs
  that arrive meanwhile go into the next `Process` together.
- **Workload.** 3 to 6 clients with 30 to 60 requests each. After 32 started requests the clients
  wait until all of them are answered. Each such window starts with no request in flight.
- **Profiles for sessions.** `evict` runs the `all` faults with two session slots for up to six
  clients.

## Checkers (`internal/check`)

`check.Run` runs one seed and reports the first violation with seed, virtual time, node and reason.
While the run goes on, after every event:

| Check | Property | Source |
|---|---|---|
| S1 | at most one leader per term | senders of `MsgApp` and `MsgSnap` |
| S2 | per node and term at most one candidate gets the vote, across restarts | granted `MsgVoteResp` |
| S3 | the apply hash at each index is the same on every node and never changes | `Status()` |
| S4 | a node's term never falls, across restarts; its commit never falls within a process | `Status()` |
| S5 | the disk keeps the node's word: at every 50th promise the disk is cloned, the clone loses power and is opened; an acknowledgement up to m is still covered, a granted vote is still in the hard state | `disk.Sim.Clone` |
| S6 | state invariants, every 64 applied entries and at the end | `CheckInvariants()` |
| F1 | a node stops only because of a disk error, never because it found a contradiction or cannot start | stop and start errors |
| L3 | a run ends within 500 000 events (about 20 times the most a correct run needs) | event count |

At the end:

| Check | Property |
|---|---|
| S7 | every node is up, with the same applied index and the same digest |
| L2 | exactly one leader, every node at the same commit index |
| L1 | every request is answered; a request open in the quiet phase within two virtual seconds |
| E1 | every reply belongs to its request: same client, number and operation |
| E2 | a client told `SessionUnknown` no longer has that session on any node; in `evict` no node holds more sessions than allowed |
| S8 | oracle replay: all confirmed writes, ordered by their time, run through `internal/oracle`; every reply matches, every successful transfer appears once, and the ledger digest equals the nodes' |
| S9 | linearizability: porcupine checks each window, starting from the oracle state at the window border |

S8 and S9 are skipped in `evict`, where the outcome of a request cut off by eviction is unknown.

S9's model is the oracle itself. A write steps only if its reply time is later than the model's
last time and its failures match what the oracle computes; a read steps only if it shows the
model's state and time. Writes therefore linearize in the order of their times, and two states
with the same set of linearized operations are equal, which keeps porcupine's cache effective.
The search is bounded by the input, not by a timeout: windows of at most 32 requests, a handful of
clients, few accounts.

### Planted bugs

Built with `-tags simbug`, `simbug.Set` switches on one of twelve bugs. The decisions they touch
live in small functions in `raft/rules.go` and `replica/rules.go`; the tagged twin files contain
the same functions with the switch. B1 to B3 wrap the disk inside the checker. Without the tag none
of this code exists.

| Bug | What breaks | Found by |
|---|---|---|
| B1 | fsync of the hard state does nothing | F1, S5 |
| B2 | fsync of the WAL does nothing | S5 |
| B3 | directory sync after a new segment does nothing | S5 |
| B4 | acknowledgements leave before the WAL sync | S5 |
| B5 | entries are stored before the hard state | F1 (the node cannot restart) |
| B6 | commit by counting replicas for entries of older terms | not found, see below |
| B7 | votes without comparing logs | S3 |
| B8 | a follower keeps a conflicting tail | S3, L3 |
| B9 | a follower commits up to its own log end | S3 |
| B10 | reads without confirmation by a majority | not found, see below |
| B11 | sessions do not recognise a repeated request | S8, S9 |
| B12 | entries are applied with the replica's own clock | S7 |

B6 does not show in the simulator: a new leader appends an empty entry and sends older entries
only together with it, so every acknowledgement already covers an entry of its own term. B10 needs
a cut-off leader that still answers reads after a client has finished a write elsewhere; check-
quorum deposes it before clients find the new leader. The rule tests in `internal/raft` catch both.

`go run ./cmd/sim -seeds 10000 -profile all` runs seeds on all cores and stops at the first failure
with the command that replays it, for example
`go run -tags simbug ./cmd/sim -bug B12 -seed 0 -profile all -trace`.
