# Architecture

This file describes what is built today: the consensus core and the contract with its driver, the
node, the client sessions, the simulator and its checkers. There is no network server yet; a node
runs only inside the simulator and the tests.

## Layers

```
internal/replica     one loop per node: raft + WAL + state
  internal/raft      consensus core, no I/O
  internal/wal       log, hard state and snapshots on disk.FS
  internal/state     ledger state machine, deterministic
internal/disk        file system interface: disk.OS and the simulated disk.Sim
internal/client      request types and the session core of the SDK
internal/oracle      second, deliberately plain implementation of the ledger rules
internal/u128        unsigned 128-bit integers for IDs and amounts
internal/sim         deterministic simulator driving the same replica code
internal/check       checkers that watch a simulated run, porcupine per window
internal/simbug      switch for the planted bugs, only in builds with -tags simbug
internal/workload    batch generator for the model test, the simulator and benchmarks
cmd/sim              runs seeds on all cores and prints the command that replays a failure
```

`raft`, `state`, `sim`, `replica`, `client` and `workload` use no clock, network, file system,
goroutines or global randomness. `depguard` in `.golangci.yml` enforces this for the imports
`time`, `net`, `os`, `sync` and `crypto/rand`. Randomness is a `*rand.Rand` passed in, time is
ticks. `check` may run seeds in parallel but does not import `time` either: a seed's verdict never
depends on the machine.

`wal` and `disk` are outside that rule. `disk.OS` is the real file system, and `wal` takes the
nonces for its segment headers from `crypto/rand` unless `Options.Nonces` is set. The simulator
hands every node a `disk.Sim` and a nonce source drawn from the seed. `TestWALNoncesAreSeeded`
checks that the seed alone decides the segment bytes, and the same seed always gives the same
`TraceHash`.

## Consensus core (`internal/raft`)

The core is a state machine. The driver feeds it events and carries out what it asks for:

- inputs: `Tick()`, `Step(Message)`, `Propose(kind, body)`, `ReadIndex(tag)`
- output: `Ready()`, a batch of work, then `Advance()` once that work is done
- `Status()` for role, term, leader, commit index and last index

Pre-vote, check-quorum and ReadIndex are built in. There are no leases, no leadership transfer, no
membership changes and no reads from followers. Messages are Go values; there is no wire format yet.

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

### Rules

The rules below are what the core implements. Each has at least one table test in
`internal/raft/rules_test.go`, listed after the rule; the scenarios of the Raft paper (Figures 7
and 8) and randomised schedules over three and five cores are in `scenario_test.go` and
`random_test.go`.

Terms:

- R1. A message with a higher term, except `MsgPreVote` and a granted `MsgPreVoteResp`, makes the
  node a follower in that term with no vote; then the message is handled. A refused pre-vote with
  a higher term is how a cut-off node learns the current term. (`TestHigherTermBecomesFollower`)
- R2. A request with a lower term (`MsgApp`, `MsgSnap`, `MsgVote`, `MsgPreVote`) is refused with
  the node's own term, a response with a lower term is dropped. (`TestLowerTermRejected`,
  `TestLeaderIgnoresStaleTermAck`, `TestCandidateIgnoresStaleTermVote`)

Elections:

- R3. The election timeout is drawn from `[ElectionTicks, 2*ElectionTicks)` and drawn again on a
  valid `MsgApp` or `MsgSnap` of the current leader, on a granted vote, when a candidacy starts and
  whenever the node becomes follower. (`TestElectionTimeoutRange`, `TestElectionTimeoutReset`)
- R4. When it runs out, the node sends `MsgPreVote` for `term+1`. Term and vote stay as they are,
  nothing is stored. (`TestTimeoutStartsPreVote`)
- R5. A pre-vote is granted if the proposed term is higher, the candidate's log is at least as
  up to date (last term, then last index) and the receiver has not heard from a leader for
  `ElectionTicks`. A leader refuses. Granting changes nothing on the receiver.
  (`TestPreVoteGrant`, `TestLeaderRejectsPreVote`)
- R6. With a majority of pre-votes for `term+1`, its own included, the node increments its term,
  votes for itself and sends `MsgVote`. (`TestPreVoteMajorityStartsElection`)
- R7. A vote is granted if the node has not voted in this term, or voted for the same candidate,
  and the candidate's log is at least as up to date. (`TestVoteGrant`, `TestRejectionsAreNotVotes`)
- R8. With a majority of votes the candidate becomes leader: progress of every follower starts
  over at `next = last+1`, `match = 0`, and the leader appends an empty entry in its term.
  (`TestBecomeLeaderAppendsEmptyEntry`, `TestReelectedLeaderResetsProgress`)
- R9. Check-quorum: every `ElectionTicks` the leader checks whether a majority, itself included,
  has answered in its term since the last check. If not, it becomes follower in the same term.
  (`TestCheckQuorum`)

Replication:

- R10. At most one `MsgApp` with entries is unacknowledged per follower; an empty one may always
  go. Every `HeartbeatTicks` an unacknowledged one counts as lost, and the leader sends again from
  `next`, at most `MaxAppendBytes` of bodies. (`TestOneAppendInFlight`,
  `TestAppendRespectsMaxBytes`, `TestAppendRespectsMaxBytesUnstable`)
- R11. If `Term(next-1)` can no longer be read, the leader sends `MsgSnap` for its snapshot and
  waits up to `SnapshotTicks` for the answer. The follower keeps getting heartbeats meanwhile, so it
  still counts for check-quorum and reads. (`TestSnapshotForCompactedFollower`,
  `TestSlowSnapshotKeepsLeader`, `TestLeaderSendsPendingSnapshot`)
- R12. A follower that gets `MsgApp` in its term notes the leader and resets its timeout. If the
  message starts below its commit index, it answers success with its commit index. If the previous
  entry is missing or has another term, it refuses with a hint: `last+1` if its log is too short,
  otherwise the first index of its own run of entries with the conflicting term, never below
  `commit+1`; a mismatch exactly at the commit index is `ErrSafety`. Otherwise it skips the
  entries it has, drops its log from the first one that differs and appends the rest. Its commit
  becomes `max(commit, min(m.Commit, last entry of the message))`, not its own log end.
  (`TestFollowerAppend`, `TestFollowerAppendCommittedConflict`, `TestFollowerRejectsSecondLeader`)
- R13. On success the leader raises `match` and sets `next = match+1`, and sends more if there is
  more; a success beyond its own last index is dropped. On a refusal it sets
  `next = max(match+1, min(hint, next))` and sends again at once; a refusal with index 0, the R2
  answer to an old message, changes nothing. (`TestLeaderHandlesAppResp`,
  `TestStaleRejectDoesNotTriggerSnapshot`)
- R14. The commit index moves to the highest index a majority holds, counting the leader with what
  it stored by the last `Advance()`, and only if that index is at or after the leader's own empty
  entry. Entries of older terms are never committed by counting, only together with a later one of
  the leader's term. (`TestCommitOnlyOwnTerm`, `TestCommitCountsLeaderAfterAdvance`, `TestFigure8`)
- R15. `Propose` appends on the leader. It returns `ErrBusy` once `last - commit` reaches
  `MaxUncommitted` and `ErrKind` for kind 0, which only the leader's empty entry uses.
  (`TestPropose`)

Snapshots:

- R16. A follower answers every `MsgSnap` with `MsgAppResp`. At or below its commit index it only
  answers. If its log holds the snapshot's index and term, it keeps its log and moves its commit
  index there. Otherwise `Ready.Snapshot` asks the driver to replace log and state, unstored
  entries are dropped and the commit index becomes the snapshot index. (`TestFollowerSnapshot`,
  `TestSnapshotDropsUnstableEntries`)

Reads:

- R17. `ReadIndex(tag)` on a leader that has committed an entry of its term increments a sequence
  number, remembers tag, commit index and sequence, and sends a `MsgApp` to every follower, empty if
  there is nothing to send. Every `MsgApp` carries the current sequence, every answer in the
  leader's term echoes it, a refusal too. (`TestReadIndex`)
- R18. Once a majority, the leader included, has echoed a sequence at or above a read's, the read
  appears in `Ready.Reads`. Reads still waiting when the node loses leadership are dropped.
  (`TestReadConfirmedByQuorum`, `TestDeposedLeaderGetsNoRead`, `TestReadSurvivesStepDown`)

## Node (`internal/replica`)

A `Node` is one loop around the core. `Tick`, `Receive` and `Request` only change memory. `Process`
runs the driver contract above, round after round, until the core has nothing left, and returns the
messages and client replies. Everything that arrived since the previous `Process` shares one fsync;
that is the whole batching, there is no timer. Any I/O error, any error from the core and any
contradiction on disk stops the node: `Process` keeps returning the error, and only a new `Open`
continues. Steps 1 to 3 of the contract live in `persist` in `replica/rules.go`, not in `node.go`,
because that is where a planted bug can switch them (see Planted bugs).

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

Every node keeps an anchor: a trusted time and the tick it was taken in. A leader trusts a clock
reading that is at most `anchor + (ticks since the anchor + 1) * ClockStep`. It stamps the reading
and moves the anchor to it, or to the last applied time if that is later, but never above
`anchor + ticks since the anchor * ClockStep`, so the anchor does not rise faster than the line it
already describes. A reading above the bound is replaced by the bound and does not move the
anchor. A node also moves the anchor when it applies a write it did not stamp in its current term,
to the time of that write and the tick it is applied in, and when it loads a snapshot. A restarted
node counts ticks from zero. A leader stamps only after it has applied an entry of its own term, so
the last applied time is the newest of the whole cluster. The state machine uses
`max(ts, last + 1)`.

What this gives:

- Ledger time never moves by more than one `ClockStep` plus `ClockStep` per tick since the anchor,
  plus one nanosecond per write, whatever the clock does. Each new leader gets that one step once.
  `ClockStep` is 1.25 ticks in the simulator, so ledger time runs at most 25 % faster than the
  leader's ticks. Ticks without a write count as well: after an idle hour, a jump of a quarter of an
  hour passes at once.
- An honest clock passes unchanged whatever the commit latency, as long as the same node leads and
  its ticks keep pace with its clock. After a stall that swallowed ticks, such as a long GC pause,
  the clock looks like one that jumped forward.
- A clock that jumps back cannot move ledger time back; writes get the last time plus one
  nanosecond until the clock is ahead again. If writes arrived meanwhile, ledger time then trails
  by as long as the clock was behind.
- After a restart or a leader change, ledger time trails the clock by the time since the last
  write the new leader had applied, and catches up by `ClockStep - tick` per tick, a quarter of
  real time when ticks run at their nominal period. A deadline set while it catches up expires
  early by at most the factor `ClockStep / tick`: a 10 s timeout lasts at least 8 s of real time,
  never zero.
- `ClockStep` must be above the longest real tick period, or a node never catches up. The
  simulator lets tick periods vary up to 1.2 ticks, against a `ClockStep` of 1.25.
- `BadClock` is returned only for a reading of 2^62 (the year 2116) or more while the node has no
  anchor yet, that is before any write with a time has been applied. This keeps the state machine
  below its limit of 2^63. Once there is an anchor, the bound is stamped instead, so from then on
  no clock value stops the ledger from accepting writes.

Until then the first write takes the leader's clock as it is: one clock is trusted once. Replicas
never read a clock: time comes from the log, so it cannot make them diverge. The tests for these
bounds are in `internal/replica/clock_test.go`; `TestStampsNeverRiseFasterThanOneStepPerTick`
drives a clock at random, forwards, backwards and in jumps, and checks the first point above.

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
  the bytes of `state.WriteSnapshot`. The same state gives the same bytes; `Digest()` is SHA-256
  over them.
- A node takes one after `SnapshotEvery` applied entries, at the end of `Process`, and blocks
  while it writes. `wal.SaveSnapshot` writes a temporary file, syncs it, renames it and keeps the
  two newest snapshots. The node then compacts the WAL before the previous snapshot, by whole
  segments, so every entry after the older of the two kept snapshots is still in the log.
- That does not hold right after a node installed a snapshot from the leader: the install empties
  the log, so it starts after the installed snapshot until the node has taken a snapshot itself.
  The same goes for a start that emptied the log (see Files and start).
- On start, `wal.LoadSnapshot` reads only the newest snapshot. If its checksum fails, `Open`
  fails; it does not fall back to the older snapshot. A crash while a snapshot is written leaves
  only a temporary file, and the previous snapshot stays the newest.
- Receiving: `Receive` keeps the payload of a `MsgSnap` by index and term. If the core asks to
  install it, the node checks the header against `Ready.Snapshot`, stores it with
  `wal.SaveSnapshot`, loads it back from disk and only then empties the log from the back. A crash
  in between leaves a log prefix that the start check empties. Payloads the core did not ask for
  are dropped after `Process`.
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
  and resends at most once per tick. `Busy` and `BadClock` wait two ticks on the same node; after
  three of them in a row the next attempt goes to the next node. No reply within `retryTicks`
  moves to the next node with the same request and the same number.
- Moving on after refusals helps when a cut-off leader answers `Busy` until check-quorum deposes
  it. It does not help against a leader that always answers `BadClock`: the other nodes send the
  client back to it with `NotLeader`.
- `Submit` copies the body, so the caller may reuse its buffer.
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

- **Fault plan.** A storm of 2 to 6 seconds with up to twelve windows (partitions of several
  kinds, process crashes, power loss on one or all nodes, crashes after the k-th disk access,
  failing and slow fsync, EIO, clock jumps, paused nodes, leader crashes), then five quiet seconds
  in which everything heals and every node runs. The profiles `calm`, `net`, `crash`, `clock`
  and `all` choose which kinds appear; `evict`, `leaders` and `split` are described below.
- **Pause.** A node stops like a process in a long GC pause or on an overloaded VM: no ticks, no
  processing, messages to it pile up. Either it stands still for the whole window, or it wakes
  every 20 to 200 ms, takes one tick and everything that arrived, and stops again. Half of the
  pauses hit whichever node leads when the window opens. Check-quorum counts the leader's own
  ticks, so a paused leader is not deposed while it stands still.
- **Leader crash.** Kills the node that leads, in one of three ways: at once when the window
  opens, after a `Process` that raised its commit index ("on commit"), or after the `Process` in
  which it became leader ("on election"). In the last two the node is stopped after its round and
  the round's messages are lost; the crash itself is a separate event at the same virtual time,
  so the checkers see the state before it. The node stays down until the window ends. If no node
  leads, the next one that does is the victim.
- **Per seed.** Besides the plan, the seed draws `MaxAppendBytes` (0 to 8 KiB, so catching up
  happens in pieces), the clients' `retryTicks` (2 to 20), snapshot interval, segment size and the
  workload's batch size.
- **Disk time.** A node is busy while its fsyncs run. Its messages leave afterwards, and inputs
  that arrive meanwhile go into the next `Process` together.
- **Workload.** 3 to 6 clients with 30 to 60 requests each. After 32 started requests the clients
  wait until all of them are answered. Each such window starts with no request in flight.
- **Sessions.** `evict` runs the `all` faults with two session slots for up to six clients.
  Clients start up to two seconds late, open a new session after one in eight answers and after
  `SessionUnknown`, so evictions hit clients with a request in flight and late duplicates of a
  `Register` arrive after their session was evicted.
- **Leaders.** `leaders` runs leader crashes of all three kinds, process crashes, isolated nodes,
  bridges and message loss, with `MaxAppendBytes` 0 so that every `MsgApp` carries one entry. An
  old entry can then reach a majority on its own before the new leader's empty entry is
  acknowledged, which is the situation of Figure 8 in the Raft paper.
- **Partitions.** `split` runs nothing but partitions. Every node and client lands on a random
  side, so clients keep talking to a leader that has lost its majority.
- **Reads.** While a partition or a pause is open, every second request is a lookup instead of
  every fifth, so reads meet leaders that have lost their majority.

## Checkers (`internal/check`)

`check.Run` runs one seed and reports the first violation with seed, virtual time, node and reason.
While the run goes on, after every event:

| Check | Property | Source |
|---|---|---|
| S1 | at most one leader per term | senders of `MsgApp` and `MsgSnap` |
| S2 | per node and term at most one candidate gets the vote, across restarts | granted `MsgVoteResp` |
| S3 | the apply hash at each index is the same on every node and never changes; every committed index holds the same term on every node | `Status()`, `EntryTerm` |
| S4 | a node's term never falls, across restarts; its commit never falls within a process | `Status()` |
| S5 | the disk keeps the node's word, see below | `disk.Sim.Clone` |
| S6 | state invariants, every 64 applied entries and at the end | `CheckInvariants()` |
| F1 | a node stops only because of a disk error, never because it found a contradiction or cannot start | stop and start errors |
| L3 | a run ends within 500 000 events (about 20 times the most a correct run needs) | event count |

At the end:

| Check | Property |
|---|---|
| S7 | every node is up, with the same applied index and the same digest |
| L2 | exactly one leader, every node at the same commit index |
| L1 | every request is answered; one still open when the faults have healed within two virtual seconds |
| E1 | every reply belongs to its request: same client, number and operation |
| E2 | a client told `SessionUnknown` no longer has that session on any node; no node holds more sessions than allowed |
| S8 | oracle replay: all confirmed writes, ordered by their time, run through `internal/oracle`; every reply matches, so a transfer applied twice shows as a wrong reply, and the ledger digest equals the nodes' |
| S9 | linearizability: porcupine checks each window, starting from the oracle state at the window border |

S5 looks at the disk at two points. After a round, at every 50th promise (a granted vote, a vote
request or an acknowledgement), the node's disk is cloned, the clone loses power and a node is
opened on it: the term must be there, a vote must be the one promised and the log must reach the
acknowledged index. In the middle of a round, at every 20th successful sync, the same is done
against a receipt of what the node has promised so far: its last vote with the term, and the
highest index it acknowledged in the last term. That second point finds writes in the wrong order
within a round, which a check after the round cannot see. Neither point samples while a node runs
`Open`, so a bug in recovery itself shows only through F1 or a later check.

A panic in a seed is a finding like a violation: `check.Sweep` reports it with the seed, and
`cmd/sim` prints the command that replays it.

In `evict` a write that ends with `SessionUnknown` may or may not have been applied. S8 then
replays only what is certain: let t be the earliest start of such a write and T the latest time of
a write answered before t. Every write with a time up to T was applied before t, so none of the
uncertain ones lies in between. S8 checks those writes and skips the final digest, S9 runs only
when nothing is uncertain.

S9's model is the oracle itself. A write steps only if its reply time is later than the model's
last time and its failures match what the oracle computes; a read steps only if it shows the
model's state and time. Writes therefore linearize in the order of their times, and two states
with the same set of linearized operations are equal, which keeps porcupine's cache effective.
The search is bounded by the input, not by a timeout: windows of at most 32 requests, a handful of
clients, few accounts.

### Planted bugs

Built with `-tags simbug`, `simbug.Set` switches on one of twelve bugs. B1 to B3 wrap the disk
inside the checker. The other nine touch eight small functions, and only those live in separate
files: in `raft/rules.go` the five safety decisions of the core (`upToDate` for votes, `merge` for a
conflicting tail, `followCommit` for the follower's commit bound, `maybeCommit` for the commit rule,
`confirmedSeq` for read confirmation), in `replica/rules.go` the write order `persist`, the session
check `fresh` and the entry time `entryTime`. Each file has a twin `rules_simbug.go` with the same
functions plus the switch, so a build without the tag contains no trace of the bugs and the code
around them stays free of switches. The twins are kept equal by hand and checked by pinned trace
hashes: `TestTracesStayPinned` in `internal/check/golden_test.go` runs eight seeds of every profile
through `check.Run` and compares the hashes with `internal/check/testdata/traces.golden`. The test
has no build tag, so it runs in the normal build and in the `simbug` build with no bug switched on,
against the same file. A twin that drifts from its original changes a hash. After a change in
behaviour that is meant, `go test ./internal/check -run TestTracesStayPinned -update` writes the
file again.

The table gives the first failing seed counted from 0, measured with
`go run -tags simbug ./cmd/sim -bug Bn -seeds N -profile P`, and the checker that reports it.

| Bug | What breaks | Profile | First seed | Found by |
|---|---|---|---|---|
| B1 | fsync of the hard state does nothing | `all` | 0 | S5, the disk does not open after a power loss |
| B2 | fsync of the WAL does nothing | `all` | 0 | S5, the disk does not open |
| B3 | directory sync after a new segment does nothing | `all` | 0 | S5, in the middle of a round |
| B4 | acknowledgements leave before the WAL sync | `all` | 0 | S5, after the round |
| B5 | entries are stored before the hard state | `all` | 60 | S5, in the middle of a round |
| B6 | commit by counting replicas for entries of older terms | `leaders` | 365 | S3 |
| B7 | votes without comparing logs | `all` | 3 | S3 |
| B8 | a follower keeps a conflicting tail | `all` | 14 | S3 |
| B9 | a follower commits up to its own log end | `all` | 14 | S3 |
| B10 | reads without confirmation by a majority | `split` | 563 | S9 |
| B11 | sessions do not recognise a repeated request | `all`, `evict` | 0 | S8 |
| B12 | entries are applied with the replica's own clock | `all` | 0 | S7 |

`TestBugsAreFound` hunts every bug in its profile and checks that the expected checker reports
it. Counted from five starting seeds (0, 10 000, 20 000, 30 000, 40 000), the first finds are 4 to
60 seeds for B5, 192 to 376 for B6 in `leaders` and 15 to 563 for B10 in `split`. Over 30 further
starting seeds they spread wider, up to 1684 seeds for B6 and 1240 for B10, so the test gives B6
and B10 6000 seeds each and B5 1000; a change that only reshuffles the random draws should not
turn it red.

B6 is found in `leaders`; in 10 000 seeds of `all` or `crash` it does not show. Losing a commit
needs Figure 8 of the Raft paper: an old entry reaches a majority by itself, the leader counts it
and dies before its own empty entry is acknowledged, and a node with a later entry at the same
index wins next. What makes this visible is that a leader crash after a round is a separate event
after the checkers have looked at that round; a crash in the same step would hide the commit from
them. The `leaders` profile also sends one entry per append message, because Figure 8 needs an old
entry to reach a majority on its own before the leader's own entry does. With the random append
size of the other profiles the simulator still finds B6, after a median of about 2 800 seeds from
five starting seeds instead of 265.

B10 needs a cut-off leader that still answers reads after a client has finished a write
elsewhere. Check-quorum leaves only a short window for that. `split` keeps clients on both sides of
a partition; in `all` the first find is seed 1149.

The planted bugs show that the checkers notice these mistakes, not that the simulator finds every
mistake in the core. Two further mutations of the core, a leader that keeps `match` from an
earlier term and a leader that lets any answer in its term confirm all waiting reads, passed
58 000 seeds of `all`, `crash`, `net`, `evict` and five-node clusters and at least 5 000 seeds
each of `leaders` and `split`; only the rule tests caught them.

`go run ./cmd/sim -seeds 10000 -profile all` runs seeds on all cores and stops at the first failure
with the command that replays it, for example
`go run -tags simbug ./cmd/sim -bug B12 -seed 0 -profile all -trace`. The built program exits
with 0 when every seed passed, 1 for a finding, 2 for a usage error and 3 when a run failed
without a finding, for example because the trace could not be written; `go run` turns every
non-zero code into 1.
