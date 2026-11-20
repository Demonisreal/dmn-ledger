# Architecture

This file describes what is built today: the consensus core and the contract with its driver, the
node, the audit trail, the client sessions, the wire format, the server, the SDK, the simulator and
its checkers, the chaos test with real processes, and the demo. What is not built, and what each
gap means for a user, is in [ADR-013](adr/013-non-goals.md).

## Layers

```
cmd/ledgerd          one node as a process, configured from LEDGER_ variables
cmd/ledgerctl        status, accounts, transfers, receipts and their checks
ledger               the public Go SDK around the session core
internal/server      second driver of replica.Node: sockets, the real clock, the snapshot stream
internal/wire        frames for requests, replies, status, the peer hello, raft messages, snapshots
internal/replica     one loop per node: raft + WAL + state
  internal/raft      consensus core, no I/O
  internal/wal       log, metadata (term, vote, seal) and snapshots on disk.FS
  internal/state     ledger state machine and its hash chain, deterministic
internal/audit       hash formats of the audit trail: Merkle tree, links, heads, receipts
internal/notary      Ed25519 stamps over heads, key files
internal/disk        file system interface: disk.OS and the simulated disk.Sim
internal/client      request types and the session core of the SDK
internal/oracle      second, deliberately plain implementation of the ledger rules and the chain
internal/u128        unsigned 128-bit integers for IDs and amounts
internal/sim         deterministic simulator driving the same replica code through the wire codec
internal/check       checkers that watch a simulated run, porcupine per window
internal/simbug      switch for the planted bugs, only in builds with -tags simbug
internal/workload    batch generator for the model test, the simulator and the chaos test
internal/faultnet    TCP proxy that delays, drops, swallows, stalls or corrupts whole frames
internal/local       three ledgerd as children on loopback, a faultnet link per direction, on Linux
  internal/local/localtest  builds ledgerd once for the tests that start it
internal/history     calls of real clients in windows, each closed window handed to check.Replayer
internal/chaos       three ledgerd processes under faults, only with -tags chaos on Linux
internal/demo        controller of the demo: cluster, load, counts, four faults, HTTP, the page
cmd/sim              runs seeds on all cores and prints the command that replays a failure
cmd/demo             the demo as a process, configured from DEMO_ variables
bench/               a module of its own: open load on ledgerd and on Postgres, raw files, reports
```

`raft`, `state`, `sim`, `replica`, `client`, `workload`, `wire` and `audit` use no clock, network,
file system, goroutines or global randomness. The `depguard` rule `deterministic` in
`.golangci.yml` enforces this for the imports `time`, `net`, `os`, `sync` and `crypto/rand`.
Randomness is a `*rand.Rand` passed in, time is ticks. `check` may run seeds in parallel but does
not import `time` either: a seed's verdict never depends on the machine. A second rule, `keyless`,
keeps `crypto/ed25519` and `internal/notary` out of `state`, `replica`, `raft`, `sim`, `client`,
`workload`, `audit` and `oracle`, tests included. No signing key reaches the replicated core; only
the server and the tools sign or verify.

`wal` and `disk` are outside that rule. `disk.OS` is the real file system, and `wal` takes the
nonces for its segment headers from `crypto/rand` unless `Options.Nonces` is set. The simulator
hands every node a `disk.Sim` and a nonce source drawn from the seed. `TestWALNoncesAreSeeded`
checks that the seed alone decides the segment bytes, and the same seed always gives the same
`TraceHash`.

## Consensus core (`internal/raft`)

The core is a state machine. The driver feeds it events and carries out what it asks for:

- inputs: `Tick()`, `Step(Message)`, `Propose(kind, body)`, `ReadIndex(tag)`
- output: `Ready()`, a batch of work, then `Advance()` once that work is done
- `Status()` for role, term, leader, commit index, last index and whether the node is in repair

Pre-vote, check-quorum and ReadIndex are built in. A node whose disk has lost data runs as a
non-voter until the leader has brought it up to date again (R19 to R24). There are no leases, no
leadership transfer, no membership changes and no reads from followers. Messages are Go values;
`internal/wire` encodes them for the network, and the simulator sends every message through the
same codec (see Wire format).

### Driver contract

After any number of inputs, while `HasReady()` is true, the driver calls `Ready()` and then, in this
order:

1. If `HardState` is set, store it durably. It holds the term, the vote and the repair flag.
2. If `Snapshot` is set, store its payload durably, load the state from it, seal the snapshot
   index and empty the log.
3. Append `Entries` durably: if that cuts the log below the seal, first lower the seal to
   `Entries[0].Index-1`; then truncate the log after that index, append, sync and seal the last
   index.
4. Send `Messages`.
5. Read the entries `(applied, Commit]` back from the durable log, apply them, answer `Reads`.
6. Call `Advance()`.

Nothing leaves a node before everything it relies on is on disk. A granted vote, a vote request
and an acknowledgement are promises; a node that sends one and forgets it after a crash can elect
two leaders in one term or lose a committed entry. The leader counts its own log towards a majority
only in `Advance()`, after its own sync and seal.

The seal is the highest index the node may have acknowledged: as a follower with an answer, as a
leader by counting its own log in `Advance()`. It is synced before either happens, in the file
that holds term and vote, not in the log it vouches for. A round without entries does not seal. On
start, whatever is broken or missing up to the seal is damage (see Files and start). After a
restart a node holds everything it ever acknowledged, or it knows that it does not and does not
vote. "Everything" leaves out what a later leader made the node replace by R12, and the sentence
assumes a disk whose fsync is honest. With both slots of the file unreadable the node does not
start at all.

The driver sets `Repair` in `Config.HardState` when its disk has forgotten something, and only
then. The core clears it (R24). Between two starts the driver never changes it.

`Rand` must be seeded anew on every start. A core that starts in repair draws its nonce from it
(R19), and a nonce that repeats lets the answer to a probe of an earlier process end the repair
(`TestRepeatedNonceLetsAnOldAnswerEndTheRepair`). The simulator forks a new generator for every
start. The server draws 16 bytes from `crypto/rand` on every `Open` and seeds a PCG with them;
`TestEveryStartDrawsItsOwnNonce` starts it twice on a directory in repair and gets two different
nonces in its repair probes.

Sending in parallel with the leader's own sync (Raft dissertation, section 10.2.1) would save
latency and is deliberately not done.

### Validity of a Ready

- Between `Ready()` and `Advance()` the driver calls nothing else on the core.
- `Commit` is always set, not only when it changed. `HardState` is nil when term, vote and repair
  flag are unchanged since the last `Advance()`.
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
`random_test.go`. The random schedules also cut the end of a node's log and restart it in repair,
one node at a time and any number at once. Their harness checks in addition that a node whose disk
carries the repair flag sends nothing a voter or a leader sends, and that a node leaving repair
holds every committed entry it ever stored.

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
- R4. When it runs out, the node sends `MsgPreVote` for `term+1`, unless it is in repair (R20).
  Term and vote stay as they are, nothing is stored. (`TestTimeoutStartsPreVote`)
- R5. A pre-vote is granted if the proposed term is higher, the candidate's log is at least as
  up to date (last term, then last index), the receiver has not heard from a leader for
  `ElectionTicks` and it is not in repair. A leader refuses. Granting changes nothing on the
  receiver. (`TestPreVoteGrant`, `TestLeaderRejectsPreVote`)
- R6. With a majority of pre-votes for `term+1`, its own included, the node increments its term,
  votes for itself and sends `MsgVote`. (`TestPreVoteMajorityStartsElection`)
- R7. A vote is granted if the node has not voted in this term, or voted for the same candidate,
  the candidate's log is at least as up to date and the node is not in repair. (`TestVoteGrant`,
  `TestRejectionsAreNotVotes`)
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
  more; a success beyond its own last index is dropped. A refusal with index 0, the R2 answer to an
  old message, changes nothing. Neither does one whose hint is at or above `next`: a fresh refusal
  always lies below `next`, so this one is old, and nothing is sent again. Every other refusal is
  believed, even below `match`: `next = hint`, `match = min(match, hint-1)`, and the leader sends
  again at once. Lowering `match` is safe: the commit index never falls, and a lower `match` only
  makes the count more careful. Without it a follower that lost entries it had acknowledged would
  refuse the same append again and again. (`TestLeaderHandlesAppResp`,
  `TestRejectBelowMatchRewinds`, `TestRejectAtOrAboveNextIsDropped`,
  `TestStaleRejectDoesNotTriggerSnapshot`, `TestLostEntriesComeBackWithoutStorm`)
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

Repair:

- R19. Repair is part of the hard state. The driver sets it in `Config.HardState` when its disk has
  forgotten something; the core never sets it and clears it only by R24. A core that starts in
  repair draws a nonce from `Rand`. (`TestRepairComesFromHardState`)
- R20. A node in repair is not a voter. It starts no pre-vote when its timeout runs out and refuses
  every pre-vote and vote with its own term. R1 still holds: it takes over a higher term and stores
  it. (`TestRepairingNodeDoesNotVote`, `TestRepairingNodeDoesNotHelpElect`, `TestTwoRepairingNodes`)
- R21. It stays a follower. It accepts `MsgApp` and `MsgSnap`, acknowledges them and follows the
  commit index. On the leader its answers count like those of any follower, for commits,
  check-quorum and reads. A node in repair counts for commits and reads, never for elections.
  (`TestRepairingNodeCountsForCommitAndReads`)
- R22. While it is in repair and has no goal for the current term, the node sends `MsgRepairProbe`
  with its term and nonce to the leader it knows, every `HeartbeatTicks`. Without a known leader it
  sends nothing. A leader answers a probe of its own term with `MsgRepairInfo`, which carries its
  last index, unstored entries included, and the nonce. Any other node drops the probe, and a
  probe with a lower term gets no answer. (`TestRepairProbe`, `TestProbeGoesOnlyToTheKnownLeader`)
- R23. The first `MsgRepairInfo` in the node's term that carries its nonce sets the goal, a pair
  of term and index. A goal counts only while its term is the current one. The answer to a probe
  of an earlier process carries another nonce and is ignored. A node that is not in repair
  ignores `MsgRepairInfo` and never probes. (`TestRepairGoal`, `TestRepairIgnoresOtherNonce`,
  `TestRepairGoalIsBoundToTheTerm`, `TestHealthyNodeNeitherProbesNorTakesAGoal`)
- R24. The core remembers the highest index it has acknowledged in the current term, for `MsgApp`
  and `MsgSnap` alike, and forgets it on every term change. In `Advance()`, once there is a goal
  for the current term and that index has reached it, the core clears repair and draws a new
  election timeout. The next `Ready` carries the hard state without repair. That is one `Ready`
  after the entries up to the goal were stored, so a crash in between never leaves a voter without
  them. (`TestRepairCountsOnlyAcksOfTheTerm`, `TestRepairCountsEveryAcknowledgement`,
  `TestCampaignForgetsWhatWasAcknowledged`, `TestRepairEndsOneReadyLater`,
  `TestRepairedNodeVotesAgain`, `TestHealedNodeKeepsItsElectionTimer`)

The goal is the leader's last index, not its commit index. The node may have acknowledged an entry
whose acknowledgement the leader has not processed yet, and that entry lies above the commit
index. A node that crashes while in repair starts again with the flag and a new nonce.

With three nodes and one of them in repair, the cluster stops when the leader becomes unreachable,
until the leader is back: the two that are left cannot elect a leader
(`TestRepairingNodeDoesNotHelpElect`). It gives no wrong answer meanwhile. As long as the leader
keeps its role, the node in repair and the leader are a majority for commits, check-quorum and
reads, even with the third node unreachable (`TestRepairingNodeCountsForCommitAndReads`). Why
this is safe, and what its limits are, is in
[ADR-009](adr/009-disk-corruption-seal-and-repair.md).

## Node (`internal/replica`)

A `Node` is one loop around the core. `Tick`, `Receive` and `Request` only change memory. `Process`
runs the driver contract above, round after round, until the core has nothing left, and returns the
messages and client replies. Everything that arrived since the previous `Process` shares one fsync
of the log and one of the seal; that is the whole batching, there is no timer. Any I/O error, any
error from the core and any contradiction on disk stops the node: `Process` keeps returning the
error, and only a new `Open` continues. That includes an entry or a snapshot that fails its
checksum when it is read. A node never applies or sends an entry that failed its checksum; the
next start decides whether the disk is damaged (`TestBitFlipFoundWhileApplyingStopsTheNode`,
`TestBitFlipFoundWhileSendingStopsTheLeader`). Steps 1 to 3 of the contract live in `persist` and
steps 5 to 9 of a start in `mend`, both in `replica/rules.go` and not in `node.go`, because that
is where a planted bug can switch them (see Planted bugs).

### Files and start

A node keeps `<dir>/meta`, `<dir>/wal/` and `<dir>/snap/`. `meta` holds term, vote, the repair
flag and the seal in two slots of 40 bytes at offsets 0 and 4096, each with a sequence number and
a CRC32C; the valid slot with the higher sequence counts. Storing term, vote or flag writes both
slots, one after the other, each with its own fsync. A seal writes one slot, the one that is not
the newest. `Open` takes these steps in this order:

1. `meta`: term, vote, repair flag, seal, and whether both slots were valid. With no valid slot
   `Open` fails.
2. Snapshots: the newest one whose checksum holds is loaded, with sessions, ledger state and apply
   hash, and `applied` is its index. Broken ones above it are noted. Without a valid snapshot the
   state is empty and `applied` is 0.
3. The WAL, judged by the seal (table below).
4. Cover: if the seal is above the loaded snapshot, the log has to start no later than the entry
   after the snapshot and reach the seal. Otherwise that is damage as well. So is a broken
   snapshot when one slot of `meta` was invalid: the seal may then be older than that snapshot.
5. `meta` is settled. With two valid slots the one just read is written back and synced, so that
   it is on disk and not only in the page cache before the other one is overwritten. With one
   valid slot nothing is written. No earlier step writes to `meta`: a power loss must not leave a
   torn slot next to a torn end of the log.
6. On damage the repair flag is stored, before anything is deleted. A cluster of one node fails
   here with `wal.ErrCorrupt` instead, because there is no other node to repair it from.
7. Broken snapshot files are deleted. A damaged WAL is cut back to its valid beginning: the
   segment is cut at the first broken place, and all later segments are removed.
8. The log is checked against the snapshot. If it ends before the snapshot index or holds another
   term there, the seal is lowered to the snapshot index and the log is emptied from the back.
   Both sides are valid data in that case and the snapshot wins; it is the state after a crash in
   the middle of an install. A log that starts after index + 1 makes `Open` fail, unless the
   repair flag is set: then it is emptied as well.
9. The last index is sealed, always. That restores a slot that was invalid, and it takes in valid
   frames after the old seal, left by a crash between the sync of the log and the seal.
10. The core starts with term, vote and repair flag, and with a log view whose last index is
    `max(wal last, snapshot index)`.

A power loss at any disk access of these steps leaves a disk that the next start judges the same
way, or one that already carries the flag. `TestCrashAtEveryDiskAccess` cuts the power at every
disk access of a three-node run, including during `Open`, and `FuzzOpenDamaged` puts several broken
places on the disk at one such point. The verdicts on the WAL are the cases of
`TestOpenJudgesDamageByTheSeal`.

| Found on start | Verdict |
|---|---|
| broken frame at the end of the last segment, after the seal, both slots valid | torn write, cut off |
| the same, but a later valid frame has a sync mark that covers it | damage |
| broken frame up to the seal | damage |
| any broken end of the log when one slot was invalid | damage, the seal may be one round old |
| broken frame in an older segment, broken segment header, gap between segments, falling term, oversized segment | damage |
| seal above the loaded snapshot, and the log ends before the seal or starts later than the entry after the snapshot | damage |
| one slot of `meta` invalid | no damage by itself, the next write restores the slot |
| both slots invalid | `Open` fails |
| newest snapshot fails its checksum | the next older one is loaded and the file deleted; damage only if the log then leaves a gap up to the seal |
| broken snapshot when one slot was invalid | damage, the seal may be older than the snapshot |
| checksum of a snapshot holds, content cannot be read | not a disk fault, `Open` fails |
| checksum of a snapshot holds, its footer names another index or term than the file name | not a disk fault, `Open` fails with `wal.ErrMisnamed` |
| unknown version of a segment, unknown flag in `meta` | not damage, `Open` fails with `wal.ErrVersion` |
| read error (EIO) | fail-stop |

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
  writes nothing. So is a lookup whose answer could not fit into 1 MiB: more than 10 922 account
  IDs or 10 485 transfer IDs (`TestLookupOverTheReplyLimitIsMalformed`). One limit then holds for
  both directions on the client port. A full core (`ErrBusy`) answers `Busy`.
- A leader stamps writes only after it has applied an entry of its own term; before that a write
  gets `Busy`. A stamp outside the valid range (see Timestamps) gets `BadClock` and writes nothing.
- A write is proposed and remembered by its index. When the entry at that index is applied, the
  reply goes out. While a node stays leader, every entry after its own empty entry is one of its own
  proposals, so the term needs no check. When the node stops being leader, every waiting write gets
  `NotLeader`. The client asks again elsewhere, and its session makes sure nothing applies twice.
- While a write waits for its entry, the leader drops every further copy of it (same client,
  session, number and op) without an answer and without a new entry. The waiting proposal is
  answered when it is applied, or with `NotLeader` when the node stops being leader. If that
  answer gets lost, nothing waits any more, and the next copy goes the usual way: a new entry, and
  the session returns the stored reply. Without this rule a client with a short timeout turns
  every retry into a log entry. The check is one map lookup. (`TestWaitingRequestIsProposedOnce`)
- Reads are collected; each `Process` issues at most one `ReadIndex` for all of them. A read
  appears in `Ready.Reads` with `Commit` at or above its index, and that commit is applied before
  the reads of the same round, so a confirmed read is answered at once. A tag the node already
  failed with `NotLeader` is skipped. `ErrNotReady` answers `Busy`. Reads need no session and are
  not stored. Receipts, heads and links (see Audit trail) are reads of the same kind.
- Reads have a budget of 8 MiB. A new read is `Busy` if the largest answers of all reads still
  waiting would pass it: the records a lookup asks for, the size of a receipt, the links asked for.
  A confirmed round answers reads until 8 MiB of work is spent, a receipt counting 100 bytes for
  every leaf of its batch, and the rest get `Busy` (`TestBusyWhenTheWaitingReadsFillTheBudget`,
  `TestProofsOfOneRoundStayWithinTheBudget`). Without the budget a client that never reads its
  answers could make one round build hundreds of megabytes of them.

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

In the server the two come from different clocks. Ticks follow the monotonic clock (see Server),
the reading a leader stamps is the wall clock, `time.Now().UnixNano()`, and `ClockStep` is
`Tick + Tick/4`, as in the simulator. A jump of the wall clock is therefore bounded by ticks that
do not see it.

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
- Every batch the state machine accepts appends one link to the audit trail: empty batches and
  batches in which every event fails as well. A rejected batch, a register entry, a repeated
  request, `Stale` and `SessionUnknown` never reach the state machine and get no link.

### Exactly once

The session is replicated state, so every node agrees on which request numbers have applied. A
retry with the same number gets the stored reply instead of a second effect. The session number
closes the remaining gap: a register request that arrives late, after its session was evicted,
creates a new session with a new number, and requests still carrying the old number get
`SessionUnknown` instead of applying again. Transfer IDs stay as a second layer for clients that
restart and resubmit.

`MaxSessions` decides which session is evicted, and so whether a request runs. It is part of the
replicated state, although every node takes it from its own configuration. Two nodes with
different values would drift apart. The node snapshot therefore carries the value. A node refuses
to start from a snapshot written with another one (`replica.ErrMaxSessions`) and stops instead of
installing one; the refused install writes nothing, so the next start finds the directory as it
was (`TestRefusedInstallKeepsTheDirectory`). The peer hello carries the value as well, so a node
with another one never exchanges a message with the others
(`TestPeerWithAnotherMaxSessionsIsRefused`, with three processes
`TestNodeWithAnotherMaxSessionsStaysOut`).

### Snapshots

- Content: magic, version 2, applied index and term, apply hash, `MaxSessions`, sessions sorted by
  client ID, then the bytes of `state.WriteSnapshot`, which hold the links of the audit trail. The
  same state gives the same bytes; `Digest()` is SHA-256 over them.
- A node takes one after `SnapshotEvery` applied entries, at the end of `Process`, and blocks
  while it writes. `wal.SaveSnapshot` writes a temporary file, syncs it, renames it and keeps the
  two newest snapshots. The node then compacts the WAL before the previous snapshot, by whole
  segments, so every entry after the older of the two kept snapshots is still in the log.
- That does not hold right after a node installed a snapshot from the leader: the install empties
  the log, so it starts after the installed snapshot until the node has taken a snapshot itself.
  The same goes for a start that emptied the log (see Files and start).
- On start the node loads the newest snapshot whose checksum holds and deletes the broken files
  above it. Left in place, a broken file would count as one of the two newest at the next
  snapshot and push out the healthy older one. If the log reaches from the loaded snapshot to the
  seal, the node applies it again and stays a voter (`TestBrokenNewestSnapshotFallsBackToTheOlder`).
  If it does not, as right after an install, that is damage and the node repairs from the leader
  (`TestBrokenInstalledSnapshotIsDamage`). A crash while a snapshot is written leaves only a
  temporary file, and the previous snapshot stays the newest.
- Receiving: `Receive` keeps the payload of a `MsgSnap` by index and term. If the core asks to
  install it, the node checks the header against `Ready.Snapshot`, stores it with
  `wal.SaveSnapshot`, loads it back from disk, seals the snapshot index and only then empties the
  log from the back. A crash in between leaves a log prefix that the start check empties. Payloads
  the core did not ask for are dropped after `Process`.
- Sending: the driver reads the bytes for a `MsgSnap` through `ReadSnapshot`, which checks the
  checksum of the whole file first. If that snapshot is gone, the message is dropped and the
  leader tries again later. If the checksum fails, the node stops. The simulator hands the bytes
  over in memory; the server streams them on a connection of their own (see Server).

`Status()` reports ID, role, term, vote, leader, commit, applied, last index, apply hash, the
number of sessions, whether the node is in repair, the ledger time (`Time`, the time of the latest
batch), the deadline of the next pending transfer to expire (`NextExpiry`, 0 for none) and the head
of the audit trail (`Head`). `CheckInvariants()` runs the full state check, the chain included.
`Digest()` hashes the snapshot bytes, `LedgerDigest()` only the ledger state, which is what an
independent model can recompute.

## Audit trail (`internal/state`, `internal/audit`, `internal/notary`)

The state machine keeps a hash chain over the batches it accepts. What goes into a link, a leaf and
a head is under "Audit trail" in [ledger-rules.md](ledger-rules.md); why it is built this way, and
what a receipt proves, is in [ADR-011](adr/011-audit-trail.md). In short:

- Every accepted batch appends a link of 113 bytes: batch time, op, number of events, number of
  leaves, the hash of the request body, the hash of the reply and a Merkle root. The root covers
  the pending transfers that expired in this batch's sweep, then the records it stored.
- A head is a pair of link number and hash, `SHA-256(0x02 | previous head | number | link)`. The
  number counts links, not log entries: the state machine does not know the log index, and a dense
  number lets a verifier count missing links.
- Links are replicated state. They are in the snapshot of the state machine, in its digest and in
  the trace hashes of the simulator. In memory they sit in blocks of 256, 152 bytes for a link and
  its head; the snapshot holds the 113 bytes and folds the heads again on load.
- `CheckInvariants` recomputes every root from the records and every head from the links, on every
  load and in the simulator every 64 applied entries. The head fixes the state: every root follows
  from the records, every head from the links, every balance from the transfers. A snapshot whose
  records were changed and whose checksum was fixed therefore does not load, as long as its links
  are left alone. Whoever changes the links to match can load it. The check finds bugs and half
  done edits; against someone who knows the format only a head kept outside the node helps.

### Receipts

A receipt is built from the state, never from the log. The log is compacted, but no transfer is
ever deleted and an account record changes only in its balances, so a receipt for a transfer of a
year ago is built the same way as one for a transfer of a minute ago.

| Field | Bytes | Kinds |
|---|---|---|
| version | 1 | always `1` |
| kind | 1 | 0 batch, 1 account, 2 transfer, 3 expiry, 4 head |
| record | 96 or 100 | 1 to 3; an account as it was created, with all balances 0 |
| leaf index | 4 | 0 to 3; 0 for a batch |
| path length `p` | 1 | 0 to 3; at most 32; 0 for a batch |
| path | 32 · `p` | 0 to 3, from the leaf to the root |
| link number | 8 | 0 to 3, never 0 |
| previous head | 32 | 0 to 3, 32 zero bytes before link 1 |
| link | 113 | 0 to 3 |
| stamp | 0 or 104 | all |

A head on its own is `1 | 4 | number | hash`, 42 bytes before the stamp. A receipt for a transfer
has `260 + 32·p` bytes, 104 more with a stamp. `audit.Decode` accepts exactly what the encoder
writes: known version and kind, the record length of the kind, a leaf index below the number of
leaves, a path of exactly the length that index and that number need (RFC 9162, section 2.1.3.2),
and nothing after it but no stamp or one stamp. Every flipped bit of a receipt fails to decode or
leads to another head (`TestEveryFlippedBitIsRefused` in `internal/audit`).

Building a receipt finds the link by binary search and hashes all leaves of that batch again:
0.27 µs for a batch of one record, 30 µs for 128 and 1.95 ms for 8000 (`BenchmarkProve` in
`internal/state`, i5-12500H). Nothing bounds how many transfers one sweep expires, so a batch that
swept many of them makes every receipt of it expensive. The read budget (see Requests on the
leader) counts these leaves.

### Reads of the audit trail

Three ops go the way of lookups: only the leader answers, after ReadIndex, without a session, and
nothing of them reaches the log or the chain. `Reply.Time` is the ledger time, as for a lookup.

| Op | Request body | Reply body |
|---|---|---|
| `Prove` (6) | kind 1 to 3 and an ID (17 bytes), or kind 0 and a batch time (9 bytes) | a receipt, or empty |
| `Head` (7) | empty | the head as a receipt of kind 4 |
| `Links` (8) | `from u64`, `to u64`, `from <= to` | links `from+1` to `to`, at most 1024 (`client.MaxLinks`) |

Any other body is `Malformed`. A `Prove` after a confirmed transfer finds it, a `Head` after it has
at least the number of its link, and a deposed leader answers neither
(`TestProveGoesThroughReadIndex`).
A follower would hold true receipts as well, but it does not serve them: there is one read path,
and "transfer confirmed, receipt not found" cannot happen. Every node gives the same bytes for the
same request (`TestSameReceiptOnEveryNode`), apart from the stamp.

### Stamps

With a key, the server signs. For every `OK` reply to `Prove` or `Head` with a body, the loop
decodes the receipt after `Process` returned, takes the head it leads to and appends
`notary.Sign(key, cluster, head)` before it encodes the reply. The stamp is 104 bytes: cluster ID,
public key and an Ed25519 signature (RFC 8032, no prehash, no context) over the 67 bytes
`"dmn-ledger head v1\n" | cluster u64 | number u64 | hash`. It covers the head of the receipt's
own link, not the head of the ledger at the time of the request; the head of the ledger is a
receipt of its own (`TestStampIsOverTheHeadOfTheReceipt`). A receipt the server cannot decode stops
the node: it signs nothing it cannot check itself. `Links` are not stamped. They prove themselves
by connecting two heads the verifier already trusts, and links that do not connect them prove
nothing about the heads (`ConnectProofs` under SDK). Nothing signed is stored.

The key never reaches `replica`, `state` or the simulator (the `keyless` rule). `ledgerd` reads it
from `LEDGER_KEY_FILE`: 64 hex digits of the Ed25519 seed and a newline, in a regular file that
group and others cannot access. `ledgerctl key new` creates such a file with mode 0600 and never
overwrites one. Without a key a node serves receipts without a stamp; `notary.Verify` returns
`ErrUnsigned` for them, and `ledgerctl verify proof` takes them only with `-unsigned`. Otherwise
`Verify` decodes, checks the signature, then the cluster ID, then that the key is on the
verifier's list (`ErrSignature`, `ErrCluster`, `ErrUntrusted`). The status frame carries each
node's public key, and `ledgerctl status -keys` prints the list. That list is trust on first
contact: the client port is not authenticated (ADR-015).

### Recounting a data directory

A data directory holds snapshots and a log, but not the commit index. `replica.Recount` opens a
copy of it as a cluster of one node (ID 1, peers `[1]`), which elects itself and commits its whole
log, the start a node takes in operation, seal and cover check included. It applies everything,
compares the head at the number of a head saved earlier, and does it twice: from the newest
snapshot and, with the newest one removed from the copy, from the older one, if the log reaches
back to it. An entry changed between the two snapshots is read only by the second run.

| Result | Meaning |
|---|---|
| no error | both runs reach the saved head at its number |
| `replica.ErrBehind` | the directory ends before that number; no verdict |
| `replica.ErrOtherHead` | another head at that number, within what the snapshots cover |
| `replica.ErrOtherLog` | another head at a number only the log reaches: an entry was changed, or it was never committed, as in the log of a deposed leader |
| `wal.ErrCorrupt` | the directory is damaged; a cluster of one refuses to start on it |
| any other error | it does not open, for example a snapshot whose records and links disagree |

Only links up to the number of the saved head are compared; whatever follows is not checked.
`MaxSessions` must be the cluster's value. `ledgerctl verify dir` reads the directory into a
`disk.Sim` in memory, holds the directory's lock while it copies, so it refuses a directory a
running node holds, and never writes to the original. The tests are in
`internal/replica/recount_test.go`, among them `TestRecountMatchesTheLiveHead`,
`TestRecountFindsChangedLogEntry`, `TestRecountFindsChangedSnapshot` and
`TestRecountOfADeposedLeaderIsNotAnotherHead`.

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
- `Unreachable(node)` tells the session that the connection to a node ended. If a request is in
  flight to that node, the session moves to the next one and sends at once, or at the next tick if
  it already sent in this one, so a client whose nodes are all down tries once per tick
  (`TestUnreachableMovesOn`, `TestUnreachableOncePerTick`). Any other call changes nothing. The
  session knows no dial pause; the SDK keeps one.
- `Submit` copies the body, so the caller may reuse its buffer.
- `SessionUnknown` ends the session with `ErrEvicted`. The outcome of the request in flight is then
  unknown; the session never registers again on its own, because that would break exactly once.
- Client IDs are random and new for every session. Reusing one would let delayed requests of the old
  session apply in the new one.

## Wire format (`internal/wire`)

Everything that crosses a socket is a frame: a header of 24 bytes and a body. All integers are big
endian, like the records of the state machine and the bodies of log entries; the WAL keeps little
endian for its own frame headers. `wire` has no clock, no network and no goroutines. It works on
byte slices and an `io.Reader`, and the simulator sends every message through it (see Simulator).

| Offset | Bytes | Field | Rule |
|---|---|---|---|
| 0 | 4 | magic | `DMNL` |
| 4 | 1 | version | `2` |
| 5 | 1 | type | table below |
| 6 | 2 | reserved | `0` |
| 8 | 8 | cluster ID | the reader's own, never 0 |
| 16 | 4 | body length | at most the limit the reader names |
| 20 | 4 | CRC32C | over bytes 0 to 19 and the body |

The reader checks magic, version, reserved bytes, cluster ID and length against its limit, in this
order, and stops at the first failure. Only then does it read the body, then the checksum. Nothing
is allocated before the length is checked (`TestLengthIsCheckedBeforeAllocation`). A decoder
accepts exactly what an encoder writes, so a decoded frame encodes back to the same bytes.

| Type | Name | Direction | Body |
|---|---|---|---|
| `0x01` | Request | client to node | 33 bytes, then up to 1 MiB |
| `0x02` | Reply | node to client | 42 bytes, then up to 1 MiB |
| `0x03` | StatusRequest | client to node | empty |
| `0x04` | Status | node to client | exactly 182 |
| `0x10` | Hello | peer to peer, first frame in both directions | exactly 25 |
| `0x21` to `0x29` | raft message, `0x20 + raft.MsgType` | peer to peer | table below |
| `0x2a` | SnapshotChunk | peer to peer, only on a snapshot connection | 1 byte to 256 KiB |
| `0x2b` | SnapshotEnd | peer to peer, only on a snapshot connection | exactly 12 |

Request: client ID (16), session number (8), request number (8), op (1), then the records or IDs
as in [ledger-rules.md](ledger-rules.md), or the body of an audit read. This is the body of a log
entry of kind 2 without the timestamp; the leader sets the time. Reply: client ID (16), request
number (8), op (1), status (1), leader (8, a node ID or 0), time (8), then the session number for
`Register`, 8 bytes per failure for a write, the records of a lookup, a receipt, a head or links.
`wire` checks neither the op nor the status: whether a body fits its op is decided by the node,
which answers `Malformed`.

Status, 182 bytes: node ID (8), role (1, 0 to 3), flags (1, bit 0 is repair), term, known leader,
commit index, applied index, last index (8 each), apply hash (32), number of sessions (4), ledger
time, the node's wall clock when it answers, next expiry or 0 (8 each), head number (8), head hash
(32), public key (32, zeros for none). An unknown role or flag bit is a format error. The node
answers a status request after the `Process` of its round, and the frame is O(1) to build: it
holds the apply hash and the head, never a digest of the whole state.

Hello, 25 bytes: `from u64 | to u64 | MaxSessions u64 | kind u8`, kind 1 for the connection that
carries raft messages and kind 2 for a snapshot connection. Raft messages begin with
`from u64 | to u64 | term u64` (24 bytes), then:

| Type | Message | Further fields | Body |
|---|---|---|---|
| `0x21` | `MsgPreVote` | last index u64, last term u64 | 40 |
| `0x22` | `MsgPreVoteResp` | rejected u8 | 25 |
| `0x23` | `MsgVote` | last index u64, last term u64 | 40 |
| `0x24` | `MsgVoteResp` | rejected u8 | 25 |
| `0x25` | `MsgApp` | previous index u64, previous term u64, commit u64, read sequence u64, count u32, entries | 60 + entries |
| `0x26` | `MsgAppResp` | index u64, read sequence u64, rejected u8 | 41 |
| `0x27` | `MsgSnap` | snapshot index u64, snapshot term u64, commit u64, read sequence u64 | 56 |
| `0x28` | `MsgRepairProbe` | nonce u64 | 32 |
| `0x29` | `MsgRepairInfo` | leader's last index u64, nonce u64 | 40 |

An entry in `MsgApp` is `term u64 | kind u8 | length u32 | body`, 13 bytes and the body of the
log entry byte for byte. Its index is the previous index plus one plus its position, so a gap
cannot be encoded. In the Go type `Message.Seq` holds the read sequence for `MsgApp`, `MsgAppResp`
and `MsgSnap` and the nonce for the two repair messages; on the wire every type names its own
fields. Fields a type does not use are not sent and are 0 after decoding.

| Limit | Value | Constant |
|---|---|---|
| body on the client port | 42 bytes + 1 MiB | `MaxClientBody` |
| body on the peer port | 4 MiB | `MaxPeerBody` |
| body of an entry in `MsgApp` | 41 bytes + 1 MiB | `MaxEntryBody` |
| hello | 25 bytes | `HelloSize` |
| snapshot chunk | 256 KiB | `ChunkSize` |
| whole snapshot stream | 8 GiB | in `internal/server` |

4 MiB on the peer port is enough because the server's fixed values bound the largest `MsgApp`: at
most 1 MiB of bodies (`MaxAppendBytes`) or one entry over it, and at most `MaxUncommitted + 1`
unstored entries. `TestLargestAppendFitsTheFrame` computes this from the constants and encodes the
largest message. An encoder asked for more fails, which is a bug, and the node stops.

| Found | Error |
|---|---|
| stream ends before the header, without a byte | `io.EOF` |
| stream ends in the header or in the body | `io.ErrUnexpectedEOF` |
| wrong magic, version or cluster ID | `ErrMagic`, `ErrVersion`, `ErrCluster` |
| reserved bytes set | `ErrFormat` |
| length over the caller's limit | `ErrTooLarge`, nothing allocated |
| checksum does not match | `ErrChecksum` |
| body of a fixed type with another length, request or reply body over 1 MiB, a reject flag other than 0 or 1 | `ErrFormat` |
| `MsgApp`: count × 13 above the rest, an entry over `MaxEntryBody` or past the end, bytes after the last entry, an index that wraps | `ErrFormat`, before the entries are allocated |
| a type the called method does not decode | `ErrFormat` |

`wire` is syntax. Whether sender and recipient match, whether terms make sense, whether a request
fits its op stays with `raft.wellFormed`, the server and `replica`. Every error ends the
connection; there are no error frames. The version is in every frame and goes up with any change
to a layout. A node of another version never joins, a client of another version gets no answer.
`TestLayoutIsPinned` keeps one frame of every type as a hex line in `testdata/frames.golden`, and
`TestEveryFlippedBitIsRefused` flips every bit of them. Four fuzz targets cover the rest:
`FuzzReadFrame`, `FuzzBody`, `FuzzMessage` and `FuzzSnapshotStream`.

## Server (`internal/server`)

The server is a second driver of `replica.Node`, next to the simulator. It adds what the simulator
does not have: goroutines, the real clock, real sockets and bytes from strangers. It adds no
decision about safety. `cmd/ledgerd` hands it two TCP listeners, a dialer and `disk.OS`; the tests
hand it `net.Pipe` pairs inside a `testing/synctest` bubble.

### Goroutines and ownership

| Goroutine | Count | Owns |
|---|---|---|
| loop | 1 | the node, the table from client ID to connection, the sweeper, the spool files of the round |
| acceptors | 2 | one listener each |
| client reader | 1 per client connection | reading from it |
| client writer | 1 per client connection | writing to it |
| peer reader | at most 1 per peer | reading the incoming message connection |
| peer writer | 1 per peer | dialing, hello, writing the outgoing connection |
| snapshot sender | at most 1 per peer, short-lived | a snapshot connection, reads the snapshot file |
| snapshot receiver | at most 1 per peer, short-lived | a snapshot connection, one spool file |
| log writer | 1 | stderr |

Only the loop calls the node, `Status()` included. There is no lock around the node and none in
the loop; the rest talk to it through channels. Shared counters are the bytes queued per peer and
the marks "sending" and "receiving a snapshot" per peer, all atomic, and the incoming connection of
each peer behind a mutex the loop never takes.

### The round

```
wait      until an event arrives or the next tick is due
feed      every event queued at this moment: Receive, Request, status requests
tick      every tick that is due, at most 40
sweep     the sweeper may hand one request to its own node
Process   hard state, snapshot, append, fsync, seal, fsync, apply
send      raft messages to the peer writers, replies to the client writers, status frames
```

`Process` returns only after fsync and seal, and the round has one place that sends: after
`Process` returned without an error. Every raft message and every reply the server sends is
encoded from the `Output` of that call; it only appends a stamp to a receipt, and a snapshot
stream sends the synced file that a `MsgSnap` of the `Output` names. A hello promises nothing, and
a status frame is built after `Process` as well and only reads. If `Process` fails, nothing more
is sent and `Run` returns the error. `TestNothingLeavesBeforeItIsSynced` wraps the file system and
holds every frame that reaches a connection against what was synced; `TestFailedProcessSendsNothing`
makes the second fsync fail.

Grouping is what arrived during the last round: while the loop waits for its disk, the readers go
on queueing, and the next round takes the whole queue into one `Process`, one append, one fsync of
the log and one of the seal. There is no timer and no batch size.
`TestRequestsDuringASyncShareTheNext` holds an fsync, sends 50 requests meanwhile and counts two log
syncs for all 51.

### Ticks

Ticks come from the monotonic clock. A round owes `elapsed / Tick - given` ticks and gets all of
them, not one, so a round that takes longer than a tick loses none and the clock bound in `stamp`
keeps up with the clock (`TestSlowRoundsLoseNoTicks`). Without debt the loop waits on a
`time.Timer` until the next tick is due or an event arrives; a `time.Ticker` would hold one tick
and drop the rest. Queued events are fed before the ticks, and a round gives at most 40 ticks, the
election timeout of 50 ticks minus two heartbeats; the rest are dropped. After a long round, a slow
fsync for example, the heartbeats that arrived meanwhile are queued and reset the timeout before
the ticks run. After the whole process was frozen, by `SIGSTOP` or a paused VM, they may still be
in the socket when the loop wakes up; then the cap alone keeps a follower below the shortest
timeout, with room for the next heartbeat (`TestStallOwesLessThanAnElection`,
`TestFrozenFollowerDoesNotCampaign`). The dropped ticks are missing from the clock bound. A node
that stayed leader through such a stall looks afterwards like a clock that jumped forward, and
ledger time catches up at a quarter of real time.

Fixed values: `HeartbeatTicks` 5, `ElectionTicks` 50, `SnapshotTicks` 500, `MaxAppendBytes` 1 MiB,
`MaxUncommitted` twice `MaxClients`, `ClockStep` `Tick + Tick/4`. With the default tick of 10 ms a
heartbeat goes every 50 ms and the election timeout is 500 to 990 ms. Only the tick is a setting.

### Queues

| Queue | From, to | Size | When full |
|---|---|---|---|
| events | readers to the loop | 256 | the reader blocks; TCP slows the sender down |
| out, per peer | loop to peer writer | 64 frames and 8 MiB | the frame is dropped; raft sends again |
| out, per client connection | loop to client writer | 4 frames | the connection is closed |
| snapshot, per peer | loop to snapshot sender | 1 running | a further `MsgSnap` to that peer is dropped |
| fatal errors | any goroutine to the loop | 1 | the first one counts |

The loop never blocks on a queue. Dropping toward a peer is loss in the network to raft. Toward a
client the server closes instead: a dropped reply would leave the client waiting for its timeout,
while after a closed connection it asks again at once and its session returns the stored reply.

### Connections

- Clients: at most `MaxClients` connections, a further one is closed at once. The first frame has to
  arrive within 5 s; after that the reader waits for a header without a deadline and wants the
  body within 10 s of it. Only `Request` and `StatusRequest` are allowed. The first request binds
  the connection to its client ID, a request with another ID closes it. If the same ID shows up on
  a new connection, replies go to the new one, and a closed connection is struck from the table.
  Every write has a deadline of 5 s.
- Peers: two TCP connections between two nodes, one per direction; whoever wants to send dials.
  The writer dials when a frame waits, with a timeout of 1 s, sends its hello and waits up to 2 s
  for the answer; on any error it closes, drops the frame and waits 50 ms, doubling up to 1 s,
  before the next try. An acceptor reads the 25-byte hello within 2 s and checks the cluster ID,
  that `to` is its own ID, that `from` is another node of the cluster, the kind and `MaxSessions`.
  At most four connections per peer, counted for all peers together, may wait for their hello at
  once; more are closed. A new message connection from a peer replaces the old one. The reader
  takes frames up to 4 MiB without an idle deadline, wants the body within 10 s of the header, and
  closes on a message whose sender is not the peer of the hello, whose recipient is not this node,
  or that is a `MsgSnap`.
- `cmd/ledgerd` adds what needs addresses or socket options: the peer listener closes every
  connection from an IP that is not in `LEDGER_PEERS`, and peer connections get TCP keepalive (5 s
  idle, 1 s interval, 3 probes) and, on Linux, a `TCP_USER_TIMEOUT` of 5 s.

### Snapshots as a stream

A leader that has to send a snapshot starts a sender for that peer, unless one is running. The
sender reads the file through `wal.ReadSnapshot`, which checks the checksum of the whole file
first, then dials a connection of its own (hello of kind 2) and sends the `MsgSnap` frame, the file
without its footer in chunks of up to 256 KiB, each with a write deadline of 5 s, and a
`SnapshotEnd` with the length and the CRC32C of all chunk bytes. It checks the file against its
footer while it streams; a file that no longer matches stops the node, a snapshot that is gone
ends the sender quietly, and a broken connection ends only this transfer.

The receiver allows one snapshot connection per peer at a time. It writes the chunks to
`<dir>/recv/<peer>.tmp` without fsync, with a read deadline of 10 s per frame, and gives up above
8 GiB. Only after a `SnapshotEnd` that matches what arrived does it hand message and file to the
loop, which passes the file to `Node.Receive` and deletes it after the `Process` of that round,
whether the core wanted the snapshot or not. A broken stream deletes the file at once, and the
server clears `<dir>/recv/` when it opens.

The stream has its own connection so that heartbeats keep flowing while it runs: behind chunks in
the send buffer of the message connection they could arrive seconds late on a slow link, and the
follower would stop counting for check-quorum (R11). The spool file keeps the loop off the network:
`install` reads from the local disk, not from a socket. The price is that the snapshot is written
twice on the receiver. Each transfer holds 256 KiB of buffer on the receiving side and two on the
sending side (`TestLaggingNodeGetsTheSnapshotAsAStream`, `TestHeartbeatsContinueDuringASnapshot`).

### Sweeper

Pending transfers expire only at the start of a batch (ledger-rules, step 2). In a quiet cluster a
reserved amount would stay reserved until somebody writes. The sweeper of the leader sends an
empty batch instead, as an ordinary request through a session of its own, handed to its own node
in the loop, without a socket. It sweeps when there is a next deadline that is not later than its
wall clock, no sweep is in flight and at least `ElectionTicks` ticks have passed since the last
one; the pause keeps it from writing an entry per round while ledger time trails the clock after
a leader change. A node that does not lead does not sweep. An evicted sweeper starts a new
session; that is safe because an empty batch has no effect that could apply twice. Every sweep is
a batch and gets a link. `LEDGER_SWEEP=off` turns the sweeper off.

### Stopping and errors

`SIGTERM` and `SIGINT` end the context. The loop finishes its round, `Run` closes listeners and
connections, waits for its goroutines, closes the node, which releases the lock on the data
directory, and returns `nil`. There is no final fsync, because everything promised is synced
already, and no handing over of leadership. Every error from `Process` is final (fail-stop):
`ledgerd` logs it and exits with 1, and the next start goes through `replica.Open`. An error of a
connection ends only that connection. A second `ledgerd` on the same directory fails on the lock
(`TestSecondServerOnTheSameDirFails`).

### Memory in the worst case

With the defaults (128 client connections, three nodes, `MaxUncommitted` 256), if every
connection sends frames of the largest kind and nobody reads:

| Item | Computation | Total |
|---|---|---|
| events | 256 × up to 2 MiB: a stalled follower gets the same append again every heartbeat | 512 MiB |
| frames held by readers | 128 × 2 MiB (the read buffer and the copy handed on) + 2 × 4 MiB | 264 MiB |
| requests in the node | one round's requests, 256 × 1 MiB, next to the unstored entries made from them, 256 × 1 MiB | 512 MiB |
| answers of one round | the read budget of 8 MiB and one more answer | 9 MiB |
| out, clients | 128 × (4 queued + 1 being written) × 1 MiB | 640 MiB |
| out, peers | 2 × (8 MiB + one frame being written) | 20 MiB |
| snapshot streams | 256 KiB per side and transfer, two buffers on a sender | under 2 MiB |

The lines add up to about 2 GiB above the state. The number is computed from the code, not
measured, it grows with `LEDGER_MAX_CLIENTS`, and no single node is in every line at its maximum at
once. A peer that does not follow raft gets further: a 4 MiB frame of empty entries decodes into
about 15 MiB. Malicious nodes are outside the fault model. A byte limit across all queues would be
tighter and is not built.

### `ledgerd` and `ledgerctl`

`ledgerd` takes its configuration only from the environment and refuses any `LEDGER_` variable it
does not know, so a typo does not let a default apply in silence.

| Variable | Range | Default |
|---|---|---|
| `LEDGER_ID` | 1 to the number of peers | required |
| `LEDGER_CLUSTER` | 1 to 2^64-1, decimal or `0x` | required |
| `LEDGER_PEERS` | 1, 3 or 5 `IP:port` peer addresses, comma-separated; the position is the node ID | required |
| `LEDGER_CLIENT_ADDR` | `IP:port` | required |
| `LEDGER_DIR` | absolute path | required |
| `LEDGER_TICK` | 1ms to 1s | `10ms` |
| `LEDGER_SNAPSHOT_EVERY` | 1 to 2^32 entries | `100000` |
| `LEDGER_SEGMENT_SIZE` | 2 MiB to 64 MiB, in bytes | 64 MiB |
| `LEDGER_MAX_SESSIONS` | 1 to 1 000 000, the same on every node | `1024` |
| `LEDGER_MAX_CLIENTS` | 1 to 4096 | `128` |
| `LEDGER_SWEEP` | `on` or `off` | `on` |
| `LEDGER_KEY_FILE` | absolute path of a key file | none: receipts without a stamp |

Any other `LEDGER_` variable is a configuration error. A `ledgerd` built with `-tags pprof` also
takes `LEDGER_PROFILE`, the absolute path of a file it writes a CPU profile to until it stops;
the benchmarks use it.

Addresses are IP and port, never host names. A node listens for peers on its own entry in
`LEDGER_PEERS`. It exits with 0 after `SIGTERM` or `SIGINT`, 1 when the node halted (the reason is
in the log), 2 for a configuration error, a `LEDGER_MAX_SESSIONS` that does not fit the data
directory included, and 3 when it cannot start: directory locked, port taken, `replica.Open`
failed. It logs to stderr one line at start, a line of counters and one line at the end, a line of
counters on `SIGUSR1`, on changes of role, leader and repair flag, around every snapshot transfer,
and for refused connections at most once a second per reason; nothing per request.

`ledgerctl` is a program over the SDK. `LEDGER_CLUSTER` names the cluster and `LEDGER_NODES` the
client addresses of its nodes in the order of their IDs. `ledgerd` does not know `LEDGER_NODES`
and refuses to start while it is set. `ledgerctl -h` lists the commands: `status [-keys]`,
`account create|get`, `transfer create|post|void|get`, `key new|show`,
`proof account|transfer|expiry|batch`, `head` and `verify proof|chain|dir`. Exit codes: 0 for a
good answer, 1 when the cluster answered but the answer is bad (a failed event, a missing ID, a
node unreachable or in repair, nodes that differ, a receipt that does not hold, two heads with the
same number and different hashes, a file of links that does not connect two heads), 2 for a usage
error, 3 when there is no verdict (no answer within 10 s, an unknown outcome, an evicted session,
a receipt without a stamp, links from the cluster that do not connect two heads, a directory
behind the head or held by a running node).

## SDK (`ledger`)

`ledger` is the one public package. It puts a goroutine, sockets and time around `client.Session`,
as the server does around `replica.Node`; the rules for exactly once stay in the session core.
`Dial` draws a random client ID from `crypto/rand`, registers a session and returns once it
stands. `Config.Nodes` are the client addresses in the order of the node IDs, `Config.Timeout` is
how long one attempt waits on one node before the next one gets the request (1 s if 0). How long a
call waits in total is up to its context. A `Client` may be used from many goroutines.

| Situation | Returned | Meaning |
|---|---|---|
| reply `OK` | `Result`, `nil` | applied, exactly once |
| more records or IDs than fit into a request | `ErrTooLarge` | nothing sent; at most 10 922 accounts or 10 485 transfers, the same for lookups |
| context ends before the request left the client | `ctx.Err()` | not applied |
| context ends after a write was sent | an error that is both `ErrUncertain` and `ctx.Err()` | outcome unknown |
| session evicted while a write was out | `ErrEvicted` and `ErrUncertain` | outcome unknown, the client is finished |
| session evicted while the call still waited | `ErrEvicted` | not applied |
| any call after an eviction | `ErrEvicted` | |
| `Close` while a write was out | `ErrClosed` and `ErrUncertain` | outcome unknown |
| reply `Malformed`, the state machine refused the batch | `ErrRejected` | applied on no node |

- A leader change is invisible: the session sends the same request with the same number to the
  next node, and the session in the cluster lets it apply once.
- A request that was sent cannot be called back. When its context ends, the caller gets
  `ErrUncertain`, and the client keeps retrying in the background until an answer comes, because
  the number would stay open otherwise. Later calls wait behind it, each with its own context.
- After `ErrUncertain` the transfer ID is the second layer of ADR-006: send the same transfers with
  the same IDs again, and those that already applied come back as `Exists`.
- After `ErrEvicted` the client never registers again on its own. The caller dials a new one and
  decides what to send again.
- Lookups and audit reads change nothing and cannot end with an unknown outcome; when their
  context ends, they return `ctx.Err()`.

One request is in flight per client. Calls that arrive meanwhile queue, and when the way is free
the client takes as many consecutive writes of the same op from the head of the queue as fit into
one request and sends them as one batch. A call whose last transfer is `Linked` ends its batch, so
its open chain never runs into another caller's transfers. Lookups are not bundled. Every caller
gets its own failures, indexed from 0, `Result.Timestamp`, the time of its first record, and
`Result.Batch`, the time of the batch it ran in. The two differ for a call bundled behind others,
and `ProveBatch` takes `Result.Batch`. A bundled batch is one batch for the state machine: one
sweep at its start, consecutive timestamps. A pending transfer whose deadline falls between the
timestamps of two bundled calls still counts for the later one (ledger-rules, expired but still
reserved until the next batch).

There is at most one connection per node. The client dials only when the session wants to send to
that node, with a timeout of 1 s, and a node it failed to dial is not dialed again for 250 ms.
When a connection's reader ends, the client drops it and calls `Session.Unreachable` at once;
without that, every dead connection cost a full timeout. A reply with a foreign client ID, a
decoder error or any frame other than a reply closes the connection. The session ticks every
20 ms, and its retry interval is `Timeout` in ticks.

`Prove`, `ProveBatch` and `Head` return receipts as bytes. `VerifyProof` checks one against a
cluster ID and a list of public keys without a client; it returns `ErrUnsigned` for a receipt
without a stamp, with the path to the head checked, and `ErrBadProof` for every other failure.
`Links` asks for at most 1024 links per request and, when one fails, returns the links it already
has with the error. `Status` asks one node for its status frame, without a session.

`ConnectProofs` takes the `Proof` of two receipts or heads that `VerifyProof` accepted, the older
one first, and the links between them; it checks no stamp itself. Nobody vouches for links, so
only the first two results say something about the heads:

| Returned | What it shows |
|---|---|
| `nil` | the links lead from the older head to the newer one: the newer head continues the older one, whoever supplied the links |
| `ErrConflict` | both have the same number and different hashes: their signers vouch for two histories. The links are not read |
| `ErrBadLinks` | these links do not lead from the older head to the newer one, by their number or by their content. The links may be wrong, or the heads may belong to two histories; the second is proven only by a stamped receipt or head with the same number as one of the two and another hash |
| any other error | the arguments are wrong: the older head has the higher number |

`ledgerctl verify chain` prints `connected` for the first and `CONFLICT` for the second. For
`ErrBadLinks` it prints `invalid` and exits with 1 when the links came from a file given with
`-links`, a verdict on that file, and `no verdict` with 3 when it fetched them from the cluster.
For two heads with the same number it asks the cluster for nothing (`TestConnectProofs` in
`ledger`, `TestProofsAndTheirChecks` in `cmd/ledgerctl`).

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
  and `all` choose which kinds appear; `evict`, `leaders`, `split`, `rot` and `beyond` are
  described below.
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
- **Audit reads.** A quarter of the reads ask for a receipt (of a batch the client wrote, an
  account, a transfer or an expiry), one in eight for the head and one in sixteen for links. The
  simulator signs nothing: a stamp is not replicated state, and its tests need no cluster.
- **Wire.** Every message goes through `internal/wire` when it leaves: the `Clone` of each body in
  `sim/ledger.go` encodes it and decodes it again, a snapshot as chunks and an end frame. A codec
  error panics, and a panic is a finding. The cluster ID is a constant.
- **Disk corruption.** `rot` runs the `all` faults and gives a quarter of the windows to damage in
  data at rest: one flipped bit or, in a quarter of the cases, a whole sector of garbage, in one
  durable file of one node. Empty and temporary files are left out. At least half of the faults
  go to the last WAL segment and half of those to its last 200 bytes, so the newest batch is hit
  often. In half of the windows the node loses power first and stays down until the window ends,
  so its next start meets the damage. Otherwise it keeps running and finds the damage when it
  reads that place, or at the process restart that every such node gets when the quiet seconds
  begin. The plan damages one node at a time: until that node has started again and left repair,
  further corruption windows are skipped. The profile needs at least three nodes, and `cmd/sim`
  refuses `rot`, `beyond` and `evict` with `-nodes 1`. `world.go` knows nothing about a node's
  files: where a fault lands is decided by the hook `Config.Rot`, which the ledger adapter fills
  with `rotTarget` in `sim/ledger.go`.
- **Corruption of `meta`.** A third of the `rot` windows that take the node down damage `meta`
  instead: one byte in one of the two slots, after a process stop without a power loss. This
  happens only to a node that is running at that moment, because then both slots are valid and
  synced. After a power loss, or on a node that is already down, one slot can be torn, and a
  fault in the other one leaves no valid slot. Such a node never starts again. That is the limit
  named in [ADR-009](adr/009-disk-corruption-seal-and-repair.md), and `rot` stays inside the
  fault model.
- **Beyond the fault model.** `beyond` plans the same windows as `rot` and drops its limits.
  Several nodes can be damaged at once, `meta` is hit after a power loss and whether the node
  runs or not, and on every power loss each sector written since the last fsync turns into
  garbage with probability 0.3, together with bytes in it that were already synced. Nothing is
  claimed about progress: a node may end in repair or never start again. The checkers that watch
  a run are on: F1, S1 to S6, S10, S12 and L3. Of the checks at the end only S11 runs, with S13
  for the audit reads, and without its last requirement, because nodes may be down or behind and
  nothing can be compared with them. `beyond` therefore shows that nodes do not diverge, do not
  forget without knowing it and do not vote when they must not, and that no answer a client got
  is wrong.

## Checkers (`internal/check`)

`check.Run` runs one seed and reports the first violation with seed, virtual time, node and reason.
While the run goes on, after every event:

| Check | Property | Source |
|---|---|---|
| S1 | at most one leader per term | senders of `MsgApp` and `MsgSnap` |
| S2 | per node and term at most one candidate gets the vote, across restarts | granted `MsgVoteResp` |
| S3 | the apply hash at each index is the same on every node and never changes; every committed index holds the same term on every node; an entry sent twice under one index and term has the same body | `Status()`, `EntryTerm`, sent entries |
| S4 | a node's term never falls, across restarts; its commit never falls within a process | `Status()` |
| S5 | the disk keeps the node's word, or the node knows that it does not, see below | `disk.Sim.Clone`, `Status()` |
| S6 | state invariants, every 64 applied entries and at the end | `CheckInvariants()` |
| S10 | a node in repair sends nothing that a voter or a leader sends | messages, `Status()` after the step |
| S12 | the head of the audit trail at each applied index is the same on every node and never changes | `Status()` |
| F1 | a node stops only because of a disk error or of damage the plan put on its disk, see below | stop and start errors, `World.Rotted` |
| F2 | a node is in repair only if the plan damaged its disk | `Status()`, the clones of S5, `World.Rotted` |
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
| S11 | log replay: the committed log, rebuilt from the entries the nodes sent each other, runs through a session table and an oracle of the checker's own; calls, replies, ledger digest and sessions must agree with it, see below |
| S12 | the head of every node is the head the oracle computes, in the replay of S8 and in the log replay of S11; checked before the digests |
| S13 | every audit read that ended with `OK` shows the ledger at its time: a receipt decodes, carries the oracle's record and leads to the oracle's head at its number, an empty one stands for something the oracle does not have, heads and links are byte for byte the oracle's |
| L4 | no node is in repair |

S5 keeps a receipt book. For every node it notes the last vote with its term, and how far the
node has vouched for its log: the highest index it acknowledged or, as leader, the last entry it
sent, in the latest term in which it did either. A leader vouches for what it sends because the
driver stores and seals before it sends. The disk owes the book a term that is not lower, the
vote of that term, and a log up to the noted index or else the repair flag. A promise about the
log from an older term stands until a leader of a later term has sent the node entries or a
snapshot, because only such a message may shorten its log. The book is held against the disk at
four points:

1. After a round, at every 50th promise (a granted vote, a vote request or an acknowledgement),
   the node's disk is cloned, the clone loses power and a node is opened on it. The term and the
   vote of that message must be there, and the acknowledged index or the repair flag.
2. At every 20th successful sync the same clone is held against the whole book, in the middle of
   a round and while a node is still in `Open`. This finds writes in the wrong order within a
   round or within a start, which a check after the round cannot see.
3. At the first status of every new process, without a clone.
4. At the moment a node leaves repair.

A clone that does not open is a finding, except in `beyond` on a disk the plan damaged. What S5
does not see: points 1 and 2 are samples. The book compares where a log ends, not what it holds;
that is left to S3. And once a node has moved to a later term and a leader of a later term has
sent it entries or a snapshot, the book does not compare the end of its log until the node
acknowledges again, because it cannot tell which of the older entries the node was allowed to
drop.

F1 allows three reasons for a node to stop: an I/O error; a failed checksum while it runs, if the
plan damaged its disk; and, only in `beyond`, a start that fails on corruption on such a disk. A
start that fails on corruption in `rot` is a finding, and so is every other stop in every
profile, such as a safety error from the core. F1 knows that the plan damaged a disk, not where:
once a disk is damaged, a failed checksum from another cause would pass on that node.

F2 is checked on every running node after every event and on every clone that S5 opens. In the
profiles without corruption no node is ever in repair. A write order that only costs
availability, such as sealing before the log is synced or cutting the log before the seal is
lowered, is therefore a finding and not a silent loss of voters. F2 is off in `beyond`, where a
power loss can turn synced bytes into garbage. It does not see a repair without cause on a node
whose disk the plan damaged earlier in the same run.

S10 reads the status after a step: a node that is in repair then has sent no `MsgVote`, no
`MsgPreVote`, no granted vote or pre-vote, no `MsgApp` and no `MsgSnap` in that step. The step in
which a node leaves repair is not judged. L4 looks only at the end, when the faults have healed
for five virtual seconds, and says nothing about how long a repair took. It is off in `beyond`
with all other checks at the end except the replay of S11.

A panic in a seed is a finding like a violation: `check.Sweep` reports it with the seed, and
`cmd/sim` prints the command that replays it.

In `evict` a write that ends with `SessionUnknown` may or may not have been applied. S8 and S9
need to know which, so in `evict` both are skipped as soon as one call ends that way. S11 does not
need to know. It takes the term of every committed index from S3 and the body from the message
that carried the entry, never from a node's disk, and replays entries 1 to the applied index by
the rules under "Applying": a second, plain implementation that shares only types with `replica`.
The log then says for every request whether it ran, at which time and with which failures. S11
requires:

- every call names a session that the log opened for its client;
- every write that ended with `OK` is executed in the log, with the op and body the client sent
  and with the time and failures of the reply; a write with an unknown outcome that the log
  executes must have the client's op and body as well;
- the log executes nothing that no client sent;
- every read that ended with `OK` names a ledger time that the ledger has after some prefix of
  the log, and shows exactly the ledger after that prefix;
- no such read shows an older ledger time than a call that ended with `OK` before the read began,
  measured in the simulator's time;
- on every node the ledger digest and the sessions with their numbers are those of the replay.

A node that applies a request twice, applies it in the wrong session or evicts the wrong session
ends with another ledger, another session table or a reply the log does not give. The rule for
the age of a read follows from linearizability and needs only confirmed calls, so it applies in
`evict` too. What S11 does not check: two calls that overlap stay unordered, and a write with an
unknown outcome sets no lower bound for later reads, even where the log executed it. The full
order inside a window stays with S9, in the runs where S9 runs. A committed entry that no
message carried is a finding by itself.

S12 compares what came out of the state machine, where S3 compares what went in: a node that
applies the same entries differently shows another head at the first batch that differs, long
before the digests at the end. It compares nodes only at an index two of them have applied, so a
node that runs ahead alone and computes wrong is seen when a second one catches up, or at the end.
S13 sits at the two places that check lookups, the step of a read in S9's model and the reads of
S11. The oracle builds no paths of its own: for a leaf and a link there is exactly one valid path,
so "decodes, same record, same head" is the same as "same bytes". S13 sees only what clients
asked for, and neither checker sees a stamp. S6 runs the chain check of `CheckInvariants`, and
every snapshot that S5 or a restart loads runs it too.

S11 runs in every profile. In `beyond` it replays the log up to the highest index that any node
showed as committed and leaves out the comparison with the nodes. A call that never got an
answer is allowed there, but what the log executes must still be a request some client sent
(`TestBeyondChecksTheAnswers`). S11 counts the request numbers of a client itself, in the order
of its calls, so a call without an answer keeps its number. A cluster of one node sends no
entries and is left out. In `evict` that would leave no check of any answer, so `cmd/sim`
refuses `-nodes 1 -profile evict`.

S9's model is the oracle itself. A write steps only if its reply time is later than the model's
last time and its failures match what the oracle computes; a read steps only if it shows the
model's state and time. Writes therefore linearize in the order of their times, and two states
with the same set of linearized operations are equal, which keeps porcupine's cache effective.
The search is bounded by the input, not by a timeout: windows of at most 32 requests, a handful of
clients, few accounts.

S8, S9 and S13 run window by window through `check.Replayer`, which has three callers: the
simulator, the chaos test and the demo in `internal/demo`. A window holds calls that all ended
with `OK`, and no call was open when it began or ended. The SDK returns no time for a lookup, so
for the chaos test and the demo a read without a time is judged by its content alone
(`TestReplayerAcceptsReadsWithoutTime`); in the simulator every read keeps its time
(`TestSimulatorReadsNeedTheirTime`).

### Planted bugs

Built with `-tags simbug`, `simbug.Set` switches on one of twenty-two bugs. B1 to B3 wrap the
disk inside the checker. The other nineteen touch sixteen small functions, and only those live in
separate files. In `raft/rules.go` these are five safety decisions of the core (`upToDate` for
votes, `merge` for a conflicting tail, `followCommit` for the follower's commit bound,
`maybeCommit` for the commit rule, `confirmedSeq` for read confirmation) and three of repair
(`voter`, `repairGoal` for the index a leader names, `ack` for the round in which repair ends). In
`replica/rules.go` they are the write order `persist`, the order of a start `mend`, the session
check `fresh` and the entry time `entryTime`. In `state/rules.go` they are the four steps of the
chain: `leafOf` for a leaf, `answered` for the reply hash, `chained` for the next head and `lapse`
for the leaf of an expiry. Each file has a twin `rules_simbug.go` with the same functions plus the
switch, so a build without the tag contains no trace of the bugs and the code around them stays
free of switches. `ledgerd` built with the tag takes a bug from `LEDGER_BUG`, for the chaos test.
The twins are kept equal by hand and checked by pinned trace hashes: `TestTracesStayPinned` in
`internal/check/golden_test.go` runs eight seeds of each of the ten profiles through `check.Run`
and compares the 80 hashes with `internal/check/testdata/traces.golden`. The test has no build
tag, so it runs in the normal build and in the `simbug` build with no bug switched on, against the
same file. A twin that drifts from its original changes a hash. After a change in behaviour that is meant,
`go test ./internal/check -run TestTracesStayPinned -update` writes the file again.

The table gives the first failing seed counted from 0, measured with
`go run -tags simbug ./cmd/sim -bug Bn -seeds N -profile P`, and the checker that reports it.

| Bug | What breaks | Profile | First seed | Found by |
|---|---|---|---|---|
| B1 | fsync of `meta` does nothing | `all` | 0 | S5, the disk does not open after a power loss |
| B2 | fsync of the WAL does nothing | `all` | 0 | F2, a power loss leaves the node in repair |
| B3 | directory sync after a new segment does nothing | `all` | 0 | F2 |
| B4 | acknowledgements leave before the WAL sync | `all` | 0 | F2 |
| B5 | entries are stored before the hard state | `all` | 3 | S5, no start after a power loss in the middle of a round |
| B6 | commit by counting replicas for entries of older terms | `leaders` | 365 | S3 |
| B7 | votes without comparing logs | `all` | 3 | S7 |
| B8 | a follower keeps a conflicting tail | `all` | 0 | S3 |
| B9 | a follower commits up to its own log end | `all` | 53 | S3 |
| B10 | reads without confirmation by a majority | `split` | 275 | S9 |
| B11 | sessions do not recognise a repeated request | `all`, `evict` | 0, 0 | S8 in `all`, S13 in `evict` |
| B11 | the same | `beyond` | 0 | S13 |
| B12 | entries are applied with the replica's own clock | `all`, `beyond` | 0, 0 | S12, during the run |
| B13 | no seal | `rot` | 16 | S5, during a start |
| B14 | the seal is written before the WAL sync | `crash` | 0 | F2 |
| B15 | a node in repair votes and stands for election | `rot` | 11 | S10 |
| B16 | the goal of a repair is the leader's commit index | `rot` | 2987 | S5, at the end of the repair |
| B17 | repair ends in the same `Ready` as the entries | `rot` | 144 | S5, in the middle of a round |
| B18 | the repair flag is stored after the clean-up and the new seal | `rot` | 49 | S5, at a restart |
| B19 | a leaf is hashed without its prefix | `all`, `calm` | 0, 0 | S5, a snapshot fails its own chain check when it is loaded |
| B20 | the reply is left out of the link | `all`, `calm` | 0, 0 | S12 at the end, the head differs from the oracle's |
| B21 | a link is hashed without the head before it | `all`, `calm` | 0, 0 | S5, as B19 |
| B22 | the expiry of a pending transfer gets no leaf | `all`, `calm` | 0, 1 | S5, as B19 |

B2 to B4 are reported by F2 and not by S5. After a power loss frames are missing below the seal,
the next start treats that as damage and goes into repair, and the plan never touched that disk.
The seal turns a sync of the log that did nothing into detected damage.

`TestBugsAreFound` hunts every bug except B16 in its profile, in twenty-eight hunts for twenty-one
bugs, and checks that an expected checker reports it. B11 and B12 are also hunted in `beyond`,
where only the log replay and S12 can see a wrong answer, and B19 to B22 in `all` and in `calm`,
the profile without faults. Counted from five starting seeds (0, 100 000, 200 000, 300 000,
400 000), the first finds are 3 to 21 seeds for B5, 0 to 171 for B9, 72 to 368 for B17, 250 to
2363 for B6 in `leaders` and 144 to 1027 for B10 in `split`. The test gives B5 1000 seeds, B9 1500,
B17 4000, B6 6000 and B10 8000, so that a change that only reshuffles the random draws should not
turn it red. The checker named in the table is the one at the first find from seed 0; from other
seeds another allowed one can come first, F1 for B5 or B6, for example, or S13 for B10.

B19, B21 and B22 are not found by the oracle but earlier, by the node itself: its state fails the
chain check when the checker of S5 loads a snapshot of it after a simulated power loss. B20 leaves
nothing in the state to check against, because the reply is not part of it; S12 finds it at the
end, against the oracle. B12 shows in S12 after four applied entries, in `all` and in `beyond`.

B16 is the one bug the simulator is slow to find. It needs an acknowledgement that is on its way
while the node is damaged, restarts and gets its probe answered, and a leader that has not
committed without that acknowledgement. From seed 0 the first find in `rot` is seed 2987.
`scripts/nightly-sim.sh` searches 40 000 new seeds for it in every run. What catches B16 at once
are the rule test `TestRepairProbe` and the random schedules of the core
(`TestRuleTestsCatchTheCoreBugs`).

B6 is found in `leaders`; in 10 000 seeds of `all` or `crash` it does not show. Losing a commit
needs Figure 8 of the Raft paper: an old entry reaches a majority by itself, the leader counts it
and dies before its own empty entry is acknowledged, and a node with a later entry at the same
index wins next. What makes this visible is that a leader crash after a round is a separate event
after the checkers have looked at that round; a crash in the same step would hide the commit from
them. The `leaders` profile also sends one entry per append message, because Figure 8 needs an old
entry to reach a majority on its own before the leader's own entry does.

B10 needs a cut-off leader that still answers reads after a client has finished a write
elsewhere. Check-quorum leaves only a short window for that. `split` keeps clients on both sides of
a partition; in `all` the first find is seed 2311.

The planted bugs show that the checkers notice these mistakes, not that the simulator finds every
mistake in the core. Three further mutations of the core, each built by hand into a copy of the
tree, pass it. A leader that keeps `match` from an earlier term and a leader that lets any answer
in its term confirm all waiting reads each passed 20 000 seeds of `rot` and 20 000 of `leaders`;
the rule tests `TestReelectedLeaderResetsProgress` and `TestReadConfirmedByQuorum` catch them. A
node in repair that takes an answer with a foreign nonce passed 4 000 seeds of `rot` and 10 000
of `beyond`. The tests of the core catch it: the rule test `TestRepairIgnoresOtherNonce`,
`TestRandomSchedules` in seed 98 with one damaged node of five, and the corpus of
`FuzzSchedules`. That corpus holds three seeds (16719, 16011, 6504) which between them catch
this mutation and two neighbours, a nonce that is the same on every start and a probe answered
by a node that does not lead, in every `go test`, with `-short` as well.

Two rules of repair have no known schedule that breaks without them: that the acknowledged index
is forgotten on every term change (R24) and that a goal counts only in its term (R23). Without
either rule the simulator passes the same 4 000 seeds of `rot` and 10 000 of `beyond`. In copies
of the tree, each mutation also passed 3.2 million random schedules of the core, and both
together another 3.2 million, with three and five nodes, with one and with any number of damaged
nodes, half of the runs with the leader cut off whenever a node is in repair. The reason is
probably this: a committed entry that the node once acknowledged is in the log of every later
leader, so it lies below every goal named after the restart, and what the node acknowledged to
an earlier leader can only be cut back above that entry. The rules stay because they keep the
argument short: a node is healed when its log agrees with the log of this leader up to this
leader's goal. `TestRepairGoalIsBoundToTheTerm` and `TestRepairCountsOnlyAcksOfTheTerm` pin
them.

The simulator is also weak on the order of steps in a start. Seven mutations of `Open`, of an
install and of the clean-up after damage, each built the same way, passed 3 000 seeds of `rot`
and 2 000 each of `crash`, `all` and `beyond`: later segments that stay on disk when a damaged
log is cut back, broken snapshots that are not reported, broken snapshot files that are not
deleted, a log emptied before the snapshot index is sealed, a log emptied on start without
lowering the seal first, a cover check that ignores where the log begins, and a gap after the
snapshot that is tolerated only while the seal is above the snapshot. The simulator seldom
crashes a node in the middle of an install or of that clean-up, and `rot` never puts two faults
on one node. The crash tests do both at every disk access. `TestCrashAtEveryDiskAccess`, the
tests in `internal/replica/open_test.go` and the crash tests in `internal/wal` catch all seven.

The seed counts of these hand-built mutations were measured before the audit trail changed the
trace of every seed, and they have not been run again since. The table of planted bugs above is
measured on the current code.

`go run ./cmd/sim -seeds 10000 -profile all` runs seeds on all cores and stops at the first failure
with the command that replays it, for example
`go run -tags simbug ./cmd/sim -bug B12 -seed 0 -profile all -trace`. The built program exits
with 0 when every seed passed, 1 for a finding, 2 for a usage error and 3 when a run failed
without a finding, for example because the trace could not be written; `go run` turns every
non-zero code into 1.

## Chaos test (`internal/chaos`)

The simulator runs the node code, not the program. The chaos test runs the program: three
`ledgerd` processes, built once by the test, each with a data directory of its own under
`$TMPDIR`. It needs Linux and the build tag `chaos`, and it does not run in CI.

```
test process
  ├─ 3 ledgerd processes, a key file each
  ├─ 6 proxies from node to peer, one per direction   (internal/faultnet)
  ├─ 3 proxies from the clients to a node              (internal/faultnet)
  ├─ 4 clients, each a ledger.Client, load from internal/workload
  └─ check.Replayer, window by window
```

- Every node gets the addresses of the proxies for its peers in `LEDGER_PEERS` and its own real
  address, so every direction can be cut on its own. A proxy reads frame headers and the kind of
  a peer hello and nothing else, so it can delay, drop, swallow, stall or corrupt whole frames
  without tearing the stream. It never drops frames on a snapshot connection, and it holds at
  most 4 MiB per connection.
- The nodes run with `LEDGER_SWEEP=off`, `LEDGER_SNAPSHOT_EVERY=500`,
  `LEDGER_SEGMENT_SIZE` of 2 MiB and `LEDGER_MAX_SESSIONS=64`, so snapshots, compaction and the
  snapshot stream come up again and again. The sweeper is off because its empty batches would
  not be in the history; the load sends empty batches itself.
- Each client calls one at a time, so the SDK does not bundle and every call is one batch, as in
  the model. Windows close as in the simulator, after 32 started calls.
- The processes and their proxies are `internal/local` (`Start`, `Node.Kill`, `Stop`, `Halt`,
  `Signal`, `Link`, `Statuses`), and the windows are `internal/history`: a `Recorder` that admits
  calls, holds the load, seals windows and hands them to the replayer in a goroutine of its own,
  and `Issue`, which turns an SDK call into the reply the replayer expects. Both are packages of
  their own, not test files of this one, because the demo starts and judges its cluster with the
  same code.

A seed (`-chaos.seed`, by default from the clock, and printed) plans the run in rounds of 30
seconds: a storm of 20 seconds with up to six windows, then 10 quiet seconds.

| Fault | What happens |
|---|---|
| crash | `SIGKILL` to a node or to the leader, restart after 0.2 to 3 s |
| freeze | `SIGSTOP` to a node or to the leader, `SIGCONT` when the window ends |
| cut | a node cut off in both directions, in one, or from the clients |
| blackhole | links that accept and read but deliver nothing |
| stall | a link that stops reading, so buffers fill up |
| noise | delay of 5 to 300 ms, loss up to 30 %, flipped bits up to 1 % on all links |
| away | in a quarter of the rounds a node is gone before the storm until the leader has compacted past it, so it needs the snapshot stream to come back |

The plan keeps at most one node down or frozen at a time; cuts may come on top, and then the
cluster stands still, which is allowed. What the test requires:

- every closed window passes `Replayer.Window` (S8, S9, S13) while the run goes on;
- every call returns `nil`; an error, `ErrEvicted` included, is a finding;
- no reply carries a time later than the wall clock at the end of its call plus the number of
  records confirmed so far: all processes share one clock, and every record moves ledger time by
  at least a nanosecond;
- a `ledgerd` that ends without the test ending it is a finding (F1), with its exit and the end of
  its log;
- in every quiet phase: every open call answered within 15 seconds (L1), one leader and one commit
  index (L2), the same applied index and apply hash on every node (S7), and no node in repair, at
  no point of the run (L4);
- at the end: lookups of every account and transfer the load knows, as ordinary calls in the last
  window, so S9 compares the whole visible state with the oracle; per ledger the debits and
  credits of those accounts balance; every node shows the same head and its own key; the stamped
  head equals the head in the status; receipts for up to 16 transfers and 4 accounts verify
  against the node keys and connect to that head over the links.

On a finding the test stops every node with `SIGSTOP` and copies the plan, `history.bin` with
every call, the node logs and the data directories to a directory `chaos-<seed>-<time>` under
`$TMPDIR`. `go test -tags chaos ./internal/chaos -run TestReplayHistory -chaos.history <file>`
judges a saved history again without a cluster. The plan is deterministic, the interleaving of
real processes is not: a finding is a lead for a test with fake time in `internal/server`, a
scenario in `replica` or a profile in the simulator, and that step is manual.

`TestChaosFindsPlantedBugs` runs the cluster with `ledgerd` built with `-tags simbug` and
`LEDGER_BUG` set. B11 must show as S8 within two minutes; B10 and B12 are searched for five
minutes each, and a find is welcome but not required. `scripts/nightly-chaos.sh` runs three seeds
of 30 minutes and the hunt.

What the test cannot do:

- Power loss. `kill -9` leaves the page cache in place, so a missing fsync or a seal written too
  early goes unseen. Only the simulator finds those.
- Disk faults. They stay with the profiles `rot` and `beyond` of the simulator.
- Lost packets under TCP. The proxy acknowledges everything it reads on the TCP level, so the path
  through `TCP_USER_TIMEOUT` is never taken.
- Clock jumps. All processes share one clock.

## Demo (`internal/demo`, `cmd/demo`)

The demo is one program that runs a cluster and shows it on a page: three `ledgerd` as its
children, a small load, a verdict on every answer, four faults a visitor can trigger, and a
receipt checked in the browser. Why it is built this way, what a stranger can do to it and what it
does not show is in [ADR-014](adr/014-public-demo-fixed-faults-no-docker-socket.md). It needs
Linux. The page and the lines the controller writes into its journal are German; the lines of the
nodes stay as the nodes wrote them.

```
browser ── HTTP ── cmd/demo  (one process)
                     ├─ demo.Web      page, /events, POST /act, GET /proof, static files
                     ├─ demo.Stage    set up, load, counts, faults, findings, new cluster
                     ├─ internal/history   windows ─→ check.Replayer (oracle, porcupine)
                     └─ internal/local
                          ├─ 3 ledgerd as child processes on 127.0.0.1
                          ├─ 6 faultnet links from node to peer, one per direction
                          └─ <DEMO_DIR>/run/n1..n3, n1.key..n3.key
```

- **`Stage` owns the cluster.** Only its loop starts, ends and signals nodes, sets links, flips
  bits and replaces the cluster. Nothing in that list runs at the same time as anything else in
  it.
- **`Web` asks and never acts.** It sees the stage through the interface `Show`: `Act` puts a wish
  into a queue of length one or refuses, `State` and `Lines` return copies, `Proof` fetches a
  receipt, `Watching` tells the stage how many streams are open, `Alive` is what the watchdog
  asks. The stage knows nothing of `Web` and never calls anything that could wait for a reader.
  The tests of the web half run against a fake `Show`, those of the stage against real processes.
- **The page is drawn from one value.** Loop, load, the look at the nodes and the checker write
  into one `State` behind a mutex. The rendered page, the first event of a stream and every tick
  come from it. `State.Rev` goes up with every change and is not part of the JSON.
- **Two ways to the nodes.** The load, the counts and the receipts go through the SDK to the
  client ports, without a proxy. The nodes talk to each other through the six links, so a node
  can be cut off. The look at the status goes to each client port directly, past a cut: the page
  shows what a cut-off node thinks of itself.

### A cluster from start to end

`Stage.Run` sets up a cluster, serves it until it has to be replaced, and starts over. Setting
up:

1. Delete `<DEMO_DIR>/run` and create it empty.
2. Draw a cluster ID of 64 bits and three Ed25519 keys from `crypto/rand`. The ID separates this
   cluster from every earlier one; the keys exist in this process and in three files.
3. `local.Start`: six ports for the nodes on `127.0.0.1`, searched upwards from a random point
   between 20000 and 29999 and always below 32768, where Linux by default begins to hand out
   ports for outgoing connections, so a node that was killed finds its port free. Every node
   gets its environment listed (`LEDGER_ID`, `LEDGER_CLUSTER`, `LEDGER_PEERS` with the link
   addresses of its peers, `LEDGER_CLIENT_ADDR`, `LEDGER_DIR`, `LEDGER_KEY_FILE`,
   `LEDGER_MAX_CLIENTS=16`, `GOMEMLIMIT=64MiB`) and nothing from the environment of the
   controller, no arguments, and `Pdeathsig: SIGKILL`, so the kernel ends it when the controller
   dies. Everything else stays at the defaults of `ledgerd`.
4. Wait up to 10 s until all three answer and agree on leader and term.
5. Dial five clients: four for the load, one, the teller, for counts and receipts.
6. Through the recorder: create seven accounts in one call, accounts 1 to 6 with
   `DebitsMustNotExceedCredits` and account 9 without a rule, then six transfers of 1 000 000
   from account 9 to each of the six. Amounts are hundredths. The six accounts together hold
   6 000 000 from then on, and that sum is what the page shows.
7. A full count (below). After it the phase is `steady`, the load starts and faults are accepted.

A setup that fails is torn down and tried again after 1, 2, 4 and 8 s. After the fifth failure
`Run` returns the error and the program exits with 1. Whatever goes wrong before the first count
has passed is a failed attempt with its reason on the page, not a finding.

A cluster is replaced for three reasons: the start, a finding, and the limit of 20 000 confirmed
transfers. At the limit the load stops for good, a last full count judges the whole cluster, the
recorder is closed and the checker gets up to 30 s for the windows that are left; a window it has
not judged by then is a finding `replay`. Then the clients and the cluster are closed and the next
one is set up with a new ID and new keys.
`State.Cluster` names the reason (`start`, `limit`, `finding`), and `State.Totals` runs on across
clusters.

### Load and books

Four goroutines, each with its own client and one call at a time. A call is `CreateTransfers`
with one to four transfers, so a batch has a Merkle tree and a receipt a path. Debit and credit
account are different and uniform from 1 to 6, the amount is uniform from 100 to 25 000, the IDs
count up from 1. Between two calls of one client lie 0.75 to 1.25 s. A transfer that would
overdraw its account is refused by the cluster with `exceeds_credits`; that happens by itself
when an account runs low, and the reply with its failures goes to the checker like any other. An
error from a call, other than at the end of the cluster, is a finding.

Every call goes through a `history.Recorder` with windows of 16 started calls. A closed window
goes to `Replayer.Window` in a goroutine of its own; when four windows wait, the load waits. The
state says how many windows and calls are judged and up to which transfer that reaches, so the
page states its verdict with its extent.

The controller keeps, per cluster, what it sent and whether each transfer was confirmed or
refused. Those are tallies over replies, not a model: what the accounts must hold only the oracle
in the checker knows. A count holds the load (`Recorder.Hold`, open calls get 15 s), asks through
the teller and pushes its lookups into the history as a window of their own, so the checker
compares them with the oracle as well.

| Count | When | What it asks |
|---|---|---|
| light | every 5 s while the load runs and no fault does | `LookupAccounts` for the seven accounts |
| full | after the setup, after every fault, every 60 s while the load runs, at the limit, when the load goes to rest | first agreement of the nodes, then the accounts, then `LookupTransfers` for every ID the load ever sent, 10 485 IDs per call |

Agreement before a full count, polled every 50 ms for up to 10 s: all three nodes answer, exactly
one leads, all have the same commit index, have applied it, and show the same head; none is in
repair; every node shows the key it was started with. From the answers the controller requires:

- the six accounts sum up to 6 000 000, account 9 has 6 000 000 posted as debits, debits and
  credits over all seven are equal, and nothing is pending;
- in a full count, every confirmed transfer is found exactly once, and nothing is found that was
  refused or never sent.

**Rest.** `Web` reports the number of open streams four times a second. When none has been open
for 30 s the stage makes a full count and the load waits; light and periodic counts stop, the
nodes run on. With the first stream the load continues after its current pause.

### The look at the nodes and the journal

Every 100 ms the stage asks all three nodes for their status, with 300 ms for the answers, and
once a second it reads the size of each node's newest WAL segment. A node the controller has
frozen itself is asked once a second only, so status connections do not pile up at a process
that cannot accept them, and an answer to a question asked before the freeze is dropped.
`State.Nodes` holds per node what the status frame says (role, term, leader, commit, applied and
last index, the first eight bytes of apply hash and head, head number, repair, key) and what only
the controller knows: the PID, since when the process runs, whether the controller has it down or
frozen, and a note on the last fault that hit it.
`State.Links` holds the six links and which of them the controller has cut.

The stderr of every node is read line by line. A line, cut to 400 bytes, goes into a ring of 200
lines per node and into the journal; the counter line a node writes at its end is left out, and
beyond 20 lines per node and second the rest is counted instead of shown. The journal is a ring of
200 lines, each with a running number, the controller's time, a source (`demo`, `n1` to `n3`,
`check`) and the text. The controller writes into it what it does at the moment it does it:
signal and PID, file, byte and bit, the links, every count with its numbers, every finding.

### The four faults

`Stage.Act(kind, target, from)` decides under the stage's lock and touches no node. It refuses
with the first that fails:

| Condition | Otherwise |
|---|---|
| a cluster is set up and no finding stands (phase `steady` or `action`) | `ErrRecovering` |
| no fault is running, its count included | `ErrBusy` |
| 10 s since the previous fault was accepted and 4 s since it ended | `ErrCooling` with the time |
| no fault accepted from `from` in the last 30 s | `ErrLimited` with the time |
| the last look is at most 300 ms old and shows three answering nodes, one leader, none in repair | `ErrRecovering` |
| for `flip`: the newest segment of the target holds at least 8 KiB | `ErrYoung` |

Only then are address and time noted and the wish queued. `from` is the visitor's address as a
prefix, /32 for IPv4 and /64 for IPv6; an entry is dropped 30 s after its fault. The target is a
node or 0 for the leader, which the loop resolves from fresh statuses when the fault runs.

The loop then runs `aim` (resolve the target, note role and term), `strike` (trigger, and return
the function that undoes it), a hold, the undo as a deferred call, and a full count:

| Kind | `strike` | Hold | Undo |
|---|---|---|---|
| `kill` | `Node.Kill` | 2 s | `Node.Start` |
| `freeze` | `SIGSTOP` | 5 s | `SIGCONT` |
| `cut` | `Fault{Cut: true}` on the four routes of `Cluster.Around(node)` | 6 s | `Fault{}` on the same |
| `flip` | `Node.Kill`, then one bit in the newest segment | none | `Node.Start`, then up to 10 s for the repair |

The load runs on through strike, hold and undo; it is held only for the count. The count has the
deadlines from above (15 s for open calls, 10 s for agreement, 5 s for the lookups); when it
passes, the action ends as `done` and `Totals.Healed` goes up.

For a `flip` the node has to be down, because a running node does not read data at rest again
(ADR-009). The controller takes the newest segment under `<node>/wal`, reads its size and picks
an offset in `[size/4, size/2)` and a bit. In a file of at least 8 KiB that range holds frames
the node acknowledged long before it was killed, so the bit counts as damage and not as a torn
write at the end. The controller reads the byte, flips the bit, writes and syncs, and puts file,
size, offset, bit and both byte values into the state. After the start it reads the node's own
lines: `started: … repair yes`, then `repair flag cleared`. The time the node spent as a
non-voter is the difference between the stamps of those two lines, not a reading of the
controller's clock. A start with `repair no` would mean that the bit fell into a write the node
had not acknowledged and cut off as a torn write; the journal and the page each have a sentence
for that case. Neither line within 10 s is a finding.

Once a finding stands, an undo opens links but neither starts nor continues a node: all three
stay stopped for the evidence.

### Findings

A finding is whatever ends a run of the chaos test, with the same names where they exist:

| Check | What happened |
|---|---|
| `S8`, `S9`, `S13` | a violation from `Replayer.Window`, with its wording |
| `replay` | the checker ended with another error, or left windows unjudged 30 s after the load stopped for good |
| `call` | a call of the load or of a count returned an error |
| `L1` | calls still open 15 s after the load was held |
| `L2` | the nodes do not agree 10 s after the load was held |
| `S7` | two nodes at the same applied index with different apply hashes |
| `L4` | a node in repair, or a log line about a repair, without a `flip` on that node |
| `key` | a node shows another key than the one it was started with |
| `books` | sums or the set of transfers are not what was confirmed |
| `F1` | a node ended without the controller ending it; with its exit and the end of its log |
| `fault` | a fault did not end as planned: a node did not start, no bit could be flipped, no word on the repair |
| `proof` | a receipt did not verify, or names another transfer than the one sent |

On a finding the state gets it with time, check and wording (cut to 300 bytes), all three nodes
are stopped with `SIGSTOP` and locked, and the stage copies the evidence to
`<DEMO_DIR>/befund-<unix time>-<number>`: the three data directories, `n1.log` to `n3.log` with
the last 200 lines of each node, `journal.txt`, `state.json` and `calls.json` with the windows
not judged yet and the calls of the open one. At most three such folders are kept; a fourth
finding is counted and shown, not copied. The cluster stands for 30 s with the finding first on
the page and no fault accepted, then a new one is set up. `Totals.Findings` keeps the last 20 and
`Totals.Found` the number, until the process ends.

A watchdog covers the case that the loop itself hangs: `Alive` is false when the loop has not
reported for 60 s or the look at the nodes for 10 s, `/healthz` then answers 503, and `cmd/demo`,
which asks once a second, ends with 1.

### HTTP

`net/http` with one `ServeMux` and these routes; anything else is 404, a wrong method 405.

| Route | Answer | Cost |
|---|---|---|
| `GET /` | the page, rendered from the state with `html/template` | one rendering a second however many ask; the same bytes in between |
| `GET /events` | the event stream; 429 when the address has 4 open, 503 when 200 are open | a seat in the hub, limited |
| `POST /act/{kind}/{node}` | 202 with the time until which this address is limited, or a refusal | a check in memory |
| `GET /proof` | a receipt with the keys, as JSON | one `Prove` a second at the cluster however many ask |
| `GET /static/{name}` | an embedded file from `web/` | none; `ETag` from the content, `Cache-Control: no-cache` |
| `GET /healthz` | `ok`, or 503 | none |
| `GET /robots.txt` | disallows everything | none |

Refusals of `POST /act` are JSON, `{"error": …, "until": <ms>}`; the sentence for each is in the
page, not in the server:

| Case | Status | `error` |
|---|---|---|
| the request has a body, on any route | 400, `Connection: close` | `body` |
| `Origin` is missing, sent twice or not `DEMO_ORIGIN`; or `Sec-Fetch-Site` is sent and not `same-origin` | 403 | `origin` |
| unknown kind or target | 404 | |
| `ErrBusy`, `ErrYoung` | 409 | `busy`, `young` |
| `ErrCooling`, `ErrLimited` | 429 with `Retry-After` | `cooling`, `limited`, with `until` |
| `ErrRecovering` | 503 with `Retry-After: 2` | `recovering` |

Rules of the server:

- **No body is read.** Kind and target are in the path and are looked up in fixed tables (`kill`,
  `freeze`, `cut`, `flip`; `1`, `2`, `3`, `leader`). There is no decoder for input and no query
  parameter with a meaning.
- **Origin.** `POST /act` as in the table. `GET /events` and `GET /proof` answer a request without
  `Origin` and `Sec-Fetch-Site`, and refuse with 403 an `Origin` that is not `DEMO_ORIGIN` and a
  `Sec-Fetch-Site` other than `same-origin` or `none`. No answer carries a CORS header.
- **The visitor's address.** If the connection does not come from a network in `DEMO_PROXIES`, its
  address counts and `X-Forwarded-For` is not looked at. If it does, the rightmost entry of all
  `X-Forwarded-For` lines that is not in `DEMO_PROXIES` counts; an entry on the way from the right
  that is not an IP address ends the search, and the address of the connection counts. The key
  for every limit is the IPv4 address, unwrapped if it came inside IPv6, or the IPv6 /64. It is
  in no state, no journal, no log and no file.
- **Deadlines and sizes.** 5 s for the request header, 10 s for the request, 60 s idle, 8 KiB
  `MaxHeaderBytes`, at most 512 connections accepted at a time. There is no `WriteTimeout`,
  because it would cut the stream; every handler sets a write deadline of 10 s, the stream 5 s
  before each write.
- **Headers on every answer of the handler:** a `Content-Security-Policy` that allows scripts,
  styles, images, fonts and connections from the own origin only, nothing inline, no base URI, no
  form target and no frame; `X-Content-Type-Options: nosniff`; `Referrer-Policy: no-referrer`;
  `Cross-Origin-Opener-Policy` and `Cross-Origin-Resource-Policy: same-origin`;
  `X-Robots-Tag: noindex`; a `Permissions-Policy` without camera, geolocation and microphone;
  and `Cache-Control: no-store` except on static files. HSTS is left to the proxy that
  terminates TLS.
- **No cookies, no storage in the browser, nothing from another origin, no access log.** The
  error log of `net/http` is discarded.

### The event stream

`GET /events` is a stream of server-sent events. It begins with `retry: 3000`; each event is an
`event:` line and one `data:` line with JSON, without an `id:`.

| Event | When | Content |
|---|---|---|
| `state` | first on every stream, and to all streams when a new cluster was set up | the whole state with the last 60 lines of the journal |
| `tick` | on the hub's beat of 250 ms if the state changed, otherwise once a second | the state without lines |
| `line` | for every new line of the journal | the line |

There is no resuming: every stream starts with `state`, so a late visitor, a reconnect and a
restart of the controller are the same case. Lines carry their number, and the page appends only
numbers above the last one it shows. The hub encodes an event once for all streams and never
waits: each stream has a buffer of 32 events, and a stream whose buffer is full is closed. At
most 4 streams per address and 200 in all are open; the fifth of one address gets 429, one beyond
the 200 gets 503, both without a body; a stream ends after 10 minutes and the page connects again.
`HEAD /events` is answered with 405.

Times in the state are Unix milliseconds, amounts are decimal strings in hundredths, hashes and
keys are lower-case hex, transfer IDs are decimal strings. The fields are the Go type
`demo.State` in `state.go`.

### The page

`web/page.html` is a Go template, and the server renders every state with it: steady, a running
fault, a finding, a cluster being set up. Without JavaScript the page is the state at the time of
the request and says so; the buttons are not there. The same template renders the sentences the
script needs for roles, phases and refusals into `<template>` elements, and the script takes them
from there.

Three ES modules, no library and no build step. `app.js` reads the stream with `fetch` (so it
can tell a 429 for a full address and a 503 for a full hub from a lost connection), writes the
values into the rendered HTML as text, appends lines and sends the four requests. After 3 s
without an event it marks every value as old, with the time of the last one, and locks the
buttons; when its tab has been hidden for 60 s it closes the stream. `proof.js` fetches `/proof`, shows each step of the check and can
change one byte of its copy of the receipt to show the check fail. `receipt.js` is the verifier
and nothing else: `check` returns every value it computed and the first step that failed,
`verify` is the form `scripts/vectors.mjs` runs the test vectors through. No hash is computed
outside this module.

`Stage.Proof` picks the newest confirmed transfer from a call with more than one transfer, so the
path is not empty, or else the newest confirmed one. It fetches the receipt through the teller
with 2 s, verifies it with `ledger.VerifyProof` against the keys it generated, and compares the
record with what the load sent; a mismatch is a finding and nothing goes out. The answer holds
the cluster ID, the three keys, the transfer, the receipt in hex and in base64, and the head and
signer as Go computed them. Without a leader in time the answer is 503 with `noleader`, before
the first confirmed transfer `empty`, while a finding stands `finding`.

### Configuration and exit codes

`cmd/demo` takes its configuration from the environment only and refuses any `DEMO_` variable it
does not know, as `ledgerd` does with `LEDGER_`. Rates, durations and limits are constants in the
code.

| Variable | Range | Default |
|---|---|---|
| `DEMO_DIR` | absolute path of a directory of its own; created with mode 0700 if missing | required |
| `DEMO_ADDR` | `IP:port`, no host name, no port 0 | `127.0.0.1:8080` |
| `DEMO_ORIGIN` | `http://` or `https://`, a host and an optional port, written as a browser sends it: lower case, no default port, no path | `http://` and `DEMO_ADDR` |
| `DEMO_PROXIES` | networks in CIDR form, separated by commas | empty: no proxy, the address of the connection counts |
| `DEMO_LEDGERD` | absolute path | `ledgerd` in the directory of the demo's own executable |

Before it listens, the program checks that `ledgerd` is there, that `DEMO_DIR` is writable and
that `<DEMO_DIR>/run`, which every setup deletes, holds nothing a demo did not write. Its first
line on stderr names what it read: address, origin, proxy networks, the path of `ledgerd` and
the directory. Its lines are English, like those of `ledgerd`: one per setup or failed attempt,
one per healed fault and one per finding, which is where a fault ends that does not heal; never a
line per request and never an address of a visitor. The German sentences stay in the journal of
the page.

| Exit | When |
|---|---|
| 0 | `SIGTERM` or `SIGINT`: the HTTP server closes within 2 s, then the stage undoes a running fault and closes the cluster. If the stage needs more than 8 s the program ends anyway and the kernel ends the nodes |
| 1 | the stage gave up after five failed setups, the HTTP server ended, or the watchdog found the stage silent |
| 2 | a configuration error, or an argument |
| 3 | it cannot start: `ledgerd` is missing, `DEMO_DIR` cannot be used, the port is taken |

`demo -check` is the only flag. It asks `/healthz` at `DEMO_ADDR`, over loopback if the address
is unspecified, and exits with 0 or 1; the container image has no shell and no `curl` for a
health check.

`deploy/demo` holds the `Dockerfile` (both binaries, built without cgo, in a distroless image
as user 65532), a `compose.yml`, an `.env.example` and a site block for a reverse proxy.
`compose.yml` reads `DEMO_ORIGIN` and `PROXY_IP` from a `.env` next to it, passes `PROXY_IP/32`
as `DEMO_PROXIES` and attaches the service to a network that is created beforehand; the command
is in ADR-014. The `Dockerfile` sets the address of the repository with `-ldflags -X …/internal/demo.repo=…`, and only then does the page
show the links to the source and the `git clone` line; a plain `go build` leaves them out. What
they restrict and what of that has run is in ADR-014.

### Tests

The tests without processes run in a `testing/synctest` bubble with the real durations and
limits. Those with processes build `ledgerd` once, need Linux, and shorten holds and pauses
through an unexported field of the stage, never through the environment. The tests named
`…InChrome` drive a headless Chrome if one is installed and skip otherwise:
`TestVectorsInChrome` runs the test vectors of the audit trail through `receipt.js`, the others
run the page against a fake `Show`.

`TestSoak` is the long run, behind the build tag `soak` and not in CI:

```
go test -tags soak ./internal/demo -run '^TestSoak$' -soak.duration 10m -timeout 15m -v
```

It starts the real stage and the real server on loopback and does everything over HTTP with the
real durations: a random fault on a random target every 3 to 12 s from one of 64 addresses in
`X-Forwarded-For`, one request in ten without `Origin`, with an unknown kind or with a body; a
reader of the whole stream, one that reads 1 KB a second and one that leaves every 20 s; and
every second the page and a receipt, each receipt verified. With `-soak.target` it drives a demo
that runs elsewhere, a container for example. It fails on a finding, on a phase `action` longer
than 45 s, on a sum other than 6 000 000 in any event, on a new cluster for another reason than
the limit, on fewer healed faults than ended ones, on a wrong answer to one of the crooked
requests, on fewer accepted faults than half of what the 10 s rule allows, on a heap above
96 MiB, and on a `ledgerd` or more than ten extra goroutines left after the end. Its last line
gives clusters, transfers, windows, faults by kind and outcome, refusals by reason, the longest
fault and setup, what the readers saw, receipts verified, heap and CPU. The runs made so far
and what they do not cover are in ADR-014.

What the demo cannot do is what the chaos test cannot do either: power loss, lost packets under
TCP, clock jumps. Its one disk fault is a flipped bit in a file of a node that is down.
