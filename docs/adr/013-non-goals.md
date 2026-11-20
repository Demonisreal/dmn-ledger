# ADR-013: Non-goals: what is not built, what that means today, and how it could be added

Status: accepted, 2026-11-20. Amended 2026-11-20: what the benchmarks of ADR-012 measured is no
longer listed as open, and the snapshot pause that costs a leader its role is a measurement.

## Context

This project shows a small number of things done with care: a deterministic state machine, one log
that is both WAL and Raft log, a consensus core with repair after disk corruption, an audit trail,
and a simulator that checks all of it under faults. A database that somebody runs needs far more,
and a reader who knows databases looks for the missing parts first.

Most gaps are named somewhere: in the README, under "Limits" in single ADRs, in half sentences of
ARCHITECTURE.md. Spread out like that, they do not tell a reader whether a gap he finds himself was
decided or overlooked. Some were named nowhere: what a node does that comes back on an empty
directory, how large a ledger can grow, what a new file format does to an existing cluster.

## Decision

The project ends at this line: one Raft group with a fixed set of one, three or five nodes, the
whole ledger in memory, every read and write through the leader, a network that is trusted, lookups
by ID, and behaviour that is tested and not proven. What lies beyond it is left out on purpose and
listed here, in one place.

Every entry says what is missing and why, what a user meets today if he needs it anyway, and how it
could be added: which parts it touches, where the hard part is and what would show that it holds.
A sketch is a paragraph, not a design, and none of them is scheduled. A limit that another ADR
already argues gets a reference instead of a repetition.

Three reasons come up again and again, so they are given once:

- **Size.** Membership changes and storage on disk are each a project of their own, not a feature.
- **The simulator has to check it.** Nothing that changes what the cluster promises is built
  without a checker for it and a planted bug that the checker finds (ADR-008). A feature whose
  checker does not exist is not built.
- **Half is worse than none.** TLS without an answer to "who may do what", or a backup without a
  tested restore, looks like protection and is not (ADR-015).

## The cluster

### A fixed set of nodes, and no way to replace one

The set of nodes is fixed when a cluster starts. `LEDGER_PEERS` lists one, three or five
addresses, the position in the list is the node ID, and the core takes the list once, in
`raft.New`. No log entry, message or command changes it. A node cannot be added or removed, there
are no members that only listen, and a node whose disk is lost cannot be replaced.

Membership changes are a second protocol on top of the first, and one where published
descriptions have been wrong: the single-server change of the Raft dissertation had a safety bug
that was found after publication. The rules R1 to R24, the random schedules of the core and the
simulator with its checkers all assume one set of nodes for a whole run. Built without a second
form of each, it would be the one part of consensus here that is not checked.

Today:

- A node that was down comes back on its own directory and is caught up with entries or with a
  snapshot (R11, `TestLaggingNodeGetsTheSnapshotAsAStream`). That works for as long as the
  directory exists.
- A node can move to another machine together with its directory, if every node is restarted with
  the new `LEDGER_PEERS`: addresses are read at start only, and the peer port takes connections
  from listed addresses only. No test does this.
- A node whose directory is gone stays gone. Three nodes without one of them still commit, but
  they are two voters that need each other: the next failure stops the cluster, and a disk fault
  on one of the two can stop it for good, as with two damaged nodes of three in ADR-009.
- Starting `ledgerd` with the ID of the lost node on an empty directory is not refused, and it is
  unsafe. `wal.OpenMeta` creates a new `meta` with term 0, no vote and seal 0, so the node is a
  voter that has forgotten its vote and everything it acknowledged. That is the node the seal
  exists to rule out, and ADR-009 names it as not covered. With three nodes, the empty node and
  one that is behind can elect a leader that lacks a committed entry. The same holds for a
  directory put back from an older copy.
- A data directory records neither the cluster ID nor the node ID nor the peers. `ledgerd` cannot
  tell that it was given the directory of another node or of another cluster.

To add it: single-server changes as in chapter 4 of the Raft dissertation. The configuration
becomes a log entry of a new kind that takes effect when it is appended; `peers`, `quorum()` and
the progress table of the core become per configuration; the node snapshot carries the
configuration in its header, which is a new snapshot version; the peer hello, which today checks
`from` against a fixed list, checks against the current one; and `LEDGER_PEERS` shrinks to the
addresses a new node asks first. A new node joins without a vote and is caught up by snapshot,
which is close to what a node in repair already is (R20, R21), except that its acknowledgements
must not count either. Replacing a lost node is then "remove ID 3, add ID 4": a new ID has never
voted, and that is what makes an empty directory safe. The mechanism is the smaller part. The hard
part is the cases: a change while the leader fails, a leader that changes the set before it has
committed an entry of its own term, a removed node that keeps asking for votes, and repair, where
the majority for commits and the one for elections already differ. It would hold when S1 to S5
pass in a simulator profile that adds and removes nodes under the faults of `all` and `rot`, with
the receipt book of S5 kept per configuration, and when planted bugs for the known mistakes (a
configuration that takes effect at commit, two changes in flight, a change before the leader's
first commit) are found.

A much smaller step closes the dangerous half. `ledgerd` cannot tell the first start of a new
cluster from an old node on a lost disk, because both are an empty directory. An explicit
`ledgerctl init` that writes cluster ID and node ID into the directory, and a `ledgerd` that
refuses a directory without them or with other ones, would turn the unsafe start into a refused
one. It needs a place for the two IDs, since the 40-byte slot of `meta` is full, and one test per
refusal.

### No leadership transfer

A leader cannot hand its role over. The core has no message for it, and a `ledgerd` that gets
`SIGTERM` finishes its round and stops without telling anyone. It was left out because its uses,
planned restarts and choosing where the leader runs, are matters of operation that the project
does not reach, and because it is a second way into an election that needs its own rule tests.

Today: stopping the leader costs an election. The followers wait out their timeout, 0.5 to 1 s
with the default tick, and a client sees the gap as a slower call, not as an error: the SDK moves
to the next node with the same request number (`TestDeadConnectionMovesOnAtOnce`) and the session
lets it apply once. Nobody can choose which node leads.

To add it: the leader stops taking proposals, waits until one follower holds its whole log and
sends it a message to campaign at once (section 3.10 of the dissertation); that follower skips the
pre-vote round of R4 to R6. It touches the core (one message, one state in which `Propose` answers
`ErrBusy`), the wire format (a new type, so a new version) and the server (hand over before
stopping, and a frame for `ledgerctl`). The hard part is the transfer that fails halfway: the
target dies or is cut off, and the leader has to take proposals again after a timeout. S1 to S5
already say whether that holds; the simulator needs a fault that starts a transfer at a random
moment.

### Reads only from the leader, no leases

Decided in [ADR-007](007-readindex-no-leases.md): reads go through ReadIndex on the leader, leases
are rejected because they rest on bounded clock drift, and reads from followers are kept as an
extension. Today a follower answers every request with `NotLeader` and the leader it knows
(`TestFollowerPointsToTheLeader`), the SDK follows the hint (`TestFollowsTheLeaderHint`), every
batch of reads costs a heartbeat round to a majority, and all reads are answered by the leader's
one loop between its writes (ADR-001).

To add reads from followers: a follower asks the leader for a read index with a new pair of
messages, waits until it has applied that index and answers from its own state. That touches the
core, the wire format, the branch of `replica` that refuses requests on a follower, and the client
session, which today sends everything to the leader. The checkers already judge a read by its
content and its time, not by the node that answered (S9, S11, S13); the simulator's clients would
have to send reads to any node. The hard part is small and easy to get wrong: the leader's
confirmation must begin after the follower got the request, or a read can return a state from
before a write that was already confirmed.

### One group, no sharding

All accounts live in one Raft group and on every node. Nothing splits a ledger over several
groups, and no transfer crosses clusters. A transfer touches two accounts, so sharding means a
commit protocol across groups, and the project is about one group.

Today: capacity is one machine's. One core applies (ADR-001), one disk syncs, and the smallest
node bounds the ledger (below). How many transfers a second that is was looked for on a laptop
and not found: one node held 256,000 a second, and three nodes on one disk at least 128,000
(128,000–256,000) with 32 client sessions and 256,000 with 4. The load generator and the plan
ended those runs, not the nodes (ADR-012). Since a transfer never leaves its ledger
(`AccountsMustHaveSameLedger`), separate ledgers can run on separate clusters with nothing built;
what is lost is a linked chain across them.

To add it: pending transfers are already the first half of a two-phase commit: reserve on both
sides, then post or void. A coordinator outside the clusters could move value between two groups
with them. The hard part is the coordinator's own recovery, and expiry: a pending transfer that
times out on one side after the other side was posted. Nothing here would check that; it would
need a simulator with two clusters and an invariant over both.

### Several data centres

Nothing is built for nodes that are far apart, and nothing was tested that way: the tests, the
chaos test, the demo and the plan in `bench/` all run their nodes on one machine.

Today: addresses are IP and port, so nothing forbids it. What a user meets: the timeouts are fixed
in ticks (heartbeat 5, election 50) and only the tick is a setting, so slow links mean a longer
`LEDGER_TICK` and with it every timeout at once; a commit waits for two fsyncs on the leader, a
round trip and two on a follower, one after the other (ADR-004, ADR-009); a batch of reads waits
for a round trip to a majority (ADR-007); the leader is wherever the last election put it; a
follower that fell behind the leader's compaction gets the whole state as one stream; and without
TLS the links need a tunnel. Clocks in different places are not a safety problem (ADR-005). The
only evidence about latency is the chaos test, which delays frames by 5 to 300 ms, and its proxy
cannot lose packets below TCP.

To add it: measure first. `internal/faultnet` can delay every frame between two nodes, so the plan
in `bench/` could run with a fixed delay per link. The features that matter over distance are the
two above, leadership transfer to place the leader and reads from followers. Nothing in the safety
of the core depends on latency, and the simulator already draws a latency for every link.

## The data

### The whole ledger is in memory

Every account, every transfer and every link of the audit trail is in the heap of every node:
accounts in a slice, transfers in blocks of 16 384, two maps from ID to position
(`state.Machine`). A snapshot is the whole state (ADR-010). There is no index on disk and no page
cache. ADR-001 gives the reason: a state machine in plain memory gives equal bytes for equal
state, which is what `Digest`, the comparison with the oracle and the full `CheckInvariants` rest
on.

Today the ledger has three bounds. The pause is measured up to a million accounts and a snapshot
of 614 MB (ADR-012); memory and the stream limit were not measured on a large ledger:

- **Memory.** A transfer takes 136 bytes in its block and an entry in a map, a batch 152 bytes for
  its link (the first figure from the struct, not measured), and nothing is ever freed. `ledgerd`
  does not watch its memory. A node that outgrows its machine is ended by the operating system and
  needs the same memory again when it restarts.
- **Pauses.** After every `LEDGER_SNAPSHOT_EVERY` entries (100 000 by default) a node writes the
  whole state inside its loop and does nothing else meanwhile; the replies of that round wait as
  well. A restart reads the snapshot back with the full invariant check, and so does a follower
  that installs one. All three grow with the ledger. ADR-010 has the times for 100 000 transfers in
  memory. On a real disk a single node stopped for about 200 ms per 100 MB of snapshot, 1,198 and
  1,273 ms with 614 MB (ADR-012); a restart and an install were not measured. A node that writes
  for longer than an election timeout, 0.5 to 1 s with the default tick, looks dead to the others
  for that time: a leader is replaced, and a leader whose followers write at the same moment steps
  down by check-quorum (R9). That follows from the rules, and it is measured: three nodes on one
  disk with a million accounts and snapshots of 308 to 315 MB had rounds of 761 to 1,129 ms, and
  the term changed in all four runs (ADR-012). No smaller state was run with three nodes, so at
  which size it begins is still not known, and no test runs a snapshot that long.
- **8 GiB per snapshot stream.** A node that receives a snapshot gives up above 8 GiB
  (`TestSnapshotOverTheLimitIsRefused`). A snapshot holds 101 bytes per transfer and 113 per
  batch, so the limit is near 85 million transfers, fewer with small batches. Beyond it a follower
  that needs a snapshot, because it fell behind the leader's compaction or lost its log to damage,
  is never caught up: the leader offers the snapshot again and again and the follower refuses it.
  The limit is a constant in `internal/server`.

To add it, in two steps. The first takes the pause out of the loop. Transfers are appended in the
order of their timestamps and never change after their batch, except for the status of a pending
one, and accounts are few. A writer in the background therefore needs a frozen copy of the
accounts, of the open pending transfers and of the counts, and can read the stored transfers below
those counts while the loop goes on appending. ADR-010 rejects this for now. Its real price is
that ADR-001's rule, one thread owns the state, gets its first exception, and that the simulator,
which has one goroutine, would run another snapshot path than the server.

The second step moves the transfers out of memory: a file of records in timestamp order, which is
already the order of the snapshot, with an index from ID to position on disk, while accounts, open
pending transfers and links stay in memory. The records of one batch are one run in that file,
because their timestamps are consecutive, so a receipt reads one run and the transfers that
expired in its sweep. This touches `state` (storage behind the record lookups), the snapshot
format (it would no longer carry the transfers), `Digest` (it can no longer hash every byte and
would rest on the head of the audit trail and the accounts) and `CheckInvariants` (the full
recount becomes a scan for `ledgerctl verify dir`, not a step of every load). The hard part is
what ADR-002 avoids today: a second file that has to agree with the log after a crash, so applying
an entry must be safe to repeat. `disk.Sim` would cover the new file like any other, and
`TestCrashAtEveryDiskAccess`, S6, S7 and the model test against the oracle are the checks. This is
a different storage design, not an extension.

### Nothing is deleted or archived

No transfer and no account is ever removed, an account cannot be closed, and the links of the
audit trail are never dropped (ADR-011). The log is compacted; the state is not. This is by design
twice over: `CheckInvariants` recomputes every balance from the stored transfers, and a receipt
for a transfer of a year ago is built from the state like one of a minute ago.

Today: the state only grows, until one of the three bounds above is reached. Sessions are bounded
by `LEDGER_MAX_SESSIONS` and the log by compaction; accounts, transfers and links are not. The
only way to start small again is a new cluster, as the demo does after 20 000 transfers (ADR-014):
a new cluster ID, new keys, empty books. No tool carries balances over.

To add it: a checkpoint. A new batch op closes a period at a ledger time: it stores, per account,
the balances at that moment as an opening record with a leaf in the chain, so the head covers
them. Transfers before it can then be written to an archive file and dropped from the state, once
no open pending transfer points to them; the links stay, at 113 bytes a batch, so the archive
still verifies against the heads and a receipt for an archived transfer can be built from the
archive. It touches `state` (the op, a record kind, invariants that start from the opening records
instead of from zero), the oracle, the snapshot format, `audit` (a leaf kind) and
`replica.Recount`. The hard part is not the balances. Transfer IDs are the second layer of
exactly-once (ADR-006): a transfer sent again after its first copy was archived must still come
back as `exists`. Either every ID stays in memory, 16 bytes each, or the ledger refuses IDs from
before the checkpoint by a rule the client can follow. The model test against the oracle and S8
would show a transfer that applies twice.

### Lookups by ID and nothing else

The cluster answers eight operations: register, create accounts, create transfers, look up
accounts by ID, look up transfers by ID, and three reads of the audit trail. There is no query
language, no secondary index, no listing, no range by time, no sum over accounts, no balance at an
earlier moment and no feed of changes for another system to follow.

Every read has to exist three times, in the state machine, in the oracle and in the model that
the linearizability check runs, and lookups by ID were enough to check what the project is about.
Reports are the work of a system that reads from a ledger.

Today: the caller has to know its IDs. A lookup takes at most 10 922 account IDs or 10 485
transfer IDs (`TestLookupOverTheReplyLimitIsMalformed`; the SDK returns `ErrTooLarge` before it
sends), and an ID the ledger does not have is left out of the answer without a word. An
application that wants the transfers of an account keeps that list itself; the demo counts its
books by looking up every transfer ID its load ever sent. A balance is the current one. Nothing
reads records out of a data directory either: `ledgerctl verify dir` recounts and prints heads,
not records.

To add the first query, the transfers of an account in time order: a list of positions per
account, which is derived state like the two maps, rebuilt on load and absent from the snapshot,
so no format changes; a read op with a start time and a limit under the 1 MiB reply, through
ReadIndex and the read budget; the same op in the oracle, where S9 and S13 then compare it. It
costs two integers of memory per transfer. A feed of changes is the same op without the account:
transfers are stored in timestamp order, and a receipt already finds its batch by binary search.
A balance at an earlier time is a sum over that list, or a stored balance per transfer for
accounts that ask for it. None of it is hard. It is breadth: every op in three places, on the
wire, in the SDK and in `ledgerctl`.

### A small data model

The model follows TigerBeetle's in a subset of fields and flags (ledger-rules). It is enough to
give consensus something real to protect: transfers over two accounts, chains that succeed or fail
as one, reservations with a timeout. What a user of an accounting system will miss:

- **No currency conversion.** A ledger has one unit and a transfer stays inside one ledger
  (`AccountsMustHaveSameLedger`, `TransferMustHaveSameLedgerAsAccounts`). An exchange is two
  transfers on two ledgers in one linked chain, through accounts the application keeps for that
  purpose; the rate is the application's. Nothing has to be built, and a table of rates in
  replicated state is out of scope.
- **Amounts are unsigned 128-bit integers.** No decimals and no scale: what one unit means is up
  to the application. The demo books hundredths.
- **No field for the application's own reference.** An account or a transfer has `Code`, 16 bits,
  and its ID. A reference to an order or an invoice goes into the ID or stays in the application.
  Adding a field is no logic at all and still expensive: records travel byte for byte through the
  wire, the log, the snapshot and the leaf hash, so every format gets a new version (see "No
  upgrade without stopping" below).
- **An account never changes.** Ledger, code and flags are fixed at creation
  (`ExistsWithDifferentFields`); an account cannot be closed, frozen or given another limit. The
  only limits are the two flags, debits within credits or credits within debits.
- **Atomicity ends with the batch.** A chain cannot be longer than one request, 1 MiB or 10 485
  transfers, and nothing spans two requests. There is no transaction that reads and then writes.
  A reservation with a later post or void is the tool for a decision in two steps.

## Security

### No TLS, no authentication, no rights

Decided in [ADR-015](015-binary-protocol-over-plain-tcp.md), with a table of what a stranger can
do. In addition to it: the state machine does not know who sends a batch, so whoever reaches the
client port can do everything, including posting or voiding a reservation another client made,
given its ID. There are no quotas. Nothing is encrypted at rest: the data directory is plain, and
a node's signing key is 64 hex digits in a file that only its mode protects.

To add the transport: `server.Config` takes two ready listeners and a dial function, so
`tls.NewListener` around both and a TLS dialer in `cmd/ledgerd`, with variables for certificate,
key and CA, change no frame. A peer is then known by its certificate instead of by its source
address. The SDK has the same seam only inside: `ledger.Config` has no field for a dialer yet. The
hard part is the half ADR-015 names, who may do what. A rule that decides whether a transfer
applies has to be replicated state, or replicas would differ, and it then belongs under the audit
trail; a rule checked only at the leader's door leaves no trace there. Choosing between the two is
a design of its own.

### No Byzantine fault tolerance

Nodes are trusted to follow the protocol. Whoever sends from a peer's address can vote and append
entries (ADR-015), a malicious leader can stamp a receipt for a transfer that was never committed
(ADR-011), and the core drops malformed messages only so that they cannot leave a log that does
not restart. This is not a gap an extension closes: tolerating f lying nodes takes 3f + 1 of them,
signatures on every message and another protocol.

### The audit trail does not stand against the operator

[ADR-011](011-audit-trail.md) lists it: a receipt is the word of one node, keys are neither
revoked nor expire, a verifier gets them on first contact over a port without authentication, and
two verifiers who never compare heads can be shown two histories. Tampering is detectable by
whoever kept an earlier head, not prevented.

Two of these are cheap to sketch. A majority's signature: every node holds the same head at every
link number (S12) and applies only committed entries, so any node could stamp the head of a link
it has applied, without ReadIndex, and a client could collect stamps from a majority of keys for
one head. That needs one read op that followers answer, with "not yet" for a link they have not
applied, plus the fan-out and the count in the SDK and in `ledgerctl verify`. It is worth
something only once the verifier knows the keys of all nodes from a path it trusts. A witness
needs no change to the cluster at all: `ledgerctl head` writes a stamped head to a file, and
publishing one a day somewhere the operator cannot rewrite gives every later `verify chain` an
anchor. It is not built because the project has no second party to be that place. Revocation would
be a key list with a range of link numbers per key, on the verifier's side.

## Operation

### No upgrade without stopping, no migration of formats

Every format carries a version, and every reader accepts exactly one: frames are version 2, WAL
segments 1, the snapshot of the node and the one of the state machine 2, receipts 1. Nothing reads
an older version and nothing converts.

Today:

- **Wire.** A node of another version has every frame refused with `ErrVersion`
  (`TestOtherVersionIsRefused`) and never joins; a client of another version gets no answer. A
  change to any frame layout means stopping all nodes and starting all of them new.
- **Disk.** A binary with another snapshot version does not open a directory that holds a
  snapshot. The node refuses the version (`TestLoadRefusesOtherSnapshotVersions`), and a snapshot
  that cannot be read is not damage: the start ends with the error, no older snapshot is tried and
  nothing is deleted (`TestSnapshotUnreadableContentStopsTheLoad`). The step from snapshot format
  1 to 2 was made this way: there is no reader for format 1. Nothing in the repository carries a
  cluster with data over such a change.
- **Rules.** No log entry says which version of the ledger rules applied it. A binary with a
  changed check replays the entries after its snapshot differently from the binary that first
  applied them, and two versions in one cluster apply the same entry differently. No node would
  notice (see "Nodes do not compare their state" below).

Compatibility is a promise over time, and a project with one author and no installation has nobody
to keep it for. The simulator runs one build of the node code.

To add it, in three layers. The peer hello names the highest version a node speaks and both sides
use the lower one, which allows restarting node by node across a wire change. Readers understand
the previous disk version as well as the current one. And an entry in the log raises the version
of the cluster at one index, proposed by the leader once every hello shows the new build; from
that index on every replica writes the new formats and applies the new rules, so the switch is as
deterministic as any other entry. The hard part is the test. The simulator would have to run two
versions of the rules in one process. The rules sit in a few small functions with twin files
already, for the planted bugs; a version switch is the same mechanism, and S3 and S12 between
nodes of mixed versions are the check. A pinned set of old snapshots and logs that every later
build must load to the same digest covers the disk.

### No backup, no restore, no way back after losing a majority

No tool takes a consistent copy of a running node, none restores one, and none builds a new
cluster from what survived. All three need either membership changes or a defined "new cluster
from this state", and tests with real copies; running a service over years is outside what the
project shows.

Today:

- The directory of a stopped node is a consistent copy, and `ledgerctl verify dir` recounts such a
  copy against a head kept earlier, which is the check a backup needs. A running node holds the
  lock on its directory, and `verify dir` refuses it. What a start on files copied from under a
  running node does is not tested.
- Putting an older copy back under a node of a running cluster is the node that forgot, above.
- Nothing stops an operator from starting nodes on copies of one directory, since it names no
  cluster and no node, and `replica.Recount` does open a copy as a cluster of one. No test starts
  a cluster that way, and no document says when it is safe.
- A majority lost for good leaves a cluster that accepts nothing: two of three directories gone,
  or the two damaged nodes without a leader of ADR-009, where the code has no way out. The
  surviving directory can still be recounted. No tool exports its records or starts a new cluster
  from it.

To add it: a backup is the snapshot stream turned towards a client. The leader already sends a
checked snapshot file to a follower as chunks with a length and a checksum at the end; the same
stream on the client port, with a stamped head for the snapshot's last link, is a file that can be
recounted offline. A restore writes that file into every directory of a new cluster as an
installed snapshot, sealed at its index. For a single survivor the recount already tells what is
certain: it prints up to which link a snapshot covers the history ("committed for sure") and which
links only the log reaches, and those may never have been committed. The hard part is to say what
a restore means. Every transfer confirmed after the copy is gone, although its client holds a
confirmation and perhaps a receipt. With the old cluster ID those receipts contradict the new
chain, which ADR-011 counts as detection; with a new one they name a cluster that no longer
exists. A simulator profile in which every node restarts from the cloned disk of one node, with
all checkers from there on, would be the test.

### No metrics endpoint

`ledgerd` has no port for metrics, no histogram of latencies, no tracing and no health check
besides its status frame. The benchmarks read what exists, and nothing the project shows needed
more.

Today there are two sources. The status frame of each node, which `ledgerctl status` prints and
turns into an exit code: 1 if a node does not answer, is in repair, or differs from another at the
same index. And one line of counters on stderr at the end and on `SIGUSR1`: rounds, time spent
waiting and in `Process`, the slowest round, and fsyncs with their time by kind of file
(`statsLine`). Latency per request is measured only at the client.

To add it: a third listener in `cmd/ledgerd` that serves the same counters as text, and a
histogram with fixed buckets filled in the loop. The counters are atomic already and are read from
outside the loop today. The one rule to keep is that of the status frame: the loop never waits for
whoever asks. It is small.

### Nodes do not compare their state, and nothing scrubs

No message between nodes carries an apply hash or a head. A replica whose state went wrong, by a
flipped bit in memory, by a bug or by a build with other rules, runs on, and as leader it answers
from that state and stamps it. Data at rest is read when it is needed and not otherwise (ADR-009).

The simulator compares apply hash and head of all nodes after every event (S3, S12). In operation
that comparison happens only from outside: `ledgerctl status` prints `nodes DIFFER` and exits with
1 when two nodes at the same applied index disagree (`TestStatusOutput`), and the chaos test and
the demo compare the nodes at their own checkpoints. The reason it is not inside the protocol: a
mismatch says that two nodes differ, not which one is wrong, and the rule for who stops then needs
a checker of its own.

To add it: the leader puts a link number and its head into each `MsgApp`, 40 bytes and a new wire
version. A follower compares as soon as its own chain has that link, which it can do for any
earlier number, because the state keeps every head. A follower that disagrees stops, as for any
contradiction on disk. If the leader is the one that is wrong, both followers stop, it loses its
majority, and the cluster stands still instead of answering from a wrong state. The check would be
a planted bug of the kind of B12, a replica that applies differently, which today only the
simulator's S12 notices and which would then have to end with a stopped node. A scrubber is a
smaller thing: a reader that goes through segments and snapshots a few megabytes per idle round,
in the loop. A broken frame found while running stops the node already and the restart repairs it
(ADR-009), and the simulator already restarts nodes with latent damage when the quiet seconds
begin.

### Smaller gaps in operation

- **Settings are read once.** `ledgerd` takes everything from the environment at start, so every
  change is a restart, and the Raft timings are constants: only the tick is a setting.
- **A full disk stops the node.** It is an I/O error like any other, and every I/O error is
  fail-stop. Nothing watches free space. A node needs room for two snapshots, the log after the
  older one and, while it receives a snapshot, a spool file of the same size (ADR-010).
- **Linux.** CI runs on Linux only, and the tests with real processes, the chaos test and the demo
  need it. `internal/disk` locks a directory with `flock`, which Go's `syscall` package does not
  have on Windows, so the module does not build there; the `TCP_USER_TIMEOUT` on peer connections
  is set on Linux only. Nothing was built or run on another system, and what the crash tests say
  about a disk they say about `disk.Sim`.
- **One SDK, in Go.** The frames are documented and pinned in `testdata/frames.golden`, and the
  rules of exactly-once sit in `internal/client`, about 300 lines without I/O, so a client in
  another language is that file and a codec. Receipts can be checked in JavaScript today
  (`receipt.js`, with the test vectors).

## Assurance

### Tested, not proven

There is no formal model and no proof. The safety of repair is an argument in three steps
(ADR-009), and two of its rules have no known schedule that breaks without them. The simulator
checks and does not prove: ADR-008 and "Planted bugs" in ARCHITECTURE.md name ten mutations made
by hand that it passes, three in the core and seven in the order of a start, all caught only by
the tests of the packages. It knows disks as `disk.Sim` models them, and no checker in it observes
time (ADR-005). A disk that confirms an fsync and drops the data, flipped bits in memory and
misdirected writes are outside the model (ADR-009).

Two things were never run at all. Power loss on a real disk: the chaos test kills processes, and
the page cache survives a kill, so a missing fsync shows only in the simulator. And packet loss
below TCP, because the proxy of the chaos test acknowledges what it reads. No one outside the
project has tested or reviewed it.

To add it: a TLA+ model of the seal, the repair flag and R19 to R24, checked with TLC for three
nodes and a short log. ADR-008 already calls that worth doing. It would also settle whether the
two rules without a counterexample are needed. The distance between such a model and the code
stays, and the simulator is what covers it. For power loss: the three processes of the chaos test
on block devices that drop what was not synced when the test cuts them.

### Measured once, on one laptop

The benchmark numbers for a cluster come from one run of the plan in `bench/`, on one evening and
one laptop, with the three nodes on one disk and Postgres next to them on the same disk (ADR-012).
`bench check` does not pass on that run: in three required groups the two runs were more than
10 % apart. Measured there, and quoted in the ADR of each: the commit latency of three nodes with
four fsyncs in sequence (ADR-004), the price of the seal (ADR-009), how far grouping goes under
load (ADR-002), the pause of a snapshot on a real disk (ADR-010) and the share of the audit trail
in the CPU time of a node (ADR-011). Still open: what reading each entry back costs on its own
(ADR-002), receipts and stamps under load (ADR-011), a follower that installs a snapshot
(ADR-010), the rate at which a node saturates, memory per transfer, the time of a restart against
the size of the ledger, a second run on another evening, a disk with power-loss protection, and
anything over a real network.

### Not for production

No installation runs this, it has never held data that anybody depends on, and the license allows
running it only to evaluate the author (LICENSE). Of the entries above, four would stop a real
deployment before the rest matter: a lost node cannot be replaced, there is no backup, the client
port has no password, and the ledger is bounded by memory. What the project claims is narrower:
the mechanisms it does have were checked under the faults named in the README.

## Limits decided in other ADRs

These are costs of decisions, argued where the decision is made:

- A commit waits for four fsyncs in sequence, and the leader does not send while it syncs
  (ADR-004, ADR-009). At most one `MsgApp` with entries is unacknowledged per follower (R10).
- One core applies, and lookups are answered in the same loop as writes (ADR-001).
- Ledger time follows the leader's clock within a bound of its ticks; a deadline can pass up to a
  fifth early while time catches up (ADR-005).
- More clients than `LEDGER_MAX_SESSIONS` evict each other, and an evicted client is finished
  (ADR-006).
- Three nodes with one in repair stop when the leader is unreachable; two damaged nodes of three
  can stop for good; read errors are fail-stop (ADR-009).
- What the demo does not show, and what a public instance is not protected against (ADR-014).
- What a stranger at a port can still do (ADR-015).

## Consequences

- There is one place to look. A gap that is not on this list is one the author does not know, and
  that makes it a defect of this document, to be fixed here.
- The list has to be kept. An entry names tests and identifiers, so it goes stale when they
  change. The limits in the ADRs of the single topics stay where they are; where this list and
  such an ADR disagree, the named test decides.
- The sketches are not a roadmap. None is scheduled, and several contradict a decision that stands
  (ADR-001 for a snapshot in the background, ADR-002 for transfers on disk); building one starts
  with amending that ADR.
- If the project had to run somewhere, the order would follow the danger, not the size: first the
  refusal of an empty or foreign directory, then backup and restore, then membership changes.

## Rejected alternatives

- **Leaving the limits where they are,** in the README and in each ADR. Every one of them is true
  there, and together they still do not say what is missing. A reader cannot tell a gap that was
  decided from one that was overlooked.
- **A roadmap.** It promises work that nobody has scheduled, and reads as "soon".
- **Building the small entries to shorten the list:** a metrics port, TLS for the transport. Each
  is a surface with its own tests and documents, and neither shows anything the project is about.
  TLS alone is the half that ADR-015 rejects.
- **Membership changes without a checker.** They would work in the demo and be the one part of
  consensus here that nothing checks.
