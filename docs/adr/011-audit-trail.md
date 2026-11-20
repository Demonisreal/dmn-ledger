# ADR-011: An audit trail: a hash chain over batches, a Merkle tree per batch, stamps on request

Status: accepted, 2026-11-16.

## Context

A checksum finds a disk that changed bytes. It does not find a person who changed bytes and fixed
the checksum. A client that got an answer has nothing to show for it later, and somebody who saw
the ledger yesterday cannot tell whether today's ledger continues it.

Three questions should have an answer that does not rest on the word of the operator:

1. Is my transfer in the ledger? A receipt the holder can check without the cluster.
2. Is today's ledger the continuation of yesterday's? Two heads connected by the links between
   them.
3. Does this data directory agree with a head I kept? A recount from snapshot and log.

## Decision

1. **One link per batch the state machine accepts.** It holds the batch time, the op, the number
   of events and of leaves, the hash of the request body, the hash of the reply and a Merkle root.
   Empty batches and batches in which every event fails get a link too: both move time forward and
   may let transfers expire. The head is `SHA-256(0x02 | previous head | number | link)`.
2. **The root covers what changed:** the pending transfers that expired in this batch's sweep,
   ordered by deadline and ID, then the records stored, in batch order. Leaves and inner nodes are
   hashed with different prefixes, and the tree is split as in RFC 6962. Failed events and rolled
   back chains have no leaf.
3. **The number counts links,** not log entries. The state machine does not know the log index,
   and a dense number lets a verifier count.
4. **Links are replicated state.** They are in the snapshot and in the digest. On every load a
   node recomputes each root from its records and each head from its links.
5. **A receipt** is the record, the path to the root, the link and the head before it. It can be
   checked without the cluster: at most 32 hashes for the path, one for the head, one signature.
6. **The answering leader stamps on request,** with its own Ed25519 key, over the cluster ID, the
   link number and the head hash of the receipt's own link. Nothing signed is stored. Receipts are
   read like lookups: by the leader, after ReadIndex.

The formats are under "Audit trail" in [ledger-rules.md](../ledger-rules.md), the read path, the
stamps and the recount under "Audit trail" in [ARCHITECTURE.md](../ARCHITECTURE.md).

## What a receipt proves and what it does not

To anyone who assigns the key K to a node of the cluster C, a stamped receipt proves one thing:
the holder of K declared for C that the chain has the head `h` at link number `s`. Everything else
follows from SHA-256, not from that declaration. Under `h` lies exactly one link with its time,
op, number of events and leaves, the hashes of request and reply, and a root. Under that root the
record of the receipt is leaf `i` of its kind: an account as it was created, with all balances 0,
a transfer as it was stored, or the expiry of a pending transfer. A receipt for a batch, together
with the body and the reply the client kept, shows what was asked and what was refused, so a
refused transfer can be shown as well. It does not show who sent the batch: client and session are
not in the hash.

A receipt does not prove:

- that a majority committed the batch. It is the word of one node.
- that the node issued this receipt. The stamp covers the head, not the receipt, so it fits every
  receipt of the same link and the bare head as well, and anybody can move it there without the
  key. A stamped head need not have been the answer to a request for the head.
- when it was stamped, or that the head is the newest. The signed message holds no time and no
  value of the verifier's.
- that the time in the link is right. It comes from the clock of a leader (ADR-005).
- the state of a transfer today or a balance. A pending transfer may have been posted, voided or
  expired since, and each of these is an event with a receipt of its own.
- that something is absent.

Tampering is detectable by anyone who kept an earlier receipt or head, and only for the history up
to its number. It is not prevented. Whoever can write to a node's data directory can change a
record, the root of its link and the checksum together, and the directory loads: the check on
load is a consistency check, not a barrier against somebody who knows the format. The node then
runs on with the changed history, leads and stamps, until somebody compares its head with another
node (`ledgerctl status`) or with a head kept earlier (`ledgerctl verify dir`). Two stamped heads
of one cluster with the same number and different hashes prove that their signers vouch for two
histories. A cluster restored from an older backup and written to again shows the same.

Two stamped heads with different numbers are connected by the links between them. If the links
lead from the older head to the newer one, SHA-256 shows that the newer head continues the older
one, whoever supplied the links. Links that do not lead there prove nothing about the heads.
Nobody vouches for links: the cluster serves them without a stamp, on a port that is not
authenticated (ADR-015). The links may be wrong, or the heads may belong to two histories, and
the verifier cannot tell which. `ledgerctl verify chain` therefore prints `CONFLICT` only for two
heads with the same number and different hashes, and it needs neither links nor the cluster for
that. For links that do not connect it prints `invalid` when they came from a file, which is a
verdict on the file, and `no verdict` when it fetched them from the cluster. The suspicion
becomes a proof only when a stamped receipt or head turns up with the same number as one of the
two and another hash.

A receipt without a stamp proves only that its bytes agree with each other.

| Who | does what | is noticed by | what remains |
|---|---|---|---|
| the disk | flips bits | the checksums of WAL and snapshot, the seal (ADR-009) | not a matter for the audit trail |
| someone who can write to a node's files, without a key | changes a record in a snapshot and fixes the checksum | the node itself on load: the root of the link does not match | |
| the same | changes the record and the root of its link | another head than the other nodes (`ledgerctl status`), another head than one kept earlier (`ledgerctl verify dir`) | without a second node or an earlier head, nothing |
| the same | changes a log entry after the snapshot | the recount gives another head | only up to the number of the head that was kept |
| the operator, with every node and every key | writes a new history and stamps it | no earlier receipt or head fits it | whoever kept none notices nothing |
| the same | shows two verifiers two histories | two stamped heads with the same number and different hashes, once the verifiers compare | without comparing, nothing; no exchange between verifiers is built |
| one malicious node | as leader, issues a receipt for a transfer that was never committed | the receipt does not connect to a head of an honest node | the holder notices only when connecting, and not connecting is a suspicion, not a proof; the cluster does not tolerate Byzantine nodes |
| someone in the network | changes a receipt on its way | a changed bit fails to decode or leads to another head, and then the signature fails | swapping it for an older valid receipt, or cutting the stamp off, which leaves an unsigned receipt |
| someone in the network, or a node | changes the links between two heads on their way, or serves wrong ones | the links no longer lead from the older head to the newer one, and `ledgerctl verify chain` gives no verdict | two heads of one chain stay unconnected for as long as this goes on; changed links yield neither `connected` nor `CONFLICT` |
| someone with a stolen key | stamps any head | not until the key is taken off the verifiers' lists | there is no revocation |
| someone with a receipt of cluster A | shows it for cluster B | the cluster ID is in the signed message | |

Assumptions: SHA-256 resists collisions and second preimages, Ed25519 signatures cannot be forged,
and the verifier got the public keys of the nodes on a path it trusts. The cluster offers only the
status frame on its unauthenticated client port for that, which is trust on first contact
(ADR-015).

## Consequences

- Applying costs about three times as much CPU. Measured with
  `go test -run '^$' -bench . -count 3 ./internal/state/` on an i5-12500H with `GOMAXPROCS=4`:
  `Apply` per transfer took 131 to 135 ns before the chain and 680 to 683 ns with it for batches of
  one, 124 to 132 ns and 424 to 428 ns for batches of 128, 133 to 152 ns and 430 to 439 ns for
  batches of 8000, still without an allocation apart from a block for every 256 links. Reading a
  snapshot of 1000 accounts and 100 000 transfers with the full check took 15 to 17 ms before and
  39.5 to 40.6 ms after. One transfer through one node with its disk in memory
  (`BenchmarkProcessTransfer` in `internal/replica`) went from 1.5 to 1.6 µs to 2.2 µs. On a
  real cluster a transfer waits for four fsyncs; the share of the chain there is not measured yet.
- Receipts: building one takes 0.27 µs for a batch of one record, 30 µs for 128 and 1.95 ms for
  8000 (`BenchmarkProve` in `internal/state`). Decoding a receipt of 484 bytes takes about 1.1 µs,
  a stamp 15 µs, checking one with its signature 35 µs (`BenchmarkSign`, `BenchmarkVerify` in
  `internal/notary`), connecting two heads 153 ns per link between them (`BenchmarkConnect` in
  `internal/audit`).
- 152 bytes of memory and 113 bytes of snapshot per batch, never dropped, and 8 bytes for every
  expired transfer.
- A snapshot whose records were edited and whose checksum was fixed does not load, unless its links
  were edited to match (above).
- The order in which pending transfers expire in one sweep is now part of the contract: deadline,
  then ID. It decides the leaf indexes.
- The snapshot of the state machine is format 2: records in timestamp order, then the links. The
  heads are folded again on load, so the file cannot contradict itself.
- Four planted bugs break the chain (B19 to B22). Three of them are found by the node itself,
  because its own state fails the chain check when a snapshot is loaded. The fourth, a reply that
  is left out of the link, leaves nothing in the state to check against; the oracle finds it (S12).
- After a leader change another node stamps, with its own key. All heads belong to one chain as
  long as all nodes apply the same entries the same way; the simulator checks that for every
  applied index (S12), and the peer hello keeps nodes with another `MaxSessions` out.

## Limits

- No signature of a majority. A receipt is the word of one node.
- No revocation and no expiry of keys. The key lives in the memory of the process; there is no HSM.
- The public keys reach a verifier on first contact over an unauthenticated port.
- Two verifiers who never compare heads can be shown two histories. There is no witness outside
  the cluster.
- Sessions and stored replies are outside the chain. Registering a session has no receipt.
- Connecting two heads is linear in the number of batches between them.
- Links are never dropped. The chain grows like the transfers.
- A recount needs the data directory in memory, checks only up to the number of the head it is
  given, and needs the cluster's `MaxSessions`.
- Nothing bounds how many transfers one sweep expires, and a receipt hashes all leaves of its
  batch. The read budget of a round counts these leaves, but one receipt of a very large batch
  stays expensive.

## Rejected alternatives

- **One Merkle tree over all transfers,** a log as in RFC 6962 with consistency proofs. Proofs
  between two heads would be logarithmic, but the tree needs its own storage and its own snapshot
  format, and a receipt grows with the ledger. A head per batch can be signed on its own.
- **The log index in the link.** The state machine does not know it, register entries and the
  empty entries of new leaders would shift it, and the oracle has none.
- **Signing in the state machine, or storing signatures.** Keys belong to one node; replicated
  state must be equal on every node.
- **Signing the current head with every receipt.** The receipt would then need every link up to
  that head. The head of the ledger is a read of its own; who wants both fetches both and connects
  them.
- **Doubling the last node of an odd level,** as Bitcoin does: the lists `[a, b, c]` and
  `[a, b, c, c]` then have the same root (CVE-2012-2459). Leaves without a prefix: an inner node
  could pass for a leaf.
- **Links only in the log.** The log is compacted, and receipts for old transfers would be gone.
- **Receipts from followers.** They would be true, but they need a second read path, and a
  confirmed transfer could come back as not found.
- **A key on every node by force.** Every start and every test would need a key file. Without
  `LEDGER_KEY_FILE` a node serves receipts without a stamp, and the tools say so.
- **Checking the chain only when a directory is recounted.** Loading would be cheaper, but a
  running node would no longer hold "the head fixes the state".
