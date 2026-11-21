# ADR-016: A data directory names its cluster and its node

Status: accepted, 2026-11-21.

## Context

Raft is only safe if a node never forgets a vote or an acknowledgement (ADR-004). The seal covers
a disk that damages them (ADR-009), not a directory that is gone: `ledgerd` created whatever was
missing and started on an empty directory as a voter with term 0, no vote and seal 0, which
[ADR-013](013-non-goals.md) named as unsafe. Of three nodes, node 3 is cut off while nodes 1 and 2
commit a transfer. Node 2 comes back on an empty directory, node 1 is cut off, node 3 returns, is
elected with the vote of node 2 and commits an entry of its own at the index of the transfer. A
directory also named neither its cluster nor its node, so a `ledgerd` given the directory of
another node started under its own ID with a foreign vote and log.

## Decision

1. **Three numbers in `meta`.** Cluster ID (`LEDGER_CLUSTER`), node ID (the position in
   `LEDGER_PEERS`) and the number of nodes are in both slots of `meta`, under the checksum that
   covers term, vote and seal: whoever can read the vote can read the identity. The slot keeps its
   40 bytes; vote, node ID, number of nodes and flags take one byte each.
2. **`ledgerd` makes no data directory,** neither the path nor `meta`. Without `meta` the start
   ends with `wal.ErrNoMeta` and exit code 2, and the log line names `ledgerctl init` as the way
   for a node that never ran and says that a node that lost its directory must stay down.
3. **Compared before the first write.** `replica.Open` compares `meta` with the ID, the number of
   peers and the cluster it was started with and returns `replica.ErrIdentity`, with both
   identities in the message, before it creates a folder, cuts the log or seals. A refused
   directory holds the bytes it held before; only the lock file can be new. The exit code is 2, as
   for a `LEDGER_MAX_SESSIONS` that does not fit the directory.
4. **Making a directory is a step of its own.** `ledgerctl init -id N -nodes N DIR` calls
   `replica.Init`, which takes only a directory without a trace of a node. It refuses one that
   holds `meta`, `wal/`, `snap/` or any file but the lock file and a leftover `meta.tmp`, so it
   does not put a new `meta` over an existing log. Besides the lock file it writes `meta` only,
   and it talks to no node. A crash at any of its disk accesses leaves a directory that `ledgerd`
   refuses and `init` makes again, or a finished one (`TestInitCrashAtEveryDiskAccess`).
5. **`init` is not repeatable.** On a directory with `meta` it exits with 2 and changes nothing,
   so a start script that runs it before every `ledgerd` fails at the second start.
6. **A recount compares the cluster only.** `replica.Recount` opens the copy of any node as a
   cluster of one and creates no `meta`; `verify dir` under another `LEDGER_CLUSTER` exits with 2.
7. **The simulator starts as before.** With `replica.Config.Create` set, `Open` creates what is
   missing as it always did and still compares what exists. Only the simulator and test helpers
   set it: every program opens a directory through `server.Open` or `Recount`, which do not. No
   write changed length or order, so the 80 pinned trace hashes stand (`TestTracesStayPinned`).

## What it does not close

Whether a node ever ran is written in its directory and nowhere else. Nothing here claims to know.

- **`init` for a node that has run.** `ledgerctl init` on an empty replacement succeeds and
  `ledgerd` starts on it: the voter without a memory, made by a command instead of by a start.
  The step is explicit and its output says that it is for a node that has never run, no more.
  `TestNodeInitialisedAgainLosesACommittedEntry` plays the schedule above on such a directory:
  node 3 leads, the confirmed transfer is not found, and node 1 halts with `raft.ErrSafety`. A
  script that swallows the refusal of a second `init` (`init || true`) has automated this.
- **An older copy put back.** It has the right identity and a valid `meta` and has forgotten every
  vote and acknowledgement since the copy. Nothing in a directory knows that it was once newer.
- **Copies of one directory on two machines** both start under the same ID.
- **Replacing a lost node.** That still needs membership changes (ADR-013).

## Consequences

- One more step before the first start, part of setting a node up and not of starting it. The demo
  and the chaos test take it through `internal/local`. The benchmark runner builds `ledgerctl` and
  runs `ledgerctl init`, because `bench/` imports only the public SDK (ADR-012).
- A `ledgerd` on an empty directory, or on one made for another node, cluster or number of nodes,
  ends with exit code 2 (`TestExitCodes` in `cmd/ledgerd`). Addresses are not recorded: a node
  moves with its directory as before (ADR-013).
- A directory from before this change is refused, not converted: its slots pass the checksum and
  name node 0, which is `wal.ErrVersion` (`TestMetaOfAnEarlierLayoutIsAnotherVersion`).
- The vote is one byte, enough for node IDs of 1 to 5 (`TestSaveRefusesAVoteThatDoesNotFit`).
- The path on which `Open` creates stays in the product, behind `Create`, and the simulator does
  not run a refused start. The tests of the packages and those with real processes do.

## Rejected alternatives

- **A file of its own.** Two more states, one file without the other, and rules for a flipped bit.
- **The header of the first WAL segment.** Segments are compacted, and an empty log has none.
- **A slot of 64 bytes, eight per field.** Easier to read, and `Open` would not have to create.
  In the simulator a write costs virtual time by its length, so every pinned trace would change,
  and with them the evidence that a running node behaves as before.
- **The unused bytes between the slots.** They share a 4 KiB page with the first slot; a torn page
  would leave the second slot with the vote and without the identity.
- **The identity as start value of the checksum.** No byte, but a foreign directory would look
  like two broken slots, and `ledgerd` could not say whose it is.
- **An `init` that succeeds on a directory it made.** A start script could run it before every
  start, and after a lost directory that script is the silent start again.
- **An `init` that asks the other nodes.** It would catch two of three still running and refuse
  the third node of a new cluster that has a leader. A switch past that helps the wrong caller too.
- **A number drawn at every `init`** and kept by the other nodes. It would close the second
  `init`, not the older copy, and it changes the wire format: part of membership changes (ADR-013).
