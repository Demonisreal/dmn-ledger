# ADR-009: Disk corruption: a seal on disk, repair as a non-voter

Status: accepted, 2026-11-13. Amended 2026-11-16: seed counts measured again.

## Context

Raft is only safe if a node never forgets a vote or an acknowledgement (ADR-004). A disk can make
it forget: a bit flips in a frame that was synced long ago, or a sector comes back as garbage. A
checksum finds the broken frame. It does not say whether the node had already acknowledged that
frame. A write torn by a power loss and a bit flip in the last synced batch look the same: the log
ends in a frame that fails its checksum.

Most logs cut such an end off and carry on. If the frame was acknowledged, the node has dropped an
entry it vouched for, does not know it and keeps voting. Together with a node that is merely
behind it can then elect a leader that lacks a committed entry. Alagappan et al. describe this
class of failures in "Protocol-Aware Recovery for Consensus-Based Storage" (FAST 2018). Their
answer is to tell a crash from corruption and to recover the damaged part from the other replicas,
using what the protocol knows. This ADR follows that idea, not their protocol.

## Decision

1. **A seal.** The file `meta` holds, besides term and vote, the seal: the highest index the node
   may have acknowledged. A round with entries is: append, fsync the log, write the seal, fsync
   `meta`, send. A leader takes the same steps before it counts its own log towards a majority.
   Before the log is cut below the seal, the seal is lowered, and an installed snapshot seals its
   index before the log is emptied. On start, a broken or missing frame up to the seal is damage.
   A broken frame after the seal, at the end of the log, is a torn write and is cut off. The seal
   is in a different file from the data it vouches for, so one bad sector never takes both.
2. **Two copies.** `meta` has two slots of 40 bytes in different 4 KiB pages, each with a sequence
   number and a CRC32C. Storing term, vote or the repair flag writes both slots, one after the
   other, each with its own fsync. Writing the seal writes one slot, the one that is not the
   newest. Both slots therefore agree on term, vote and flag, and a broken slot costs at most the
   seal of the last round. A node that found one slot invalid treats every broken end of the log
   as damage, because the seal it read may be one round old.
3. **Repair as a non-voter.** A node that finds damage on start stores a repair flag in `meta`
   before it deletes anything. Then it removes broken snapshot files and cuts the log back to its
   valid beginning. With the flag set it starts no election and grants no vote and no pre-vote. It
   stays a follower: it takes entries and snapshots and acknowledges them, and those
   acknowledgements count. It asks the leader for the leader's last index. Once it has
   acknowledged up to that index in that leader's term, the core clears the flag, one `Ready`
   after the entries were stored. The node votes again, and nobody had to step in.
4. **The leader believes a refusal.** A follower that refuses an append says where its log ends.
   The leader goes back to that index even when the follower had acknowledged more, and lowers
   what it counts for that follower. Otherwise it would send from the old position again and
   again, and the repair would never end.

The rules are R13 and R19 to R24 under "Rules" in [ARCHITECTURE.md](../ARCHITECTURE.md); the file
and the steps of a start are under "Files and start".

## Why it is safe

This is an argument, not a proof. It has three steps.

- **Nothing acknowledged is lost unnoticed.** An acknowledgement leaves a node only after the seal
  that covers it is synced. The seal goes down only before a cut that Raft itself asks for, or to
  the index of a snapshot that replaces the log. On start everything broken or missing up to the
  seal is damage, and with one slot invalid every broken end is. The flag is on disk before
  anything is deleted or sealed again. So a restarted node holds what it acknowledged, or the flag
  is set before it sends its first message. Term and vote are in both slots and survive one broken
  slot. `TestCrashAtEveryDiskAccess` cuts the power at every disk access of a three-node run,
  including during a start, with and without a flipped bit or a garbled sector afterwards, and
  checks exactly this. The run includes a follower that stores a new term and several entries in
  one round, loses the end of its log, starts in repair, loses power while in repair, is brought
  up to date and writes on. `FuzzOpenDamaged` does the same with several damaged places on the
  disk at one such point. A log that simply ends before the seal, with every frame it holds
  intact, is damage as well (`TestLogEndingBeforeTheSealIsDamage`).
- **A healed node has caught up with a leader.** The flag falls only when the node has
  acknowledged, in the current term, up to the index that the leader of this term named as its
  last. The node takes that answer only with the nonce it drew when the process started, so an
  answer to a probe from before the damage does not count (`TestRepairIgnoresOtherNonce`,
  `TestRepairCountsOnlyAcksOfTheTerm`). By log matching the node's log then equals that leader's
  up to the named index.
- **Every later leader still holds every committed entry.** Take a committed entry and the first
  later leader without it. Its voters and the majority that acknowledged the entry share a node.
  That node voted, so it was not in repair (`TestRepairingNodeDoesNotVote`). If it never lost the
  entry, this is the argument of the Raft paper. If it lost the entry, that was after it had
  acknowledged it to the leader that committed it, and it was healed after that: by the same
  leader, or by a later one that comes before the first without the entry. Both held the entry.
  So the node holds it again and does not vote for a log without it.

What a node acknowledged before the damage keeps counting on the leader. It was true when it was
sent, and until the node votes again every election has to succeed without it. What it
acknowledges while in repair it holds, under a new seal. Reads through ReadIndex only need to know
that no higher term has begun, and a node never forgets its term, so the answers of a node in
repair are valid confirmations for reads.

The core has a rule test for each of R19 to R24. `TestRandomSchedules` also cuts the end of a
node's log and restarts it in repair, one node at a time and any number at once, on three and five
cores.

## Consequences

- One more fsync per round with entries, on every node. `TestSealCostsOneSyncPerRound` pins the
  count: a transfer costs 2 fsyncs on a single node and 6 on three, one per node into `meta`,
  where the log alone needs 1 and 3. Idle ticks cost none. A transfer therefore waits for four
  fsyncs in sequence, two on the leader and two on a follower, where the log alone needs two.
- `BenchmarkProcessTransferDisk` runs one node on the real file system with one transfer per
  round and reports 2 fsync/op. The command is
  `TMPDIR=<dir> go test -run '^$' -bench ProcessTransferDisk -count 12 ./internal/replica/`,
  with `<dir>` on a disk and not on a tmpfs. What the second fsync costs in time cannot be
  measured from this repository alone, because the commit before the seal does not have the
  benchmark. It was measured with the same benchmark added to the tree before the seal, both
  programs run in turns on the NVMe disk (ext4) of an i5-12500H laptop: a transfer took between
  one and a half times and twice as long, the lower end on a quiet machine, the upper end with
  other work running. Only that ratio is worth quoting. The disk has two speeds: after some
  seconds of load an fsync takes several times as long. A clean measurement of latency with
  three nodes is still open.
- Whatever arrives while a node waits for its disk still shares the next round (ADR-002). The
  second fsync is paid per round, not per request.
- Slower rounds mean more retries from clients. A leader therefore drops the copy of a write that
  still waits for its entry instead of proposing it again. Without that rule retries become log
  entries and slow the rounds down further (`TestWaitingRequestIsProposedOnce`).
- One damaged node beside a healthy majority becomes a voter again on its own, and clients keep
  getting answers from the same leader meanwhile (`TestBitFlipInTheLastSealedBatchIsRepaired`).
- A node that reads a broken entry or snapshot while it runs stops. It never applies or sends what
  failed its checksum. The restart decides whether the disk is damaged.
- Every broken frame up to the seal is damage, even where a snapshot covers it. The node then
  drops the valid entries after the broken place and has them sent again. That costs bandwidth in
  a rare case and avoids a second code path that replaces segments in the middle of the log.
- A broken newest snapshot is replaced from the older one and the log where the log reaches back
  (ADR-010).
- The simulator runs corruption in two profiles. `rot` stays inside the fault model, with one
  damaged node at a time among three or five, and runs every checker, including that no node is
  left in repair at the end. `beyond` leaves the model, with several damaged nodes at once, damage
  to `meta` at any time and synced sectors that turn into garbage on a power loss. It checks
  safety and the answers clients got, not progress. Three checks belong to this decision: a node
  is in repair only where the plan damaged a disk (F2), a node in repair sends nothing a voter
  sends (S10), and after a power loss the log a node vouched for is there or the repair flag is
  (S5).
- Six planted bugs break the seal, the order of a start on a damaged disk and the repair rules
  (B13 to B18). The simulator finds five of them within 144 seeds from seed 0. The sixth, the
  commit index as the goal of a repair, it finds only after thousands of seeds, and a nonce that
  is not checked not at all; the tests of the core catch both at once. The table is under
  "Planted bugs" in [ARCHITECTURE.md](../ARCHITECTURE.md).
- Two of the repair rules are there for the argument, not because a failure is known without
  them: that a goal counts only in its term, and that the acknowledged index is forgotten on a
  term change. No schedule is known that breaks safety when either is dropped. They stay because
  the argument above needs only one leader's log with them.
- A disk that confirms an fsync and drops the data is outside the fault model. The seal catches
  part of it: when frames are gone and the seal is not, the log ends before the seal, and that is
  damage. When the disk drops the writes to `meta` as well, nothing notices.

## Limits

- Three nodes with one in repair cannot elect a new leader when the leader fails or cannot be
  reached. The cluster stops until the leader is back, and gives no wrong answer meanwhile. With
  five nodes the three healthy ones elect a leader (`TestRepairingNodeDoesNotHelpElect`). A
  leader that keeps its role is not affected: together with the node in repair it is a majority
  for commits, check-quorum and reads, even when the third node is unreachable
  (`TestRepairingNodeCountsForCommitAndReads`).
- Two damaged nodes of three can stop the cluster for good. If the third node still leads, it
  heals both. If nobody leads, nobody is elected, even with all three nodes running: the healthy
  node must not elect itself, because the other two may have committed an entry it never got. The
  cluster then accepts no requests, and it never gives a wrong answer
  (`TestTwoDamagedNodesOfThreeElectNoOne`). The code has no way out of that state.
- With both slots of `meta` unreadable the node does not start. It no longer knows its term and
  could vote twice in one term. One way there is a bit flip in the newest slot followed by a power
  loss in the very next write to `meta` (`TestMetaFlipThenTornWriteStopsTheNode`).
- A read error (EIO) stays fail-stop and is not repaired. It can be transient, and deleting data
  because of a transient error would be the wrong direction. A sector that stays unreadable makes
  every start fail; the node then counts as down.
- A cluster of one node does not start when it is damaged, and deletes nothing. There is no other
  node to repair it from (`TestSingleNodeRefusesToStartDamaged`).
- One fault can cost a voter although no data is missing. A latent bit flip in one slot of `meta`
  and an ordinary power loss are enough: with one slot invalid, the torn end of the log that the
  power loss leaves counts as damage. The node goes into repair and is a voter again once the
  leader has caught it up.
- The guarantee is for one fault on a node at a time. `FuzzOpenDamaged` covers several faults that
  are on the disk together at one start. Faults that arrive between several starts, with crashes
  in between, are not tested. One such sequence is known to leave a voter without frames it
  acknowledged: a flip in the newest slot and one in a frame after the older seal, a crash after
  the first write of the repair flag, a crash before the next start has sealed again, and a third
  flip in the slot that holds the flag.
- Safety does not rest on the assumption that a sector holding synced bytes survives a crash as
  either old or new. If such a sector turns into garbage, that is a broken frame up to the seal,
  so it is damage. Only availability rests on the assumption: without it an ordinary power loss can
  send a node into repair. The variants of `TestCrashAtEveryDiskAccess` with shredded sectors check
  the first half.
- Nothing scans data at rest. A latent fault is found at the next read or the next start, and a
  second node can take damage in between.
- Bit flips in memory or in pages that are not on disk yet, misdirected writes, a node with an
  empty disk in place of an old one and malicious nodes are not covered. Detection is as strong as
  CRC32C.

## Rejected alternatives

- **Cutting the end off and carrying on.** Simple, and wrong as soon as the frame was
  acknowledged.
- **Refusing to start on any damage.** Safe and a few lines of code. But there are no membership
  changes, so a node lost that way cannot be replaced, and one flipped bit shrinks the cluster for
  good.
- **The seal as a frame in the log.** Costs the same fsync, but lies in the same sector as the
  data it vouches for, or the next one. A bad sector takes both, and the end looks torn again.
- **Letting the next batch seal the previous one.** The sync mark in every frame does that already
  and stays as a second proof. It does not help the last round. Holding acknowledgements back by
  one round would need a filler frame and an fsync whenever the cluster goes idle.
- **Not counting a node in repair for commits.** Less available: three nodes with one in repair
  and one down could no longer commit. Not safer, because the node holds whatever it acknowledges
  after the damage.
- **The leader's commit index as the goal of a repair.** The node may have acknowledged an entry
  whose acknowledgement the leader has not seen yet. The commit index is then below that entry,
  and the node would count as healed without it. Planted as bug B16, this fails `TestRepairProbe`
  and the random schedules (`TestRuleTestsCatchTheCoreBugs`).
- **A third slot in `meta`.** Closes the one case named under "Limits" in which both slots end up
  unreadable. It costs three fsyncs for every change of term or vote and more cases on start, to
  avoid a node that does not start, not a wrong answer.
