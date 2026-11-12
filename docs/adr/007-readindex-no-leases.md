# ADR-007: Reads through ReadIndex, no leases, only the leader reads

Status: accepted.

## Context

Lookups must be linearizable: a read that starts after a transfer was confirmed must see it. A
leader that was cut off and replaced does not know that it is no longer leader, and could answer
from an old state.

## Decision

Reads go through ReadIndex. The leader notes its commit index, sends one heartbeat round, and
answers once a majority has confirmed it is still leader in its term and its state has caught up
to the noted index. All reads collected during one loop iteration share one round. A new leader
answers reads only after it has committed an entry of its own term. Reads are not written to the
log and need no session.

## Consequences

- Linearizable reads without trusting any clock and without a log entry per read. The price is a
  round trip per batch of reads.
- Followers do not serve reads.
- The planted bug B10 (answer without the majority round) is hard for the simulator to find.
  Check-quorum makes a cut-off leader step down within two election timeouts, and a client needs
  about as long to reach the new leader. A stale read only fits into the small overlap. The rule
  tests catch B10 directly (`TestReadConfirmedByQuorum`, `TestDeposedLeaderGetsNoRead`).

## Rejected alternatives

- **Leases.** Save the round trip, but rest on bounded clock drift, which the system trusts nowhere
  else (ADR-005).
- **Reads through the log.** Simple and obviously correct, but every read costs an fsync on a
  majority and grows the log and the snapshot.
- **Follower reads with ReadIndex from the leader.** Spreads load. Not needed at this size; kept as
  an extension.
