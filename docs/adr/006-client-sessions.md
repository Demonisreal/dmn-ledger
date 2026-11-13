# ADR-006: Client sessions for exactly-once, transfer IDs as a second layer

Status: accepted, 2026-11-11.

## Context

A client sends a batch, the leader applies it, and the reply is lost. The client times out and
sends the same batch to the new leader. Transfer IDs catch the obvious case: the second attempt
fails with `exists`. They do not make the system linearizable. If transfer B failed on the first
attempt for lack of funds and succeeds on the retry, the batch has taken effect at two different
moments. A history checker rejects that, and so should a ledger.

## Decision

Exactly-once comes from sessions in the replicated state (Raft dissertation, section 6.3):

- A client registers first. The register entry's log index becomes its session number.
- Every request carries client ID, session number and a request number that grows by one per
  request, reads included. One request is in flight per session.
- Applying a request with the number already seen returns the stored reply. A lower number is
  `Stale`. An unknown session, or a known client with another session number, is `SessionUnknown`.
- The session table has a size limit. A full table evicts the session that was used longest ago.
  An evicted client gets `SessionUnknown` and the SDK returns `ErrEvicted`. It never registers again
  on its own, because the outcome of the request in flight is unknown.

Transfer IDs stay as a second layer for clients that restart and resubmit.

## Consequences

- The planted bug B11 (sessions do not recognise repeats) is found by the oracle replay S8 in
  the first seed of `all`, and by the log replay S11 in seed 2 of `evict`, where S8 cannot run.
- The session number closes a gap a plain client ID would leave: a register request that arrives
  late, after its session was evicted, creates a new session with a new number, and delayed
  requests of the old one get `SessionUnknown` instead of applying a second time.
- Sessions are part of every snapshot. The simulator's `evict` profile runs with fewer sessions
  than clients, starts clients late and lets them open new sessions, and checks that nobody
  receives another client's reply and that an evicted client gets `SessionUnknown`. The case the
  session number guards against does occur there, but its failure would hide among requests whose
  outcome is unknown; the scenario test `TestEvictionSurvivesSnapshotAndFailover` in
  `internal/replica` turns red without the number.

## Rejected alternatives

- **Transfer IDs only.** Not linearizable, see above.
- **Silent re-registration after eviction.** Breaks exactly-once for the request in flight.
- **Sessions with expiry by time.** Would put a clock into the state machine (ADR-005).
