# Ledger rules

This is the contract of the state machine in `internal/state`. The reference model in
`internal/oracle` implements the same rules independently, and the model test checks both against
each other. Every replica must reach the same result for the same batch, so the order of the checks
below is part of the contract, not an implementation detail.

## Accounts

| Field | Type | Notes |
|---|---|---|
| `ID` | u128 | not 0, not 2^128-1 |
| `DebitsPending`, `DebitsPosted`, `CreditsPending`, `CreditsPosted` | u128 | 0 on creation |
| `Ledger` | uint32 | not 0; transfers only move value inside one ledger |
| `Code` | uint16 | not 0; chosen by the user, e.g. account type |
| `Flags` | uint16 | see below |
| `Timestamp` | uint64 | 0 on creation, set by the state machine |

| Flag | Bit | Meaning |
|---|---|---|
| `DebitsMustNotExceedCredits` | 0 | `DebitsPosted + DebitsPending` must stay ≤ `CreditsPosted` |
| `CreditsMustNotExceedDebits` | 1 | `CreditsPosted + CreditsPending` must stay ≤ `DebitsPosted` |

Both flags together are invalid. Accounts cannot be linked.

## Transfers

| Field | Type | Notes |
|---|---|---|
| `ID` | u128 | not 0, not 2^128-1 |
| `DebitAccountID`, `CreditAccountID` | u128 | may be 0 on post/void, then taken from the pending transfer |
| `Amount` | u128 | 0 on post/void means the full pending amount |
| `PendingID` | u128 | only on post/void |
| `Timeout` | uint32 | seconds, only on pending transfers; 0 means no expiry |
| `Ledger` | uint32 | may be 0 on post/void |
| `Code` | uint16 | may be 0 on post/void |
| `Flags` | uint16 | see below |
| `Timestamp` | uint64 | 0 on creation, set by the state machine |

| Flag | Bit | Meaning |
|---|---|---|
| `Linked` | 0 | the next event in the batch belongs to the same chain |
| `Pending` | 1 | reserve the amount on `*Pending` instead of posting it |
| `PostPending` | 2 | post a pending transfer, fully or partially |
| `VoidPending` | 3 | release a pending transfer |

At most one of `Pending`, `PostPending`, `VoidPending`.

- **Normal:** debits and credits are posted at once.
- **Pending:** the amount goes to `DebitsPending` of the debit account and `CreditsPending` of the
  credit account. With `Timeout > 0` it expires at `Timestamp + Timeout * 10^9`.
- **Post:** refers to an open pending transfer via `PendingID`. The full pending amount leaves
  `*Pending`; `Amount` (or the full amount if 0) is added to `*Posted`. A smaller amount posts
  partially and releases the rest.
- **Void:** releases the full pending amount. `Amount` must be 0 or exactly the pending amount.

A stored post or void transfer carries the resolved values: accounts, ledger and code of the
pending transfer and the amount actually posted (for void: the pending amount).

## Batches and time

`Apply(op, batchTimestamp, body, dst)` takes one operation (`CreateAccounts` or `CreateTransfers`)
and a body of fixed-size big-endian records (96 bytes per account, 100 per transfer, fields in the
order of the tables above). Big-endian, so that comparing two IDs byte by byte gives the same order
as comparing them as numbers. It appends the failures to `dst` and returns it, so a caller that
reuses the slice gets through a batch without allocating, even when events fail.

The whole batch is rejected with `ErrMalformedBatch`, before anything changes, when:

- the op is unknown,
- the body is not a whole number of records, or
- a timestamp would reach 2^63: `batchTimestamp >= 2^63` or
  `max(batchTimestamp, last + 1) + max(n, 1) > 2^63` for `n` events. Below that bound neither the
  timestamps nor `Timestamp + Timeout * 10^9` (at most about 4.3·10^18) can overflow uint64. An
  empty batch counts as one event here because it moves `last` (step 1).

Otherwise:

1. `now = max(batchTimestamp, last + 1)`, and `last` becomes `now`. Event `i` gets the timestamp
   `now + i`, and `last` follows the last event. Timestamps are unique and strictly increasing, even
   when the leader's clock goes backwards. An empty batch hands out no timestamp to an event but
   still moves `last` to `now`, so every batch has its own point in time and no later event gets a
   timestamp before a sweep that already ran.
2. Pending transfers with `Timestamp + Timeout * 10^9 <= now` expire. Their amounts leave
   `*Pending`; they can no longer be posted or voided. This runs for empty batches too, which is how
   a quiet cluster lets deadlines pass. Expiring only releases amounts and nothing records it, so the
   order among transfers expiring in the same sweep is not part of the contract.
3. Events run in order. The result lists only the events that failed, as `(index, reason)` in index
   order; an empty list means every event succeeded.

A pending transfer is also treated as expired when a post or void runs at a timestamp at or after
its expiry, even if the sweep in step 2 has not removed it yet. Its amount stays reserved until the
next batch sweeps it, so a transfer later in the same batch is still checked against it.

### Chains

`Linked` joins an event with the next one. A chain ends with the first event without `Linked`.
A chain is atomic:

- If one event fails, every effect of the chain is rolled back. The failing event gets its own
  reason, all other events of the chain get `LinkedEventFailed`, and events after the failure are not
  evaluated.
- If the last event of the batch has `Linked`, the chain is open. None of its events are evaluated;
  all of them get `LinkedEventChainOpen`.

Events later in a chain see the effects of earlier ones, so a chain can create a pending transfer
and post it.

## Check order

The first failing check decides the reason.

### Create account

| # | Check | Reason |
|---|---|---|
| 1 | `ID` is 0 | `IDMustNotBeZero` |
| 2 | `ID` is 2^128-1 | `IDMustNotBeMax` |
| 3 | unknown flag bits | `FlagsUnknown` |
| 4 | both limit flags | `FlagsAreMutuallyExclusive` |
| 5 | `Ledger` is 0 | `LedgerMustNotBeZero` |
| 6 | `Code` is 0 | `CodeMustNotBeZero` |
| 7 | a balance is not 0 | `BalancesMustBeZero` |
| 8 | `Timestamp` is not 0 | `TimestampMustBeZero` |
| 9 | `ID` exists with other `Ledger`, `Code` or `Flags` | `ExistsWithDifferentFields` |
| 10 | `ID` exists | `Exists` |

### Create transfer: normal or pending

| # | Check | Reason |
|---|---|---|
| 1 | `ID` is 0 | `IDMustNotBeZero` |
| 2 | `ID` is 2^128-1 | `IDMustNotBeMax` |
| 3 | unknown flag bits | `FlagsUnknown` |
| 4 | more than one of `Pending`, `PostPending`, `VoidPending` | `FlagsAreMutuallyExclusive` |
| 5 | `DebitAccountID` is 0 | `DebitAccountIDMustNotBeZero` |
| 6 | `CreditAccountID` is 0 | `CreditAccountIDMustNotBeZero` |
| 7 | both account IDs are equal | `AccountsMustBeDifferent` |
| 8 | `PendingID` is not 0 | `PendingIDMustBeZero` |
| 9 | `Timeout` without `Pending` | `TimeoutRequiresPending` |
| 10 | `Ledger` is 0 | `LedgerMustNotBeZero` |
| 11 | `Code` is 0 | `CodeMustNotBeZero` |
| 12 | `Amount` is 0 | `AmountMustNotBeZero` |
| 13 | `Timestamp` is not 0 | `TimestampMustBeZero` |
| 14 | debit account does not exist | `DebitAccountNotFound` |
| 15 | credit account does not exist | `CreditAccountNotFound` |
| 16 | accounts are on different ledgers | `AccountsMustHaveSameLedger` |
| 17 | transfer is on another ledger than the accounts | `TransferMustHaveSameLedgerAsAccounts` |
| 18 | `ID` exists with any other field (timestamp aside) | `ExistsWithDifferentFields` |
| 19 | `ID` exists | `Exists` |
| 20 | `DebitsPosted + DebitsPending + Amount` of the debit account overflows u128 | `OverflowsDebits` |
| 21 | `CreditsPosted + CreditsPending + Amount` of the credit account overflows u128 | `OverflowsCredits` |
| 22 | debit account has `DebitsMustNotExceedCredits` and its debits would exceed `CreditsPosted` | `ExceedsCredits` |
| 23 | credit account has `CreditsMustNotExceedDebits` and its credits would exceed `DebitsPosted` | `ExceedsDebits` |

### Create transfer: post or void

Checks 1–4 as above, then:

| # | Check | Reason |
|---|---|---|
| 5 | `PendingID` is 0 | `PendingIDMustNotBeZero` |
| 6 | `Timeout` is not 0 | `TimeoutRequiresPending` |
| 7 | `Timestamp` is not 0 | `TimestampMustBeZero` |
| 8 | `ID` exists with another `PendingID`, or with other fields once the zero fields are filled from the pending transfer | `ExistsWithDifferentFields` |
| 9 | `ID` exists | `Exists` |
| 10 | pending transfer does not exist | `PendingTransferNotFound` |
| 11 | referenced transfer is not a pending transfer | `PendingTransferNotPending` |
| 12 | `DebitAccountID` is set and differs | `PendingTransferHasDifferentDebitAccount` |
| 13 | `CreditAccountID` is set and differs | `PendingTransferHasDifferentCreditAccount` |
| 14 | `Ledger` is set and differs | `PendingTransferHasDifferentLedger` |
| 15 | `Code` is set and differs | `PendingTransferHasDifferentCode` |
| 16 | pending transfer was posted | `PendingTransferAlreadyPosted` |
| 17 | pending transfer was voided | `PendingTransferAlreadyVoided` |
| 18 | pending transfer expired | `PendingTransferExpired` |
| 19 | post: `Amount` is greater than the pending amount | `ExceedsPendingTransferAmount` |
| 20 | void: `Amount` is neither 0 nor the pending amount | `PendingTransferHasDifferentAmount` |

Post and void cannot overflow or break a limit: they only move value from pending to posted on the
same side, or release it.

## Reasons

The numbers are fixed. New reasons are only appended.

| Number | Name |
|---|---|
| 0 | `OK` (never reported) |
| 1 | `LinkedEventFailed` |
| 2 | `LinkedEventChainOpen` |
| 3 | `IDMustNotBeZero` |
| 4 | `IDMustNotBeMax` |
| 5 | `FlagsUnknown` |
| 6 | `FlagsAreMutuallyExclusive` |
| 7 | `LedgerMustNotBeZero` |
| 8 | `CodeMustNotBeZero` |
| 9 | `BalancesMustBeZero` |
| 10 | `TimestampMustBeZero` |
| 11 | `ExistsWithDifferentFields` |
| 12 | `Exists` |
| 13 | `DebitAccountIDMustNotBeZero` |
| 14 | `CreditAccountIDMustNotBeZero` |
| 15 | `AccountsMustBeDifferent` |
| 16 | `PendingIDMustBeZero` |
| 17 | `PendingIDMustNotBeZero` |
| 18 | `TimeoutRequiresPending` |
| 19 | `AmountMustNotBeZero` |
| 20 | `DebitAccountNotFound` |
| 21 | `CreditAccountNotFound` |
| 22 | `AccountsMustHaveSameLedger` |
| 23 | `TransferMustHaveSameLedgerAsAccounts` |
| 24 | `PendingTransferNotFound` |
| 25 | `PendingTransferNotPending` |
| 26 | `PendingTransferHasDifferentDebitAccount` |
| 27 | `PendingTransferHasDifferentCreditAccount` |
| 28 | `PendingTransferHasDifferentLedger` |
| 29 | `PendingTransferHasDifferentCode` |
| 30 | `PendingTransferAlreadyPosted` |
| 31 | `PendingTransferAlreadyVoided` |
| 32 | `PendingTransferExpired` |
| 33 | `ExceedsPendingTransferAmount` |
| 34 | `PendingTransferHasDifferentAmount` |
| 35 | `OverflowsDebits` |
| 36 | `OverflowsCredits` |
| 37 | `ExceedsCredits` |
| 38 | `ExceedsDebits` |

## Lookups

`LookupAccounts(ids, dst)` and `LookupTransfers(ids, dst)` append the records found, in the order of
`ids`, to `dst`. Unknown IDs are skipped; every record carries its ID, so the caller can match them.
`Last` returns `last`, the point in time of the latest batch.

## Invariants

No transfer is ever deleted, so the whole state follows from the stored transfers. `CheckInvariants`
recomputes it after any batch and fails on the first difference:

- `last < 2^63`, no undo entries left, every account and transfer reachable through its index
- every account passes the field checks of its creation (ID, flags, ledger, code), posted plus
  pending fits into u128 on both sides, and the limit flag holds
- per ledger: Σ `DebitsPosted` = Σ `CreditsPosted` and Σ `DebitsPending` = Σ `CreditsPending`
  (summed without wrap-around)
- every transfer passes the field checks of its creation; both accounts exist, differ and are on
  the transfer's ledger; only pending transfers have a status
- an expired transfer has a timeout and its deadline is at most `last`
- every post or void points to a pending transfer with the matching status, the same accounts and
  code, an amount it may take (post: at most the pending amount, void: exactly), and a timestamp
  after the pending transfer and before its deadline; every posted or voided pending transfer has
  exactly one
- the expiry queue holds exactly the open pending transfers with a timeout, at their recorded
  position and in heap order
- timestamps of accounts and transfers are not 0, unique and at most `last`
- per account: all four balances equal the sums of its transfers (posted: normal transfers and
  posts; pending: open pending transfers)

## Snapshot and digest

`WriteSnapshot` writes the whole state; `ReadSnapshot` restores it. The bytes depend only on the state,
not on the order in which it was built:

1. version byte `1`
2. `last` as uint64
3. the number of accounts as uint64, then every account record in ID order
4. the number of transfers as uint64, then every transfer record in ID order, each followed by one
   status byte: 0 none, 1 pending, 2 posted, 3 voided, 4 expired

All integers are big-endian. Indexes and the expiry queue are not stored; they follow from the
records. `ReadSnapshot` rejects with `ErrBadSnapshot` every input that `WriteSnapshot` cannot have
produced: another version, records out of ID order, transfers on unknown accounts, missing or
trailing bytes, and any state that fails `CheckInvariants`. Since the invariants recompute the
whole state from the transfers, every rule of this document that the final state can show is
checked. What only the history could show is not: the snapshot does not record batch boundaries,
so neither the limit flags at each step in between nor the members of a chain can be replayed. An
error of the reader itself is returned as it is, not as `ErrBadSnapshot`, so the caller can tell a broken file
from a failing disk.

`Digest` is SHA-256 over the snapshot bytes. Two replicas with the same digest hold the same
accounts, transfers, pending states and `last`, so they hand out the same timestamps from then on.
