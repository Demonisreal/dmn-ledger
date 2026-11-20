# Security

dmn-ledger is a portfolio project. Nobody runs it in production, and it is not meant to be.

## What is known

`ledgerd` speaks plain TCP without TLS and without authentication, on purpose: whoever reaches the
client port can read and write the ledger. The decision and what a stranger at a port can still do
are in [ADR-015](docs/adr/015-binary-protocol-over-plain-tcp.md). What the audit trail does not
prove is in [ADR-011](docs/adr/011-audit-trail.md), the threat model of the public demo with what
remains in every row is in
[ADR-014](docs/adr/014-public-demo-fixed-faults-no-docker-socket.md), and everything that is not
built is listed in [ADR-013](docs/adr/013-non-goals.md). None of that needs a report.

## What is worth a report

Anything those documents do not name, for example:

- a request that makes the demo page show something that is not happening, removes a finding, or
  reaches a file or a process outside the three nodes;
- input on the client or the peer port that crashes a node, makes it allocate without bound or
  leaves a data directory that does not restart;
- a receipt that `ledgerctl verify proof` accepts although it was changed, or links that
  `ledgerctl verify chain` reports as connected although they are not;
- a schedule of faults within the fault model (crashes, lost or reordered messages, partitions,
  wrong clocks, a disk that returns broken data) after which an acknowledged transfer is lost or
  applied twice.

## How to report

Privately, through "Report a vulnerability" on the Security tab of the repository on GitHub.
Please include the commit, the steps and, for the simulator, the seed and the profile. There is no
bounty.
