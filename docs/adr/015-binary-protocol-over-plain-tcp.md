# ADR-015: A binary protocol of its own over plain TCP, in a trusted network

Status: accepted, 2026-11-16.

## Context

A node talks to two kinds of peers. Clients send requests and ask for the status; other nodes send
raft messages and snapshots. Both arrive over sockets, so bytes from outside reach the process:
whoever reaches a port can send anything, of any length, as slowly as they like.

The node defines the bodies already. Records are fixed-size and big endian (ledger-rules), and a
log entry body is what the leader appends. The simulator has to run the same encoding, with no
goroutines and no clock, or the simulated cluster would speak another protocol than the real one.

## Decision

- **A frame of its own.** A header of 24 bytes with magic, version, type, cluster ID, body length
  and a CRC32C over header and body, then the body. Records and entry bodies travel byte for byte
  as in the log. The layouts are under "Wire format" in [ARCHITECTURE.md](../ARCHITECTURE.md).
- **Two ports.** Clients and peers have separate listeners, separate limits (1 MiB and 42 bytes on
  the client port, 4 MiB on the peer port) and separate frame types. A firewall can open the peer
  port only between the nodes, and a bug on the client path never reaches the code that takes raft
  messages.
- **Every length is checked before anything is allocated.** A decoder accepts exactly what an
  encoder writes, so a decoded frame encodes back to the same bytes. There are no error frames:
  any error closes the connection, and the node logs a line.
- **The cluster ID is in every frame.** It keeps two clusters from mixing, two demos on one machine
  or a test against production, and a client from writing to the wrong cluster. It is no secret
  and it is not stored: a node started with the wrong ID reaches nobody and never leads.
- **The version is in every frame** and goes up with every change to a layout. All nodes of a
  cluster run the same version; switching a running cluster to another one is not supported.
- **No TLS and no authentication.** Both ports belong in a network one trusts. The client port is a
  database port without a password, the peer port belongs to the nodes. This is a stated limit,
  not an oversight. The seam is in place: `server.Config` takes ready listeners and a dial
  function, so TLS for the transport needs no change to the protocol. Who may do what would still
  be open.
- **Node keys on first contact.** The status frame carries the public key of each node, and
  `ledgerctl status -keys` prints them as the list a verifier of receipts trusts (ADR-011). That
  list is only as good as the network it came over.

## Consequences

What the server does against a stranger who reaches a port:

| Attack | Countermeasure | What remains |
|---|---|---|
| sending garbage | magic, version, cluster ID, checksum; the first error closes the connection | |
| a large length in a header | a limit per port before any allocation (`ErrTooLarge`) | |
| a large count in `MsgApp` | count × 13 bytes against the rest, before the entries are allocated | a peer that does not follow raft decodes a 4 MiB frame of empty entries into about 15 MiB |
| starting a frame and leaving it | the first frame on the client port within 5 s, every body within 10 s of its header, the hello on the peer port within 2 s | a client connection that sent one valid frame may stay idle and keep its slot |
| taking every client slot | `LEDGER_MAX_CLIENTS`, the first frame within 5 s | whoever sends valid frames can hold every slot; only the network helps |
| holding the peer port before the hello | only IPs from `LEDGER_PEERS`; the hello read with its own limit of 25 bytes; at most four connections per peer, all peers together, may wait for their hello | from an allowed IP a stranger can hold those places for 2 s at a time and delay a peer that reconnects |
| not reading | 4 frames queued per client connection, 5 s per write, then the connection is closed | |
| many lookups or receipts | a read budget of 8 MiB, over the answers waiting and over the work of a round; beyond it `Busy` | one receipt of a batch that swept many expirations stays expensive |
| filling memory | a limit on every queue | about 2 GiB above the state with the defaults, computed in "Memory in the worst case" in [ARCHITECTURE.md](../ARCHITECTURE.md); it grows with `LEDGER_MAX_CLIENTS` |
| many client IDs | one per connection | |
| evicting sessions | | `MaxSessions` new registrations push every real session out. Their clients learn it (`ErrEvicted`); nothing applies twice |
| writing transfers, growing the state | | yes. The client port is a database port without a password |
| posing as a node on the peer port | only IPs from `LEDGER_PEERS`; the hello names cluster ID, both node IDs and `MaxSessions` | whoever sends from one of those IPs, or sits in the network between, can vote and append entries. Malicious nodes are outside the fault model |
| frames for the other port | client types on the peer port and peer types on the client port are format errors | |
| a snapshot without end, with a gap or cut short | 8 GiB, one transfer per peer at a time, `SnapshotEnd` with length and checksum | |
| asking for the status in a loop | the status is O(1): the apply hash and the head, never a digest of the state | |
| reading or changing frames on the wire | the checksum finds accidents only | everything, without TLS. A receipt carries its own signature, and a changed one fails to verify |

- The simulator runs the same codec for every message it delivers, so every encoder and decoder
  runs under every fault the simulator plays, and a message that does not survive the round trip
  is a finding. A corrupted byte on a real link is a closed connection, which the simulator
  already plays as lost messages.
- Fuzz targets read random bytes as frames, as bodies without a header, as raft messages and as a
  snapshot stream, and a pinned file of frames shows every layout change.
- Every node reads at most 4 MiB per peer frame and 1 MiB and 42 bytes per client frame, whatever
  the peer claims; a frame the encoder would make larger is a bug and stops the node.

## Rejected alternatives

- **gRPC, or HTTP with JSON or protobuf.** Dependencies, and the simulator would have to run them
  too. Frames of fixed fields are a few hundred lines and need nothing.
- **TLS, half built.** Mutual TLS between nodes answers "who is a node". Clients would also need
  certificates or tokens and an answer to "who may do what". Half of that is worse than a limit
  that is stated.
- **One connection per pair of nodes for both directions.** It needs a rule for which of two
  connections opened at the same moment wins. With one per direction, whoever wants to send dials.
- **Snapshots on the message connection.** Heartbeats would wait behind the chunks in the send
  buffer and the follower could stop counting for check-quorum (R11). A snapshot gets a connection
  of its own.
- **A checksum over the header alone, as in the WAL.** A flipped length makes the reader wait for
  bytes that never come, and the read deadline ends that. Against attackers the limits help, not a
  checksum.
