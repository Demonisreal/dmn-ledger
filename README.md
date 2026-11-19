# dmn-ledger

A replicated double-entry ledger in Go: accounts and transfers, pending transfers that are posted,
voided or expire, and linked chains that succeed or fail as a whole. The ledger is kept on three or
five nodes with its own Raft implementation and its own write-ahead log.

The aim: no acknowledged transfer is lost or applied twice, and the books always balance, as long
as a majority of nodes is up. That has to hold when nodes crash in the middle of a write, when
messages are lost, delayed or reordered, when the network splits, when clocks are wrong and when
a disk returns broken data. Today this is checked, not proven: a deterministic simulator runs the
real node code against those faults and checks every run. The same code runs as a server,
`ledgerd`, with a Go client and a command-line tool, and a chaos test runs three such processes
under `kill -9`, frozen processes and a faulty network.

A node that finds a flipped bit or a garbled sector in data it had acknowledged stops voting
until the leader has brought it up to date again. It still counts for commits and reads, so the
current leader keeps working with it. With three nodes, one damaged node and an unreachable
leader stop the cluster until the leader is back. Two damaged nodes of three can stop it for
good. In both cases it accepts no requests, but it never gives a wrong answer.

Every stored transfer has a receipt that can be checked afterwards. The state machine keeps a hash
chain over its batches, and the leader hands out a receipt for a transfer, stamped with its own
key, that anyone can check without access to the cluster.

## Status

Works today:

- State machine with the ledger rules in [docs/ledger-rules.md](docs/ledger-rules.md), tested
  against a second, deliberately plain implementation.
- Write-ahead log with segments, checksums and torn-write handling, a metadata file with term,
  vote and a seal, and snapshots.
- Raft core without I/O: pre-vote, check-quorum, ReadIndex, snapshots for lagging followers.
- Repair after disk corruption. A seal on disk tells a torn write from a broken frame the node
  had acknowledged. A damaged node runs as a non-voter, is caught up by the leader and votes
  again, without anyone stepping in. Tested with a power loss at every disk access of a
  three-node run, with randomised schedules of the core and in the simulator. Read errors still
  stop a node, and the limits are in [ADR-009](docs/adr/009-disk-corruption-seal-and-repair.md).
- Node layer: one loop per node, client sessions for exactly-once, linearizable reads through
  ReadIndex, timestamps that come from the log.
- Server and wire protocol: `ledgerd` drives the same node over TCP and the real clock, with one
  place that sends, after the disk, and snapshots streamed on a connection of their own. The
  simulator sends every message through the same codec.
- The Go SDK (package `ledger`): retries across nodes with exactly-once, bundling of concurrent
  writes, and errors that tell a write that was not sent from one whose outcome is unknown.
- `ledgerctl`: status of every node, accounts, transfers, keys, receipts and their checks.
- Audit trail: a hash chain over batches, a Merkle tree per batch, receipts stamped with Ed25519,
  connecting two heads, and recounting a data directory against a head kept earlier. Test vectors
  check the same receipts in Go and with WebCrypto in Node.
- Simulator with network, disk and clock faults and disk corruption, checkers for safety,
  linearizability, progress and the audit trail, and twenty-two planted bugs behind a build tag.
- Chaos test with three real processes under `SIGKILL`, `SIGSTOP`, cut, stalled and lossy links,
  judged by the same replay and linearizability check as the simulator. It needs Linux, the build
  tag `chaos` and real time, so it runs by hand or from `scripts/nightly-chaos.sh`, not in CI.
- A demo, `cmd/demo`: one program that runs three `ledgerd` as its children with a small load and
  serves a page that shows them. A visitor can kill a node, freeze it, cut it off or flip a bit
  in its log; the program takes every fault back itself, counts the whole ledger afterwards and
  has every answer judged by the checker of the chaos test. A receipt is checked in the browser.
  It runs locally and in a local container (see "The demo" below).

Not built yet:

- TLS and authentication.
- Published benchmark numbers. The plan that measures a cluster of processes on a real disk, with
  Postgres next to it, is in `bench/`; its results are not in the repository yet, and a real
  network is not measured.
- A public instance of the demo. It is not deployed anywhere, and the files for that in
  `deploy/demo` have only run in part; see "The container" in
  [ADR-014](docs/adr/014-public-demo-fixed-faults-no-docker-socket.md).
- Membership changes, leadership transfer and reads from followers.
- Signatures of a majority, revocation of keys, a witness outside the cluster.

## Try it

From the root of the repository, in bash: build the two programs, make a key for each node and
start three nodes on loopback, with client ports 7101 to 7103 and peer ports 7201 to 7203.

```
$ dir=$(mktemp -d)
$ go build -o "$dir" ./cmd/ledgerd ./cmd/ledgerctl
$ export PATH=$dir:$PATH
$ cd "$dir"
$ for i in 1 2 3; do ledgerctl key new n$i.key; done
7f34e6432538e7bf669a80216dbbf6c1797bc475cc30c27d22876c532f20840e
cd020782875b156cacdfc60528a91de91a6d35308aa12f2f6c7b64f050a48f16
7cc2ea66d9ab38b251bb3ff1532bdf3004656ce97a8aa77012923bf6708ef778
$ peers=127.0.0.1:7201,127.0.0.1:7202,127.0.0.1:7203
$ for i in 1 2 3; do
    LEDGER_ID=$i LEDGER_CLUSTER=1 LEDGER_PEERS=$peers LEDGER_CLIENT_ADDR=127.0.0.1:710$i \
      LEDGER_DIR=$dir/n$i LEDGER_KEY_FILE=$dir/n$i.key ledgerd 2>n$i.log &
    pid[$i]=$!
  done
$ sleep 2
```

`ledgerd` takes its configuration from `LEDGER_` variables only and refuses any it does not know,
so the variables for `ledgerctl` are set after the start:

```
$ export LEDGER_CLUSTER=1 LEDGER_NODES=127.0.0.1:7101,127.0.0.1:7102,127.0.0.1:7103
$ ledgerctl status
node  address         role      term  commit  applied  apply hash        head                repair  ledger time  clock  next expiry
1     127.0.0.1:7101  follower  1     1       1        b976e321401b66f3  0:0000000000000000  no      -            +0ms   -
2     127.0.0.1:7102  follower  1     1       1        b976e321401b66f3  0:0000000000000000  no      -            +0ms   -
3     127.0.0.1:7103  leader    1     1       1        b976e321401b66f3  0:0000000000000000  no      -            +0ms   -
nodes agree at index 1
$ ledgerctl account create -id 1 -ledger 1 -code 10
ok 1 at 2026-11-16T09:48:23.481697531Z
$ ledgerctl account create -id 2 -ledger 1 -code 10
ok 2 at 2026-11-16T09:48:23.510605650Z
$ ledgerctl transfer create -id 10 -debit 1 -credit 2 -amount 500 -ledger 1 -code 1
ok 10 at 2026-11-16T09:48:23.539407057Z
$ ledgerctl account get 1 2
id=1 ledger=1 code=10 flags=0 debits_pending=0 debits_posted=500 credits_pending=0 credits_posted=0 timestamp=2026-11-16T09:48:23.481697531Z
id=2 ledger=1 code=10 flags=0 debits_pending=0 debits_posted=0 credits_pending=0 credits_posted=500 timestamp=2026-11-16T09:48:23.510605650Z
```

`head` is the number of the last link of the audit trail and the start of its hash; `clock` is how
far a node's wall clock is from the caller's. Now kill the leader, here node 3, without warning
and book on:

```
$ leader=$(ledgerctl status | awk '$3 == "leader" {print $1}')
$ kill -9 ${pid[$leader]}
$ ledgerctl transfer create -id 11 -debit 1 -credit 2 -amount 250 -ledger 1 -code 1
ok 11 at 2026-11-16T09:48:24.117697923Z
$ ledgerctl status
node  address         role      term  commit  applied  apply hash        head                repair  ledger time                     clock  next expiry
1     127.0.0.1:7101  follower  2     10      10       364a747ea0e23be0  3:9c4df04a85671fb2  no      2026-11-16T09:48:23.539407057Z  +0ms   -
2     127.0.0.1:7102  leader    2     11      11       8223cbcf9076f709  4:1f3501e41522b668  no      2026-11-16T09:48:24.117697923Z  +0ms   -
3     127.0.0.1:7103  unreachable: dial tcp 127.0.0.1:7103: connect: connection refused
no two nodes at the same index
```

Node 2 won the election in term 2, and the client found the new leader on its own. Node 1 had not
applied the last transfer yet when it was asked, so no two nodes could be compared;
`ledgerctl status` exits with 1 here because node 3 does not answer. Start node 3 again on its old directory:

```
$ env -u LEDGER_NODES LEDGER_ID=$leader LEDGER_PEERS=$peers LEDGER_CLIENT_ADDR=127.0.0.1:710$leader \
    LEDGER_DIR=$dir/n$leader LEDGER_KEY_FILE=$dir/n$leader.key ledgerd 2>>n$leader.log &
$ pid[$leader]=$!
$ sleep 1
$ ledgerctl status
node  address         role      term  commit  applied  apply hash        head                repair  ledger time                     clock  next expiry
1     127.0.0.1:7101  follower  2     11      11       8223cbcf9076f709  4:1f3501e41522b668  no      2026-11-16T09:48:24.117697923Z  +0ms   -
2     127.0.0.1:7102  leader    2     11      11       8223cbcf9076f709  4:1f3501e41522b668  no      2026-11-16T09:48:24.117697923Z  +0ms   -
3     127.0.0.1:7103  follower  2     11      11       8223cbcf9076f709  4:1f3501e41522b668  no      2026-11-16T09:48:24.117697923Z  +0ms   -
nodes agree at index 11
```

Then fetch a receipt for transfer 10 and check it offline against the node keys. The status frame
carries each node's public key, and `status -keys` writes them as a list:

```
$ ledgerctl status -keys > cluster.keys
$ ledgerctl proof transfer 10 -o t10.proof
wrote 364 bytes to t10.proof
$ ledgerctl verify proof t10.proof -keys cluster.keys
file=t10.proof kind=transfer id=10 timestamp=2026-11-16T09:48:23.539407057Z time=2026-11-16T09:48:23.539407057Z seq=3 head=9c4df04a85671fb20ff330db51a79d98ba95b98ac2b36831f435653874307490 key=cd020782875b156cacdfc60528a91de91a6d35308aa12f2f6c7b64f050a48f16
```

Node 2 stamped it: link 3 of the chain, the batch of transfer 10, has this head. Keep a head, book
on, keep another, and connect the two over the links between them:

```
$ ledgerctl head -o old.head
wrote 146 bytes to old.head
$ ledgerctl transfer create -id 12 -debit 2 -credit 1 -amount 100 -ledger 1 -code 1
ok 12 at 2026-11-16T09:48:25.222160410Z
$ ledgerctl head -o new.head
wrote 146 bytes to new.head
$ ledgerctl verify chain old.head new.head -keys cluster.keys
connected: head 4 1f3501e41522b668 leads to head 5 2c46727a9cd5cd6f over 1 links
```

Last, stop a follower and recount its data directory against the head kept last. `verify dir`
works on a copy in memory and refuses a directory that a running node holds:

```
$ follower=$(ledgerctl status | awk '$3 == "follower" {print $1; exit}')
$ kill ${pid[$follower]}
$ wait ${pid[$follower]}
$ ledgerctl verify dir n$follower -head new.head -keys cluster.keys
recounted from snapshot 0: 5 links, committed for sure up to link 0
the directory leads to head 5 2c46727a9cd5cd6f9e6cd59c8f764525a17e24a003621d8bb80d221f3c9fdef2; 0 links after it are not checked
$ kill ${pid[@]} 2>/dev/null; wait
```

With the default `LEDGER_SNAPSHOT_EVERY` of 100 000 entries this short run has no snapshot yet, so
the recount starts from the beginning of the log, and no link is covered by a snapshot ("committed
for sure up to link 0"). On a node with snapshots it counts from the newest one and again from the
older one, if the log reaches back to it.

`ledgerctl -h` lists all commands, among them `transfer post` and `transfer void` for pending
transfers and `proof account|expiry|batch`.

### The SDK

The same steps in Go, with the cluster from above. This is `Example` in
[ledger/example_test.go](ledger/example_test.go), compiled by every `go test`:

```go
ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()

c, err := ledger.Dial(ctx, ledger.Config{
	Cluster: 1,
	Nodes:   []string{"127.0.0.1:7101", "127.0.0.1:7102", "127.0.0.1:7103"},
})
if err != nil {
	log.Fatal(err)
}
defer c.Close()

_, err = c.CreateAccounts(ctx, []ledger.Account{
	{ID: ledger.From64(1), Ledger: 1, Code: 10},
	{ID: ledger.From64(2), Ledger: 1, Code: 10},
})
if err != nil {
	log.Fatal(err)
}

pay := []ledger.Transfer{{
	ID:              ledger.NewID(),
	DebitAccountID:  ledger.From64(1),
	CreditAccountID: ledger.From64(2),
	Amount:          ledger.From64(500),
	Ledger:          1,
	Code:            1,
}}
res, err := c.CreateTransfers(ctx, pay)
switch {
case errors.Is(err, ledger.ErrUncertain):
	log.Fatal("sent, but no answer in time: send the same transfers again, they keep their IDs")
case err != nil:
	log.Fatal(err)
}
for _, f := range res.Failures {
	fmt.Println("transfer", f.Index, "failed:", f.Reason)
}
```

Whoever sends the same transfers again after `ErrUncertain` gets `exists` for every one that had
already applied. Any other error means the write was not applied, or, after `ErrEvicted`, that
the client is finished and has to be dialed again.

### The simulator

Run 1000 seeds of the simulator, each a full cluster with faults and a workload:

```
$ go run ./cmd/sim -seeds 1000
seeds 0 to 999 profile all: ok, widest window 32, 18.598s on 4 cores
```

Plant a bug, here B15 (a node whose disk has lost acknowledged entries still takes part in
elections), and let the simulator look for it in `rot`, the profile with disk corruption:

```
$ go run -tags simbug ./cmd/sim -bug B15 -seeds 100 -profile rot
seed 11 profile rot failed: seed 11 at 3806635025: S10 on n2: sent PreVote to n1 while in repair
reproduce: go run -tags simbug ./cmd/sim -bug B15 -seed 11 -profile rot -trace
exit status 1
```

Checker S10 saw node 2 ask for a pre-vote while it was in repair, 3.8 virtual seconds into the
run. The printed command replays exactly that run and writes every event, here about 7500 lines.
They show how the node got there:

```
$ go run -tags simbug ./cmd/sim -bug B15 -seed 11 -profile rot -trace > b15.txt
exit status 1
$ grep -n -e 'rot n2' -e 'start n2' b15.txt | tail -n 2
6942:3467786555 rot n2 /ledger/wal/wal-00000000000000000058.log 7664 bit
7218:3648150370 start n2
$ tail -n 3 b15.txt
3806635025 tick n2
seed 11 profile rot failed: seed 11 at 3806635025: S10 on n2: sent PreVote to n1 while in repair
reproduce: go run -tags simbug ./cmd/sim -bug B15 -seed 11 -profile rot -trace
```

At 3.47 s the fault plan cut the power of node 2 and flipped a bit in one of its WAL segments. The
node started again at 3.65 s, in repair. At 3.81 s its election timeout ran out, and with the
planted bug it asked for votes.

`go run ./cmd/sim -h` lists the flags and the fault profiles: `calm`, `net`, `crash`, `clock`,
`all`, `evict`, `leaders`, `split`, `rot` and `beyond`. Built with `go build -o sim ./cmd/sim`, it
exits with 0 when every seed passed, 1 for a finding, 2 for a usage error and 3 when a run failed
without a finding; `go run` prints the real code as "exit status N" and itself exits with 1.
`go test ./...` runs all tests of the main module, three `ledgerd` processes included, and
`go test -tags simbug ./...` also checks that the planted bugs are found, twenty-one of them by the
simulator and one by the tests of the core. The chaos test runs with
`go test -tags chaos ./internal/chaos -run '^TestChaos$' -chaos.duration 2m`; set `TMPDIR` to a
directory on a real disk for real fsyncs.

### The demo

The demo is one program that starts the three nodes itself, books a small load on them and
serves a page with four buttons: kill a node, freeze it, cut it off from the others, or flip a
bit in its log. It needs Linux. Build it next to `ledgerd`, which it looks for in its own
directory, and give it a data directory of its own:

```
$ dir=$(mktemp -d)
$ go build -o "$dir" ./cmd/demo ./cmd/ledgerd
$ DEMO_DIR=$dir/data "$dir"/demo
```

Its first line on stderr names what it read: the address it listens on, the origin it accepts,
the path of `ledgerd` and the data directory. A line follows for every cluster it sets up. Open
`http://127.0.0.1:8080`, with exactly that address: the buttons answer only to the origin the
program was started for, and without `DEMO_ORIGIN` that is `http://` followed by `DEMO_ADDR`. The
page is in German.

The page shows the sum of the accounts, confirmed against found transfers, the verdict of the
checker, the three nodes with what each reports about itself, and a journal with the lines of
the nodes and what the program did. Each fault ends by itself after a few seconds, and a count
of the whole ledger follows. One more button fetches a receipt for a recent transfer and checks
it in the browser; the page prints the `ledgerctl verify proof` command for the same receipt.
When nobody has had the page open for 30 seconds, the load rests. Ctrl-C stops the program and
its three nodes.

Like `ledgerd`, the program takes its configuration from the environment only, here from `DEMO_`
variables, and refuses any it does not know. The variables, the routes and the rules of its
server are under "Demo" in [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md). What a stranger can and
cannot do to a public instance, and what the demo does not show, is in
[ADR-014](docs/adr/014-public-demo-fixed-faults-no-docker-socket.md). A long run that presses
the buttons at random over HTTP and fails on any finding:

```
$ go test -tags soak ./internal/demo -run '^TestSoak$' -soak.duration 2m -v
```

## Limits

There is no TLS and no authentication. The client port is a database port without a password,
and the peer port belongs to the nodes: both belong in a network you trust. The cluster ID in
every frame keeps clusters from mixing up; it is not a secret. Why this is so, and what the server
does against strangers anyway, is in [ADR-015](docs/adr/015-binary-protocol-over-plain-tcp.md).

A stamped receipt shows that one node declared the head of the chain at one link number, and
SHA-256 puts the record under that head. It does not show that a majority agreed, that the head is
the newest, a balance or what became of a transfer later. Changes to the history can be detected
by anyone who kept an earlier receipt or head, up to that point, but they are not prevented:
whoever can write to a node's files can rewrite a record together with its link, and only a second
node or an earlier head shows it. The full account is in
[ADR-011](docs/adr/011-audit-trail.md).

There are no benchmark numbers for a cluster yet; the ADRs quote micro-benchmarks of single
packages. The benchmarks are a module of their own: `cd bench && go test ./...`, and with
`-tags pg` and Docker also the tests against Postgres.

## Requirements

Go 1.27.4 or newer, as set in `go.mod`. The main module has one dependency,
[porcupine](https://github.com/anishathalye/porcupine) for the linearizability check; `bench/`
adds [pgx](https://github.com/jackc/pgx) and needs Docker for Postgres. The tests
with real processes, the chaos test and the demo need Linux; `scripts/vectors.mjs` needs Node.

## Documentation

- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md): layers, the contract between the Raft core and its
  driver, the core's rules, the node, the audit trail, the wire format, the server, the SDK, the
  simulator, the checkers, the planted bugs, the chaos test and the demo.
- [docs/adr/](docs/adr/): the decisions and what they cost.
  [ADR-009](docs/adr/009-disk-corruption-seal-and-repair.md) covers disk corruption,
  [ADR-011](docs/adr/011-audit-trail.md) the audit trail and what a receipt proves,
  [ADR-014](docs/adr/014-public-demo-fixed-faults-no-docker-socket.md) the demo and its threat
  model, [ADR-015](docs/adr/015-binary-protocol-over-plain-tcp.md) the protocol and the network.
- [docs/ledger-rules.md](docs/ledger-rules.md): fields, check order, time, chains, the audit
  trail, invariants and the snapshot format of the state machine.
- [DESIGN.md](DESIGN.md): the tokens and components the demo page is built from, its states and
  its contrast figures.

## License

All rights reserved. Prospective employers and clients may download, read and run the code to
evaluate the author. See [LICENSE](LICENSE).
