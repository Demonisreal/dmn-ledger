# ADR-014: A public demo that can only disturb itself: one program, four faults, no Docker socket

Status: accepted, 2026-11-19. The demo is built and runs locally and in a local container. It is
not deployed.

## Context

The simulator and the chaos test check that the ledger holds under faults, but nobody watches
them. A demo should show the same thing to two readers: in ten seconds to somebody who does not
know what a replicated ledger is, and in two minutes to somebody who knows Raft and fsync and
looks for the trick. Three real `ledgerd` processes, a small load, a button that kills one of
them, and a sum over all accounts that stays where it was.

Such a page is public and has no login, and its buttons send signals to processes and write to
files. That needs a decision of its own: what a stranger may trigger, what the demo may cost the
machine it runs on whatever strangers do, and what keeps the page honest. A demo that stages a
recovery, or that clears away its own failures, would do this project more harm than no demo.

## Decision

1. **One program, three child processes, no Docker socket.** `cmd/demo` starts exactly three
   `ledgerd` as its own children on loopback (`internal/local`) and never another program. A fault
   is a signal to a child, a setting on a proxy between two nodes that lives in the same process
   (`internal/faultnet`), or one byte written to a file in a node's directory. Nothing in the demo
   talks to a container runtime or starts a shell.
2. **Four faults from a fixed list, with the node as the only parameter.** The kind and the target
   are in the path of the request, each looked up in a fixed table: `kill`, `freeze`, `cut` or
   `flip`, and `1`, `2`, `3` or `leader`. `leader` is resolved when the fault runs, not when the
   button was pressed.

   | Fault | What the controller does | Held for | How it takes it back |
   |---|---|---|---|
   | `kill` | `SIGKILL` to the node | 2 s | starts the node again on its directory |
   | `freeze` | `SIGSTOP` to the node | 5 s | `SIGCONT` |
   | `cut` | cuts the four links between the node and the two others; clients still reach it | 6 s | opens the links |
   | `flip` | `SIGKILL`, then flips one bit in the node's newest WAL segment, at an offset in the second quarter of the file | no hold | starts the node and waits up to 10 s until the node's own log says how it started and, after `repair yes`, that the repair flag is cleared |

3. **The controller takes every fault back itself.** The step that undoes a fault is deferred at
   the place where the fault is triggered. It runs when the hold ends, when the controller is told
   to stop and when the code in between panics. After the undo the controller counts the whole
   ledger (decision 6). No request can undo, extend or repeat a fault. Only a finding changes the
   undo: links are opened again, but the nodes stay stopped (decision 7).
4. **One fault at a time, the same for every visitor.** There is one cluster. A fault is accepted
   in one place, under one lock, if all of this holds, checked in this order: the cluster is set
   up and no finding stands; no fault is running, its count afterwards included; 10 s have passed
   since the previous fault began and 4 s since it ended; no fault was accepted from this address
   in the last 30 s; the last look at the nodes is at most 300 ms old and shows three nodes that
   answer, exactly one leader and no node in repair; and for `flip`, the newest segment of the
   target holds at least 8 KiB. A refusal is a check in memory. It touches no node, writes no
   line and does not count against the address.
5. **No input of a visitor reaches a node, a file or the log.** No request body is read: a request
   with one is answered with 400 and the connection is closed. The query string is never looked
   at. The controller picks the file, the offset and the bit of a `flip` and the transfer a
   receipt is fetched for. The state the page is rendered from holds no byte that came from a
   request.
6. **The verdict comes from `check.Replayer` and a full count, not from a counter of the demo.**
   Every call of the load goes into a window, and every closed window through
   `Replayer.Window`: the replay against the oracle and the linearizability check that judge the
   simulator and the chaos test, where the planted bugs show that they find faults
   (`TestChaosFindsPlantedBugs`). After the setup, after every fault, every 60 s while the load
   runs and before a cluster is replaced at the limit, the controller holds the load, requires
   that all three nodes agree on commit, applied index, apply hash and head, looks up every
   account and every transfer the load ever sent, and requires that the transfers found are
   exactly the confirmed ones and that the sum of the accounts has not moved. These lookups go
   through the replayer as well.
7. **A finding stays.** A violation from the replayer, a call that fails, books that do not add
   up, nodes that do not agree in time, a node that ends by itself, a fault that does not end as
   planned, a receipt or a key that does not check: each stops all three nodes with `SIGSTOP`,
   copies their data directories, their logs, the journal and the state to a folder (at most
   three folders are kept), and stands on the page for 30 s before a new cluster is set up. It
   stays in the totals on the page until the controller process itself restarts. Nothing a
   visitor does removes it.
8. **The load is fixed and small, and it rests when nobody watches.** Four clients, each with one
   call at a time, one to four transfers per call, a pause of 0.75 to 1.25 s between calls.
   Visitors set neither rate nor size nor content. When no event stream has been open for 30 s
   the load waits and the nodes run on. Every transfer costs fsyncs on three nodes, and a page
   nobody looks at should not write to its host's disk day and night.
9. **A new cluster after 20 000 confirmed transfers,** with a new cluster ID, new keys and empty
   books. The limit bounds the checker, not the nodes: the replayer holds a second copy of the
   ledger, the oracle, and clones it while it judges a window, so every verdict costs more as
   the ledger grows. It also bounds the disk. The page says that the books start again.
10. **A receipt is checked in the browser, and the page says what that is worth.** `GET /proof`
    returns a receipt for a transfer the controller picks, already verified in Go with
    `ledger.VerifyProof` and compared with what the load sent. The browser computes leaf, path,
    head and the Ed25519 stamp again with WebCrypto, in the module the test vectors of ADR-011
    run through in Node and in Chrome. The node keys come from the same page as the receipt. That
    shows the arithmetic is right; it does not show that the operator need not be trusted. The
    page says so and prints the `ledgerctl verify proof` command for the same receipt.

The parts, the routes, the constants and the tests are under "Demo" in
[ARCHITECTURE.md](../ARCHITECTURE.md).

## Threat model

What is protected, in this order: the machine and its other services (what the demo takes at most
is fixed beforehand and does not depend on what visitors do); the honesty of the page (no visitor
can make it show what is not happening, or make a finding go away); the visitors (no cookies,
nothing from third parties, no stored address); and last the demo itself (after any sequence of
requests the cluster stands again).

Against whom, from likely to unlikely: a reader with `curl` who wants to know whether it holds, a
script that hammers the buttons, somebody with many addresses, a foreign website that makes its
visitors' browsers send requests, and somebody who exploits a bug in `net/http` or in the
controller and stands inside the container.

The tests named below are in `internal/demo` unless a package is given.

| # | Attack | Defence | What remains | Test |
|---|---|---|---|---|
| 1 | hammering the buttons from one address | one fault at a time, 10 s between two beginnings, 4 s after an end, 30 s per address; a refusal is decided in memory | one fault every 30 s from that address. That is the button | `TestOneActionAtATime`, `TestPerAddressLimit`, `TestRefusedActionTouchesNothing` |
| 2 | the same from many addresses | the limits that do not depend on the address: one at a time, 10 s, 4 s, and a full count after every fault | the demo is disturbed most of the time. It still shows what happens, and other visitors do not get to the buttons | `TestCooldownAfterAnAction` |
| 3 | a foreign site makes its visitors trigger faults | `POST /act` needs exactly one `Origin` header equal to `DEMO_ORIGIN`, and `Sec-Fetch-Site`, if sent, must be `same-origin`; no CORS headers on any answer, and a preflight (`OPTIONS`) gets 405 like any wrong method | whoever sets the header by hand has the button every visitor has, from their own address | `TestPostNeedsTheOrigin`, `TestPreflightFromAForeignSiteIsAWrongMethod`, `TestSecurityHeaders` |
| 4 | a foreign site embeds `/events` or `/proof` (an image, a frame, a prefetch send no `Origin`) to take seats or keep the load awake | both refuse an `Origin` other than `DEMO_ORIGIN` and a `Sec-Fetch-Site` other than `same-origin` or `none` | a client that sends neither header is answered, within the limits of rows 7 and 14 | `TestReadsRefuseAForeignOrigin` |
| 5 | forging the address with `X-Forwarded-For` | the header counts only on connections from `DEMO_PROXIES`, and then the rightmost entry outside those networks; an entry on the way that is not an IP address, one with a port included, ends the search and the address of the connection counts | whoever sends from one of those networks can claim any address | `TestAddressOnlyFromTheProxy` |
| 6 | rotating IPv6 addresses | the key is the /64; for IPv4 the address, unwrapped if it came inside IPv6 | whoever has many /64 is row 2 | `TestAddressKey` |
| 7 | taking every seat on the event stream | 4 streams per address, 200 in all, 10 minutes per stream; beyond the first 429, beyond the second 503, both with `Retry-After: 3`, the `retry` of the stream | 50 addresses hold every seat. Others see the page as rendered on request, with its time and with the buttons locked | `TestStreamCaps`, `TestStreamEndsAfterTenMinutes` |
| 8 | opening a stream and not reading it | a buffer of 32 events per stream; when it is full the stream is closed and its seat freed; 5 s per write; the sender never waits for a reader | more than 32 events within one beat of 250 ms would drop every viewer at once; a browser reconnects and starts with the full state | `TestSlowReaderIsDropped` |
| 9 | `HEAD /events`, which would hold a seat and a proxy connection without a byte to read | 405 with `Allow`, before a seat is taken | | `TestWrongMethodIs405` |
| 10 | holding connections open, sending slowly | 5 s for the header, 10 s for the request, 60 s idle, at most 512 connections accepted at a time | 512 connections can be held, and new ones wait beyond that. The site block in `deploy/demo` sets no rate and no connection limit of its own | `TestServerLimits` |
| 11 | large headers, large bodies | headers above about 12 KiB are refused by `net/http` (8 KiB `MaxHeaderBytes` plus its own slack); a body is never read, on any route | | `TestBodyIsRefused`, `TestServerLimits` |
| 12 | smuggling a second request past the proxy inside a body | a request with a body, chunked or with a length, gets 400 and `Connection: close` before the router sees it | | `TestBodyIsRefused` |
| 13 | reaching a node, a file or the journal with input | kind and target from fixed tables, anything else 404; file, offset and bit chosen by the controller inside the target's directory; nodes are started without arguments and with an environment the controller lists | | `TestUnknownActionIs404`, `TestFlipStaysInTheNodeDir`, `TestPickStaysInTheSealedHalf`, `TestNoRequestDataInState`, `TestEnvIsExplicit` in `internal/local` |
| 14 | repeating expensive requests | the page is rendered at most once a second and the same bytes go to everyone in between; `/proof` asks the cluster at most once a second; an event is encoded once for all viewers; static files are embedded and carry an `ETag` | bandwidth. The site block sets no rate and no connection limit, so nothing bounds how often somebody fetches the same bytes | `TestPageRendersOncePerSecond`, `TestProofAsksOncePerSecond` |
| 15 | script or markup in the page | nothing from a request is in the state; `html/template` escapes what nodes and checker wrote; the script writes values as text and no markup; the policy allows scripts and styles from the own origin only, none inline | an escaping mistake would stay without effect as long as `script-src 'self'` holds. Answers that `net/http` writes itself carry no policy (below) | `TestNoRequestDataInState`, `TestNoInlineScriptOrStyle`, `TestScriptsWriteNoMarkup`, `TestSecurityHeaders` |
| 16 | putting the page into a frame | `frame-ancestors 'none'` | | `TestSecurityHeaders` |
| 17 | reaching the ports of the nodes | the nodes and the links between them listen on `127.0.0.1` only; the container publishes no port | the node ports have no TLS and no authentication (ADR-015). That matters only to somebody who is already inside the container | `TestNodesListenOnLoopback` |
| 18 | leaving the cluster broken | the undo is deferred with the fault (decision 3); every step has a deadline; a cluster that does not agree again in time is a finding and is replaced; a watchdog ends the process when the loop has not reported for 60 s or the look at the nodes for 10 s | while a fault runs the cluster is disturbed. That is its purpose | `TestEveryFaultIsLifted`, `TestFaultIsLiftedOnShutdown`, `TestKillHealsItself`, `TestFreezeHealsItself`, `TestCutHealsItself`, `TestFlipIsRepaired` |
| 19 | driving the cluster into the limit of ADR-009: one node in repair and the leader gone | one fault at a time; none is accepted while a node is in repair, does not answer, or there is not exactly one leader; a `flip` ends only when the node has logged that its repair flag is cleared | if a second node dies by itself during the milliseconds of a repair, the cluster stands still: a finding, then a new cluster | `TestNoActionDuringRepair`, `TestNoActionWithoutThreeNodes` |
| 20 | filling the disk | a cluster is replaced after 20 000 transfers and its directory deleted; the logs of the nodes are kept in memory, not in files; at most three evidence folders | no request leads to a finding. If one did, three folders would be written and no more | `TestResetAtTheLimit`, `TestEvidenceIsCapped` |
| 21 | filling memory | every queue, ring and buffer has a fixed size; the load waits when the checker falls four windows behind; `GOMEMLIMIT` for every node and, in the container, for the controller; a memory limit on the container | if the limit of the container strikes, it dies and restarts with a new cluster, and the totals since the start are gone | `TestStateIsBounded`, `TestBacklogHoldsTheLoad` in `internal/history` |
| 22 | multiplying processes | three children, started by the controller only; starting a node that runs is an error; `pids_limit` on the container | | `TestNeverMoreThanThreeNodes`, `TestStartOfARunningNodeFails` in `internal/local` |
| 23 | forcing a new cluster again and again | a cluster is replaced for three reasons only: the start, the limit of transfers, a finding | whoever can cause a finding has found a bug in the product, and it is then on the page | `TestResetAtTheLimit`, `TestNodeThatDiesIsAFinding` |
| 24 | breaking out after taking over the controller | the container: no root, no capabilities, no new privileges, a read-only file system, no Docker socket, no host path, an internal network. The only secrets are the node keys of one cluster | kernel bugs. The reverse proxy is reachable from the container, and through it whatever else that proxy serves; the host is too, unless the bridge of the network drops what it sends to the host. See "The container" for what of this has run | none: configuration |
| 25 | recognising or tracking visitors | no cookies, no storage in the browser, nothing loaded from another origin, no access log, and the error log of `net/http` is discarded because it would name the caller when a handler panics. The key of an address is in memory for 30 s after an accepted fault and as long as a stream from it is open, and nowhere else | the reverse proxy sees every address. The site block has no access log, but a request that fails is named with its address in the proxy's error log, which is kept as long as the proxy's own settings say | `TestNoCookies`, `TestServerLogsNoRequest`, `TestAddressIsForgottenWithoutAnotherAction`, `TestNoRequestDataInState` |
| 26 | making the page lie | every value comes from a status frame, a line a node wrote, an answer to the load or a step the controller took itself; after 3 s without an event the page marks its values as old and locks the buttons; findings stay (decision 7) | whoever controls the controller, the operator included, can show anything. That is why the page tells how to run it oneself | `TestPageInChrome`, `TestNodeThatDiesIsAFinding` |
| 27 | bots keep the load running | `X-Robots-Tag: noindex` and a `robots.txt` that disallows everything; the load runs only while an event stream is open; the page closes its stream when its tab has been hidden for 60 s | a bot that runs JavaScript is a viewer, and a viewer costs at most the fixed load | `TestLoadRestsWithoutViewers`, `TestLoadRestsWhileTheHubReportsNoViewers`, `TestHiddenTabClosesItsStreamInChrome` |
| 28 | the supply chain | no JavaScript dependency, no CDN, no build step for the page; the main module still has one dependency; both base images are pinned by digest; `govulncheck` runs in CI and once a day on its own schedule, and Dependabot watches the Go modules, the Actions and the images | | CI |
| 29 | passing a receipt of the demo off as proof of something else | the cluster ID is in the signed message and random for every cluster; the keys are new for every cluster | a receipt of the demo proves something about the demo | |

Known gaps, none of which changes what a visitor can trigger:

- **Answers that `net/http` writes before a handler runs carry none of the headers:** 431 for a
  header that is too large, 400 for a malformed request, 501 for an unknown transfer encoding.
  Their text is fixed and reflects nothing from the request. A 416 for a range outside a static
  file loses its `Cache-Control`. Every answer of the handler carries all headers, 403, 404, 405
  and 503 included.
- **The key of an address lives in the stream hub as long as a stream from that address is open,**
  up to 10 minutes per stream and longer when the browser reconnects. It is an IPv4 address or an
  IPv6 /64, held in memory to count streams, never written.
- **The node keys come from the same answer as the receipt and the cluster ID.** "Valid" in the
  browser means that this answer agrees with itself. The check becomes independent only with
  keys obtained on another path, and the cluster itself offers only its unauthenticated status
  frame for that (ADR-011, ADR-015).

## The container

`deploy/demo` holds a `Dockerfile`, a `compose.yml`, an `.env.example` and a site block for the
reverse proxy that terminates TLS.

| Measure | In `deploy/demo` |
|---|---|
| a base image without shell or package manager | `gcr.io/distroless/static-debian12:nonroot`, pinned by digest; the build image as well |
| not root | `user: "65532:65532"` |
| no capabilities, no new privileges | `cap_drop: [ALL]`, `security_opt: ["no-new-privileges:true"]` |
| read-only file system | `read_only: true` and `ipc: none`; only the named volume under `/data` is writable, and `/dev/shm` is not mounted |
| no Docker socket, no host path | no volume besides the named one |
| limits | `cpus: "1.0"`, `mem_limit` and `memswap_limit` of 384 MiB, `pids_limit: 128`, `GOMEMLIMIT` of 96 MiB for the controller and 64 MiB for each node |
| no port to the outside | `expose`, no `ports` |
| no way to the internet | the network `ledger-demo` with `internal: true`, created beforehand and declared `external` in `compose.yml`, so that neither project owns it; only this service and the reverse proxy are attached |
| the proxy and nobody else sets the visitor's address | the proxy has a fixed address in that network, and `DEMO_PROXIES` is that address as `/32` (`PROXY_IP` in `.env`), not the subnet |
| a name that nothing else uses | `name: dmn-ledger-demo` for the project and the alias `dmn-ledger-demo` for the service in the network, which is the upstream in the site block |
| restart | `restart: unless-stopped`; the watchdog ends the process with 1; a health check runs `/demo -check`, with 30 s of grace at the start |
| bounded logs | `json-file` with 10 MB and three files |
| a small build context | `.dockerignore` lets in `go.mod`, `go.sum`, `cmd`, `internal` and `ledger`, without tests and test data |

The site block sends everything to one upstream with `flush_interval -1`, so events go out at
once, and compresses `/static/*` only. Its connections to the demo are kept idle for 50 s, under
the 60 s after which the demo closes them, so that a `POST` does not meet a connection that is
just being closed, and it waits 10 s for the header of an answer.

The network is made once, by hand, with the proxy's fixed address outside the range Docker hands
out. The subnet is an example and must be free on the host:

```
docker network create --driver bridge --internal \
  --subnet 172.29.10.0/24 --ip-range 172.29.10.128/25 \
  --opt com.docker.network.bridge.name=br-ledger-demo \
  ledger-demo
```

The reverse proxy lists `ledger-demo` as an external network with `ipv4_address: 172.29.10.2` in
its own compose file. The bridge carries an address of the host, and a service that listens on
all addresses of the host would be reachable from a container that someone has taken over. A
rule in the host's firewall drops what the bridge sends to the host itself, for example
`iptables -I INPUT -i br-ledger-demo -j DROP`. Traffic between the proxy and the demo is
forwarded, not input, and is not touched by it.

The controller needs no capability to signal its children: they run under the same user. It is
PID 1 in the container, reaps its children and handles `SIGTERM` itself. The `Dockerfile` sets
the address of the repository with `-ldflags -X`, so the page in the image links to the source; a
local `go build` leaves the links out. The image was built again with that flag and started once:
the page carries the links, a kill of the leader healed, and the container stopped with 0.

What has run, on the development laptop: the image builds; a container started with `docker run`
and the switches of the table (user, read-only, no capabilities, no new privileges, one CPU,
384 MiB without swap, 128 PIDs), an anonymous volume under `/data` and the port published on
`127.0.0.1` only. In it the volume took the owner 65532 and the cluster wrote to it, four
processes ran as 65532, `/demo -check` answered 0, the image had no shell, `docker stop` ended it
with 0 in under a second, and the two-hour run under "Consequences" passed against it.

`compose.yml` has run there too, once, on an internal network created as below, with a Caddy
container at the proxy's fixed address in front and the site block of the `Caddyfile` on plain
HTTP: the health check reported healthy, the container ran with `ipc: none` and a read-only root,
the first event of the stream arrived through the proxy in 0.35 s, only `/static/` was
compressed, a `POST` with a forged `X-Forwarded-For` 16 s after an accepted fault was refused as
`limited`, and from a second container on the network neither an address outside nor a name
could be reached, while the proxy could. `caddy validate` accepts the `Caddyfile` as it stands.

What can only be checked where the demo is deployed: the host's firewall towards the bridge, TLS
and the real name, the log limits over time, and everything the proxy serves besides. Open for
that day:

- That the proxy sees the visitor's own address. That holds if the DNS entry points at the host
  and no CDN stands in front: with one, every visitor from a region arrives from a few of its
  addresses and counts as one. It holds for IPv6 only if the proxy's network has IPv6 or the
  entry has no AAAA record. If IPv6 visitors reach the proxy through an IPv4 hop, they share one
  address, and with it four streams and one fault in 30 s.
- That the proxy of the host behaves like the one in the local run. The address rule holds
  either way, because the rightmost entry counts.
- That what else the proxy serves does not treat the network of the demo as a trusted one. A
  site that admits callers from private address ranges, or that trusts `X-Forwarded-For` from
  them, is open to a taken-over container, and so is an administration endpoint of the proxy
  that listens on more than the loopback.
- That a container on the network cannot resolve names outside it: Docker's resolver forwards
  them for internal networks in some versions.

## What the demo does not show

- **No power loss.** `kill -9` leaves in place what the operating system has not written yet, so a
  missing fsync would go unseen here. The simulator checks that, with a power loss at every disk
  access.
- **No two faults at once.** One fault runs at a time. With three nodes, two damaged ones can stop
  the cluster (ADR-009); it then gives no answer, never a wrong one.
- **No real network.** Three processes on one machine talk over loopback, and a proxy between
  them does the cutting.
- **No wrong clock, no disk that returns read errors, no malicious node.**
- **No long-running ledger.** The books start again after 20 000 transfers.
- **No speed.** The load is a few calls a second by design, and the page shows no rate and no
  latency.

The demo runs four sequences by hand that the simulator and the chaos test run many thousand
times with randomness. It shows that the sequences exist. That they hold is what the tests show.

## What is not protected

- **The availability of the demo against somebody with many addresses** (rows 2 and 7). They can
  make it useless for others. They cannot load the machine beyond the fixed bounds with it, and
  they cannot falsify the page.
- **Bandwidth.** The controller answers what is asked. The site block sets no rate and no
  connection limit; bounding either takes a plugin or something in front of the proxy.
- **The page against its operator.** Whoever runs the container can show anything.

## Consequences

- The demo is a second program in the main module, with HTML, CSS and JavaScript embedded under
  `internal/demo/web`. It adds no dependency.
- The code that runs three `ledgerd` as children and the code that records calls in windows moved
  out of the test files of the chaos test into `internal/local` and `internal/history`. The chaos
  test and the demo use the same two packages, so the demo is judged by code that the planted
  bugs have tested.
- Rates, durations and limits are constants, not settings. What cannot be set cannot be set wrong
  in operation, and the tests check the values that run.
- A reader sees the faults of other visitors, and may wait for a button.
- A repair after a flipped bit is over before anybody can watch it. In single runs on the laptop
  named below the node was a non-voter for 54 ms after a flip on a follower and, in a test with a
  shorter pause of the load, for 543 ms after one on the leader, where the two others elect a
  new leader first. The page shows a repair as an event with the node's own two log lines and
  the time between them, not as a state, and nothing is stretched to be seen.
- A finding on the public page stays there (decision 7), also one that is a fault of the machine
  and not of the ledger, for example a node the kernel ended at the memory limit.

Measured with `TestSoak` on an i5-12500H laptop with `GOMAXPROCS=4`, one run each. The test drives
the real controller and its HTTP server with the real durations: a fault of a random kind every 3
to 12 s, some requests without `Origin`, with an unknown kind or with a body, three stream
readers, and a page and a receipt every second, each receipt verified.

| Run | Transfers | Faults (kill, freeze, cut, flip) | Longest fault, count included | Longest count after an undo | Receipts verified | Heap at most | CPU of a core: controller, nodes |
|---|---|---|---|---|---|---|---|
| 2 hours, native | 69 403 | 99, 115, 133, 122 | 7.04 s | 1.77 s | 7 027 | 73.4 MiB | 3.0 %, 5.6 % |
| 2 hours, against the container | 69 111 | 147, 111, 120, 99 | 7.04 s | 1.73 s | 7 029 | not measured | not measured |

Neither run had a finding or a stream dropped for a full buffer. Each replaced its cluster three
times, every time at the limit of 20 000 transfers, about 35 minutes apart. All 221 flipped bits
ended in a repair. The heap and the controller's CPU in the native run include the test's own
clients. At the end of the run against the container, `docker stats` showed 108.8 MiB of 384, 36
of 128 PIDs, 6.3 % of a core and 765 MB written; the container had not restarted, `docker stop`
ended it with 0 in 0.3 s, and no line of its log held the address of a caller.

What these numbers do not cover:

- The outcome of a `flip` in which the node starts without repair, because the bit fell into a
  write it had not acknowledged, never occurred. The line the controller writes for it has not
  been seen in a run.
- The memory of the container was read once, at the end, not as a maximum over the run.
- One laptop, one run each. A slower machine with other services next to the demo is not
  measured.

## Rejected alternatives

- **The Docker socket and a container per node.** `docker kill` and `docker pause` look more like
  operations, but whoever takes over a controller that holds the socket owns the host. Signals
  to child processes need no privilege at all, and the faults are the same signals.
- **Free parameters:** a duration, a rate, a batch size, a transfer to prove. Each one is input
  that reaches the cluster and a bound that somebody has to defend. With fixed values the tests
  check exactly what runs.
- **A recorded video or a replayed trace.** Nothing to attack and nothing to believe. The reader
  who looks for the trick would be right to assume one.
- **A counter of its own instead of the checker.** A demo that counts confirmed transfers and
  compares sums would first have to show that it finds faults. The replayer has shown it, and
  the full count adds only numbers a reader can add up.
- **WebSocket.** The page only listens; the four buttons are plain requests. A stream of events
  over HTTP needs no upgrade, no second protocol behind the proxy and no library.
- **Hiding a finding after a while,** or when the next cluster is up. A criterion that can be met
  by clearing away is none.
- **Keeping the ledger across restarts of the controller.** What an earlier run confirmed only its
  oracle knew, and that died with the process. Carrying the data on would mean running without a
  checker.
- **A data directory in memory.** Every confirmation would no longer wait for a real fsync, and
  the page would have to say so.
