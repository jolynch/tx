# Fault Injection

`internal/faults` puts a thin shim in front of tx's network, disk, and syscall
IO so property tests can check that a transfer either finishes with correct
data or fails loudly when that IO misbehaves. The approach follows
FoundationDB's simulator and Cassandra's Harry: the real code runs unchanged,
and a seeded injector decides where faults fire, so a failing seed shows
which faults fired and where.

Unlike FoundationDB, tx does not simulate scheduling. Goroutines, real TCP
sockets, and splice cannot be made deterministic, so a seed can at most fix
which faults fire on a given connection or file, never the order in which
goroutines interleave. See [Reproducing a run](#reproducing-a-run).

## Cost when disabled

A nil `*faults.Injector` is the disabled state, as a nil `events.Sink` is.
Every wrapper returns what it was given, and every gate returns nil. The shims
sit at connection, file, request, and syscall-site granularity, never inside
the read or write loops: production pays a nil check per connection accepted
or dialed, per request, per file opened, per SEND frame, and per gated
syscall, plus one context lookup per SEND and CXSUM request. SEND's buffered
reads go through an `io.ReaderAt` whether or not they are wrapped, one
interface call per read. A wrapped connection or file adds one more call per
read or write.

## Where the shims sit

| IO          | Shim                                | Sites                                                        |
| ----------- | ----------------------------------- | ------------------------------------------------------------ |
| Connections | `WrapConn` returns a `*faults.Conn` | every client dial (`dialTCP`), every server accept (`Serve`) |
| Dial        | `DialErr` gate                      | every client dial, before connecting                         |
| Accept      | `AcceptErr` and `Busy` gates        | every server accept; busy only where auth is required        |
| Requests    | `DelayRequest`, `DupRequest`        | every client request; every ACK                              |
| Responses   | `LoseResponse` gate                 | every server command                                         |
| File opens  | `OpenErr` gate                      | SEND and CXSUM, before `Deps.OpenFileRef`                    |
| File reads  | `WrapReaderAt`                      | SEND buffered frames, CXSUM, client `--verify`               |
| File writes | `WrapWriter`                        | client download outputs (`openDownloadOutput`)               |
| Syscalls    | `internal/sysio`, `Sys`, and syscall wrappers | see below                                           |

Raw data-path syscalls live in `internal/sysio`. Its faultable functions take
the injector first and apply `Sys` or the short-call wrappers
(`WrapSplice`, `WrapTee`, `WrapPoll`, and `WrapCopyFileRange`). Existing FTCP
helpers retain their gates where they wrap test-supplied operations or keep
an ungated helper signature. SEND and CXSUM gate `os.File.Stat` at their
call sites, and socket setters keep their `setsockopt` gate there too.

| Syscall | Adapter or gate | Sites |
| ------- | --------------- | ----- |
| `splice`, `tee`, `poll` | `sysio.ZeroCopyCalls`, FTCP fault wrappers | zero-copy SEND loop |
| `pipe`, `fcntl` (`F_SETPIPE_SZ`), `dup` | `sysio.Pipe`, `SetPipeSize`, `DupConn` | zero-copy SEND setup |
| `fadvise`, `readahead` | `sysio.Fadvise`, `Readahead` | SEND sequential hints and frame readahead |
| `fstat` | `Sys` before `os.File.Stat`, or `sysio.FileID` | SEND and CXSUM file stats, background fsync batch |
| `fdatasync`, `syncfs`, `dup` | `sysio.Fdatasync`, `Syncfs`, `Dup` | fsync worker (`fsync.WithFaults`) and `SyncfsDir` |
| `copy_file_range` | `sysio.CopyFileRange` | daemonless local copies |
| `mmap`, `mincore`, `fadvise` | `sysio.Mmap`, `Mincore`, `Fadvise` | page-cache probe, restore, and eviction |
| `setsockopt` | `Sys` before TCP socket setters | `SetNoDelay`, socket buffer sizes, and `TCP_USER_TIMEOUT` on dial and accept; `TCP_NOTSENT_LOWAT` on probes |

Ungated adapters cover TCP byte counts, idle-socket peeks, descriptor close,
unmapping, system memory, metadata extraction, and directory or progress
opens. The events target writer keeps its own FIFO open: faults depends on
events, so events cannot import sysio.

`make lint` rejects raw `syscall` and `golang.org/x/sys/unix` imports outside
sysio, fault shims, harnesses, signal setup, and the events target writer;
test files are exempt. Opens and dials in the data-path packages require a
`//nolint:forbidigo` reason naming the gate or explaining an ungated
exception. Manifest and resume state, diagnostic output, and local-copy
setup remain outside the DISK gates. These exceptions are explicit at their
call sites; the deferred filesystem boundary is listed in
[Ideas](../IDEAS.md#code-health).

Advisory syscalls (fadvise, readahead, pipe resize, page cache, setsockopt)
are best-effort: an injected failure skips them as a real one would.

The page-cache package is reached from many entry points that carry no
request, so its injector is process-wide (`pagecache.SetFaults`): the last
caller wins, and clearing removes only the caller's own injector, never
reinstating an earlier one. Every other site takes the injector from its
server, client, or command.

## Zero-copy

A plaintext fast-mode SEND splices frames of at least 1 MiB from the file
straight to the socket, which needs the raw `*net.TCPConn`. A `*faults.Conn`
keeps that path: `ZeroCopyConn` hands the SEND its socket, and the splice,
tee, and poll calls go through the syscall gates instead. A zero-copy frame's
header, payload, and trailer go to the socket directly, past the wrapper, so
a wrapper's writes are synchronous and never hold back bytes that would then
arrive out of order. Encrypted sessions never splice, so they lose nothing.

Write faults (NET `delay`, `pause`, `reset`, `partial`) cannot reach a
spliced payload. When any is on, half the connections, chosen by the seed,
give up zero-copy, so write faults reach SEND payloads on those and the
syscall faults on the rest. A spliced payload also skips the file reader, so
with DISK `read.flip` on, SEND reads every frame through it instead.

## Enabling

Library code takes an explicit injector: `ftcp.ServerOptions.Faults`,
`tx.WithFaults`, and `fsync.WithFaults`. `faults.Build` makes one from a
`faults.Config`. The `tx` binary builds one from the environment, prints a
warning with the line that reproduces the run, and passes it to the server
or the client command:

```text
TX_FAULTS_SEED=42          the seed for every fault decision
TX_FAULTS_PROFILE=bad-day  a named set of family specs
TX_FAULTS_NET              network faults
TX_FAULTS_MSG              request and response faults
TX_FAULTS_DISK             file open, read, and write faults
TX_FAULTS_SYS              syscall faults
```

Each family variable is a comma-separated list of `kind=rate[:dist]`
entries, overriding the profile kind by kind:

```text
TX_FAULTS_NET='reset=0.001, flip=1e-6, pause=0.01:β(100ms,500ms,4s,5s)'
TX_FAULTS_DISK='read.eio=0.001, read.latency=1:gp3'
TX_FAULTS_SYS='*.fail=0.002, fdatasync.fail=0.05, splice.latency=0.01:β(10us,100us,1ms,2ms)'
```

The rate is the probability that the kind fires on each operation the kind
applies to: each read or write call for NET, each read call and each
64 KiB block written for DISK, the requests, ACKs, commands, or accepts the
catalog names for MSG, each open, dial, or accept, and each syscall. A kind marked ⏱ needs a duration unless its rate
is 0, so `kind=0` turns off a profile's kind. In SYS, a kind is
`<syscall>.fail`, `<syscall>.latency`, or `<syscall>.short`, and `*` stands
for every syscall; within one variable, a specific entry beats `*` whatever
the order, and a variable's `*` entry overrides the profile's specific
ones. A variable
set to blank space counts as unset, so when none has a value the injector is
nil. `TX_FAULTS_SEED` alone turns on every wrapper while injecting nothing,
which tests use to show the shims are transparent; `faults.Injector.Counts`
reports which sites each injector reached and which kinds fired.

### Durations

Every kind that takes time (marked ⏱ below) takes a beta distribution:

| Form                         | Meaning                                                   |
| ---------------------------- | --------------------------------------------------------- |
| `β(min,avg,p99,max)`         | values in increasing order; the usual form                |
| `β(1ms,5ms,p75=6ms,50ms)`    | a key replaces a middle slot's meaning: here avg and p75  |
| `β(1ms,p50=4ms,40ms,50ms)`   | a median and a p99; two percentiles fit as well as a mean |
| `β(min,α=A,β=B,max)`         | the shape directly, when it is already known              |
| `io2`, `gp3`, `i4i`, `ephem` | drive read-latency profiles                               |

`avg=`, `mean=`, `μ=`, and `µ=` are the same key. A distribution takes one
average and one percentile, or two percentiles. `Β(`, `beta(`, and `Beta(`
spell `β(`, and `alpha=` and `beta=` spell `α=` and `β=`. A shape that no beta
distribution on the range can match is an error at startup. `internal/stats`
fits the shape: from a mean and one percentile, or from two percentiles.

### Profiles

`bad-day` turns on most of the catalog at once, at rates low enough that an
in-memory transfer still finishes with correct bytes within a few resume
rounds; `TestBadDayCompletes` in `internal/memxfer` holds it to that in every
session mode, each with its own seed. It leaves out `read.flip`,
`write.lost`, and `write.flip`, which no layer detects without `--verify`, so
a run could succeed with wrong data, and `open.enoent`, after which a file
stays missing until a later round delivers it. Its per-file kinds stay near
0.1–0.3%: one fault fails a whole SEND batch, so a batch of small files must
usually get through.

## Catalog

The last column says what should catch each fault. The pbt-faults property
tests check it.

| Family | Kind                         | Injected at                                                          | Expected detector or outcome                                                 |
| ------ | ---------------------------- | -------------------------------------------------------------------- | ---------------------------------------------------------------------------- |
| NET    | `delay` ⏱                    | each read and write                                                  | liveness only                                                                |
| NET    | `pause` ⏱                    | each read and write                                                  | deadlines fire, even if set mid-pause; `Close` ends it                       |
| NET    | `reset`                      | each read and write; later reads fail as reset                       | loud error; the pool evicts; ACK retries; resume converges                   |
| NET    | `partial`                    | each write: a prefix, then a reset                                   | loud error                                                                   |
| NET    | `eof`                        | each read: the stream ends early, and later writes fail with `EPIPE` | premature EOF is an error, never success                                     |
| NET    | `flip`                       | each read: one bit                                                   | AEAD tag, framed-body checksum, window hash; plaintext lines are unprotected |
| NET    | `drop`                       | each read: a span removed                                            | as `flip`, plus frame offsets and lengths                                    |
| NET    | `dup`                        | each read: a span delivered twice                                    | as `drop`; a duplicated `OK` must not end a second request                   |
| NET    | `reorder`                    | each read: two adjacent spans swapped                                | as `flip`                                                                    |
| NET    | `dial.fail`, `dial.slow` ⏱   | each client dial                                                     | dial error, counted; the request fails or retries                            |
| NET    | `accept.fail`                | each server accept: a temporary error in its place                   | `Serve` retries the accept; the client sees EOF or a reset                   |
| MSG    | `lose-response`              | each server command, after applying it                               | the client sees a reset; ACK retries must be idempotent                      |
| MSG    | `dup`                        | each client ACK: sent twice                                          | idempotent server state                                                      |
| MSG    | `delay` ⏱                    | each client request                                                  | requests on other connections overtake it                                    |
| MSG    | `busy`                       | each accept, on a server requiring auth                              | `ERR BUSY`; the client retries the handshake                                 |
| DISK   | `open.enoent`, `open.eio`    | each SEND and CXSUM open                                             | 404 until a later round delivers it; or a loud error                          |
| DISK   | `read.eio`, `read.latency` ⏱ | each buffered SEND, CXSUM, and verify read                           | loud error; liveness. Spliced frames: SYS `splice`                           |
| DISK   | `read.flip`                  | each SEND, CXSUM, and verify read: one bit                           | none without `--verify`: the server hashes what it read                      |
| DISK   | `write.eio`, `write.enospc`  | each 64 KiB block of a client output                                 | loud error at the block; resume repairs                                      |
| DISK   | `write.short`, `write.torn`  | each output block: stops inside it                                   | loud error; a short write breaks `io.Writer` and must be caught              |
| DISK   | `write.latency` ⏱            | each output block                                                    | liveness only                                                                |
| DISK   | `write.lost`, `write.flip`   | each output block: skipped (seekable outputs only), or one bit       | none without `--verify`: the client hashes before writing                    |
| SYS    | `<syscall>.fail`             | each gated syscall                                                   | hard errors loud; advisory calls skipped; `EINTR` retried                    |
| SYS    | `<syscall>.latency` ⏱        | each gated syscall                                                   | liveness only                                                                |
| SYS    | `<syscall>.short`            | each splice, tee, or copy_file_range: fewer bytes                    | handled by the caller's loop                                                 |

TCP turns lost, duplicated, or reordered packets into delay. NET `drop`,
`dup`, and `reorder` break that contract on purpose, as middleboxes, offload
firmware, and kernel bugs occasionally do, to show which layer catches it. In
encrypted sessions the AEAD must reject every case.

Each syscall's `fail` kind returns one of the errors that call can really
return at tx's call sites, such as `EIO` or `ENOSPC` from `fdatasync`,
`EMFILE` from `pipe`, and `EXDEV` from `copy_file_range`.

## Reproducing a run

Every decision is a hash of the seed, the kind, a key, and an operation
number:

- Disk faults key on the site, the file's manifest ID, a byte offset (the
  window's start for an open or a write, the read's offset for a read), and
  how many times that offset was tried before. A write's operation number is
  the 64 KiB block of the file, so a window tried again gets fresh decisions
  for all its blocks. Deciding writes per block rather than per write call
  keeps them independent of how the writes are cut, which in plaintext
  follows the network's reads. Blocks start on 64 KiB file boundaries, so a window that
  starts mid-block has no write faults before its first boundary. Concurrent
  windows of a file start at different offsets, and an offset is tried
  again only after its earlier attempt ended, so disk faults reproduce from
  the seed whatever the goroutines do, apart from the two cases below,
  while a retry still gets a fresh decision. What reproduces is where
  writes fail and which bytes change; fault events and counts can differ,
  since a write that spans several blocks decides them all at once, and a
  position drawn past a file's end counts as injected without applying.
- Connection faults key on the side and the connection's ordinal and number
  reads and writes separately; request, dial, accept, and syscall faults
  number operations in the order they happen. These reproduce per ordinal,
  but under concurrency which request or connection gets an ordinal can vary
  between runs. A NET read fault also lands on whatever bytes that read
  returned, and where reads split a stream follows segment arrival, so the
  bytes it hits can vary even on one connection.

Two cases break exact disk reproduction:

- A server still handling a request whose client has given up can read an
  offset at the same time as the client's retry, and then the two attempts
  race for that offset's count.
- A spliced SEND frame reads its file without the faulted reader, so it
  takes no DISK read decision and uses no attempt at its offset. Which
  frames splice can vary between runs: with NET write faults on, half the
  connections by ordinal keep zero-copy, and `adapt` compression picks
  identity from measured latency. A frame that spliced in one run can then
  read through the faulted reader in the next, shifting later attempts at
  that offset. With `read.flip` on, every frame reads through it.

The per-offset attempt counts live as long as the injector, one entry per
offset tried, which suits test runs rather than a long-lived server. NET
pauses end at a deadline or a close, and DISK `read.latency` ends with its
request's context; the other injected waits (MSG `delay`, DISK
`write.latency`, and SYS `latency`) take no context and run to the end of
their sample, at most the distribution's maximum.

The repro line is the seed, the profile, and every family variable, quoted
for a shell.

## Property tests (pbt-faults)

`make pbt-faults` runs three properties under faults, each also its own
target (`pbt-memxfer`, `pbt-tree`, `pbt-pool`), so `make -k pbt-faults` runs
every one past a failing one. Each input picks a
fault set from its seed: a swarm (`faults.Swarm`), which turns on each kind
with probability one half at a log-uniform rate; the `bad-day` profile; or a
swarm of benign kinds only. The same seed derives the schedule, and a
failure message ends with the line that reruns its faults; which operation
a fault lands on can still vary with timing, as
[Reproducing a run](#reproducing-a-run) describes.

| Property                     | Package                    | What it drives                                                         |
| ---------------------------- | -------------------------- | ---------------------------------------------------------------------- |
| `FuzzMemTransferUnderFaults` | `internal/memxfer`         | memfd transfers in every session mode, then verify, then a clean round |
| `TestTreeCopyUnderFaults`    | `internal/cmd/filexfercli` | `tx recv copy` of a tmpfs tree with links, resumed, then a clean copy  |
| `FuzzTCPConnPoolUnderFaults` | root                       | the connection pool's bookkeeping under NET faults                     |

They check that tx either finishes correctly or fails loudly:

- A file the server marks done, or a copy that exits 0, holds exactly the
  source's bytes and metadata.
- A round that reports no errors had no background output sync fail or
  get skipped.
- A verify pass that reports success means every byte matches.
- Latencies and failed advisory syscalls alone never fail a transfer.
- After the faults stop, one clean round or copy converges.
- Every step returns within its budget, and memxfer releases every
  goroutine and descriptor it used.

Go's fuzzer stops any input that runs past 10s, with no message, so the two
fuzz targets keep each input near 8s at worst and report an overrun, with
every goroutine's stack, before then. A CLI copy cannot be bounded that
tightly: it may wait out tx's own timeouts, such as 15s for an ACK. So
`TestTreeCopyUnderFaults` sweeps seeds in a plain test, allowing each copy
90s.

The tests never touch a disk: memxfer keeps files in memfds, and the tree
copy runs on tmpfs (`/dev/shm`, or `TX_PBT_TMPFS`), skipping when neither
is tmpfs. They sit outside `acceptance`, because they fail until the bugs
they find are fixed; each moves to `fuzz-long` once it passes.

| Variable                  | Effect                                                                                                         |
| ------------------------- | -------------------------------------------------------------------------------------------------------------- |
| `TX_PBT_FAULTS`           | turns the properties on; `make pbt-faults` sets it, and without it they skip                                   |
| `TX_PBT_FAULTS_EXCLUDE`   | kinds the swarms never draw, as `disk.write.lost`; `make pbt-faults PBT_EXCLUDE=known` lists the recorded ones |
| `TX_FAULTS_*`             | when any is set, every input runs under those faults instead of its own                                        |
| `TX_PBT_FAULTS_SEED`      | the one seed `TestTreeCopyUnderFaults` runs                                                                    |
| `TX_PBT_FAULTS_SEED_BASE` | the sweep's first seed; random by default, and logged                                                          |
| `TX_PBT_FAULTS_TIME`      | how long the sweep runs; 2m by default, `PBTTIME` under make                                                   |

To rerun a failure, set its repro line and run the input:

```sh
TX_PBT_FAULTS=1 TX_FAULTS_SEED=7 TX_FAULTS_DISK='write.flip=0.2' \
    go test -run 'FuzzMemTransferUnderFaults/seed#0' ./internal/memxfer
TX_PBT_FAULTS=1 TX_PBT_FAULTS_SEED=9 go test -run TestTreeCopyUnderFaults ./internal/cmd/filexfercli
```

The [Ideas](../IDEAS.md) bug list records each failure the properties have
found, with its repro line.
