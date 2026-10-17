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
goroutines interleave.

## Cost when disabled

A nil `*faults.Injector` is the disabled state, as a nil `events.Sink` is.
Every wrapper returns what it was given, and every gate returns nil. The shims
sit at connection, file, and syscall-site granularity, never inside the
read or write loops: production pays a nil check per connection accepted or
dialed, per file opened, per SEND frame, and per gated syscall, plus one
context lookup per SEND and CXSUM request. SEND's buffered reads go through
an `io.ReaderAt` whether or not they are wrapped, one interface call per read.
A wrapped connection or file adds one more call per read or write.

## Where the shims sit

| IO | Shim | Sites |
|---|---|---|
| Connections | `WrapConn` returns a `*faults.Conn` | every client dial (`dialTCP`), every server accept (`Serve`) |
| File opens | `OpenErr` gate | SEND and CXSUM, before `Deps.OpenFileRef` |
| File reads | `WrapReaderAt` | SEND buffered frames, CXSUM, client `--verify` |
| File writes | `WrapWriter` | client download outputs (`openDownloadOutput`) |
| Syscalls | `Sys` gate, and `WrapSplice`, `WrapTee`, `WrapPoll`, `WrapCopyFileRange` | see below |

Gated syscalls:

| Syscall | Sites |
|---|---|
| `splice`, `tee`, `poll` | the zero-copy SEND loop, through its per-call `zeroCopySyscalls` |
| `pipe`, `fcntl` (`F_SETPIPE_SZ`), `dup` | zero-copy SEND setup |
| `fadvise`, `readahead` | SEND sequential hints and frame readahead |
| `fstat` | SEND and CXSUM file stats, the background fsync batch |
| `fdatasync`, `syncfs`, `dup` | the fsync worker (`fsync.WithFaults`) and `SyncfsDir` |
| `copy_file_range` | daemonless local copies |
| `mmap`, `mincore`, `fadvise` | the page-cache probe, restore, and eviction |
| `setsockopt` | `SetNoDelay` and socket buffer sizes on dial and accept |

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

## Enabling

Library code takes an explicit injector: `ftcp.ServerOptions.Faults`,
`tx.WithFaults`, and `fsync.WithFaults`. The `tx` binary builds one from the
environment, prints a warning with the line that reproduces the run, and
passes it to the server or the client command:

```text
TX_FAULTS_SEED=42   the seed for every fault decision
TX_FAULTS_NET       network faults
TX_FAULTS_MSG       request and response faults
TX_FAULTS_DISK      file open, read, and write faults
TX_FAULTS_SYS       syscall faults
```

Each family variable is a comma-separated list of `kind=rate[:dist]`
entries. No fault kinds exist yet, so any entry is an error. A variable set
to blank space counts as unset, so when none has a value the injector is nil.
`TX_FAULTS_SEED` alone turns on every wrapper while injecting nothing, which
tests use to show the shims are transparent; `faults.Injector.Counts` reports
which sites each injector reached.
