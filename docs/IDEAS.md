# Ideas

Work worth doing later. Each entry says what to change and why, so it can be
picked up cold. Add ideas you find while working on something else here
instead of widening the current change; remove an entry when it ships.

## Bugs

- **Trailer user and group names can corrupt the line.** The server turns
  spaces in `meta:user` and `meta:group` into `_`, but passes other bytes
  through: a `\n` ends the trailer early, and a `\r` either stays inside the
  name or, at the end of the line, is trimmed silently. Escape or reject them.
- **Cancelled split downloads can leave their initial output writer open.**
  `downloadWindows` opens the writer before acquiring a SEND slot and
  launching the first window. Cancellation at either step leaves it open.
  Guard ownership until the first window takes it, closing an unclaimed
  writer on every exit.
- **`tx recv get` drops flags for local sources.** The local branch in
  `cli_get.go` builds `localGetArgs` and silently discards `--compress`,
  `--encrypt`, `--auth-token`, `--auth-token-file`, `--keys`,
  `--require-server-recipient`, `--require-server-recipient-file`,
  `--ack-every`, `--deadline`, and `--concurrency`. Warn like `tx recv copy` does (`warnIgnoredLocalFlags`), or
  reject them.
- **A long-lived client keeps a stale server key.** `resolveTCPAuthState`
  discovers the server key once per client. After the server restarts with a
  new key (the default when its keys directory is missing or read-only), every
  handshake fails as `server rejected authentication` until the client is
  recreated. On a refusal from a client that does not require the server's
  recipient, re-discover once, and retry if the key changed. Pools copy the
  auth state, so the key must be shared by pointer, as `tokenPref` is.
- **A budgeted `--verify` always checks the same files first.** The seed
  spreads samples within each file, but `verifyCursor` takes files in manifest
  order, so a run that keeps hitting its budget re-checks the start of the tree
  and never reaches the end. Walk the file list with the seeded coprime step
  as well, but only under a budget, so `--verify full` stays sequential.
- **Unconfirmed durability exits 0.** A background drain that
  `defaultFsyncTimeout` cuts short, and a failed or timed-out final `syncfs`,
  are `--stats` warnings, not failures. With `--fsync-interval -1`, `syncfs`
  is the only durability signal. Decide whether either should fail the run,
  and report the files whose durability is unknown. `tx-bench report` and
  the `run_end` event do not show the warnings yet. A sync-path copy drains
  its sync worker after every round, so a drain the timeout cuts short can
  leave that round's worker syncing while the next round runs, and failures
  it records afterwards are never marked for refetch.
- **Bytes changed after hashing pass without `--verify`.** The server hashes
  what it read and the client hashes the buffer before writing it, so a
  flipped read on the server (`read.flip`) or a lost or flipped write on the
  client (`write.lost`, `write.flip`) leaves a file marked done with wrong
  bytes. Only `--verify` catches them, as FAULTS.md's catalog says.
  pbt-faults confirms all three, for example `TX_PBT_FAULTS=1
  TX_FAULTS_SEED=7 TX_FAULTS_DISK='write.flip=0.2' go test -run
  'FuzzMemTransferUnderFaults/seed#0' ./internal/memxfer`. Rereading written
  ranges before the ACK would catch the client's; the server's needs a
  second read or a checksum from below the page cache.
- **A revived file can make a logged transfer incomplete again.** A file
  acknowledged missing (`-1`) counts as finished, so a transfer whose last
  pending file hits a transient `ENOENT` logs `txfer-complete` and emits
  `transfer_done` before a later round delivers that file. Completion is
  logged once, so `--stats` and the event timeline end at the early
  completion. A late split-window ACK that the server applies after the
  client's `-1` can also revive a file the client gave up on, leaving the
  transfer incomplete until a resume. Hold completion while the client may
  still retry a missing file, or emit a second record.
- **`StartFromManifest` reports nothing for batches it never starts.** When
  its context ends, it stops submitting batches and returns without an
  error for the files they held, so a caller that checks only `Errors`
  takes an unfinished transfer for a finished one. Report the context's
  error, or count the skipped files as failed.
- **A trickling peer can hold a connection.** The server's read progress
  deadline re-arms on every read, so a peer that sends one byte just inside
  the idle timeout keeps a command line or request body open for as long as
  its size allows. Cap the total time to read one command line.
- **Heartbeats run every half window, not every quarter.** `takeStale`
  probes a session only when its age reaches the interval, but a session
  probed at one tick is a round trip younger than that at the next, so it is
  skipped. At an `--idle-timeout` of about 200ms or less, sessions can die
  between heartbeats. Compare against the previous tick. The heartbeat loop
  also keeps running after keep-alive is withdrawn, and re-enqueues a
  session whose probe came back without a grant; discard it instead.
- **Requests without keep-alive dial inline.** A pool without keep-alive no
  longer pre-dials, since its single-use connections cannot be heartbeated,
  so every SEND, ACK, and CXSUM pays a dial, and a Noise handshake when
  encrypted. `BenchmarkTransferInMemoryModes` puts it at +6.5% wall time
  for encrypted 4 KiB files, with CPU per GiB unchanged, and within noise
  for plaintext or 1 MiB files. If it matters, pre-dial at most a few
  connections and evict any older than a fraction of the server's idle
  timeout.
- **The cost-ratio benchmark never uses the pools.** `startMemTransfer`
  builds its client after the probe, so `BenchmarkTransferInMemory` and
  `TestTransferCostRatio` dial every request directly, and their `+ka` modes
  measure no keep-alive. `BenchmarkTransferInMemoryModes` probes first;
  move the cost grid onto it.
- **Fast-mode probes open one connection per server CPU.** `probeLink` runs
  `probeParallel` with no cap, so a probe to a 192-core server uploads
  192 MiB at the default probe size. Cap the fan-out, for example at 16, if
  the measurement holds up.
- **Measure the probe's send-queue cap on a fast link.** Probe connections
  set `TCP_NOTSENT_LOWAT` to 16 KiB, so on a 10 Gbps link the writer is
  woken every few microseconds of data, and sealing an encrypted batch no
  longer overlaps sending. Probe throughput may read low there, hardest for
  the single-connection `probeLinear`. Loopback shows no difference; A/B it
  between two hosts, and raise the cap (any value that drains well within
  30s works) if it costs throughput.
- **A buffering proxy hides probe progress.** The probe client's writes
  keep pace with the bytes its peer acknowledges, so a proxy that
  acknowledges the whole upload early, then forwards it slower than 30s
  allows, fails a working probe. Only the server sees that progress; a
  client allowance sized by the bytes in flight would cover it.
- **Request-body errors leak internal text onto the wire.** A stalled ACK
  body is answered `ERR BAD_REQUEST invalid ACK request body: read frame
  payload: read tcp <addr>->...: i/o timeout`, which carries socket addresses
  and Go error strings. Map read errors to a fixed message, and log the
  detail on the server.
- **`StartFromManifest` ignores its entries' progress.** It passes only file
  IDs to `GetFiles`, which looks each entry up in `Manifest`, so
  `StartFromManifestRequest.Entries[i].Progress` never takes effect and a
  caller that sets it there restarts every file from 0. Resume from the
  request's entries, or document that progress belongs on the manifest.
- **A stale SEND handler can overwrite a newer window's hash.** `Serve`
  does not join its handlers, and a client that gives up on a SEND and
  retries it on a new connection can race the old handler, which records
  its window hash when it finishes. With an unchanged source the hashes
  match; if the file changed between the two reads, the client's ACK fails
  with `CONFLICT`. That is loud, not silent, but a server could drop a
  window's hash recorded by a SEND whose connection has already closed.
- **A replayed ACK fails with `CONFLICT`.** The first ACK of a window
  consumes its recorded hash, so the same ACK sent again cannot verify and
  the server answers `CONFLICT window ack hash token mismatch`. When an ACK's
  response is lost and `retryAck` resends it, the client reports a failed
  batch whose files the server already counted as done. Accept an ACK that
  matches a file's acknowledged state as a no-op. MSG `dup` and
  `lose-response` faults reproduce it.
- **One fault in a SEND batch discards the whole batch's progress.** The
  client ACKs a batch only after reading its entire response, so a failure
  at file 30 of 40 leaves files 1 to 29 written but unacknowledged, and the
  next round downloads all 40 again. Under a steady per-file fault rate, a
  large batch of small files can stop making progress. ACK the files read
  before the failure.
- **Repair within the same run.** A verify mismatch marks the file and exits
  1, and the next run fetches it. The run could instead refetch and reverify
  the mismatching files before it exits.

## Performance

- **Bound batch lookup metadata.** SEND and CXSUM resolve all request items
  together to avoid per-file transfer locking. Their lookup and reference arrays
  add peak memory beyond the decoded body limit, especially for many tiny items.
  Measure dense requests and consider bounded lookup chunks if that memory cost
  outweighs the extra lock acquisitions.
- **Choose checksum workers by average work size.** Evaluate a fixed pool on
  both the verifier and CXSUM server, chosen once per batch/request from average
  requested range size: four workers at 4 KiB, three at 16 KiB, two at 64 KiB,
  and one at 256 KiB or larger. Pass reusable work objects that own their
  buffer, file/range, hasher, and result index. Keep transfer connection counts
  unchanged and bound queued work and bytes. Preserve file grouping, ordered
  results/errors, cancellation, and cleanup. Workers read and hash their owned
  ranges. The cached profiles in [VERIFY.md](bench/VERIFY.md) identify per-file
  overhead; worker scheduling may cost more than it saves there.
- **Observed-link probes compete with the transfer.** The fast-mode limiter
  fix removes the unused limiter, but repeated throughput probes still compete
  with SEND and make observed-link rates noisy. Measure probe scheduling before
  using those samples for more tuning.
- **`bufpool.Acquire` allocates a release closure.** `Get`/`Put` are
  allocation-free, but `Acquire` returns a new closure per call, and
  `Release` boxes a pointer for a bare slice. Move the per-frame callers
  (`send.go`, the client's frame buffers) to `Get`/`Put`.
- **Probes bypass the pools.** `probeTCP` dials directly, and a fast-mode
  throughput probe opens one connection per server CPU, in both the transfer
  and converge phases of `tx recv copy`. Probe over pooled connections, or
  once per client.
- **Data-frame compressors are built per frame.** `WrapCompressedWriter`
  creates a zstd or lz4 writer for every compressed SEND frame, sized to
  GOMAXPROCS in fast mode. At 24 CPUs a 4 KiB frame costs about 19
  allocations and 630 KiB with zstd (7 and 200 KiB with lz4), which dominates
  small compressible files sent with `comp=zstd`. Pool them with
  `Reset`, apart from the framed-body pool, which must keep zstd's checksum.
- **Framed-body zstd buffer churn.** `CompressZstdPooled` reallocates about
  8 MiB of encoder history and decoder block whenever GC empties the pool:
  about 60% of the bytes allocated during a small-file `--verify full`.
- **Per-file verification overhead.** CXSUM still opens and stats every item,
  and the client's `ReadString` allocates each frame's header and trailer
  line: response parsing is still about 4% of CPU. The cached profiles in
  [VERIFY.md](bench/VERIFY.md#profiling-findings) support testing a Linux
  `unix.Open`/`os.NewFile` helper to skip failed regular-file poller setup. Keep
  close-on-exec, EINTR handling, cleanup, and error mapping; measure real paths
  because the memfd server duplicates descriptors instead of opening paths.
- **Explicit concurrency one opens two pooled connections.** `ProbeLink`
  clamps explicit `WithConcurrency(1)` to two. The verifier stays at one worker;
  the benchmark reports the actual pool size separately.
- **Server I/O depth defaults disagree.** CLI defaults to four, but a
  zero-valued server option advertises eight. Unify these separately.
- **Copy into an existing destination walks the source twice.** `runCopyCLI`
  runs `runTransfer` (`PROBE` + full `TXFER` walk) before `runSync`, which
  probes again and sends `SYNC`, a second walk. `runSync` uses only the
  `TXFER` manifest's header fields (root, mode, link, concurrency) before the
  `SYNC` result replaces it. Skip the `TXFER` when `LOCAL_DST` exists, except
  under `--skip-fetch`, and reuse the first probe.
- **Per-file cost of small files.** `tx-bench local --profile small` on
  tmpfs, pinned to 4 cores, copies 4–64 KiB files at about 5.5 client and 4
  sender core-s/GiB, against under 1 for 16 MiB files. The in-memory
  `make bench-throughput` puts tx's own share at 5–9.5x the CPU of 16 MiB
  files for the same bytes (4 KiB files); the goal is 1x. In its profile, ACK
  handling is about 12% of CPU (client and server together) and client
  trailer parsing about 5%. CPU profiles of the tx-bench run show what is
  left:
  - Receiver: creating output files is about 22% (`MkdirAll` 5% of it), and
    applying trailer metadata (chmod, chown, utimes) about 16%. Create files
    with their final mode, skip chown when the owner already matches, and
    remember directories already created.
  - Receiver: zstd-compressing each ACK body is about 5%; small ACK bodies
    may not need compression.
  - The transfer lock still caps throughput past about 8 CPUs: 4 KiB
    files peak near 640 MiB/s at 8 CPUs and fall to about 500 at 24. SEND
    and ACK take it once per request, but at 24 CPUs each SEND carries
    only about 20 files, so its exclusive side is still taken thousands of
    times a second. In a block profile, `SetTransferFilesState` (SEND's
    Running mark) is 45% of blocked time and `AcknowledgeTransferFiles`
    17.5%, and each waits for readers to drain. Two fixes, best together:
    - Make SENDs and ACKs larger. Batches are cut by bytes only and split
      greedily into one group per free SEND slot. Cap files per batch so
      there are a few times more batches than slots, and split a batch
      only when it is larger than an even share.
    - Take no exclusive lock per request: keep per-file state and
      `StateCounts` in atomics, and the TTL as an atomic deadline, so the
      transfer lock guards only registration and deletion.
  - CPU per GiB grows with available cores even though the work does not,
    mostly because of the host, not tx. On a Ryzen AI 9 HX 370 (4 Zen5 and
    8 Zen5c cores, two threads each), firmware drops core clocks from about
    4.9 GHz with one busy core to about 1.7 GHz with four or more, whatever
    the governor, so core-s/GiB triples while cycles per GiB stay roughly
    flat. Rows above the 12 physical cores add SMT, and unpinned
    `THROUGHPUT_CPUS` rows also use cores outside GOMAXPROCS for kernel
    loopback work. A single grid cell can vary by 45% between runs, so judge
    a change with interleaved A/B runs on pinned cores, not one grid.
  - About 25 allocations per 4 KiB file remain. `CollectFileFrameMetadata`
    formats mode, uid, and gid as strings (4 per file); `streamSendItem`
    moves `maxWSizeHint`, `md`, and `frameOffset` to the heap; the client
    reads each frame's header and trailer with `ReadString`, which also has
    no length bound; and `buildAckToken` builds each ACK token as a string
    before copying it into the request body.

## Security

- **Credentials carry no scope.** Any allowlisted token or age key can list
  and read every transfer the server holds. Bind a transfer to the credential
  that created it, or allow per-credential roots, if operators need isolation.
- **Per-source connection limits.** `MaxUnauthenticatedConns` bounds sockets
  that have not finished a handshake, but one host can still fill it, and an
  any-key server spends X25519 work on every opening. A per-address cap or
  rate limit would stop a single host from filling the global cap.
- **Rate-limit the legacy AUTH log line.** Any unauthenticated peer can make
  the server log `legacy AUTH from <addr>` once per connection; the admission
  cap bounds concurrent connections, not their rate. Log once per address per
  interval, or count and summarize.
- **Check credential file permissions.** The server key file and token files
  load even when group or others can read them. Warn, as ssh does, when a
  private key or token file has group or other permission bits set.
- **Record the proven client key.** In token mode and on any-key servers the
  server accepts any key it can verify and records nothing about it. Logging
  the credential ID or a key fingerprint in connection events would give
  operators a stable identity to audit.
- **Rediscover a changed server key.** A client caches the discovered server
  key, so after a server with an ephemeral key restarts, every new connection
  fails until the client restarts. Rediscovering once after a refusal before
  message 2, unless the key is pinned, would recover without weakening pins.

## Testing

- **Page-cache tests do not check advisory syscall arguments.** The gate
  tests pass if `touchPages` uses `FADV_DONTNEED` instead of `FADV_WILLNEED`.
  Check advice, offsets, and lengths with syscall interception. Exact
  residency assertions can fail under unrelated kernel eviction, and
  `WILLNEED` schedules reads asynchronously.
- **Benchmark varied filesystem layouts.** The generated `mixed` dataset used
  by `TestBenchAcceptance` has nonempty regular files in three flat part
  directories. Add a seeded layout profile with nested and empty directories,
  zero-byte files, symlinks, and hardlink groups, and verify the copied tree
  with the existing oracle. Keep its results separate from the current
  profile so historical throughput comparisons retain the same workload.
- **Fault-injection properties.** `make pbt-faults` fuzzes whole transfers
  and CLI copies under swarms of faults (FAULTS.md). Still missing: server
  restarts and client crashes mid-transfer, which would test the
  `.tx/` state and durability claims; a property input that varies the
  zero-copy threshold, which `ServerOptions` does not expose; reaching the
  page-cache `readahead` and `mincore` sites, which no property's copies
  call; more DISK and SYS faults in `TestTreeCopyUnderFaults`, whose small
  trees give low rates few operations to hit; and merging `internal/benchio`
  into `internal/faults`.
- **A tx-bench harness test is load-sensitive.** One full `go test ./...` run
  failed `internal/bench/harness`'s
  `TestLocalReadOnlyImportedDirectoryStartsFreshRuns...` with "1 of 2
  measured runs failed", and it passed on an immediate rerun. Find what the
  measured run depends on that load can starve.
- **`TestUpdateBatchThresholdConcurrentGrowthLogsOnce` is flaky.** About 1
  run in 20 fails with "timed out waiting for first growth log" under
  `-race`, already at commit c480f1e, so the growth log is sometimes never
  written within its one-second wait. Find which interleaving skips it.
- **A lost response logs negative byte counts.** After MSG `lose-response`
  resets the socket, `tcpByteCounts` returns zeros, so `cmd_end` reports
  `req_bytes` and `resp_bytes` as zero minus the counts at the command's
  start. Clamp the deltas or read the counts before the reset.
- **Verify reads share one fault key.** The client's source and destination
  verify reads both use `SiteVerifyRead` with the same file ID and offset,
  so the destination read takes attempt 1 at the source's offset. Give the
  destination its own site.
- **Wrapper-held bytes hide from the liveness peek.** Bytes a NET `dup`
  holds in `faults.Conn` are invisible to `sessionConnAlive`'s `MSG_PEEK`,
  so a pooled connection holding them is reused where a real duplicate
  would show in the peek and evict it. A pbt-faults failure that needs this
  may be the wrapper's, not tx's.
- **SYS errnos are per syscall, not per call site.** `splice.fail` picks
  `EINTR` or `EIO` at every splice, so the zero-copy loop never sees an
  injected `EAGAIN` and reaches `waitSocketWritable` only on a real one.
  `EAGAIN` is real only at the splice into the nonblocking socket, not the
  one from the file, so it needs an errno set per call site.
- **Fault shims do not reach all filesystem IO.** TXFER and
  SYNC walk the tree with `filepath.WalkDir` and `Lstat`, and the client
  writes `.tx/` manifests and progress through `os` directly. Faulting them
  needs a filesystem seam on a per-file path, so measure its cost first.
  Local copies gate `copy_file_range`, but their opens, buffered fallback,
  and stdout reads do not use DISK gates.
- **Untested failure paths.** `mapLookupError` (every `ACK`, `CXSUM`, and
  `SEND` file-lookup failure), `checkTransferDeadline` (gentle-mode
  `TOO_SLOW`), and `EnqueueCacheRestoreBatch` have no
  coverage, and `internal/cliflags` and `internal/metrics` have no tests.
- **CLI tests reimplement the server.** `serveFTCPConn` in `cli_test.go`
  reimplements AUTH handling, `PROBE`, and `SYNC` framing instead of running
  `ftcp.Serve` against a fake `Deps`, so protocol changes must be mirrored by
  hand.

- **tx-bench never runs the sender's default `--exit-after`.** The harness
  always passes `--exit-after never`, so it skipped the `exitAfterDeps` path
  that once cloned the whole transfer per ACKed file. Run with the default,
  or add a small-file acceptance case with `--exit-after 60s`.

## Code health

- **The IO lint has known gaps.** forbidigo matches calls by name, so it cannot
  see a method reached through an embedded `*os.File` or `net.Dialer`, or
  through an interface such as `interface{ Sync() error }`, or an open through
  an `fs.FS` obtained some other way. golangci-lint analyzes only `GOOS=linux`
  files, so `_other.go` files go unchecked. A call through an interface, such
  as `http.DefaultTransport.RoundTrip`, escapes it too, and a nil injector
  passed to a sysio function (`sysio.Fdatasync(nil, fd)`) skips every gate
  and lints clean. Stdlib methods that make a cataloged syscall are not banned
  either: `(*net.TCPConn).SetNoDelay` and the buffer setters (setsockopt),
  `(*net.TCPConn).File` (dup), and `io.Copy` between two files
  (copy_file_range). forbidigo covers only the data-path packages, not
  `internal/filexfer` or `internal/filexfer/encoding`, where the manifest walk
  lives. The lint guards against raw IO added by accident; review still has to
  catch these.
- **A full filesystem boundary needs a file wrapper.** sysio gates raw
  calls, but `*os.File` still exposes reads, writes, syncs, and descriptors.
  A wrapped file type could enforce DISK and SYS gates across those paths.
  Measure its cost before extending the boundary.
- **Unify injector plumbing.** The injector travels through client and
  server options, request contexts, `fsync.WithFaults`, and the process-wide
  `pagecache.SetFaults`. Give each server or client one explicit owner so
  independent instances can inject faults without sharing page-cache state.
- **The pool's liveness peek allocates on every borrow.** `sysio.PeekAlive`'s
  closure, byte buffer, and result escape to the heap: 3 allocations per
  borrow. Pass them through a small reusable struct, and lock the path in with
  a zero-allocation test.
- **`client_pool_test.go` is past the size rule.** It holds about 1,500
  lines. Fold the pool's overlapping unit cases into `FuzzTCPConnPool`, then
  split what remains by area: acquire and return, heartbeats, and refills.

- **Transfer states start at 1.** `defaultTransferTTL` opens the `iota`
  block in `store.go`, so `TransferStateStarted` is 1 and `Missing` is 4,
  against the rule that keeps unrelated constants out of `iota` blocks. Give
  the states their own block, and check what reads the numeric values.
- **`internal/aead/session_test.go` is past the size rule.** It holds about
  1,300 lines. Split it by area: handshake, record framing and batching, and
  the progress deadlines.
- **`store_test.go` is past the size rule.** It holds about 1,700 lines.
  Fold overlapping state-transition cases into `FuzzStoreStateCounts` and
  `FuzzAckMissingRevival`, then split by area.

- **Split the oversized files.** `client.go` (about 3,850 lines: options,
  manifest model, download orchestration, probe math, ACK plumbing),
  `cli.go` (about 2,200), `client_tcp.go` (about 1,500: pool, auth, verbs),
  and `internal/filexfer/ftcp/send.go` (about 1,500: requests, windows,
  buffering, and zero-copy)
  mix several concerns, as do `cli_test.go` and `client_test.go`. Split along
  those seams; the 2026-05 split of `cli.go` into per-command files is the
  model.
- **TXFER and SYNC repeat setup code.** Their transfer setup overlaps, and the
  close-the-updates-channel closure is copied three times in `txfer.go` and
  `sync.go`.
- **Short forms for common auth flags.** The client has `-t` for
  `--auth-token` and `-k` for `--keys`, but `tx send tree` has no short form
  for `--require-auth-token`, the usual way to start an authenticated
  server. `-t` is free there and would mirror the client.
