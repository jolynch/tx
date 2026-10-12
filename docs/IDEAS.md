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
- **Fast transfers rebuild and log a limiter they never use.** The client's
  10s probe reporter sends `obs-link-mbps` in every mode, so the server
  rebuilds the gentle limiter and logs `txfer-probe ... limiter=`, but `SEND`
  applies it only to gentle items. The probe also competes with the transfer,
  so `observed_link` swings between 200 and 2800 Mbps. Skip the report, or at
  least the log line, in fast mode.

## Performance

- **`bufpool.Acquire` allocates a release closure.** `Get`/`Put` are
  allocation-free, but `Acquire` returns a new closure per call, and
  `Release` boxes a pointer for a bare slice. Move the per-frame callers
  (`send.go`, `cxsum.go`, the client's frame buffers) to `Get`/`Put`.
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
- **`CXSUM` always computes xxh64.** `hashChecksumRange` hashes every range
  with both xxh128 and xxh64, doubling server hash CPU when only one was
  requested.
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

- **Fault injection.** Inject disk faults (short reads, `EIO`, `ENOSPC`),
  network faults (resets, stalls, partial writes, slow peers), and server
  restarts during `SEND`, `ACK`, `CXSUM`, and `SYNC`. Every run must either
  finish with correct data or fail loudly. The per-call syscall struct behind
  the zero-copy `SEND` tests is a model for injecting faults without global
  hooks.
- **Untested failure paths.** `mapLookupError` (every `ACK`, `CXSUM`, and
  `SEND` file-lookup failure), `checkTransferDeadline` (gentle-mode
  `TOO_SLOW`), `isDirectIOReadError`, and `EnqueueCacheRestoreBatch` have no
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

- **Split the oversized files.** `client.go` (about 3,850 lines: options,
  manifest model, download orchestration, probe math, ACK plumbing),
  `cli.go` (about 2,200), and `client_tcp.go` (about 1,500: pool, auth, verbs)
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
