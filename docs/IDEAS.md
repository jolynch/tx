# Ideas

Work worth doing later. Each entry says what to change and why, so it can be
picked up cold. Add ideas you find while working on something else here
instead of widening the current change; remove an entry when it ships.

## Bugs

- **`tx recv get` drops flags for local sources.** The local branch in
  `cli_get.go` builds `localGetArgs` and silently discards `--compress`,
  `--encrypt`, `--auth-token`, `--ack-every`, `--deadline`, and
  `--concurrency`. Warn like `tx recv copy` does (`warnIgnoredLocalFlags`), or
  reject them.
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

- **Variable-size AEAD chunks for streaming responses.** The AEAD writer seals
  only full 64 KiB chunks until the response ends, and the reader treats any
  short chunk as the final one. An encrypted `CXSUM`, manifest, or `STATUS`
  stream therefore shows no progress until 64 KiB fills, which is why `CXSUM`
  uses one work-sized deadline instead of a per-frame idle timeout. Two
  options:
  - Pick a smaller chunk size for streaming responses. The size is already in
    each stream's header, so this needs no format change.
  - Add a `Flush` that seals a short non-final chunk. The final-chunk flag is
    already authenticated, so truncation protection holds, but older readers
    reject such a chunk, so the client must advertise support.
- **One key per keep-alive session.** Every response starts a new AEAD stream
  with a fresh X25519 key wrap, so pooled connections still pay a key exchange
  per command.
- **Probes bypass the pools.** `probeTCP` dials directly, and a fast-mode
  throughput probe opens one connection per server CPU, in both the transfer
  and converge phases of `tx recv copy`. Probe over pooled connections, or
  once per client.
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
  - The transfer lock still limits scaling past about 8 CPUs. SEND and
    ACK now take it once per request, but `GetFileRef` (per SEND item) and
    `VerifyTransferFileWindowHash` (per ACK item) still take its read side
    once per file, and at 24 CPUs each SEND carries only about 20 files.
    Resolve file refs and verify ACK hashes in one call per request, or make
    per-file state atomic so the per-file path takes no lock at all.
  - Batches are cut by bytes only, so 100 MiB of 10 KiB files becomes about
    13 batches of 800 files. A batch splits into groups using only the SEND
    slots free when it starts, so late batches can stream all their files on
    one connection while freed slots sit idle. Cap files per batch so there
    are more batches than slots.
  - CPU per GiB grows with available cores even though the work does not.
    In `make bench-throughput`, 4 KiB files cost 9.7 core-s/GiB with
    `THROUGHPUT_CPUS=2` and 62 with 24 (16 MiB files: 1.0 and 4.8). That
    suggests spinning or contention that scales with concurrency; profile
    an unpinned run for scheduler and lock time.

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
  reimplements AUTH, AEAD, `PROBE`, and `SYNC` framing instead of running
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
