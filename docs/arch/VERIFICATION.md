# Verification

This document describes how `tx` verifies a completed copy, how it checks
integrity while bytes are still in flight, and how the deterministic sampled
data verifier works.

## Overview

There are three integrity layers:

1. **Metadata verification** after copy.
2. **Optional sampled or full data verification** after copy.
3. **In-flight per-window integrity** during `SEND`/`ACK`.

The CLI reports post-copy verification with bracketed statuses:

- `[ok]`: verification completed fully and found no mismatch
- `[partial-ok]`: a duration-bounded data verify stopped after its budget and
  grace period, reported what it did verify, and still returned success
- `[fail]`: metadata mismatch, checksum mismatch, or verification transport
  failure

## Metadata Verification

`copy` runs metadata verification by default with `--verify meta`.

The client rebuilds a local manifest view of `LOCAL_DST` and compares it to the
server manifest captured during transfer. It checks:

- regular files: size, nanosecond mtime, and mode bits
- hardlinks: mode bits and link target identity
- symlinks: mode bits and link path

If any file is missing, stale, or unexpectedly present, the CLI reports:

```text
copy-verify-meta: [fail] mismatch new=<n> stale=<n> rm=<n>
```

On success it reports a typed summary:

```text
copy-verify-meta: [ok] total=<n> files=<n> hardlinks=<n> symlinks=<n> dirs=<n>
```

Metadata verification catches truncated writes, missing files, permission drift,
and link-target drift without rereading file contents.

## Data Verification

Data verification extends the metadata pass. Available modes are:

- `--verify N%data`: sample `N%` of 4 MiB frame slots from each file
- `--verify full`: verify every frame slot of every file
- `--verify <duration>`: run full data verification under a wall-clock budget

The data path is:

1. Build a deterministic sample generator per file.
2. Read the selected ranges locally and hash them with `xxh128`.
3. Issue `CXSUM` requests to the server for the same ranges.
4. Compare returned checksum tokens to the local hashes.

The verifier prints one final summary line:

```text
copy-verify-data: [ok] files=<n> samples=<n> pct=<n> elapsed=<dur>
copy-verify-data: [partial-ok] files=<n> samples=<n> budget=<dur> elapsed=<dur>
copy-verify-data: [fail] <reason>
```

### Partial Verification Under a Time Budget

When `--verify` is a duration, the client behaves as a bounded full verifier:

- it stops dispatching new file verification tasks when the budget expires
- it allows already-started checksum work to finish for a short grace period
- if the grace period also expires, it cancels in-flight checksum transport,
  logs how much verification completed, and returns success

This is why a budgeted verify can end in `[partial-ok]` instead of `[ok]`.

Real checksum mismatches found before the forced stop still fail the command.

### Checksum Batching

The verifier does not send one giant `CXSUM` request per file. Instead it:

- generates samples incrementally
- batches checksum targets per request
- caps each command under an internal size budget so it stays comfortably below
  the FTCP 4 MiB command-line limit

This keeps both memory usage and command size bounded on very large files.

## Deterministic Sampling Algorithm

The sampler lives in `internal/sampler` and is designed to be:

- deterministic per file across runs
- bounded-memory even for multi-TiB files
- broad in coverage
- friendly to mostly sequential I/O for partial sampling

### File Identity and Seed

Each file's sample sequence is seeded from:

- manifest root path
- entry path
- file size
- file ID

The resulting seed is stable for the same file identity, so repeated runs pick
the same sample layout.

### Frame Slots

The file is divided into fixed-size 4 MiB frame slots, matching the transfer
frame size. For a file of size `S` and frame size `F`, the number of slots is:

```text
frameSlots = ceil(S / F)
```

The sample count for `N%data` is computed from frame slots, not from raw bytes,
using the same rounded-up percentage rule the CLI already used before the
sampler rewrite:

```text
sampleCount = ceil(frameSlots * N / 100)
```

That keeps user-visible sampling density stable while avoiding a giant
precomputed permutation.

### Partial Sampling: Stratified Buckets

When `sampleCount < frameSlots`, the sampler uses deterministic stratified
sampling:

1. Split the slot domain into `sampleCount` buckets.
2. Pick exactly one slot from each bucket.
3. Derive the within-bucket offset from the deterministic seed.
4. Emit buckets in ascending order.

This gives broad coverage without duplicates, and because bucket order is
ascending, local and remote reads remain mostly sequential.

### Full Sampling: Coprime-Step Permutation

When `sampleCount == frameSlots`, the sampler must visit every slot exactly
once without allocating `frameSlots` entries.

It does this with a modular walk:

```text
slot(i+1) = (slot(i) + step) mod frameSlots
```

The starting slot and step are both seed-derived. The only extra rule is that
`step` must be coprime with `frameSlots`, meaning:

```text
gcd(step, frameSlots) = 1
```

That property guarantees the walk is a permutation of all slots rather than a
short cycle. In practice this means `--verify full` can stop early under a time
budget and still have touched slots spread across the file instead of only the
front of the file.

### Intra-slot Jitter

Selecting a slot does not force sampling the slot's first bytes. For each chosen
slot, the sampler also derives a deterministic intra-slot jitter and selects a
small byte range within the slot. That keeps the sample reproducible while
avoiding always hashing the same leading bytes of each 4 MiB region.

## In-flight Integrity

`tx` also validates integrity while data is still being transferred.

During `SEND`, the server computes and emits a per-window checksum token in the
final `FXT/1` trailer. The client includes that token in its `ACK`:

```text
ACK <txferid> fd=<fid> <path> ack-token=<ack-bytes>@<server-ts>@<hash-token>
```

The server compares the presented hash token against the stored hash for the
served window. This confirms that:

- the server sent the bytes it expected to send
- the client received and acknowledged that exact window
- the framing/compression/decompression path did not silently corrupt the data

This is separate from post-copy verification:

- `ACK` integrity is a transport/window correctness check
- `CXSUM` verification is a post-write local-vs-remote content check

## Failure Semantics

Verification fails immediately on:

- metadata mismatch
- local sample read failure
- `CXSUM` request failure before a forced timeout path decides to stop
- checksum response mismatch
- malformed checksum response or range mismatch

Budgeted verification may still succeed with `[partial-ok]` when:

- the time budget expires
- already-started work is allowed to finish for the grace period
- remaining in-flight work is force-stopped afterward

When that happens, the CLI logs how many files and samples were verified before
stopping.
