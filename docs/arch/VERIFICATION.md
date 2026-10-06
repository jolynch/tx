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
copy-verify-meta: [fail] mismatch new=<n> (<bytes>) stale=<n> (<bytes>) rm=<n>
```

On success it reports a typed summary:

```text
copy-verify-meta: [ok] total=<n> files=<n> hardlinks=<n> symlinks=<n> dirs=<n>
```

Metadata verification catches truncated writes, missing files, permission drift,
and link-target drift without rereading file contents.

## Data Verification

Data verification extends the metadata pass. Available modes are:

- `--verify N%data`: read and compare about `N%` of the bytes, as whole
  4 MiB frame slots
- `--verify full`: verify every byte of every file
- `--verify <duration>`: run full data verification under a wall-clock budget

The data path is:

1. Draw a random run seed and build a sample generator per file from it.
2. Read the selected ranges locally and hash them with `xxh128`.
3. Issue `CXSUM` requests to the server for the same ranges.
4. Compare returned checksum tokens to the local hashes.

The verifier prints one final summary line. `files` and `bytes` report how
much was verified out of every regular file with data:

```text
copy-verify-data: [ok] files=<n>/<n> (<pct>) bytes=<size>/<size> (<pct>) samples=<n> pct=<n> seed=<16 hex> elapsed=<dur>
copy-verify-data: [partial-ok] files=<n>/<n> (<pct>) bytes=<size>/<size> (<pct>) samples=<n> budget=<dur> seed=<16 hex> elapsed=<dur>
copy-verify-data: [fail] seed=<16 hex> <reason>
```

The seed identifies the run's samples. There is no flag to set it; tests
replay a run from the printed value.

Local copies print the same lines with the `local-verify-data` prefix.

### Partial Verification Under a Time Budget

When `--verify` is a duration, the client behaves as a bounded full verifier:

- it visits each file's slots in a permuted order, so a run cut short has
  covered slots spread across the file (see Full Sampling)
- it stops dispatching new verification batches when the budget expires
- remote copies let already-started checksum work finish for a short grace
  period; if that also expires, in-flight checksum transport is cancelled
- it logs how much verification completed and returns success

This is why a budgeted verify can end in `[partial-ok]` instead of `[ok]`.

Real checksum mismatches found before the forced stop still fail the command.
Local copies have no grace period: hashing in progress stops at its next
1 MiB read.

### Checksum Batching

The verifier does not send one giant `CXSUM` request per file. Instead it:

- generates samples incrementally
- packs samples into batches of at most 16 MiB or 1024 samples, filled from
  consecutive files, so a tree of small files costs about one request per
  16 MiB instead of one per file, and a batch may span files. A large file
  splits across batches that workers verify in parallel, so it uses every
  worker. A file counts as verified once all its samples pass, in whichever
  batches they ran. A single slot larger than the cap still forms a batch
- runs twice as many workers as data connections, so one worker hashes local
  ranges while another waits on the server. Each worker hashes its batch's
  local ranges, sends the `CXSUM` request, then compares every item in order:
  file ID, offset, size, and hash. Local copies hash source and destination
  in the same worker
- scales each batch's sample cap with the worker count so every worker gets
  work on small runs
- caps each request body at 3 MiB, or the server's target if smaller
- sets one read deadline for the whole `CXSUM` response, 30 seconds per
  started 4 MiB requested in total (`HashTimeout`), for plaintext and encrypted
  connections alike

This keeps memory use, request size, and the server's hashing per request
bounded on very large files.

## Deterministic Sampling Algorithm

The sampler lives in `internal/sampler` and is designed to be:

- deterministic for a given seed and tree
- bounded-memory even for multi-TiB files
- broad in coverage
- friendly to mostly sequential I/O for partial sampling

### Seed and Determinism

Each run draws a random 64-bit seed from `crypto/rand` and prints it as
`seed=<16 hex>`. Each file's sample sequence is seeded from:

- the run seed
- manifest root path
- entry path
- file size
- file ID

The same seed over the same tree gives the same samples. Each run picks a
new seed, so repeated `N%data` runs cover different data over time.

### Frame Slots

The file is divided into fixed-size 4 MiB frame slots, matching the transfer
frame size. For a file of size `S` and frame size `F`, the number of slots is:

```text
frameSlots = ceil(S / F)
```

Each sample is a whole slot (the last may be shorter), so sampling `N%` of
slots reads `N%` of the bytes. Slot counts use systematic rounding over the
tree's slots, concatenated in manifest order. The seed gives a start `u` in
`[0, 1)`. With `acc` slots in the files before it and `s` slots of its own, a
file gets:

```text
sampleCount = floor((acc + s) * N / 100 + u) - floor(acc * N / 100 + u)
```

computed in integers. Each file's expected slot count is `N%` of its slots,
and the tree's total is `floor` or `ceil` of `totalSlots * N / 100`. A small file may
get no slot, and rounding every file up instead would read small files in
full. If the tree has data but the total would be zero, the file holding one
seed-chosen slot gets exactly that one, so a data check never passes after
reading nothing. At `100%` every slot of every file is read.

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
short cycle. Under a time budget (`--verify <duration>`), this means a run cut
short has touched slots spread across the file instead of only the front.

Without a budget (`--verify full` or `100%data`) nothing cuts the run short, so
the plan sets `Sequential`: each file's slots come in ascending order, which
keeps reads sequential on both hosts. The walk then starts at slot 0 with
step 1. The set of slots does not change.

## In-flight Integrity

`tx` also validates integrity while data is still being transferred.

During `SEND`, the server computes and emits a per-window checksum token in the
final `FXT/1` trailer. The client includes that token in its `ACK`:

```text
ACK <txferid>
fd=<fid> <path> ack-token=<ack-bytes>@<server-ts>@<hash-token>   (one framed body line per file)
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
- already-started remote work is allowed to finish for the grace period
- remaining in-flight work is force-stopped afterward

When that happens, the CLI logs how many files and samples were verified before
stopping.
