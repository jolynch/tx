# Architecture

`tx` is designed to fully saturate modern cloud hardware - SSD, network, and CPU
- during file transfers. Three capabilities make this possible: adaptive
concurrency, adaptive compression, and lightweight verification.

## Concurrency

A single TCP connection cannot fill a 25 Gbps NIC, and a single thread cannot
keep an NVMe SSD busy. `tx` solves both problems with a probe → plan → execute
pipeline that auto-tunes parallelism to match the hardware on both ends.

### Probe

Before any data moves, the client runs a two-phase `PROBE`:

1. **Discovery** — a 1-byte probe measures round-trip latency and retrieves
   server capabilities: CPU count, IO depth per CPU, socket buffer sizes, and
   the server's gentle-mode budgets.

2. **Throughput** — a configurable payload probe (default 1 MiB) measures
   actual link bandwidth. In fast mode the client fans out `server-cpu`
   parallel probe connections to measure aggregate throughput; in gentle mode
   it runs sequential samples.

From these measurements the client computes:

- **Link speed** (Mbps) — rounded to the nearest 100 Mbps.
- **Suggested concurrency** — `server_cpus × io_depth` in fast mode (default
  IO depth is 4), or a server-advertised percentage of CPUs in gentle mode.
  Clamped to `[2, 256]`.
- **Suggested cipher** — resolved during the AUTH key exchange if encryption
  is enabled.

### Connection pool

Once the probe completes, the client pre-warms a TCP connection pool sized to
`concurrency × 1.25` (25 % headroom). Every connection in the pool has already
completed the AUTH handshake, so SEND requests start immediately. When a
connection is returned to the pool after use, it is closed and a background
goroutine opens and authenticates a fresh replacement — the pool is
continuously refilled without blocking the data path. If the pool is empty when
a worker needs a connection it falls back to a synchronous dial.

### Windowing and batching

The probe results feed a planning step that sizes the unit of work each TCP
connection handles. The goal is fixed-size batches: whether a batch contains
many small files or a single slice of a large file, each TCP connection does
roughly the same amount of work.

**Batch size.** The client computes a batch size from the probe:

```
perFileWorkers = suggestedConcurrency / windowConcurrency
batchMaxBytes  = windowBytes / perFileWorkers          (rounded to power-of-2 MiB)
```

The default window is 512 MiB with a window concurrency of 4. On a 24-CPU
server in fast mode (IO depth 4), suggested concurrency is 96, giving
`perFileWorkers = 24` and a batch size of ~32 MiB. The result is clamped: the
floor is the larger of the server and client socket buffers (no point making a
batch smaller than what the OS has already allocated for the connection), and
the ceiling is the window size.

**Small files → packed batches.** The manifest is walked in order and files are
packed into batches until the next file would exceed `batchMaxBytes`. A batch
of 1000 tiny files and a batch containing one 32 MiB file are the same unit of
work — each becomes a single multi-file `SEND` request over one TCP connection.
This avoids the per-connection overhead that makes small-file transfers slow in
tools that open one connection per file.

**Large files → split windows.** When a single file exceeds `batchMaxBytes`,
it is split into windows of that size. Each window is downloaded on its own TCP
connection in parallel, with the number of concurrent windows capped by
`windowBytes / batchMaxBytes`. The pieces are written to the correct offsets in
the output file and acknowledged independently. This means a 1 GB file on a
fast link is not bottlenecked by a single TCP stream — it is sliced into
parallel chunks that saturate the NIC.

**Gentle mode scaling.** When concurrency is low (gentle mode defaults to 25%
of server CPUs), the planner detects that there are not enough slots for both
window concurrency and per-file parallelism. It reduces window concurrency to
match suggested concurrency, giving all slots to concurrent file downloads
rather than splitting individual files. This keeps the batch size large and
avoids fragmenting work on a resource-constrained server.

### Parallel execution

With batches planned and the pool warm, the client issues `SEND` requests
across concurrent workers. Each worker:

1. Borrows a pre-authenticated connection from the pool.
2. Sends a `SEND` command requesting one or more file windows.
3. Reads the `FX/1` frame stream and writes frames to disk.
4. Sends a batched `ACK` confirming received windows.
5. Returns the connection to the pool.

Because every worker has its own TCP connection, there is no head-of-line
blocking: a slow file on one connection does not stall transfers on the others.
On the server side, each `SEND` handler uses `fadvise(SEQUENTIAL)` and
background `readahead(2)` to prefetch the next frame into the page cache while
the current frame is being written to the socket. When encryption is disabled
and the output is a raw TCP socket, the server uses a zero-copy
`splice`/`tee` path that moves file data from the page cache to the NIC
without copying through userspace.

The net effect is that disk reads, compression, network writes, and disk writes
on the receiver all overlap — keeping SSD, CPU, and NIC busy simultaneously.

## Adaptive compression

Compression helps when data compresses well, but hurts when it doesn't — the
CPU time spent compressing incompressible data is pure overhead. `tx` solves
this with per-frame adaptive compression that continuously adjusts its strategy
based on observed bottlenecks.

### Frame-level adaptation

Files are streamed as a sequence of 4 MiB frames. After each frame the server
records two signals:

- **Compression ratio** — logical bytes / wire bytes.
- **Read-over-write ratio** — time spent preparing the frame (disk read +
  compress) vs. time spent writing it to the network.

These signals are smoothed with an exponential moving average (α = 0.80). The
policy then makes a decision:

| Signal | Action |
|--------|--------|
| Read-over-write < 0.10 | **Upgrade** — CPU is idle relative to network; try stronger compression |
| Ratio < 0.90 or read-over-write > 1.10 | **Downgrade** — compression is a bottleneck; use lighter or no compression |
| Otherwise | **Hold** — current mode is balanced |

To avoid flapping, changes require two consecutive frames agreeing on the same
direction (hysteresis streak = 2).

### Compression ladder

The adaptation walks a four-rung ladder:

```
none (default) → lz4 → zstd (level 1) → zstd (default)
```

The default starting point is `none`. On slow links with compressible data, the
server upgrades toward `zstd` and can effectively exceed line rate because
compressed frames are smaller than the raw data. On fast networks with
incompressible data (e.g. already-compressed media), the server stays at `none`
or downgrades back — avoiding any CPU overhead. Mixed datasets (e.g. a
directory with both text logs and JPEG images) see the server adapt
frame-by-frame as data characteristics change.

The client can also force a specific compression mode (`--compress none|lz4|zstd`)
to skip adaptation.

## Verification

Cloud block storage is generally reliable at the block level, but filesystems
and transfer tools can still introduce silent corruption through bugs,
truncation, or interrupted writes. `tx` provides two levels of post-transfer
verification, designed to be practical on large datasets stored on slow cloud
drives where full re-reads are expensive.

### Metadata verification (default)

`--verify meta` compares every file in the local copy against the server
manifest by checking three properties:

- **Size** — byte count must match exactly.
- **Modification time** — nanosecond mtime must match.
- **Permissions** — POSIX mode bits must match.

This catches truncated writes, missing files, and permission drift without
reading any file data. For hardlinks and symlinks, the check validates link
targets instead of size/mtime.

### Sampled data verification

`--verify N%data` extends metadata verification with a randomized data
check. The client:

1. Divides each file into 4 MiB frame slots (matching the transfer frame size).
2. Randomly selects N% of slots per file.
3. Requests `CXSUM` checksums from the server for the selected ranges.
4. Reads the same byte ranges from the local copy and computes xxh128 checksums.
5. Compares local and remote checksums.

This approach is designed for the reality of cloud drive performance:

- A 10 TB dataset verified at 5% reads ~500 GB instead of 10 TB — an order of
  magnitude less IO required.
- The random sampling distribution means that a systematic corruption affecting
  even a small fraction of files is likely to be caught.
- Because the frame size matches the transfer frame size, checksum ranges align
  with what the server already knows how to read efficiently.

For environments where full verification is acceptable, `--verify full`
checks every byte. `--verify <duration>` (e.g. `--verify 30s`) runs full
data verification under a time budget: it verifies all metadata and as many
data frames across as many files as possible within the allotted time, then
reports partial results as success.

### In-flight integrity

During transfer, every frame carries an xxh128 hash accumulated across the
entire file window. The terminal frame trailer includes a `file-hash` token
that the server stores. When the client sends an `ACK`, it must present this
hash token — the server validates it against its stored value, confirming
that the bytes the client received match what the server sent. This catches
corruption introduced by the network or by bugs in the compression/
decompression path, without requiring a separate verification pass.
