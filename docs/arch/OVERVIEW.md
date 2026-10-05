# Architecture

`tx` is designed to fully saturate modern cloud hardware - SSD, network, and CPU
- during file transfers. Four capabilities make this possible: adaptive
concurrency, adaptive compression, background durability, and lightweight
verification.

The architecture sits on top of the FTCP command protocol documented in
[docs/ftcp/OVERVIEW.md](../ftcp/OVERVIEW.md). Operator-facing command behavior
and flags live in [docs/pub/CLI.md](../pub/CLI.md).

Mode-specific transfer ordering and source-side load goals are described in
[TRANSFER.md](./TRANSFER.md), including the difference between `fast` and
`gentle` transfer/sync paths.

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

Once the probe completes, the client opens two connection pools that never
share connections, so quick commands never queue behind file transfers:

| Pool    | Commands                         | Size                                  |
|---------|----------------------------------|---------------------------------------|
| data    | `SEND`, `CXSUM`                  | at most `concurrency`                 |
| control | `ACK`, `STATUS`, `TXFER`, `SYNC` | `concurrency`, grows during a burst   |

`concurrency` is the probe's suggestion, or `--concurrency` when set, clamped
to `[2, 256]`. Both
pools work the same way and differ only in growth:

- **Warm.** Each pool opens `concurrency` connections up front, during the
  probe, so the first `SEND` and `ACK` requests find a connection ready.
- **Most recently used first.** Idle connections form a stack. A borrower
  always gets the most recently used one, so steady traffic reuses the same
  connections and the unused ones sink to the bottom.
- **Scale down.** When the server grants keep-alive, once per heartbeat
  interval (one quarter of the keep-alive window: 15s at the default 60s), a
  pool that has been used keeps the most connections it had in use at once
  during that interval, plus a quarter for bursts, and at least one. It closes
  the rest from the bottom of the stack. For example, 90 open connections with
  at most 40 in use shrink to 50. Because borrowers take from the top, this
  never closes a connection that steady traffic is about to reuse.
- **Grow.** A pool below its size dials on demand. The data pool never grows
  past `concurrency`: when every data connection is busy, a `SEND` waits for
  one to come back. A transfer is scheduled so that this does not happen; see
  [Parallel execution](#parallel-execution). The control pool dials past
  `concurrency` during a burst and keeps up to `concurrency` idle afterwards.

`tx recv copy` uses one client for all its phases (transfer, data, converge,
and verification), so the pools warm once.

Every pooled connection has already completed the AUTH handshake. When the
server grants keep-alive (negotiated on the discovery probe via
`keep-alive=auto` / `keep-alive-ms`), pooled connections are long-lived
sessions: each is upgraded with a zero-payload keep-alive PROBE when dialed, and
a connection whose response was cleanly consumed returns to its pool instead of
being closed. Reused connections skip the TCP and AUTH handshakes and keep their
congestion window warm across batches. Two guardrails keep silently dead
connections out of the pools: once per quarter of the granted idle window, a
background loop heartbeats each pooled connection idle for that long with a
zero-payload PROBE round trip (a failed heartbeat closes the connection and
starts a replacement, and borrowers peek for a pending EOF before reuse), and
the server independently reaps connections that send nothing for the keep-alive
window (`--idle-timeout`, default 60s).

Against servers without keep-alive, or after a dirty response (error
mid-stream, or any `CXSUM` stream), a connection is closed after one use and
a background goroutine opens and authenticates a replacement. Replacements
restore a pool only to the size it last scaled to, and fill a slot the
closed connection freed, so they never push a pool past its size or undo a
scale-down.

### Windowing and batching

The probe results feed a planning step that sizes the unit of work each TCP
connection handles. The goal is fixed-size batches: whether a batch contains
many small files or a single slice of a large file, each TCP connection does
roughly the same amount of work.

**Batch size.** The client computes a batch size from the probe:

```
perFileWorkers = suggestedConcurrency / windowConcurrency
batchMaxBytes  = windowBytes / perFileWorkers          (rounded up to power-of-2 MiB)
```

The default window is 512 MiB with a window concurrency of 4. On a 24-CPU server
in fast mode (IO depth 4), suggested concurrency is 96, giving
`perFileWorkers = 24` and a batch size of ~32 MiB. The result is capped at half a second of
per-connection link bandwidth, then clamped: the floor is the larger of the
server and client socket buffers (no point making a batch smaller than what the
OS has already allocated for the connection), and the ceiling is the window
size.

**Small files → packed batches.** The manifest is walked in order and files are
packed into batches until the next file would exceed `batchMaxBytes`. A batch
of 1000 tiny files and a batch containing one 32 MiB file are the same unit of
work. Each becomes one multi-file `SEND` request, or several in parallel when
SEND slots are free (see [Parallel execution](#parallel-execution));
metadata-heavy batches can require several requests on the same worker.
This avoids the per-connection overhead that makes small-file transfers slow in
tools that open one connection per file.

**Request memory.** PROBE separately advertises `target-request-bytes` (8 MiB)
and `max-request-bytes` (64 MiB). These count encoded metadata before
compression, including line terminators, rather than the file content scheduled
by a work batch. SEND groups, directory metadata, ACK lists, and CLI checksum
batches split at the target without exceeding the maximum. Work-size overrides
do not override these limits. Direct `GetChecksum` calls remain single requests.

The server verifies one bounded request body and parses its entries once into
compact ordered records. All entries are validated before processing, and the
decoded body is released before applying the records. Invalid or truncated ACK
requests apply no acknowledgments; earlier successful requests remain applied
when a later request fails. Records, frame buffers, and bounded decoder history
are additional memory, so the request limit is not a total heap limit. Many
short entries can require more record storage than their encoded payload.

Logical transport frames remain 4 MiB, independently of request and work sizes.
SYNC has its own `max-sync-request-bytes` advertisement (1 GiB by default) and
is checked before transmission, without splitting the manifest.

**Large files → split windows.** When a single file exceeds `batchMaxBytes`,
it is split into windows of that size. Each window is downloaded on its own TCP
connection, in parallel when SEND slots are free, with the number of
concurrent windows capped by `windowBytes / batchMaxBytes`. The pieces are written to the correct offsets in
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

With batches planned and the pools warm, `concurrency` workers each take a
batch and issue `SEND` requests. A transfer holds at most one SEND slot per
data connection, so it never issues a `SEND` that would wait for a connection.
Each worker:

1. Waits for one SEND slot, then takes any other slots free at that moment
   to split the batch into parallel `SEND` requests, or the large file into
   parallel windows. A batch never waits for extra slots, so splits use only
   idle capacity, such as when other workers are acknowledging or the
   transfer is near its end.
2. Borrows a pre-authenticated connection from the data pool for each slot.
3. Sends a `SEND` command requesting one or more file windows, reads the
   `FX/1` frame stream, and writes frames to disk.
4. Returns each connection to the data pool and frees its slot.
5. Sends a batched `ACK` confirming received windows on a control
   connection.

Because every worker has its own TCP connection, there is no head-of-line
blocking: a slow file on one connection does not stall transfers on the others.
On the server side, each fast-mode `SEND` handler uses `fadvise(SEQUENTIAL)` and
background `readahead(2)` to prefetch the next frame into the page cache while
the current frame is being written to the socket. When encryption and
compression are off and the output is a raw TCP socket, the server uses a
zero-copy `splice`/`tee` path that moves file data from the page cache to the
NIC without copying through userspace.

The net effect is that disk reads, compression, network writes, and disk writes
on the receiver all overlap — keeping SSD, CPU, and NIC busy simultaneously.

## Adaptive Compression

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

The adaptation walks a three-rung ladder:

```
none (default) → lz4 → zstd (level 1)
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

`tx` verifies integrity in two layers:

- **Post-copy metadata verification** checks size, mtime, mode, and link
  targets after every copy by default.
- **Optional sampled data verification** extends that with checksum comparison
  of selected byte ranges using `CXSUM`.

For operators, the main CLI modes are:

- `--verify meta`
- `--verify N%data`
- `--verify full`
- `--verify <duration>`

The detailed design and current implementation live in
[VERIFICATION.md](./VERIFICATION.md), including the deterministic sampling
algorithm, partial-success behavior (`[partial-ok]`), and the relationship
between `SEND`, `ACK`, and `CXSUM`.

## Durability

Naive transfer tools call `fdatasync` inline after every file write, which
serializes disk flushes and stalls the download pipeline. On cloud NVMe,
inline fdatasync per file costs 1–5 ms; for a million small files that adds
15–80 minutes of pure sync overhead. Skipping fsync entirely risks data loss
on crash. `tx` solves this with a three-layer durability strategy controlled
by `--fsync-interval` (default 512 MiB).

### Background batch fdatasync (default)

After each file's writes complete, the file descriptor is `dup()`'d and enqueued
to a background channel along with its written byte count. A reader goroutine
accumulates requests; when the accumulated bytes reach the threshold (512 MiB by
default, raised under backlog) or 4096 files, it spawns a worker goroutine that:

1. Groups requests by `(dev, ino)` via `fstat` — hardlinked files are synced
   once.
2. Calls `fdatasync` on each unique inode.
3. Closes the dup'd file descriptors.

Because the sync callback returns immediately after enqueue, the download and
ACK path is never blocked by disk flushes.

### Inline fdatasync

`--fsync-interval 0` switches to inline mode: each file blocks on `fdatasync`
immediately after its writes complete. This gives per-file durability
guarantees at the cost of throughput.

### Syncfs-only

`--fsync-interval -1` skips per-file fdatasync entirely and relies on the
final filesystem sync.

### Final syncfs

After all downloads complete and the background batcher drains, `tx` calls
`syncfs` on the target filesystem with a 10-second timeout. This catches any
writes the OS hasn't flushed yet and ensures the entire transfer is durable
before reporting success. `--skip-fsync` disables both per-file and final
syncing.
