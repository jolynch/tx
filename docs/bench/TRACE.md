# tx-bench Traces

`--trace PATH` records an event timeline. On `recv-copy` it covers the client side
of every run. On `send-tree` it covers the sender side of every transfer. Together
they reconstruct a transfer end to end. Go runtime tracing is separate
(`--go-trace`).

The events come from the forked tx processes themselves, through tx's
`events` progress format (`-p PATH -f events`; see
[Changes to tx](./OVERVIEW.md#changes-to-tx)). tx always writes JSON lines.
`tx-bench` adds only:
- the `run` label;
- its own `prep_*` and `rss` events;
- the per-transfer split;
- conversion to text when the output path asks for it.

## Files

| Flag | Writes |
|------|--------|
| `send-tree --trace PATH` | Each run's `tx send tree` process writes its events with `-f events`. Each process's complete event file, including connection events that carry no tid, becomes `BENCH_DIR/runs/server-<tid>.trace.jsonl` for that run's data transfer, which `recv-copy` can fetch. All of them, plus `tx-bench`'s `prep_*` events, are also appended to `PATH`. |
| `recv-copy --trace PATH` | Each run's `tx recv copy -f events` output, appended to `PATH` with its `run` label. The fetched sender files, converted to the same format, go to `PATH` with `.server` inserted before the extension. |
| `send-tree --go-trace PATH` | `tx send tree --trace` per server process: `PATH` with `.<k>` inserted, where `k` is the process number recorded in `server-<tid>.json` |
| `recv-copy --go-trace PATH` | `tx recv copy --trace` per run: `PATH` with `.<run>` inserted (`trace.w1.out`, `trace.1.out`, …) |

Because every run has its own server process, the sender's events for a run
are simply that process's events, and no splitting by tid is needed.

The format follows the extension: `.json` or `.jsonl` gives JSON lines;
anything else gives space-separated text. When `send-tree` runs without `--trace`,
`recv-copy` notes that the sender timeline is unavailable.

## Record Format

Every record carries these fields:

| Field  | Meaning |
|--------|---------|
| `t`    | Emitting host's wall clock, unix nanoseconds. Durations are taken from the monotonic clock. |
| `side` | `c` (client) or `s` (sender) |
| `run`  | Client run label (`w1`, `1`, …), added by `tx-bench`; `-` on the sender |
| `tid`  | Transfer ID; `-` before `TXFER` returns |
| `ev`   | Event name (below) |

Event-specific fields follow. In JSON lines they are object keys. In text,
each line is:

```text
t side run tid ev file off len dur k=v...
```

`-` marks an empty column, and `file off len dur` are fixed so `awk` and
`sort -n` work without parsing. The first line is a header comment:

```text
# tx-bench trace v1 side=c host=recv01 start=2026-10-01T14:22:33.000Z clock_offset_ns=-1830000 clock_err_ns=1210000
```

## Client Events

| Event                        | Fields |
|------------------------------|--------|
| `run_start` / `run_end`      | `dst`; end: `status`, `bytes`, `files`, `err` |
| `probe`                      | `dur`, `rtt`, `keepalive_ms`, advertised limits |
| `manifest_start` / `manifest_end` | end: `files`, `bytes`, `dur` |
| `conn_dial`                  | `conn`, `dur`, `sync` (true for synchronous pool fallback) |
| `conn_reuse` / `conn_close`  | `conn`; close: `reason` |
| `heartbeat` / `heartbeat_fail` | `conn`, `rtt` / `err` |
| `req_start` / `req_end`      | `verb` (SEND, ACK, CXSUM), `conn`, `items`, `body_bytes`; end: `dur`, `err` |
| `file_start` / `file_done`   | `file`, `path`, `len`; done: `dur` |
| `window`                     | `file`, `off`, `len`, `wire`, `codec`, `server_ts_ms`, `first_byte` (ns after `req_start`), `dur`, `write_dur` |
| `fsync`                      | `file`, `dur` |
| `ack`                        | `files`, `bytes`, `dur`, `attempt` |
| `retry`                      | `verb`, `attempt`, `err` |
| `error`                      | `verb`, `file`, `err` |
| `rss`                        | Emitted by `tx-bench` from `/proc/<tx pid>/status`. `bytes`; sampled every `--trace-rss-interval` (default 100ms) |
| `verify_start` / `verify_end`| end: `files`, `bytes`, `dur`, `mismatches` |
| `verify_fail`                | `path`, `expected`, `got` |

Both sides also emit:

| Event      | Fields |
|------------|--------|
| `dropped`  | `n`: records lost because the `events` target stayed unwritable until its buffer filled |

## Sender Events

| Event                        | Fields |
|------------------------------|--------|
| `prep_start` / `prep_end`    | Emitted by `tx-bench`, not tx. `seq`, `warm`, `skew`; end: `evicted`, `warmed`, `hot_pct`, `dur` |
| `accept` / `conn_close`      | `conn`, `remote`; close: `reason` |
| `cmd_start` / `cmd_end`      | `verb`, `conn`, `req_bytes`; end: `resp_bytes`, `dur`, `err` |
| `file_open`                  | `file`, `path`, `len`, `dur` |
| `window`                     | `file`, `off`, `len`, `wire`, `codec`, `send_path` (`sendfile` or `buffered`), `read_dur`, `comp_dur`, `write_dur` |
| `file_done`                  | `file`, `dur` (first window start to last window end) |
| `ack`                        | `files`, `bytes` |
| `transfer_done`              | `files`, `bytes`, `dur` |
| `rss`                        | `bytes`, sampled as on the client |

The `file` field holds manifest file IDs, which match across sides within a
`tid`.

## Clock Alignment

The two hosts' clocks are aligned without a side channel, using timestamps
already on the wire:

- Each FX/1 frame header carries the sender's `ts` (unix ms), and the client
  records each frame's arrival time.
- `min(client_arrival − server_ts)` across windows is the clock offset plus
  the minimum one-way delay.
- Subtracting half the minimum probe RTT gives
  `clock_offset_ns`, with error at most `rtt/2 + 1ms` (`clock_err_ns`).

`recv-copy` records both values in the trace header and the metrics file.
`report --clock-offset 0` overrides the estimate when the hosts are
PTP-synchronized.

## Trace Analysis in `report`

```text
tx-bench report [--run 1] [--top 20] [--format text|json] CLIENT_TRACE [SERVER_TRACE]
```

When `report` is given a trace, it adds a timeline analysis to the summary.
`SERVER_TRACE` defaults to the trace's `.server` sibling. `report` shifts
sender events by the clock offset, joins the two sides on
`(tid, file, off)`, and reports:

- **Per-file breakdown** (slowest `--top`): sender read, compress, and socket
  write; time in flight; client write and fsync; ACK.
- **Concurrency timeline:** files and windows in flight against the target
  concurrency, with gaps where either side was idle for more than 10ms.
- **Throughput:** client goodput and sender wire rate in 1s buckets.
- **Anomalies:** retries, heartbeat failures, synchronous dials, errors, and
  the windows with the largest `first_byte`.

Without a sender trace, `report` analyzes the client side only.
