# tx-bench Overview

`tx-bench` benchmarks `tx` between two machines with as little setup as
possible. One host prepares a deterministic dataset and serves it; the other
pulls it repeatedly, verifies every byte against an independent oracle, and
reports a short human summary plus machine-readable metrics and traces.

`tx-bench` is a harness around real `tx` processes. On the sender it starts a
fresh `tx send tree` for every run, and on the receiver it forks one
`tx recv copy` per run. Each run is therefore a true end-to-end test of the
shipped binary, measured per process. `tx-bench` never speaks FTCP itself.

```text
host A (sender)
$ ./tx-bench remote send-tree --size 10GiB
  prep: generate, check, cache
  start tx send tree #1, print the recv-copy command
  on each prep request: stop tx #k, record runs/server-<tid>.json,
                        cache step, start tx #k+1, write prep.json

host B (receiver)
$ ./tx-bench remote recv-copy 10.0.4.17:3453 --expect 7f3a9c21e0b4d8aa
  tx recv get bench.json, files.tsv, ...
  for each warmup + measured run:
    clean DST, request prep (tx recv get runs/prep), wait for prep.json
    tx recv get the previous run's runs/server-<tid>.json
    fork: tx recv copy <data root> DST
    verify DST against files.tsv
  request a final flush, fetch the last server-<tid>.json
  print report, write metrics
```

Companion references:

- [CLI](./CLI.md): commands, flags, defaults, pass-through rules, and
  example output.
- [Dataset](./DATASET.md): layout, generation, import, fingerprint, and the
  mix and size grammars.
- [Trace](./TRACE.md): event schema, trace formats, clock alignment, and
  trace analysis in `report`.

## Design Rules

- **Real binaries end to end.** Every byte on the wire comes from a forked
  `tx`; `tx-bench` only prepares data, supervises processes, and verifies
  results.
  - The `tx` binary is `--tx`, which defaults to `$TX_BIN`, then the `tx`
    next to `tx-bench`, then `$PATH`.
  - Its path and xxh128 are recorded on both sides, and `recv-copy` warns when
    the two hosts run different builds.
- **One process per run on both sides.** Every measured run has its own
  `tx send tree` and its own `tx recv copy`. `wait4` therefore gives exact
  rusage for each, and no heap, transfer state, or connection pool carries
  over between runs. This matches how tx is normally run: `tx send tree`
  defaults to exiting after its transfer (`--exit-after 60s`).
- **One binary, two commands.** `tx-bench remote send-tree` and
  `tx-bench remote recv-copy` are all a benchmark needs; `prep` is the
  sender's setup step exposed on its own.
  - Data is only written when a shape is given (`send-tree --size 10GiB`),
    and every unspecified shape setting has a default: size `10GiB`, profile
    `mixed`.
  - Paths (`./tx-bench-src`, `./tx-bench-dst`), port (`3453`), and run counts
    (`-w 1 -n 3`) are defaulted too.
- **No side channel.** The sender writes all coordination state as ordinary
  files under its served root. The receiver reads them, and requests preps,
  with `tx recv get`. Only the FTCP port needs to be reachable.
- **The oracle is independent of tx.** `files.tsv` lists the xxh128 of every
  file, and its fingerprint is printed on the sender's terminal. Generated
  files also carry their hash in their names. `recv-copy` rehashes what landed
  on disk; tx's own verification plays no part.
- **Corruption fails the whole benchmark.** A single mismatched byte stops the
  run immediately with exit code 3.
- **Nothing destructive without a marker.** `tx-bench` deletes or regenerates
  only directories it created and marked
  ([Directory Ownership](#directory-ownership)).
- **Observability is opt-in and costs nothing when off.** Event traces and Go
  runtime traces are only written when asked for, on either side.

## Served Root

Each forked `tx send tree` uses `BENCH_DIR` (default `./tx-bench-src`) as its
chroot:

```text
tx-bench-src/                  BENCH_DIR
  .tx-bench                    ownership marker (Directory Ownership)
  bench.json                   dataset description, fingerprint, remote paths (DATASET.md)
  files.tsv                    every entry's type, mode, size, hash, mtime, path: the oracle
  server.json                  sender config and host facts, rewritten at startup
  data/                        the tree that recv-copy copies (generated datasets)
  in-cache.tsv                 stat cache for an --in import (imported datasets)
  runs/
    prep                       1-byte file; fetching it requests a prep
    flush                      1-byte file; fetching it records the last run without a prep
    stop                       1-byte file; fetching it records the last run and ends send-tree
    prep.json                  result of the latest prep or flush
    server-<tid>.json          sender metrics for the run whose data transfer had this tid
    server-<tid>.trace.jsonl   that run's tx send tree events (--trace only)
    tx-send.<k>.stats.jsonl    --stats output of tx send tree #k, tailed by tx-bench
    tx-send.<k>.events.jsonl   tx send tree #k events (--trace only)
    tx-send.<k>.log            stderr of tx send tree #k
```

The request files are 1 byte, not empty, so a fetch is an ordinary
transfer that completes.

**With `--in DIR`.** tx resolves every request path inside one chroot, so
`tx-bench` uses the deepest directory that contains both `BENCH_DIR` and `DIR`
as the chroot.
- `bench.json` records the remote paths of the bench root and the data root.
- `recv-copy` learns them from the `SERVER` argument (`host:port/bench/path`),
  which `send-tree` prints whenever the path is not `/`.
- When that common directory contains more than the two trees, `send-tree`
  warns that all of it is readable over the port.
- `BENCH_DIR` and `DIR` may not be nested inside each other in either
  direction. Otherwise `runs/` would sit inside the data being verified, or
  the data inside `BENCH_DIR`'s managed files.

**Access control.** `--auth auto|on|off`, default `auto`, which means on for
`--in` and off for generated data.
- Generated data is not sensitive. Default results stay plaintext so they
  measure tx without encryption.
- User data is sensitive. With `--in`, `send-tree` generates a token, passes
  `--require-auth-token` to tx, and prints the token and `--encrypt auto` in
  the `recv-copy` command.
- With auth off on a non-loopback listen address, `send-tree` warns that
  `BENCH_DIR` (or the widened chroot) is readable by anyone who can reach the
  port.

This relies on tx enforcing its chroot ([Changes to tx](#changes-to-tx)).

`server.json` records:
- the tx-bench version, and the tx binary's path and xxh128;
- hostname, kernel, CPU count, MemTotal, and the filesystem and device under
  the data root (with a warning for filesystems that ignore `fadvise`, such as
  ZFS);
- the chroot, the auth mode, the cache-warm settings, whether tracing is on,
  and the listen address.

## Directory Ownership

`tx-bench` writes a `.tx-bench` marker as the first thing it creates in
`BENCH_DIR`, and a `.tx-bench-dst` marker in the receiver work directory. It
only deletes or regenerates inside a marked directory:

- **`prep`/`send-tree`** create `BENCH_DIR` if it is absent. If it exists, is
  not empty, and has no marker, they refuse to touch it. `--regen` and the
  "incomplete dataset" regeneration act only on marked directories, and never
  touch an `--in` directory.
- **`recv-copy`** treats `DST` as a work directory.
  - It copies into `DST/data`, and tx keeps its resume state in `DST/.tx`.
  - It deletes only those two, and only when `DST` carries the marker.
  - An existing, non-empty, unmarked `DST` is a usage error, so
    `recv-copy HOST ~/data` cannot delete your data.
  - The oracle walks `DST/data`.

## Run Protocol

### Sender

1. **Startup prep.** Run [prep](#prep) with every enabled step: `generate` or
   `import`, then `check`, then `cache`.
2. **Coordination files.** Write `server.json`, the request files
   `runs/prep`, `runs/flush`, and `runs/stop`, and `runs/prep.json` (with
   `seq: 0` and `served_tid: null`).
3. **Start `tx send tree` #k** (k starts at 1). Its command line is
   assembled from:
   - `--listen` and the chroot;
   - `--stats runs/tx-send.<k>.stats.jsonl`;
   - `-p <file> -f events` when tracing, and `--trace` for `--go-trace`;
   - `--require-auth-token` when auth is on;
   - `--exit-after never` as an overridable default;
   - `--exit-with stdin`, with tx's stdin a pipe that `tx-bench` holds
     open and never writes. If `tx-bench` dies, even by SIGKILL, the kernel
     closes the pipe and tx shuts down cleanly instead of serving forever;
   - everything after `--` on the `send-tree` command line
     ([Pass-Through Arguments](./CLI.md#pass-through-arguments)).
4. **Ready.** Wait until the port accepts connections. If tx exits first,
   stop with the tail of its log and exit 2.
5. **Print the `recv-copy` command,** only for k = 1. It includes `--expect`,
   the bench path when it is not `/`, and the auth token when auth is on.
6. **Tail tx #k's `--stats`.** Each transfer produces a `start` record and an
   `end` record, both carrying the transfer's full requested path.
   `tx-bench` acts on the path:
   - **The data root, `start`:** this is run k's data transfer. If one
     already started in this process, warn that transfers overlap and mark
     both runs `overlap`.
   - **`runs/prep` or `runs/flush`, `start`:** a prep request from tid T.
     1. Wait for that transfer's `end` record, so the receiver's fetch
        completes cleanly.
     2. Stop tx #k with SIGTERM. tx flushes `--stats` and its progress
        targets on exit.
     3. `wait4` it for exact rusage, and read its process summary from the
        last `--stats` record.
     4. Write `runs/server-<tid>.json` for run k's data transfer (if there
        was one). If tracing, also write `server-<tid>.trace.jsonl`.
     5. Print one summary line.
     6. For `prep` only: run the `cache` step.
     7. Start tx #k+1 (step 3) and wait for it to be ready (step 4).
     8. Write `runs/prep.json` with `seq` incremented, `served_tid: T`, and
        the cache results.
   - **`runs/stop`, `start`:** wait for its `end`, stop tx #k and record its
     run as above, then exit 0 without starting another tx. The receiver
     fetches it last (`recv-copy --stop-sender`), after the final flush has
     recorded the last run, since nothing answers once the sender is gone.
   - **Anything else:** ignored. These are coordination fetches such as
     `bench.json` and `prep.json`. They never become runs and never trigger
     a prep.
7. **Unexpected exit.** If tx #k exits without being stopped, `send-tree`
   exits with an error and the tail of `tx-send.<k>.log`.

Writes are atomic (temp file + rename). Each prep is requested by
`recv-copy` before its run, so a failed or aborted run cannot leave the next
run with a stale cache or a dirty server. A run's server process does serve a
few small coordination fetches: the poll of `prep.json`, the previous run's
`server-<tid>.json`, and the next prep request. Those are a few KiB against
the whole dataset.

### Prep

Prep puts the sender's disk and page cache into a known state. It is
`tx-bench prep` as a standalone command; `send-tree` runs it once at startup and
then its `cache` step once per prep request.

Prep is a sequence of independent steps. Each one runs only when you pass
its options; otherwise it is off:

| Step       | Turned on by                                              | Does |
|------------|-----------------------------------------------------------|------|
| `generate` | any shape flag: `--size`, `--fill`, `--profile`, `--mix`, `--seed` | Create the dataset, or reuse it when `bench.json` matches the requested shape ([Generation](./DATASET.md#generation), [Reuse](./DATASET.md#reuse)). Shape flags left out take their defaults (`10GiB`, `mixed`, seed `1`). A mismatch is an error unless `--regen` is given. |
| `import`   | `--in DIR`                                                | Use your own tree, read-only and in place: hash it into `files.tsv` (only changed files on re-import) ([User-Supplied Data](./DATASET.md#user-supplied-data)). |
| `check`    | `--check`                                                 | Rehash every file and compare the result to its `files.tsv` hash. |
| `cache`    | `--cache-warm`                                            | Evict every dataset page, then read the selected warm set back in ([Cache State](#cache-state)). |

Rules:

- **The steps always run in that order:** `generate` or `import`, then
  `check`, then `cache`. `generate` and `import` are mutually exclusive:
  giving `--in` together with any shape flag is a usage error.
- **Without shape flags or `--in`, prep never writes data.** It loads the
  existing dataset as it is, and a stat-only walk must still match
  `files.tsv` (for an import, `in-cache.tsv`). If there is no dataset, prep
  fails and suggests `--size 10GiB` or `--in DIR`. `--regen` without a shape
  flag or `--in` is a usage error.
- **Residency is always measured.** Every prep, including one with every step
  off, reads dataset residency with `mincore` and records it as
  `cache.hot_pct`.
- **Under `send-tree`, only the startup prep runs `generate`, `import`, and
  `check`.** Prep requests run `cache` alone, plus the `--in` source check
  below. Data is created exactly once per
  `send-tree`, and nothing between runs costs more than the cache work.
- **An `--in` source is re-checked at every prep.** A stat-only walk of
  `DIR` against `in-cache.tsv` runs each time, keyed on ctime as well as
  mtime, because tools such as `touch -r` and `rsync -t` preserve mtime.
  Changed paths are listed in `prep.json`, so `recv-copy` reports them as
  "source changed" rather than corruption. A live directory therefore fails
  individual runs, never the whole benchmark.
- **Plain tx clients get no per-run prep.** Clients other than `tx-bench remote recv-copy`
  (for example a hand-run `tx recv copy`) never request one, so they see the
  dataset as the startup prep left it. Request one by hand with
  `tx recv get tx://HOST/runs/prep` into any local file.

Standalone `prep` exits after its steps. It prints the dataset table, the
fingerprint, the measured `hot_pct`, and the `send-tree` command that serves the
result. Typical uses:

| Goal                                         | Command |
|----------------------------------------------|---------|
| Generate ahead of time, leave cache alone    | `tx-bench prep --size 1TiB` |
| Benchmark your own tree, cold every run      | `tx-bench remote send-tree --in /srv/photos --cache-warm 0%` |
| Make the cache cold without touching data    | `tx-bench prep --cache-warm 0%` |
| Warm 30% in contiguous runs, no generation   | `tx-bench prep --cache-warm 30% --cache-warm-skew 8` |
| Warm 30% of RAM's worth of the dataset       | `tx-bench prep --cache-warm 30%mem` |
| Audit an existing dataset                    | `tx-bench prep --check` |
| Generate, then serve                         | `tx-bench remote send-tree --size 10GiB` |
| Serve an existing dataset exactly as it is   | `tx-bench remote send-tree` |

### Receiver

All coordination fetches are `tx recv get` with `--skip-fsync
--progress=false --stats`, writing into a temp directory on tmpfs
(`$XDG_RUNTIME_DIR`, else `/dev/shm`). Their `syncfs` at exit therefore never
touches the filesystem `DST` is on. Polls back off from 50 ms to 1 s.

1. **Check the dataset.** Fetch `bench.json`, `server.json`, `files.tsv`, and
   `runs/prep.json` from the bench root. If the fingerprint of `files.tsv`
   does not match `--expect` (or `bench.json` when `--expect` is absent),
   abort before transferring anything: wrong dataset or wrong host.
2. For each run, warmups first (`w1..wW`), then measured (`1..N`):
   1. **Clean the destination.** Remove `DST/data` and `DST/.tx`, then
      `syncfs` the destination filesystem so writeback from the previous run
      does not leak into this run. Cleaning comes before the prep, so it
      cannot disturb the cache state the prep sets.
   2. **Request a prep.**
      1. `tx recv get` `runs/prep`, and learn its tid T from that get's
         `--stats`.
      2. Poll `runs/prep.json` until `served_tid` is T, up to
         `--server-wait` (default 2m). Connection-refused during the
         sender's restart counts as "not yet".
      3. Record the cache results for this run.

      Because the handshake matches T, a late or stray prep, such as one
      left over from an aborted invocation, cannot satisfy this run.
   3. **Fetch the previous run's sender metrics.** For the run before this
      one, `tx recv get` its `runs/server-<tid>.json`, and its
      `server-<tid>.trace.jsonl` when tracing.
   4. **Fork `tx recv copy`** on the data root into `DST/data`. Its command
      line has `--stats`, `--verify none` as an overridable default,
      `-p <file> -f events` when tracing, `--trace` for `--go-trace`, and
      everything after `--` on the `recv-copy` command line.
      - `wait4` on that process gives exact rusage for the run: peak RSS,
        CPU, faults, block I/O, and context switches.
      - Its `--stats` gives the tid, phase timings, wire bytes, connection
        counters, and retries.
   5. **Verify** `DST/data` against `files.tsv`. This is timed separately and
      is never part of transfer time; see
      [Failure Semantics](#failure-semantics).
3. **Flush the last run.** `tx recv get` `runs/flush`, wait for `prep.json`
   to acknowledge it, then fetch the last `server-<tid>.json`. With
   `--stop-sender`, then fetch `runs/stop`.
4. **Report.** Aggregate the measured runs, print the report, and write
   `--metrics`.

**Forever mode.** `--forever` replaces `--iterations`: after the warmups,
measured runs continue until tx-bench is interrupted. An interrupt, with or
without `--forever`, stops the copy in flight (which does not count), then
flushes, reports, and writes metrics as usual; a second interrupt quits at
once. Forked tx processes run in their own process group, so a terminal
Ctrl-C reaches only tx-bench, which stops them after recording what it needs.

**Streaming metrics.** `--metrics` to a `.jsonl` or text path is written as
the benchmark runs: a header record, one record per run once its sender
metrics are in (when the next run starts, or at the final flush), and a
summary record at the end. A FIFO works, for exporting metrics while a
`--forever` run goes on: the stream never blocks the benchmark, holds
records while no reader is attached, and replaces the oldest with a
`dropped` record if more than 1 MiB waits. A `.json` path is one object,
rewritten after each run, so it cannot be a FIFO or a `--forever` target.

Only one receiver may run against a sender at a time. The tid-matched
handshake keeps a second receiver from corrupting the first one's cache
state, but the two would share server processes. The sender detects
overlapping data transfers and marks those runs.

## Cache State

`--cache-warm` chooses how much of the dataset is resident in the sender's
page cache at the start of every run. `--cache-warm-skew` chooses *where* that
resident portion sits.

### Amount

A percentage, of either the dataset or memory:

| Value              | Warm set |
|--------------------|----------|
| unset (default)    | Cache untouched; residency is still measured |
| `N%`               | N percent of the dataset's bytes, `0%`–`100%`. `0%` is cold; `100%` is fully hot. |
| `N%mem`            | N percent of MemTotal, taken from the dataset. `30%mem` on a 64 GiB host warms 19.2 GiB of the dataset. |

Bare numbers and byte sizes are rejected, so `30` is never ambiguous.

- **`N%` larger than MemTotal** (for example `100%` of a dataset bigger than
  RAM) is an error at startup.
- **`N%mem` larger than the dataset** warms the whole dataset, and the output
  says it was clamped.
- **Either form larger than MemAvailable** (measured after the first
  eviction) produces a warning, and the shortfall shows up in the measured
  `hot_pct`.

Generation fsyncs every file, so the dataset pages are clean and
`FADV_DONTNEED` can drop them.

### Placement

Whenever the warm set is neither empty nor the whole dataset:

1. Order the dataset files with a seeded shuffle (dataset seed), so the warm
   set cuts across all mix parts rather than following directory order.
2. Concatenate the files in that order and split them into blocks of
   `--cache-warm-block` (default `1MiB`). A file smaller than a block is one
   block; a file's tail is a short block. Block `i` has normalized position
   `x_i` (its midpoint over the total) in `(0, 1)`.
3. Give each block weight `w_i = pdf_Beta(1, skew)(x_i) ∝ (1 − x_i)^(skew − 1)`.
4. Choose blocks by weighted sampling without replacement (Efraimidis–Spirakis:
   key `u_i^(1/w_i)` with seeded `u_i`, highest keys first) until the warm
   byte target is reached.
5. Warm the chosen ranges with `FADV_WILLNEED` plus read-touch (the
   `pagecache.TouchEntries` path).

`--cache-warm-skew` is `skew ≥ 1`, default `1`:

- **`skew = 1`:** uniform. The warm set is scattered `1MiB` blocks across
  every file.
- **Larger `skew`:** mass concentrates toward the front of the shuffled
  address space, so the warm set forms longer contiguous runs and covers
  whole files.
- **`skew → ∞`:** the warm set is one contiguous prefix of the shuffled
  order.

Selection is deterministic in (seed, dataset, amount, skew, block), so every
run warms the same pages.

### What "Cold" Means

`FADV_DONTNEED` drops file data pages, not cached dentries, inodes, or
directory blocks. The startup walk and earlier runs keep that metadata hot.
On many-file profiles (`small`, `mixed`), the manifest phase and file opens
therefore run against hot metadata even at `--cache-warm 0%`.

- `prep.json` records `meta_cold: false`, and the report labels the run
  "data-cold".
- `--cache-drop-meta` (requires root) writes `2` to
  `/proc/sys/vm/drop_caches` after warming data pages, measuring residency,
  and checking imported sources. That is host-wide, so it says so when it
  runs. It records `meta_cold: true`, and the report labels the run "cold".
  The kernel does not reclaim inodes that still have cached pages, so the
  inodes of warmed files stay cached: with `--cache-warm 30%`, metadata is
  cold only for the files outside the warm set.
- ZFS ignores `fadvise` (the ARC is separate). On ZFS, `prep` warns that
  `--cache-warm` cannot be honored, and `hot_pct` reports what was actually
  measured.

## Metrics

Per run, then aggregated over measured runs. The report shows min, p50, and
max. `--metrics` additionally carries mean and stddev, plus every per-run row.
`tx-bench report METRICS` re-renders the same report offline from that file.

| Area       | Metrics                                                                                 | Source |
|------------|-----------------------------------------------------------------------------------------|--------|
| Time       | wall; phases: probe, manifest, data, finalize; verify (separate)                        | `tx recv copy --stats`; verify timed by tx-bench |
| Throughput | logical bytes/s, files/s, wire bytes, compression ratio, windows per codec              | `tx recv copy --stats` |
| Client     | peak RSS, user/sys CPU (core-s per GiB), major/minor faults, block I/O, context switches | `wait4` rusage of `tx recv copy` |
| Sender     | the same rusage, plus conns accepted, peak concurrent conns, sendfile vs buffered windows | `wait4` of that run's `tx send tree` + its `--stats`, via `runs/server-<tid>.json` |
| Cache      | warm spec, skew, measured `hot_pct`, `meta_cold`, prep duration                          | `runs/prep.json` |
| Conns      | dials, sync fallbacks, reuses, heartbeats, heartbeat failures                           | `tx recv copy --stats` |
| Errors     | request errors, ACK retries, failed runs                                                | `tx recv copy --stats` + exit status |
| Verify     | entries and bytes checked, mismatches                                                   | oracle |

Sender rusage covers the whole run's server process. That includes Go
runtime startup and the few small coordination fetches listed under
[Sender](#sender); the report says so in a footnote.

## Throughput Suite

`make bench-throughput` measures two goals for copying many files: small
files should cost no more CPU per byte than large ones, and throughput should
grow with CPUs. `TestTransferCostRatio` in `internal/bench` runs a real `ftcp`
server and `tx.Client` in one process over loopback TCP, with no filesystem:

- **Sender:** the store registers the files as `TXFER` would, but
  `OpenFileRef` returns a duplicate of one `memfd` the size of the largest
  file instead of opening a path. Memory therefore does not grow with the file count or
  `THROUGHPUT_SIZE` (default `256MiB` per copy), apart from the manifest.
- **Receiver:** `StartFromManifest` writes to `io.Discard`, so file creation
  and the CLI's metadata step are not measured. `tx-bench` covers those end
  to end.
- **Plan:** concurrency and batch size come from the same functions
  `tx recv copy` uses, for a server with N CPUs (also applied as
  `GOMAXPROCS`) on a fixed 10 Gb/s link, so the plan does not vary between
  runs.
- **Encryption:** `THROUGHPUT_ENCRYPT` (default `none,aes`; `chacha20` is
  also accepted) lists the session encryptions to measure. An encrypted mode
  sets up keys as `tx send tree` and `tx recv copy --encrypt` do: a server
  identity, and an ephemeral client identity sent in `AUTH`.

The result is a set of grids per encryption mode: a row per CPU count N and
a column per file size. `THROUGHPUT_CPUS` takes a comma-separated list of N;
by default it covers 2, 4, 8, and so on below the host's CPU count, then the
count itself, so the last row is the whole machine. Counts above the host's
CPUs are skipped.

For each N, it copies `THROUGHPUT_SIZE` as 4 KiB, 16 KiB, 64 KiB, 1 MiB, and
16 MiB files, one size after another in rounds, for at least three rounds
and five seconds (`TX_BENCH_THROUGHPUT_MIN_WALL`). Interleaving the sizes
keeps clock and load drift from landing on one of them. Each copy has an
untimed setup, which registers the files with the store, and a timed data
phase. A GC after each copy counts toward its CPU but not its wall time, so
no copy pays for another's garbage. CPU is the process's user plus system
time, so it covers both sides.

The grids:

- **Cost ratio:** a size's CPU per GiB divided by the 16 MiB files' CPU per
  GiB in the same row. 1.0 is the goal. Both sides of the ratio run on the
  same host, so it is far steadier than absolute CPU: with `GOMAXPROCS` from
  2 to 24 on one host, 4 KiB CPU per GiB ranged from 9.7 to 62 core-s, while
  its ratio stayed between 9.5 and 15. It still varies between kinds of
  hosts, because per-file work (mostly syscalls) and per-byte work (copies
  and hashing) scale differently with CPU model and virtualization: a GitHub
  Actions runner (EPYC 9V45, 4 CPUs) measured 17.8–21.8.
- **Speedup:** a size's rate divided by its rate in the mode's first row.
  Linear scaling with cores is N divided by the first row's N.
- **CPU** in core-s per GiB, and **rate** in MiB/s, each row with the plan
  tx made for that N.

The test fails when any cost ratio exceeds `TX_BENCH_THROUGHPUT_MAX_RATIO`
(default 40). That is a regression bar for any CI host, not the goal. On the
Ryzen host, the code before the indexed manifest lookup and the zero-copy
size limit measured 2.5x its current ratio. Lower the default as the ratio
improves; compare grids from the same host to judge a change.

A line above the grids names the host's CPU model, kernel, and CPU count.
With `TX_BENCH_THROUGHPUT_OUT` set (the Makefile uses `bench/throughput`),
the test also writes every row, with files/s, core-µs per file, setup time
per copy, and peak heap, to `throughput.json`; the `throughput` CI job
uploads that file. Both modes on a 24-CPU host take about 80 seconds.
Without `TX_BENCH_THROUGHPUT_SIZE`, the test is skipped, so `go test ./...`
stays fast. `BenchmarkTransferInMemory` runs the unencrypted copies under
`make bench`; use `-cpu N` to plan for N CPUs.

## Failure Semantics

| Condition                                                                 | Behavior |
|---------------------------------------------------------------------------|----------|
| Content hash, type, mode, size, mtime, link target, or hardlink group ≠ `files.tsv`; missing or extra entry | **Corruption.** Stop immediately; report up to 20 offending paths; keep `DST` for inspection; exit 3 |
| Mismatch on a path the sender's prep reported as changed in an `--in` source | **Source changed.** Fail that run (not corruption), name the paths, continue |
| `files.tsv` fingerprint ≠ `--expect`                                      | Abort before the first run; exit 2 |
| tx exits non-zero before reporting a tid (bad pass-through flag, unreachable host, auth failure), on the first run or before `send-tree` listens | Stop with tx's stderr; exit 2 |
| `tx recv copy` fails after it started transferring                        | Mark the run failed, keep its stderr in the metrics, exclude it from stats, continue (`--fail-fast` stops); exit 1 at the end |
| Prep not acknowledged within `--server-wait`                              | Mark the run failed (unknown cache state) and continue |
| `runs/server-<tid>.json` not available within `--server-wait`             | Warn; sender columns show `-` for that run; does not change the exit code |
| `tx send tree` exits while serving                                        | `send-tree` exits non-zero with the tail of its log |
| Cleaning `DST` fails (removal or `syncfs`)                                | Stop before the next run, without the final flush, so the last run has no sender metrics; exit 1 |
| All runs succeeded and verified                                           | Exit 0 |

## Changes to tx

### New Features

These are user-facing tx features, useful outside `tx-bench`. They are
documented in the [CLI reference](../pub/CLI.md), and none of
them changes behavior when its flag is absent.

| Flag | On | Writes |
|------|----|--------|
| `--stats PATH` | `tx send tree` | JSON lines. Per transfer, a `start` record (tid, full requested path including a single file's name, time) and an `end` record (tid, files, bytes, wire bytes, windows per codec and send path, time). At exit, a `process` record (conns accepted, peak concurrent conns, heartbeats). |
| `--stats PATH` | `tx recv copy`, `tx recv get` | One JSON object at exit: tid, status and error, phase timings, bytes, wire bytes, windows per codec, dials, sync fallbacks, reuses, heartbeats and failures, ACK retries, request errors |
| `-f events` | `tx send tree`, `tx recv copy`, `tx recv get` | A new `--progress-format` value: the event timeline in [Trace](./TRACE.md) as JSON lines, written to the matching `-p/--progress-path` target |
| SIGTERM | `tx send tree` | A clean shutdown that writes the final `--stats` and progress records before exiting |
| `--exit-with none\|stdin` | `tx send tree` | Default `none`: no change. `stdin`: the same clean shutdown when stdin (which must be a pipe or socket) closes, so whoever holds the pipe ties tx's lifetime to its own. A mode, not a boolean, so ties to a pid or descriptor can follow |

**The `events` progress format.** It uses the existing progress targets:
files, FIFOs, and `-` for stdout, mixed freely with `json` and `int` targets
through the usual one-format-per-target pairing. What it writes is different:

- **Every event, not a snapshot.** `json` and `int` write one snapshot per
  tick. `events` writes every event since the last tick, each stamped with
  the time it happened. `--progress-interval` sets how often the buffer is
  written out, not the timestamp resolution.
- **Nothing is silently skipped.** A `json` target skips a tick when it
  can't be opened (a FIFO with no reader). An `events` target keeps its
  records until a later tick or exit succeeds. If the buffer limit (1 MiB of
  encoded records per target) fills first, the oldest records are dropped
  and a `dropped` event with the count is written in their place, so gaps
  in the timeline are always visible.
- **A final write at exit,** after the last transfer event, so the file is
  complete when the process ends.

Internally:

- **`internal/events`:** a nil-safe `Sink` and the JSON-lines encoder. It is
  wired through a `tx.WithEventSink` client option and
  `ftcp.ServerOptions.Events`, and the `events` progress target is its
  consumer. Store transfer-created and transfer-finished events drive the
  sender's `--stats` `start` and `end` records.
- **`ClientMetrics`:** new counters for ACK retries (exposed from
  `retryAck`), request errors, wire bytes, and windows per codec.

## Code Layout

- `cmd/tx-bench`: entry point; `make build` builds `tx-bench` next to `tx`
  in the repo root.
- `internal/bench/dataset`: generation, import, reuse, fingerprint, oracle
  verify, cache-warm selection.
- `internal/bench/harness`: `prep`, `remote send-tree`, `remote recv-copy`,
  `local`, tx process supervision, stats tailing, directory ownership.
- `internal/bench/report`: the summary that `recv-copy` prints and
  `tx-bench report` re-renders, plus trace merge and analysis.
- `internal/bench` (tests only): the existing Go microbenchmarks.
  - `make bench` keeps running them with `go test -bench` into
    `bench/results/latest.txt`, without a summary step.
  - The old `bench generate` and `bench report` helpers (`generate.go`,
    `report.go`, `main.go`) are removed.

## Delivery Order

0. **tx prerequisite fixes** (separate change): transfer TTL, chroot
   containment, zero-byte completion.
1. **Dataset: the `prep` steps `generate`, `import`, and `check`.**
   - Seeded generation, hash-in-name, `files.tsv`, `bench.json`,
     fingerprint, mix and profiles, size grammar, `--fill`, parallel
     generation, `--in` import, reuse, directory markers, and the oracle
     verifier.
   - Fuzz: generate, flip one byte (or one mode bit or mtime); the verifier
     must fail.
2. **tx `--stats`** on `send tree`, `recv copy`, and `recv get`, the SIGTERM
   flush, and the new `ClientMetrics` counters.
3. **`remote send-tree`, `remote recv-copy`, and `local`.**
   - The `prep` step `cache`, per-run server restarts, tid-matched prep
     requests, metrics, and the report.
   - Integration test: `local` forking a freshly built `tx` over a tiny
     dataset, asserting the report, the corruption exit code, and the
     refusal to delete an unmarked `DST`.
   - At this point the two-command workflow is complete.
4. **Tracing.** `internal/events`, the tx `events` progress format on both
   sides, `--trace` and `--go-trace` pass-through.
5. **Trace analysis in `report`,** then retire `bench/run`.
