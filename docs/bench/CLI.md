# tx-bench CLI

`tx-bench` is a harness around real `tx` binaries; see the
[overview](./OVERVIEW.md) for the run protocol. Help output below is rendered
by `internal/cliflags`, the same formatter as `tx`.

## Quick Reference

```text
$ tx-bench --help
usage: tx-bench <command> [options]

Commands:
  prep       Generate or import a dataset, check it, and set its page cache
  remote     Benchmark tx between two hosts (send-tree, recv-copy)
  local      Run remote send-tree and recv-copy against each other on this host
  report     Print the benchmark report from recv-copy metrics and traces

Run 'tx-bench <command> --help' for command-specific options.
```

### `tx-bench prep`

```text
$ tx-bench prep --help
usage: tx-bench prep [options] [BENCH_DIR]

Prepare a benchmark dataset: generate or import it, check it, and set its
page-cache state. Each step runs only when its options are given.

  BENCH_DIR    bench root; generated data lives in BENCH_DIR/data
               (default ./tx-bench)

Steps, in order:
  generate   any of --size, --fill, --profile, --mix, --seed
  import     --in DIR (read-only, in place); excludes generate
  check      --check
  cache      --cache-warm

Without shape flags or --in, the existing dataset is used as is.

Options:
  -s, --size string              Dataset size: <bytes>|N%mem|N%disk (default: 10GiB when
                                 generating)
      --fill string              Size the dataset so its filesystem ends up N% used;
                                 replaces --size (default "")
      --profile string           Preset mix: mixed|small|large|random|compressible
                                 (default: mixed when generating)
  -m, --mix string               Explicit mix, e.g.
                                 rand=40%@1GiB,silesia:osdb=60%@16MiB; overrides
                                 --profile (default "")
      --seed int                 Dataset seed (default: 1 when generating)
  -i, --in string                Import this directory as the dataset, read-only and in
                                 place; excludes the shape flags above (default "")
      --regen                    Replace an existing dataset that does not match; never
                                 touches an --in directory (default false)
      --check                    Rehash every file against files.tsv (default false)
  -c, --cache-warm string        Page cache to set up: N% of the dataset (0% cold, 100%
                                 hot) or N%mem of RAM taken from the dataset; unset
                                 leaves the cache alone (default "")
      --cache-warm-skew string   Beta(1, skew) warm placement; 1 is uniform, larger is
                                 more contiguous (default "1")
      --cache-warm-block string  Warm and evict granularity (default "1MiB")
      --cache-drop-meta          Also drop cached dentries and inodes (host-wide
                                 drop_caches=2; requires root) so --cache-warm 0% is
                                 fully cold (default false)
  -j, --jobs int                 Parallel generate, import, and check workers (0=CPU
                                 count) (default 0)
      --silesia-cache string     Silesia corpus cache directory (default:
                                 $XDG_CACHE_HOME/tx-bench/silesia)
```

### `tx-bench remote`

```text
$ tx-bench remote --help
usage: tx-bench remote <command> [options]

Benchmark tx between two hosts, forking the matching tx command on each.

Commands:
  send-tree  Prepare a dataset and serve it with tx send tree
  recv-copy  Pull it repeatedly with tx recv copy, verify, and report

Run 'tx-bench remote <command> --help' for command-specific options.
```

#### `tx-bench remote send-tree`

```text
$ tx-bench remote send-tree --help
usage: tx-bench remote send-tree [options] [BENCH_DIR] [-- TX_ARGS...]

Prepare a benchmark dataset, serve it with a forked 'tx send tree', and print
the matching 'tx-bench remote recv-copy' command.

  BENCH_DIR    bench root (default ./tx-bench)
  TX_ARGS      passed to tx send tree as given (see 'tx send tree --help')

Behavior:
  - Runs every enabled prep step once (see 'tx-bench prep --help'), then
    starts tx send tree --exit-after never TX_ARGS...
  - Each prep request from recv-copy stops that tx send tree, writes
    runs/server-<tid>.json with its exact rusage, re-runs the cache step,
    and starts a fresh tx send tree for the next run
  - BENCH_DIR must be absent, empty, or already marked by tx-bench
  - Serves until interrupted, or exits if tx send tree exits unexpectedly

Options:
  -s, --size string                Dataset size: <bytes>|N%mem|N%disk (default: 10GiB
                                   when generating)
      --fill string                Size the dataset so its filesystem ends up N% used;
                                   replaces --size (default "")
      --profile string             Preset mix: mixed|small|large|random|compressible
                                   (default: mixed when generating)
  -m, --mix string                 Explicit mix, e.g.
                                   rand=40%@1GiB,silesia:osdb=60%@16MiB; overrides
                                   --profile (default "")
      --seed int                   Dataset seed (default: 1 when generating)
  -i, --in string                  Import this directory as the dataset, read-only and
                                   in place; excludes the shape flags above (default "")
      --regen                      Replace an existing dataset that does not match;
                                   never touches an --in directory (default false)
      --check                      Rehash every file against files.tsv (default false)
  -c, --cache-warm string          Page cache to set up: N% of the dataset (0% cold,
                                   100% hot) or N%mem of RAM taken from the dataset;
                                   unset leaves the cache alone (default "")
      --cache-warm-skew string     Beta(1, skew) warm placement; 1 is uniform, larger is
                                   more contiguous (default "1")
      --cache-warm-block string    Warm and evict granularity (default "1MiB")
      --cache-drop-meta            Also drop cached dentries and inodes (host-wide
                                   drop_caches=2; requires root) so --cache-warm 0% is
                                   fully cold (default false)
  -j, --jobs int                   Parallel generate, import, and check workers (0=CPU
                                   count) (default 0)
      --silesia-cache string       Silesia corpus cache directory (default:
                                   $XDG_CACHE_HOME/tx-bench/silesia)
      --tx string                  tx binary to fork (default: tx next to tx-bench, then
                                   $PATH)
  -l, --listen string              Listen address (host:port) (default "0.0.0.0:3453")
      --advertise string           Host printed in the recv-copy command (default: first
                                   non-loopback address)
      --auth string                Require a generated auth token: auto|on|off; auto is
                                   on for --in and off for generated data. On forces
                                   encryption, and the printed recv-copy command carries
                                   the token (default "auto")
  -o, --metrics string             Append one record per run to this file; .json/.jsonl
                                   for JSON lines, else space-separated rows (default
                                   "")
      --trace string               Write the sender event timeline (tx -f events) to
                                   this file; .json/.jsonl for JSON lines, else
                                   space-separated (default "")
      --trace-rss-interval string  Interval for sampling the tx process's RSS into the
                                   trace (default "100ms")
      --go-trace string            Write runtime/trace output of each tx send tree
                                   process (tx --trace) to PATH.<k> (default "")
```

#### `tx-bench remote recv-copy`

```text
$ tx-bench remote recv-copy --help
usage: tx-bench remote recv-copy [options] [SERVER] [DST] [-- TX_ARGS...]

Pull a served benchmark dataset repeatedly with forked 'tx recv copy'
processes, verify every run, and report.

  SERVER    sender host[:port][/bench/path] (default 127.0.0.1:3453/)
  DST       work directory, marked by tx-bench; each run copies into DST/data
            after deleting DST/data and DST/.tx (default ./tx-bench-dst)
  TX_ARGS   passed to tx recv copy as given (see 'tx recv copy --help')

Behavior:
  - Runs --warmup unmeasured copies, then --iterations measured copies
  - Before each copy, cleans DST, then requests a sender prep and waits for
    the sender to acknowledge that exact request
  - Refuses an existing, non-empty DST that tx-bench did not mark
  - Each copy is one tx recv copy --verify none TX_ARGS... process, so its
    rusage is exact per run; --encrypt, -k, and -t also apply to the small
    tx recv get fetches of sender state
  - After each copy, DST/data is checked against the sender's files.tsv;
    --skip-write in TX_ARGS turns that check off
  - Exit codes: 0 ok, 1 failed run(s), 2 usage or --expect mismatch,
    3 corruption (stops immediately and keeps DST)

Options:
  -w, --warmup int                 Unmeasured runs before measuring (default 1)
  -n, --iterations int             Measured runs (default 3)
  -e, --expect string              Dataset fingerprint printed by send-tree; checked
                                   against the fetched files.tsv (default "")
      --oracle string              Check of DST/data against files.tsv after each run:
                                   full|names (default "full")
  -j, --jobs int                   Verify workers (0=CPU count) (default 0)
  -o, --metrics string             Metrics file; .json/.jsonl for JSON, else
                                   space-separated rows; empty disables (default:
                                   ./tx-bench-<UTC timestamp>.json)
      --trace string               Write the client event timeline (tx -f events) to
                                   this file; fetched sender events go to its .server
                                   sibling (default "")
      --trace-rss-interval string  Interval for sampling the tx process's RSS into the
                                   trace (default "100ms")
      --go-trace string            Write runtime/trace output of each tx recv copy (tx
                                   --trace) to PATH.<run> (default "")
      --server-wait string         How long to wait for a prep acknowledgement or
                                   runs/server-<tid>.json (default "2m")
      --fail-fast                  Stop at the first failed run (default false)
      --keep                       Keep DST after the last run (default false)
      --tx string                  tx binary to fork (default: tx next to tx-bench, then
                                   $PATH)
```

### `tx-bench local`

```text
$ tx-bench local --help
usage: tx-bench local [options] [BENCH_DIR] [DST] [-- TX_ARGS...]

Run 'tx-bench remote send-tree' on 127.0.0.1 with an ephemeral port and
'tx-bench remote recv-copy' against it, both forking real tx processes.
Accepts every send-tree and recv-copy option; -j and --tx apply to both
sides, and the sender's output files take the --send- options below. TX_ARGS
go to tx recv copy; use --send-arg for tx send tree. Both sides share one page
cache, so --cache-warm warns unless the dataset and DST fit in MemAvailable.
Exits with recv-copy's exit code.

Options:
      --send-arg string       Argument passed through to tx send tree; repeatable, in
                              order (default "")
      --baseline string       Reference copy instead of tx recv copy: none|rsync
                              (default "none")
      --send-metrics string   Sender --metrics file (default "")
      --send-trace string     Sender --trace file (default "")
      --send-go-trace string  Sender --go-trace file (default "")
```

### `tx-bench report`

```text
$ tx-bench report --help
usage: tx-bench report [options] FILE [FILE...]

Print the benchmark report from files written by 'tx-bench remote recv-copy'.

  FILE    a recv-copy --metrics file, a recv-copy --trace file, or both

A metrics file re-renders the summary recv-copy printed at the end of its runs.
A trace file adds timeline analysis: per-file breakdowns, concurrency gaps,
throughput over time, and anomalies. Sender events are read from the trace's
.server sibling when present.

Options:
  -r, --run string           Trace analysis: only this run (e.g. 1, w1); empty for all
                             measured runs (default "")
      --top int              Trace analysis: slowest files to list (default 20)
      --format string        Report format: text|json (default "text")
      --clock-offset string  Trace analysis: override the estimated sender clock offset
                             (e.g. 0, -1.8ms) (default: from trace header)
```

## Detailed Behavior

### Two-Host Workflow

```text
host-a$ ./tx-bench remote send-tree --size 10GiB
prep    ./tx-bench/data  profile=mixed size=10GiB seed=1  generating (-j 16)
  0-rand        4.0GiB       4 x 1GiB
  1-silesia     3.0GiB     192 x 16MiB   osdb+dickens+nci+xml
  2-rand        3.0GiB  52,240 x 4KiB..256KiB
generated 52,436 files, 10.0GiB in 41s  fingerprint 7f3a9c21e0b4d8aa
cache-warm unset (hot 3.1%)  auth off (generated data)  trace off
tx #1   ./tx (xxh128:5e0c…91)  pid 48213  serving tx://10.0.4.17:3453 (root ./tx-bench)

Run the bench client with:
  ./tx-bench remote recv-copy 10.0.4.17:3453 --expect 7f3a9c21e0b4d8aa

prep    seq=1  for tid=a01f  hot 3.0%  0.4s  tx #2 pid 48290
run     tx #2  tid=8c1e  52,436 files  10.0GiB  8.41s  rss 188MiB  cpu 11.2s
prep    seq=2  for tid=b7c3  hot 3.0%  0.4s  tx #3 pid 48377
...
```

Each `prep` line is one `recv-copy` request. It stops the previous `tx send
tree`, records that run's line from its exact rusage, and starts the next.

```text
host-b$ ./tx-bench remote recv-copy 10.0.4.17:3453 -e 7f3a9c21e0b4d8aa
tx      ./tx (xxh128:5e0c…91)  matches sender
warmup 1/1   10.0GiB  9.80s
run    1/3   10.0GiB  8.41s  1.19GiB/s  verify ok
run    2/3   10.0GiB  8.37s  1.19GiB/s  verify ok
run    3/3   10.0GiB  8.55s  1.17GiB/s  verify ok

tx-bench  10.0GiB / 52,436 files  sender hot 3.0%  compress=adapt  encrypt=none
            min      p50      max
  wall      8.37s    8.41s    8.55s     probe 3ms  manifest 0.21s  data 8.10s  | verify 6.2s
  rate      1.17     1.19     1.19 GiB/s   wire 0.94 GiB/s (ratio 1.27)
  client    rss 412MiB  cpu 2.1 core-s/GiB  majflt 0
  sender    rss 188MiB  cpu 1.1 core-s/GiB  conns 36 (peak 34)
  conns     dials 34  reuse 9,812  sync-fallback 0  heartbeat-fail 0
  errors    0   ack-retries 0   failed runs 0/3
  verify    52,436/52,436 files  fingerprint ok
metrics: ./tx-bench-20261001T142233Z.json
```

### Pass-Through Arguments

Arguments after `--` go to the forked tx command exactly as typed. `tx-bench`
does not re-declare tx's flags, so the binary that is actually forked
validates them. That stays correct when `--tx` points at a different build
whose flags differ. For `local`, `--` goes to `tx recv copy` and repeatable
`--send-arg` values go to `tx send tree`.

`tx-bench` assembles each command as `tx <cmd> DEFAULTS... TX_ARGS... MANAGED...`:

| Kind | `tx send tree` | `tx recv copy` | Rule |
|------|----------------|----------------|------|
| Defaults | `--exit-after never` | `--verify none` | Placed before `TX_ARGS`, so yours win (Go flags keep the last value) |
| Managed | `--listen`, `--stats`, `--trace`, `CHROOT`; `-p`/`-f` only with `--trace` | `--stats`, `--trace`, `REMOTE_SRC`, `LOCAL_DST`; `-p`/`-f` only with `--trace` | Set by `tx-bench`; passing one in `TX_ARGS` is a usage error naming the `tx-bench` option to use instead (`-l`, `--trace`, `--go-trace`, …). Without `--trace`, your own `-p`/`-f` progress targets pass through. |
| Read, then passed through | `--require-auth-token` | `--skip-write`, `--encrypt`, `-k`, `-t` | `--skip-write` turns off the `DST` check. `--encrypt`, `-k`, and `-t` are also given to the `tx recv get` fetches of sender state. A user-supplied `--require-auth-token` turns `--auth` off and is not echoed into the printed `recv-copy` command. |

`tx-bench` recognizes these flags in every form Go's `flag` package accepts:
`-x v`, `--x v`, `-x=v`, and `--x=v`. Repeatable tx flags (`-t`,
`--require-auth-token`, and `-p`/`-f` when not managed) append rather than
replace, which is why the managed ones are rejected rather than overridden.

Everything else is opaque to `tx-bench`. It is recorded verbatim as
`tx_args` in the metrics and shown in the report header, so a result
always says how tx was invoked.

**Fail fast.** tx reports a bad flag with exit code 2 or through
`log.Fatalf` (exit code 1), so `tx-bench` keys on timing rather than the
code. It stops with tx's stderr and exits 2 in either case:
- `tx send tree` exits before it starts listening;
- the first `tx recv copy` exits non-zero before its `--stats` reports a tid.

A bad flag never becomes a string of failed runs.

When auth is on (`--auth on`, or `auto` with `--in`), `send-tree` generates a
token, passes `--require-auth-token TOKEN` to every `tx send tree` it starts,
and prints `... -- --encrypt auto -t TOKEN` in the `recv-copy` command.

### Common Commands

| Goal | Command |
|------|---------|
| Default two-host benchmark | `tx-bench remote send-tree -s 10GiB` / `tx-bench remote recv-copy HOST -e FP` |
| Cold sender cache every run | `tx-bench remote send-tree -c 0%` |
| Dataset twice the size of RAM | `tx-bench remote send-tree -s 200%mem` |
| Half-full disk | `tx-bench remote send-tree --fill 50%` |
| Your own data (auth and encryption on by default) | `tx-bench remote send-tree -i /srv/photos -c 0%` |
| Truly cold, metadata included (root) | `sudo tx-bench remote send-tree -c 0% --cache-drop-meta` |
| zstd only, 8 workers | `tx-bench remote recv-copy HOST -e FP -- --compress zstd --concurrency 8` |
| Gentle mode, limited sender | `tx-bench remote send-tree -- --bwlimit 1GiB` / `tx-bench remote recv-copy HOST -- --mode gentle` |
| More measured runs, no warmup | `tx-bench remote recv-copy HOST -w 0 -n 10` |
| Full timelines on both sides | `tx-bench remote send-tree --trace s.jsonl` / `tx-bench remote recv-copy HOST --trace c.jsonl`, then `tx-bench report c.jsonl` |
| A/B two tx builds | `tx-bench remote recv-copy HOST --tx ./tx-new` (the sender's `--tx` sets its side) |
| Re-print an earlier report | `tx-bench report tx-bench-20261001T142233Z.json` |
| Single host smoke test | `tx-bench local -s 256MiB -n 1` |
| Single host, zero-copy off | `tx-bench local -s 1GiB --send-arg --disable-zero-copy` |
