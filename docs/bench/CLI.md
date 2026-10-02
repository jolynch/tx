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
               (default ./tx-bench-src)

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

  BENCH_DIR    bench root (default ./tx-bench-src)
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
      --tx string                  tx binary to fork (default: $TX_BIN, then tx next to
                                   tx-bench, then $PATH)
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
  - --forever keeps copying until interrupted; an interrupt always stops the
    copy in flight, flushes, and reports
  - --metrics streams: .jsonl and text get one record per run as it completes
    (a FIFO works, for exporting); .json is rewritten after each run
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
      --forever                    After the warmups, copy and verify until interrupted
                                   instead of --iterations times; metrics default to
                                   .jsonl
      --stop-sender                After the last run, fetch runs/stop so send-tree
                                   records it and exits (default false)
      --tx string                  tx binary to fork (default: $TX_BIN, then tx next to
                                   tx-bench, then $PATH)
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
dataset ./tx-bench-src/data  profile=mixed size=10GiB seed=1  generating (-j 16)
  part            bytes        files  sizes           corpus
  0-rand         4.0GiB            4  1GiB            -
  1-silesia      3.0GiB          192  16MiB           osdb+dickens+nci+xml
  2-rand         3.0GiB       52,240  4KiB..256KiB    -
generated 52,436 files, 10.0GiB in 41s  fingerprint 7f3a9c21e0b4d8aa
cache-warm unset (hot 3.1%)  auth off (generated data)  trace off
tx #1   ./tx (xxh128:5e0c…91)  pid 48213  serving tx://10.0.4.17:3453 (root ./tx-bench-src)

Run the bench client with:
  ./tx-bench remote recv-copy 10.0.4.17:3453 --expect 7f3a9c21e0b4d8aa

event   seq    tx  tid             files      bytes       dur     hot        rss       cpu
prep      1    #2  a01f6c3e            -          -     412ms    3.0%          -         -  pid 48290
run       -    #2  8c1e04b7       52,436    10.0GiB      8.4s       -   188.0MiB     11.2s
prep      2    #3  b7c3e912            -          -     398ms    3.0%          -         -  pid 48377
...
```

Every output is a fixed-width table, so rows line up with their header and
details that do not fit a column (a pid, an error) trail the row. Each `prep`
row is one `recv-copy` request: its `tid` is the request's, and `tx` is the
fresh `tx send tree` it started. Handling it stops the previous process,
whose `run` row (exact rusage, data-transfer `tid`) is printed first. The
report lists every metric with min, p50, and max in one unit per row, then
any failed runs, one per row with the error at the end.

```text
host-b$ ./tx-bench remote recv-copy 10.0.4.17:3453 -e 7f3a9c21e0b4d8aa
tx      ./tx (xxh128:5e0c…91)  matches sender
run   tid             files      bytes      wall         rate     hot  status
w1    a01f6c3e       52,436    10.0GiB      9.8s     1.0GiB/s    3.0%  ok
1     8c1e04b7       52,436    10.0GiB      8.4s     1.2GiB/s    3.0%  ok
2     3f90a2d1       52,436    10.0GiB      8.4s     1.2GiB/s    3.0%  ok
3     e4b1c077       52,436    10.0GiB      8.6s     1.2GiB/s    3.0%  ok

tx-bench report
  dataset   10.0GiB  52,436 files  profile=mixed  fingerprint 7f3a9c21e0b4d8aa
  client    compress=adapt  encrypt=none  oracle=full
  sender    cache left as found  sendfile 100% of windows
  runs      3 measured, 0 failed, 1 warmup
  verify    52,436/52,436 files, fingerprint ok

  metric                     min         p50         max  unit
  wall                      8.37        8.41        8.55  s
    probe                    3.1         3.2         3.4  ms
    manifest               208.0       211.0       215.0  ms
    data                    8.06        8.10        8.24  s
    finalize                 210         262         301  µs
  verify (oracle)           6.10        6.20        6.31  s

  rate                       1.2         1.2         1.2  GiB/s
  wire rate                963.1       967.4       972.0  MiB/s
  files/s                  6,132       6,235       6,265  count
  compress ratio            1.26        1.27        1.27  x
  sender hot                 3.0         3.0         3.0  %
  ...
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
