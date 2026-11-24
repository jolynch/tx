# Benchmarks

End-to-end benchmarks run with `tx-bench`, which forks real `tx` processes on
two hosts (or one), verifies every byte against an independent oracle, and
reports. Its design and reference live in [docs/bench](../docs/bench/OVERVIEW.md).

Build both binaries into the repo root:

```bash
make build                  # ./tx and ./tx-bench
```

Commands below run from the repo root.

`tx-bench` forks the `tx` next to it unless `--tx` or `$TX_BIN` names another,
for example `TX_BIN=./tx-new ./tx-bench local -s 1GiB` to A/B a build.

## Two hosts

```bash
# sender: generate 10GiB (mixed profile), serve it, print the client command
./tx-bench remote send-tree --size 10GiB

# receiver: paste the printed command
./tx-bench remote recv-copy 10.0.4.17:3453 --expect 7f3a9c21e0b4d8aa
```

## One host

```bash
./tx-bench local -s 1GiB -n 3                      # tx against itself
./tx-bench local -s 1GiB --baseline rsync          # rsync on the same dataset
./tx-bench local -s 1GiB -- --compress zstd        # pass flags to tx recv copy
./tx-bench local -s 1GiB --send-arg --disable-zero-copy
./tx-bench local -s 1GiB --go-trace c.out --send-go-trace s.out   # runtime/trace
```

Datasets are described by a size and a mix (`--profile mixed|small|large|random|compressible`
or `--mix rand=40%@1GiB,silesia:osdb=60%@16MiB`); see
[Dataset](../docs/bench/DATASET.md). `tx-bench prep` generates or imports one
ahead of time and sets the sender's page cache.

## Profiling

CPU flamegraphs are an external `perf record` against the forked processes,
for example `perf record -g -p "$(pgrep -f 'tx recv copy')"`.

## Acceptance

```bash
make bench-acceptance BENCH_SIZE=1GiB   # default 5GiB; needs ~2.5x free disk
```

It runs `remote send-tree` and `remote recv-copy` as separate processes and
checks correctness, shutdown, and resource budgets; CI runs it at 5GiB.

## Microbenchmarks

```bash
make bench                  # go test -bench, results in bench/results/latest.txt
```

## Published performance suite

On Linux, run `make bench-suite` for a smoke run on at least two available
CPUs. The full suite needs at least 16 available CPUs and runs the in-memory
Go benchmarks followed by the four SSD workloads on the data root.

```bash
make bench-suite BENCH_SUITE=full BENCH_SUITE_DATA_ROOT="/path/to/bench data" \
  BENCH_SUITE_OUT="/path/to/fresh results"
make bench-suite-commands BENCH_SUITE=full
```

`BENCH_SUITE_DATA_ROOT` is an existing directory dedicated to the suite's
reference trees and copy scratch. Any disk works; the results describe the
drive you choose. Reserve about 200 GiB for a first full run. The suite
locks the data root, verifies new references before publishing them under
`sources/`, and reuses them by recipe and fingerprint. A copy child holds the
lock while it runs, so a later suite cannot reclaim a copy that outlived an
interrupted one. The suite reclaims interrupted runs' staging trees and copy
scratch on the next run. It provisions a cached reference again when its
metadata no longer matches, or when it fails a rehash after a copy
mismatched it. It removes a reference for an earlier recipe or a retired
workload once no run has used it for `--keep-unused` (7 days by default),
and sooner when it needs the space. Runs of older commits also count as
use, so runs of an older and a newer commit do not regenerate each other's
references. Each full run writes 258 GiB of destination payload.

Smoke runs use a temporary data root under `bench/data/` by default. Set
`BENCH_SUITE_DATA_ROOT` to reuse smoke references under its `smoke/`
subdirectory. The suite refuses a data root on tmpfs, ramfs, or ZFS, which
cannot evict cached pages, and an output directory inside the data root. On
a checkout on one of those filesystems, set `BENCH_SUITE_DATA_ROOT` for
smoke runs too.

| Workload | Full dataset per copy |
|----------|-----------------------|
| `ssd-small` | 2 GiB of random data in 16 KiB files |
| `ssd-large` | 40 GiB of random data in 2 GiB files |
| `ssd-mixed` | 32 GiB split equally by bytes between random files and compressible Silesia files |
| `ssd-compressible` | 12 GiB of Silesia data |

The full Silesia recipes draw file sizes from 128 MiB to 1 GiB. The mixed
recipe splits its random bytes between small files of 4 KiB to 256 KiB
and large files of 1 GiB. The [dataset guide](../docs/bench/DATASET.md#sources)
describes the Silesia cache.

Each workload runs three measured copies with the tested tx's compression
and encryption defaults. Before each copy, the harness clears the
destination and evicts every source page it can. After timing, it runs
full destination verification. Eviction is advisory, so if more than 10% of
the source stays cached, the suite stops before that copy and records
`setup_failed`. It prints each
copy's rate in MiB/s and records separate copy and verification times and
the client frame counts by codec.

On the first SIGINT or SIGTERM, the suite stops its current child: it sends
SIGTERM to a copy or prep and kills the child's process group after 2
minutes. A second signal kills that process group at once.

Use an empty `BENCH_SUITE_OUT` for each run. It defaults to
`bench/results/suite` and holds raw logs, metrics, and the suite results.
`status.json` records each case's outcome after the suite finishes. A
case may be `ok`, `failed`, or `build_failed`. A case is `setup_failed`
when its reference could not be provisioned, its source stayed cached, or a
copy mismatched a reference that then failed its own rehash. It measured
nothing, and the scheduler may retry it. A case is `missing` when the
tested commit doesn't define its benchmark or test, and `partial` when it
defines only some of a Go case's benchmarks. Neither counts as a failure, so
the suite can measure older commits. A case is `skipped` when its worst-case
time no longer fits before `--deadline`; it measured nothing and is not
retried. The scheduled run passes `--deadline 450m`, so a case added past
that budget is skipped instead of timing out the job. `--describe`
prints each case's worst case and the total. The command listing uses `jq`
to read each dataset's fingerprint for manual reproduction.

[`suite-results-format`](suite-results-format) identifies the `suite.json`
format for external runners. The report contains the profile and a list of
cases with unique IDs. Each case declares its configuration, an `identity`,
and optional chart definitions. The `identity` holds the settings shared by
every series in the case but not the list of series, so a consumer can track
each series across commits while cases gain or lose series. Its `samples` maps each series to an array of measurements
whose keys identify the metrics. SSD samples include `MiB/s`, `files/s`,
`copy_ns`, and `verify_ns`. The SSD `details` include the dataset
fingerprint, file and byte counts, the highest pre-copy cache residency
across repeats, and `windows`, which totals frames by client-reported codec.
A chart declares an ID, a title, an optional series prefix, and `metrics`,
which maps each charted metric to the direction that counts as better: `inc`
or `dec`. Metric keys that start with `ns/` or end in `_ns` are durations in
nanoseconds.
Cases can share a chart ID and title to group related results. Keep case
IDs stable across revisions of a workload and use a new ID for a distinct
workload. Every case carries a `revision` for its kind
(`suiteGoRevision`, `suiteGridRevision`, `suiteSSDRevision`). Bump it when
harness code changes what that kind measures or how it times it.
