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
Go benchmarks followed by three same-drive copies on the data root.

```bash
make bench-suite BENCH_SUITE=full BENCH_SUITE_DATA_ROOT="/path/to/bench data" \
  BENCH_SUITE_OUT="/path/to/fresh results"
make bench-suite-commands BENCH_SUITE=full
```

`BENCH_SUITE_DATA_ROOT` is an existing directory dedicated to the suite's
reference trees and copy scratch. Any disk works; the results describe the
drive you choose. Reserve about 450 GiB for a first full run. The suite
locks the data root, verifies new references before publishing them under
`sources/`, and reuses them by recipe and fingerprint. A copy child holds the
lock while it runs, so a later suite cannot reclaim a copy that outlived an
interrupted one. The suite reclaims interrupted runs' staging trees and copy
scratch on the next run. It provisions a cached reference again when its
metadata no longer matches, or when it fails a rehash after a copy
mismatched it. It removes references for earlier recipes only when it needs
their space. Each full run copies about 258 GiB.

Smoke runs use a temporary data root under `bench/data/` by default. Set
`BENCH_SUITE_DATA_ROOT` to reuse smoke references under its `smoke/`
subdirectory. The suite refuses a data root on tmpfs, ramfs, or ZFS, which
cannot evict cached pages, and an output directory inside the data root. On
a checkout on one of those filesystems, set `BENCH_SUITE_DATA_ROOT` for
smoke runs too.

Each copy case (`ssd-small`, `ssd-varied`, `ssd-large`) requests 0% source
cache warmth, measures a single copy with compression and encryption
disabled, and runs the full destination oracle after timing. A copy whose
source was more than 1% cached at the start measured memory rather than the
drive, and is recorded as `setup_failed`. The suite records the realized
file count and mean size from the seeded reference and prints the copy rate
in MiB/s.

On the first SIGINT or SIGTERM, the suite stops its current child: it sends
SIGTERM to a copy or prep and kills the child's process group after 2
minutes. A second signal kills that process group at once.

Use an empty `BENCH_SUITE_OUT` for each run. It defaults to
`bench/results/suite` and holds raw logs, metrics, and `suite.json` with the
workload definitions. `status.json` records each case's outcome after the
suite finishes: `ok`, `failed`, or `build_failed`. A case is `setup_failed`
when its reference could not be provisioned, its source stayed cached, or a
copy mismatched a reference that then failed its own rehash. It measured
nothing, and the scheduler may retry it. A case is `missing` when the
tested commit doesn't define its benchmark or test, and `partial` when it
defines only some of a Go case's benchmarks. Neither counts as a failure, so
the suite can measure older commits. The command listing uses `jq` to read
each dataset's fingerprint for manual reproduction.
