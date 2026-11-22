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
Go benchmarks followed by three same-drive SSD copies.

```bash
make bench-suite BENCH_SUITE=full BENCH_SUITE_ROOT="/path/to/dedicated SSD" \
  BENCH_SUITE_OUT="/path/to/fresh results"
make bench-suite-commands BENCH_SUITE=full
```

Use an existing, dedicated SSD directory for `BENCH_SUITE_ROOT` and reserve
about 450 GiB for initial provisioning. The suite locks this root, verifies
new references before publishing them under `sources/`, and reuses them by
recipe and fingerprint. It reclaims interrupted runs' staging trees and copy
scratch on the next run. Each full run copies about 258 GiB. Smoke runs use a
temporary workspace by default. Set `BENCH_SUITE_ROOT` to reuse smoke sources
under its `smoke/` subdirectory.

Each SSD case requests 0% source cache warmth, measures a single copy with
compression and encryption disabled, and runs the full destination oracle
after timing. The suite records the realized file count and mean size from
the seeded reference and prints the copy rate in MiB/s.

Use an empty `BENCH_SUITE_OUT` for each run. It defaults to
`bench/results/suite` and holds raw logs, metrics, and `suite.json` with the
workload definitions. `status.json` records each case's outcome after the
suite finishes. The command listing uses `jq` to read each dataset's
fingerprint for manual reproduction.
