# Benchmarks

This directory holds the helper CLI, benchmark runner, and scratch space used
for transfer benchmarking.

Commands below assume your current directory is `bench/`.

## Layout

- `run`: benchmark runner for `tx` and an `rsync` baseline.
- `bench`: helper CLI built from `./internal/bench`.
- `tx`: local transfer binary used by `./run`.
- `data/src`: source tree used as the remote dataset.
- `data/dst`: target tree used as the local destination.
- `data/silesia`: cached original Silesia corpus files used by `-source silesia:<csv>`.

`./run` clears `data/dst` and `data/.tx` before each run.

## Quickstart

Build the benchmark tools:

```bash
make -C .. build-bench
```

This creates both `./bench` and `./tx` in the `bench/` directory.

Generate a dataset into `data/src`:

```bash
./bench generate -source rand data/src:81920@64KiB
```

Run the copy benchmark:

```bash
./run data/src --target-dir data/dst
```

`./run` resolves `data/src` to an absolute server path automatically, so these
relative examples work from inside `bench/`.

Useful runner variants:

```bash
./run data/src --skip-write
./run data/src --compress zstd
./run data/src --encrypt chacha20
./run data/src --rsync
```

See `./bench generate -h` and `./run --help` for the full command
surface.

## Generate Datasets

`bench generate` accepts one or more specs:

```text
<outdir>:<count>@<size>
```

Examples:

```bash
./bench generate data/src:100@10MiB
./bench generate -source rand data/src-small:81920@64KiB
./bench generate -source silesia:osdb,nci data/src:10@100MiB
```

Before regenerating `data/src`, clear prior files but keep the placeholder:

```bash
find data/src -mindepth 1 ! -name .keep -delete
```

## Example: 5 GiB Of Small Files

This uses `64KiB` files, which gives exactly `5GiB` total across `81,920`
files.

```bash
find data/src -mindepth 1 ! -name .keep -delete
./bench generate -source rand data/src:81920@64KiB
./run data/src --target-dir data/dst
```

## Example: 5 GiB As Two Large Files

This creates two `2560MiB` files, for exactly `5GiB` total.

```bash
find data/src -mindepth 1 ! -name .keep -delete
./bench generate -source rand data/src:2@2560MiB
./run data/src --target-dir data/dst
```

## Example: Mixed 10 GiB Dataset

This mix uses:

- `2 x 1GiB` files from `silesia:osdb,dickens`
- `409 x 10MiB` files from `rand`
- `2 x 2GiB` files from `rand`

```bash
find data/src -mindepth 1 ! -name .keep -delete
./bench generate -source silesia:osdb,dickens data/src:2@1GiB
./bench generate -source rand data/src:409@10MiB
./bench generate -source rand data/src:2@2GiB
./run data/src --target-dir data/dst
```

This lands `6MiB` short of exact `10GiB`, because `10MiB` files do not divide
evenly into `4GiB`. If you want the total to be exactly `10GiB`, add one more
small file:

```bash
./bench generate -source rand data/src:1@6MiB
```

## Generate From Silesia

Use one cached corpus file:

```bash
find data/src -mindepth 1 ! -name .keep -delete
./bench generate -source silesia:osdb data/src:10@100MiB
./run data/src --target-dir data/dst
```

Use multiple corpus files in round-robin order:

```bash
find data/src -mindepth 1 ! -name .keep -delete
./bench generate -source silesia:osdb,nci data/src:10@100MiB
./run data/src --target-dir data/dst
```

When `-source silesia:<csv>` is used:

- requested originals are downloaded once into `bench/data/silesia/`
- cached originals are reused on later runs
- output files are built by repeating and truncating the selected original bytes
- multiple selected corpus files are assigned to outputs in round-robin order

Useful small Silesia subsets include `osdb`, `nci`, `mr`, and `dickens`.

## Notes

- `-source rand` creates deterministic random data, which is useful for stressing I/O, framing, and crypto.
- `-source silesia:<csv>` produces more compression-realistic content while still letting you scale each generated file to any target size.
