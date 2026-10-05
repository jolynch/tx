# tx-bench Dataset

A dataset is either **generated** (deterministic and self-verifying: any file
can be regenerated from `(seed, part, index)`, and every file name carries its
own content hash) or **imported** from a directory you supply with `--in`.
Either way, `files.tsv` lists every entry with its content hash, and a single
fingerprint over that list identifies the whole tree.

## Layout

```text
tx-bench-src/
  bench.json
  files.tsv                            every entry's type, size, hash, path
  data/
    0-rand/0000.9f12ab…e2.bin          <index>.<xxh128 hex32>.bin
    0-rand/0001.04c7d1…5a.bin
    1-silesia/0000.4be0c3…71.bin
    2-rand/00000.77e2f0…0c.bin
```

- Each mix part gets its own directory, `<part index>-<source>`.
- `<index>` is zero-padded to the digit count of the part's file count, and
  to at least 4 digits.
- The hash is the lowercase hex xxh128 of the file's full contents: the same
  algorithm as the `file-hash` tokens in [FX/1](../ftcp/FRAMING.md). With the
  hash in the name, generated data can be checked by any tool, without
  `files.tsv`.

An imported dataset has no `data/` directory; see
[User-Supplied Data](#user-supplied-data).

## Generation

The `generate` prep step runs when `prep` or `send-tree` is given any shape flag (`--size`, `--fill`, `--profile`, `--mix`, `--seed`); under `send-tree`, only in the first prep. It works in four steps:

1. Resolve the total size ([Size Grammar](#size-grammar)) and the mix
   ([Mix Grammar](#mix-grammar)).
2. Split each part's byte budget into file sizes:
   - **Fixed size `S`:** `floor(budget / S)` files of size `S`, plus one file
     holding any remainder. When the budget is smaller than `S`, the part is
     one file of the whole budget.
   - **Range `lo..hi`:** sizes are drawn log-uniformly from a PRNG seeded by
     `(seed, part)` until the budget is used. The last file is truncated to
     the remainder, with a minimum of 1 byte.

   Either way, the dataset total equals the requested size exactly.
3. Write files with `-j` workers (default: CPU count). Each file is:
   1. written to a temp name, hashing as it goes;
   2. `fdatasync`ed, so its pages are clean and evictable;
   3. renamed to `<index>.<hash>.bin`.

   Directories are fsynced after their files.
4. Write `files.tsv`, compute the fingerprint, and write `bench.json` last
   (each via temp file + rename). A marked `BENCH_DIR` without `bench.json`
   is incomplete and is regenerated. An unmarked one is never touched
   ([Directory Ownership](./OVERVIEW.md#directory-ownership)).

Generation checks free space first and refuses to fill the filesystem past
`--fill`, or past 95% when `--fill` is unset.

### Sources

| Source                     | Content |
|----------------------------|---------|
| `rand`                     | ChaCha8 stream (`math/rand/v2`) seeded with `xxh3(seed, part, index)`. Incompressible. |
| `silesia[:a+b+…]`          | The named [Silesia](http://sun.aei.polsl.pl/~sdeor/silesia.html) corpus files, concatenated in the order given into a ring buffer. Each file starts at a seeded offset into the ring and repeats it, so files differ while staying realistically compressible. No names means `dickens+mozilla+mr+nci+ooffice+osdb+reymont+samba+sao+webster+xml+x-ray`. |

Silesia originals are downloaded once into `--silesia-cache` (default
`$XDG_CACHE_HOME/tx-bench/silesia`). For air-gapped hosts, place the unzipped
files there by hand. A failed download names the missing files, the cache
directory, and the archive URL, so pre-seeding is one copy away. `bench.json` records each original's xxh128, so a dataset
regenerated elsewhere is checked against the same corpus.

This fixes two problems in the old `bench generate`: specs repeated each
other's bytes (every spec reseeded with 1), and specs writing to one directory
overwrote each other's `file-N.bin`.

## User-Supplied Data

`--in DIR` turns on the `import` prep step instead of `generate`: benchmark
your own tree. It cannot be combined with shape flags.

- **Read-only and in place.** Nothing is copied, linked, or written under
  `DIR`. The forked `tx send tree` chroots to the deepest directory containing
  both `BENCH_DIR` and `DIR`, and `bench.json` records both remote paths
  ([Served Root](./OVERVIEW.md#served-root)).
- **Not nested.** `BENCH_DIR` and `DIR` may not be nested inside each other in
  either direction. `--in .` with the default `./tx-bench-src` is a usage error
  that suggests a sibling `BENCH_DIR`.
- **Hashed once.** Import walks `DIR` and hashes every regular file with
  `-j` workers, then writes `files.tsv`, the fingerprint, and `bench.json`.
  `BENCH_DIR/in-cache.tsv` records each file's `(dev, inode, size,
  mtime_ns, ctime_ns)`, so a later import of the same `DIR` rehashes only
  changed files. ctime is included because tools such as `touch -r` and
  `rsync -t` preserve mtime but cannot set ctime.
- **Entry types.**
  - Regular files, directories, and symlinks are supported.
  - Hardlinks are recorded as groups (type `h` in [files.tsv](#filestsv)),
    and the oracle checks that they arrive as hardlinks.
  - Sockets, FIFOs, and device nodes make import fail with a list of their
    paths.
- **Described for the report.** `bench.json` records the input path, file and
  byte counts, a log2 file-size histogram, and an estimated zstd ratio. The
  ratio is measured on a seeded sample of up to 64 MiB, so compression
  results can be read in context.
- **Page cache.** The `cache` step evicts and warms the pages of `DIR`'s files
  exactly as for generated data. On a shared host that also affects other
  users of those files.

Later preps without `--in` or shape flags reuse the imported dataset.

- **At startup,** `DIR` is stat-walked and compared with `in-cache.tsv`. Any
  change is an error that tells you to rerun with `--in DIR`, which
  re-imports, prints the new fingerprint, and leaves `DIR` untouched.
- **At every later prep,** the same walk lists changed paths in `prep.json`.
  `recv-copy` then reports mismatches on those paths as "source changed" and
  fails only that run ([Prep](./OVERVIEW.md#prep)).

## Mix Grammar

```text
MIX    := PART ("," PART)*
PART   := SOURCE "=" SHARE "@" SIZES
SOURCE := "rand" | "silesia" [":" NAME ("+" NAME)*]
SHARE  := INT "%"                      shares must sum to 100%
SIZES  := BYTES | BYTES ".." BYTES     fixed size, or log-uniform range
```

Example: `rand=40%@1GiB,silesia:osdb+dickens=30%@16MiB,rand=30%@4KiB..256KiB`.

### Profiles

`--profile` names a preset mix. `--mix` overrides it.

| Profile          | Mix |
|------------------|-----|
| `mixed` (default)| `rand=40%@1GiB,silesia:osdb+dickens+nci+xml=30%@16MiB,rand=30%@4KiB..256KiB` |
| `small`          | `rand=100%@4KiB..64KiB` |
| `large`          | `rand=100%@2GiB` |
| `random`         | `rand=100%@16MiB` |
| `compressible`   | `silesia=100%@16MiB` |

At the default `10GiB`, `mixed` is 4 × 1GiB, 192 × 16MiB, and about 52k small
files.

## Size Grammar

`--size` sets the dataset total.

| Form       | Meaning |
|------------|---------|
| `10GiB`    | Absolute bytes (`encoding.ParseByteSize` units) |
| `25%mem`   | Percent of MemTotal; above 100% guarantees a dataset larger than the page cache |
| `10%disk`  | Percent of the capacity of the filesystem holding `BENCH_DIR` |

`--fill N%` replaces `--size`; giving both is a usage error. It sizes the
dataset so that the filesystem holding `BENCH_DIR` ends up N% used:
`N% × capacity − used`, counting any existing dataset as reclaimable. A result of zero
or less is an error. Use it to measure behavior on a disk that is, for example,
half full.

## bench.json

```json
{
  "version": 1,
  "seed": 1,
  "size_spec": "10GiB",
  "bytes": 10737418240,
  "files": 52436,
  "mix": "rand=40%@1GiB,silesia:osdb+dickens+nci+xml=30%@16MiB,rand=30%@4KiB..256KiB",
  "parts": [
    {"dir": "0-rand", "source": "rand", "share_pct": 40, "sizes": "1GiB", "files": 4, "bytes": 4294967296}
  ],
  "silesia": {"osdb": "xxh128:…", "dickens": "xxh128:…"},
  "in": null,
  "remote": {"bench": "/", "data": "/data"},
  "fingerprint": "7f3a9c21e0b4d8aa",
  "created": "2026-10-01T14:22:33Z"
}
```

`remote` holds the paths `recv-copy` uses, relative to the `tx send tree` chroot.
`bytes` and `files` are resolved values. `size_spec` is what the user asked for
(`25%mem` resolves differently on different hosts). For an imported dataset,
the generation fields are `null` and `in` holds:

```json
"in": {"path": "/srv/photos", "size_histogram": {"4KiB": 812, "1MiB": 9150}, "zstd_ratio_est": 1.04}
```

## files.tsv

One line per entry under the data root, sorted bytewise by path,
tab-separated:

```text
<type>\t<mode>\t<size>\t<hash>\t<mtime_ns>\t<path>\n
```

| Field      | Value |
|------------|-------|
| `type`     | `f` file, `d` directory, `l` symlink, `h` hardlink to an earlier `f` |
| `mode`     | Permission bits in octal (`0644`); `-` for `l` |
| `size`     | Bytes for `f` and `h`; `0` otherwise |
| `hash`     | `xxh128:<hex32>` for `f`; the link target for `l`; for `h`, the path of the group's first entry in sorted order; `-` for `d` |
| `mtime_ns` | Modification time in nanoseconds, which tx preserves; `-` for `l` |
| `path`     | Relative to the data root. Last, so it may contain tabs; tx already forbids `\n` and `\r` in paths. |

Generated data uses mode `0644` for files and `0755` for directories.
Ownership is not recorded, because it legitimately differs between hosts.

## Fingerprint

The fingerprint is the first 16 hex digits of xxh3-64 over the lines of
`files.tsv` with the `mtime_ns` field removed. It commits to every path, type,
mode, size, link target, hardlink group, and content hash, so it changes if
any entry is missing, extra, renamed, resized, retyped, re-moded, or
rewritten.

mtimes are excluded because generation time differs per host, so
regenerating with the same seed reproduces the same fingerprint on any host.
The `mtime_ns` column is still checked by the oracle. It is protected in
transit only by tx's own integrity checks, not by the fingerprint.

## Oracle Verification

`recv-copy` checks `DST/data` without consulting tx:

1. Once, before the first run: fetch `/files.tsv` and fingerprint it. The
   result must equal `--expect`, or `bench.json`'s fingerprint when
   `--expect` is absent. This anchors the list to the value printed on the
   sender's terminal.
2. After each run, walk `DST/data`. Its entries must match `files.tsv`
   exactly:
   - the same set of paths;
   - the same type, mode, size, mtime, and symlink target for each;
   - every `h` entry sharing an inode with its group's first path.
3. Under `--oracle full` (the default), hash every regular file with xxh128
   using `-j` workers and compare the result to its `files.tsv` hash.

`--oracle names` stops after step 2, which catches structural and metadata
damage but not content corruption. It is only for quick iteration on huge
datasets.

A mismatch on a path that the sender's latest `prep.json` lists as changed
in an `--in` source is reported as "source changed", not corruption.

## Reuse

`prep` and `send-tree` reuse an existing generated dataset when `bench.json`
matches the request: same seed, same resolved mix, same resolved byte total. A
stat-only walk of `data/` must then match `files.tsv` (paths, types, sizes).

- **Parameters match but the walk does not:** an error. The tree was
  modified; rerun with `--regen`.
- **Parameters differ, or `BENCH_DIR` holds an imported dataset:** an error
  naming the difference, unless `--regen` is given. The tool never deletes a
  dataset silently, and `--regen` never touches an `--in` directory.
- **`--check`** (a separate prep step) additionally rehashes every file
  against `files.tsv`.
- **No shape flags and no `--in`:** nothing is compared against a request.
  The existing dataset, generated or imported, is loaded as it is, and its
  stat walk must still match ([Prep](./OVERVIEW.md#prep)).
