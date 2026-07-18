# Filexfer Manifest Specification (FM/1)

This document defines the strict manifest format emitted by `TXFER` and consumed by `start`/`get`.

When requesting a manifest via `TXFER`, clients may first issue `AUTH`.
If `AUTH` provides a client recipient, the manifest stream is age-encrypted for that recipient.

The FM/1 byte stream is identical regardless of the TXFER wire compression
(`comp=none` or `comp=zstd`). The TXFER response wraps it in a sequence of
FX/1 + FXT/1 frames carrying `file_id=0` (see [OVERVIEW](./OVERVIEW.md));
the concatenated frame payloads — decompressed when `comp=zstd` — form the
single FM/1 byte stream described here.

## Structure

Manifest is line-oriented UTF-8 text:

1. Header line (`FM/1`)
2. Root entry (`D0`)
3. Child entry lines

Empty lines and `#` comments are ignored.

## Header

Format:

```text
FM/1 <transfer_id> [<len>:<root>] mode=<fast|gentle> link-mbps=<int> concurrency=<int> [deadline-ms=<int>]
```

Fields are required unless marked optional.

- `FM/1`: manifest version token.
- `<transfer_id>`: transfer identifier.
- `<len>:<root>` (optional): length-prefixed absolute root path. Accepted on
  parse for compatibility; current servers do not emit it (the root travels
  in the `D0` entry).
- `mode`: transfer mode (`fast` or `gentle`).
- `link-mbps`: client-reported link estimate in Mbps (`>= 0`).
- `concurrency`: planned client concurrency (`> 0`).
- `deadline-ms` (optional): transfer deadline in milliseconds (`>= 0`);
  emitted only when a deadline is set.

Any unknown header option is invalid.

## Entry Lines

Format:

```text
<id> <size> <mtime> <mode> <path> [trailing tokens...]
```

- `<id>`: unsigned file id, optionally prefixed by an entry-type byte (`F`/`H`/`D`/`S`); a leading digit is treated as `F`.
- `<size>`: file size bytes (unsigned integer).
- `<mtime>`: front-coded mtime token: `<prefix_len>:<suffix_data>`. For `H` entries this field carries the link-target file id instead of an mtime.
- `<mode>`: octal unix mode bits (`0000`-`7777`).
- `<path>`: front-coded path token: `<prefix_len>:<suffix_len>:<suffix_data>`.

Fields are separated by one ASCII space. The path token is self-delimiting via its `<suffix_len>` prefix, so additional trailing tokens may follow on the same line.

### Trailing tokens

`S` (symlink) entries always append a length-prefixed link target (`<n>:<target>`) immediately after the path token. After that, any entry type may carry zero or more optional trailing tokens, separated by single spaces. Currently defined tokens:

- `pc:<hex>` (regular files only) — encoded page-cache residency hint produced by `EncodePageCacheEntry`. The hex bytes decode to `[1 byte: padding (0..7)][zstd(packed_bitmap_LE)]`, where the bitmap covers `ceil(size/page_size)` pages with low-bit-first ordering and the padding header is the number of unused trailing bits in the final byte.

Unknown trailing tokens are rejected so the wire stays explicit.

## Root Entry

```text
D0 0 <mtime> <mode> <absolute-root-path>
```

- ID `0` is reserved for the transfer root.
- The root entry type is always `D`.
- The root path is absolute and is encoded with the same front-coded path token as other entries.
- Child entries start at ID `1`.

## Mtime Front Coding

Token format:

```text
<prefix_len>:<suffix_data>
```

Decoded value:

- first entry: `<suffix_data>` (`prefix_len` must be `0`)
- next entries: `prev_mtime[:prefix_len] + suffix_data`

Decoded mtime must be unsigned Unix nanoseconds.

## Path Front Coding

Token format:

```text
<prefix_len>:<suffix_len>:<suffix_data>
```

Decoded value:

- first entry: `<suffix_data>` (`prefix_len` must be `0`)
- next entries: `prev_path[:prefix_len] + suffix_data`

Path constraints:

- `D0` must be the first entry and carries the absolute transfer root path
- child entries must be relative (no leading `/`)
- `..` traversal is rejected
- `\\` is rejected; use `/`

## Validation Rules

- Header must be `FM/1` and include required metadata fields.
- Unknown header options are rejected.
- Entry IDs must be unique and strictly increasing.
- The first entry must be `D0` with an absolute path.
- `size` must be unsigned and fit `int64`.
- `mtime` token must decode to decimal digits and fit `int64`.
- `mode` must be octal and `<= 07777`.
- Each entry must have at least 5 space-separated fields; trailing tokens are optional except for `S` entries which require a link target.
- Path token must parse and remain traversal-safe after decode.
- `pc:` tokens are valid only on regular-file entries; they may appear at most once per entry.

## Example

```text
FM/1 9f83ab12 mode=fast link-mbps=1200 concurrency=16
D0 0 0:1735771234000000000 0755 0:10:/repo-root
F1 4096 13:567890123 0644 0:14:data/chunk-000
F2 4096 14:90123 0644 11:3:001
F3 1024 15:1350 0644 11:3:002
F4 88 0:1736000000000000000 0600 0:15:logs/result.txt
```
