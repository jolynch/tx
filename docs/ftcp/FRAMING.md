# Filexfer Framing Specification (FX/1)

This document defines the wire framing for fast efficient
file transfers over plain TCP sockets.

## Scope

- Transport: plain TCP stream.
- This framing is for per-file transfer messages after a manifest has been exchanged out of band.
- `file_id` is an integer index into that exchanged manifest (0-based).

## Frame Structure

Each file transfer response frame is:

1. Header line (ASCII, UTF-8 safe)
2. Raw payload bytes
3. Trailer line (ASCII, UTF-8 safe)

Header and trailer lines are terminated by `\n`.

### Header Line

Format:

```text
FX/1 <file_id> <properties...>
```

- `FX/1`: protocol version token.
- `<file_id>`: manifest-relative file ID (unsigned integer).
- `<properties...>`: space-separated `property=value` tokens.

### Trailer Line

Format:

```text
FXT/1 <file_id> status=ok ts=<unix_ms> [file-hash=<algo>:<value>] next=<offset> [meta:*=...] [hash=xxh64:<value>]
```

- `status=<code>`: required. Receivers reject any trailer whose status is
  not `ok`.
- `ts=<unix_ms>`: required server timestamp (unix milliseconds) when the
  trailer is emitted.
- `file-hash=<algo>:<value>`: authoritative checksum token for the served
  request window; emitted on the final trailer of a window. Current
  implementation emits `file-hash=xxh128:<hex32>`.
- `next=<offset>`: the offset the following frame starts at
  (`offset + size`). The final trailer uses `next=0` as a terminal marker.
- `meta:*=<value>`: file metadata tokens on `SEND` final trailers (see
  [TCP Command Contract](#tcp-command-contract)).
- `hash=<algo>:<value>`: whole-frame checksum, always the last token when
  present. Computed as `xxh64` over the header line, the payload bytes, and
  the trailer bytes preceding ` hash=`. Emitted on manifest (`TXFER`/`SYNC`)
  and `CXSUM` frames; **not** emitted on `SEND` file frames, which rely on
  the window `file-hash` plus `ACK` validation instead.

### Example

```text
FX/1 12 offset=0 size=1048576 wsize=262144 comp=zstd ts=1736000000000
<262144 payload bytes>
FXT/1 12 status=ok ts=1736000000421 file-hash=xxh128:9f12ab... next=0 meta:size=1048576 meta:mtime_ns=1735771234000000000 meta:mode=0644 meta:uid=1000 meta:gid=1000 meta:user=jolynch meta:group=jolynch
```

## Header Properties

Properties are ASCII and case-sensitive.

### Required

- `comp=<mode>`: compression mode.
- `offset=<n>`: byte offset within the logical file where payload data belongs.
- `size=<n>`: number of original (logical, uncompressed) bytes represented by this frame.
- `wsize=<n>`: number of payload bytes on the wire for this frame.
- `ts=<unix_ms>`: server timestamp (unix milliseconds) when this frame header is emitted.

### Optional

- `hash=<algo>:<value>`: per-chunk checksum of this frame's logical
  (decompressed) bytes. Emitted on manifest (`TXFER`/`SYNC`) and `CXSUM`
  frame headers (currently `xxh128`); not emitted on `SEND` file frames.
  At most one `hash` token per header.
- `max-wsize=<bytes>`: server hint for maximum wire payload bytes per frame for this response window.
  - Emitted on the first frame of a `SEND` response.
  - Current bucket algorithm is ceiling in `{1,2,4,8,16,32,64} MiB`.

## Compression

`comp` allowed values:

- `none`
- `zstd`
- `lz4`

Receiver behavior:

- `none`: write bytes directly.
- `zstd` or `lz4`: decompress before writing to destination offset.

## Parsing Rules

- Maximum header/trailer line bytes: 4 MiB (defensive limit, matching the
  command-line cap).
- Unknown properties are ignored.
- Missing required fields (`comp`, `offset`, `size`, `wsize`, `ts`) reject frame.
- Invalid `file_id` reject frame.
- Invalid numeric value formats reject frame.
- Header must be exactly one line; no multi-line property blocks.

## Semantics

- `mtime`, mode/permissions, and full-file `size` are manifest properties, not framing properties.
- `offset` allows resumable/partial writes.
- `size` is the logical uncompressed bytes covered by this frame.
- `wsize` is the exact payload byte count that follows the header newline.
- For `comp=none`, `size` must equal `wsize`.
- For `comp=zstd|lz4`, decompressed bytes must equal `size`.
- Trailer `hash=<algo>:<value>`, when present, is used for frame-integrity
  validation/logging.

## Error Handling

Receiver must close the TCP connection on framing errors:

- malformed version token
- malformed property syntax
- invalid numeric conversion
- payload shorter/longer than declared `wsize`
- decompressed segment length not equal to declared `size`
- trailer `status` other than `ok`

Receiver should emit protocol error code in logs with offending `file_id` when available.

## Versioning

- `FX/1` is the current version.
- Breaking framing changes require new token (`FX/2`).
- New optional properties are backward-compatible within `FX/1`.

## TCP Command Contract

The command protocol is line-based:

- command line: `<VERB> <args...>\r\n`
- status line:
  - `OK\r\n`
  - `OK <message>\r\n`
  - `ERR <code> <message>\r\n`

The full command grammar (`AUTH`, `TXFER`, `SYNC`, `SEND`, `ACK`, `CXSUM`,
`STATUS`, `PROBE`), including path/blob token encoding, is specified in
[OVERVIEW.md](./OVERVIEW.md). This section covers only how `SEND` and
`TXFER` responses use FX/1 framing.

`SEND` returns one or more `FX/1` frame triplets, then a terminal status line:

1. `FX/1` header line
2. `wsize` payload bytes (raw or compressed per `comp`)
3. `FXT/1` trailer line
4. `OK` or `ERR ...` status line

The server repeats these triplets until each requested window is complete.
Default logical frame size cap is `4 MiB`.

Directory `SEND` requests are metadata-only. For a manifest directory entry,
the server emits exactly one empty frame:

```text
FX/1 <file_id> offset=0 size=0 wsize=0 comp=none ts=<unix_ms>
FXT/1 <file_id> status=ok ts=<unix_ms> file-hash=xxh128:<empty-hash> next=0 meta:size=0 meta:mtime_ns=<n> meta:mode=<octal07777> meta:uid=<uid> meta:gid=<gid> meta:user=<user> meta:group=<group>
```

The transfer root is queried the same way as manifest entry `D0`, using
`file_id=0`; the path token is the manifest root path itself.
Directory/root metadata frames are not acknowledged as copied payload bytes.

For `SEND` responses, header properties are emitted in this order:
`offset`, `size`, `wsize`, `comp`, optional `max-wsize`, then `ts`.
`SEND` file-frame headers carry no `hash` token.
Current implementation supports adaptive compression and may vary `comp` per frame.

Current trailer shape for `SEND` (no whole-frame `hash` token; see
[Trailer Line](#trailer-line)):

```text
FXT/1 <file_id> status=ok ts=<unix_ms> [file-hash=<algo>:<value>] next=<offset> [meta:*=...]
```

`next` is the offset that the following frame starts at (`offset + size`).
The final trailer uses `next=0` as a terminal marker.

`file-hash` is emitted on final trailer as the checksum token for the served request window.
The final trailer also includes file metadata tokens:
`meta:size`, `meta:mtime_ns`, `meta:mode`, `meta:uid`, `meta:gid`, `meta:user`, `meta:group`.

`TXFER` returns the manifest body as a sequence of `FX/1` + `FXT/1` frames
that reuse the same wire grammar as `SEND`, with `file_id=0` reserved as a
sentinel meaning "the manifest byte stream". `wsize`-bounded payload bytes
(decompressed when `comp=zstd`) concatenate to form the FM/1 manifest.
The terminal trailer carries `next=0`; the final verb-level `OK\r\n`
follows it. Manifest trailers do not carry `meta:*` tokens (no analog).
Manifest clients validate the FX/1 `hash=xxh128:<chunk>` over each decoded
logical chunk, the FXT/1 `hash=xxh64:<frame>` over header + wire payload +
trailer prefix, and the terminal `file-hash=xxh128:<manifest>` over the full
decoded FM/1 stream. This is intentionally stricter than SEND file-frame
validation because the manifest is transfer control data.
Clients may use `meta:mode`, `meta:uid`, and `meta:gid` to mirror ownership/permissions
only after payload integrity verification succeeds.

`file-hash=<algo>:<value>` on terminal trailer is the authoritative per-window
checksum token. Current implementation emits `file-hash=xxh128:<hex32>`.

`max-wsize` is a first-frame hint only. Clients may use it to pre-size a reusable
frame buffer, but they may cap allocation (current client default cap is `64 MiB`)
and still stream larger frames in multiple reads.
