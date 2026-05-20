# FTCP Protocol (`-file-listen`)

This document defines the TCP file-transfer command protocol implemented by Pinch.

## Transport

- Listener: `-file-listen` (for example `127.0.0.1:3453`)
- One connection serves at most one command (optionally preceded by `AUTH`)
- Server closes the connection after command completion (or on error)

## Line Protocol

All commands are single lines terminated by `\r\n`.

- Request line: `<VERB> <args...>\r\n`
- Optional command-specific payload bytes (depends on command semantics)
- Response status line:
  - `OK\r\n`
  - `OK <message>\r\n`
  - `ERR <code> <message>\r\n`

For `TXFER`, `SEND`, and `CXSUM`, the payload interval is a streaming body
(`FM/1` for `TXFER`, `FX/1` for `SEND`/`CXSUM`) between the request line and
the terminal response status line. `PROBE` also has a request payload and
response payload body.

Maximum command line size is 4 MiB.

## Connection Flow

1. Client connects.
2. Client sends either:
   - command line (`TXFER|SEND|ACK|CXSUM|STATUS|PROBE`), or
   - `AUTH` first, then exactly one command line.
3. Server writes response.
4. Server closes connection.

If `-fs-require-auth=true`, first line must be `AUTH`.

## Token Encoding

Most args are plain space-delimited tokens.

Path/blob arguments use one of:

- quoted text: `"..."` (supports escapes like `\"` and `\\`)
- length-prefixed bytes: `<len>:<bytes>`

For this line protocol, token bytes cannot span command newlines.

## AUTH

### Request

Three forms:

- `AUTH key` — key exchange: server returns its recommended cipher and age public key.
- `AUTH aes <blob>` — encrypted session setup using AES-GCM.
- `AUTH chacha20 <blob>` — encrypted session setup using ChaCha20-Poly1305.

`<blob>` is length-prefixed (`<len>:<bytes>`).

### Key Exchange Flow

1. Client sends `AUTH key\r\n`.
2. Server responds `OK <recommended-cipher> <server-age-public-key>\r\n` and closes the connection.
3. Client opens a new connection and sends either `AUTH aes <blob>\r\n` or `AUTH chacha20 <blob>\r\n`.

If the client requested `auto`, it first resolves `auto` to the server's
recommended cipher from step 2 and then uses that resolved value in the second
connection.

### Encrypted Connection Flow

After `AUTH aes <blob>` or `AUTH chacha20 <blob>`:

- Server treats the first AUTH token (`aes` or `chacha20`) as the session
  cipher.
- Server decrypts `<blob>` using its identity and the selected AEAD algorithm
  to recover the client's identity material.
- Decrypted plaintext is a space-delimited sequence:

  ```
  <client_age_public_key> [<token1> <token2> ...]
  ```

  The first field is the client's age X25519 recipient; remaining fields are
  zero or more opaque identity tokens. A token is any printable string with
  length > 8 bytes, no ASCII spaces, and no newlines (arbitrary encoding —
  hex, base64, bech32, an `age1…` recipient, a UUID, etc.).
- Authorization check (only if the server was started with
  `--require-auth-token`):
  - Build the presented set `{client_age_public_key} ∪ {token1, token2, ...}`.
  - At least one presented value must exactly match an allowlisted token
    configured on the server. The client's age public key counts as an
    identity token, so an operator may allowlist the `age1…` string directly
    without a shared secret.
  - Comparison is constant-time (`crypto/subtle.ConstantTimeCompare`).
- If the allowlist is empty, any decryptable client is accepted (any tokens
  presented are ignored).
- If valid:
  - subsequent command bytes from the client must be encrypted to the server's
    public key using that same AEAD algorithm.
  - subsequent response bytes are encrypted to the client's public key using
    that same AEAD algorithm.
- If invalid: `ERR NOT_AUTHORIZED authorization failed`.

### Unencrypted Connection Flow

When no encryption is desired, the client omits AUTH entirely and sends the
command directly.

## TXFER

Creates a transfer and streams a manifest.

### Request

`TXFER <path> mode=<fast|gentle> link-mbps=<int> concurrency=<int> [verbose=<0|1|true|false>] [deadline-ms=<int>] [cache-map=<0|1|true|false>] [comp=none|zstd]`

- `<path>` must be quoted or length-prefixed.
- directory must be absolute, existing, and readable.
- `mode`, `link-mbps`, and `concurrency` are required.
- `link-mbps` must be `>= 0`.
- `concurrency` must be `> 0`.
- `cache-map=1` asks the server to attach a per-file page-cache residency hint
  (`pc:<hex>` trailing token) to each FM/1 entry; see
  [MANIFEST.md](./MANIFEST.md). Linux-only on the server; on other platforms
  the flag is honored but no pagecache tokens are emitted.
- `comp` selects the per-frame wire compression. Supported values are `none`
  (literal FM/1 bytes per frame) and `zstd` (each frame is an independent
  zstd frame). Default is `zstd`. The framing is unconditional — `comp` only
  affects per-frame compression, not whether framing is present.

### Response

The response is **always** a sequence of paired FX/1 + FXT/1 frames carrying
`file_id=0` (a reserved sentinel meaning "the manifest stream"), terminated
by the verb-level `OK\r\n` line. Each `FX/1` header is followed by exactly
`wsize` bytes of payload, then the matching `FXT/1` trailer.

```
FX/1 0 offset=<chunk-start> size=<logical> wsize=<wire> comp=<none|zstd> hash=xxh128:<chunk-content> ts=<ms>\n
<wire bytes>
FXT/1 0 status=ok ts=<ms> [file-hash=xxh128:<full-manifest>] next=<next-offset|0> hash=xxh64:<frame-line>\n
... repeat ...
OK\r\n
```

Per-chunk details:

- `offset` — cumulative logical bytes of FM/1 emitted before this chunk.
- `size` — logical (uncompressed) bytes in this chunk.
- `wsize` — wire bytes following the header newline. For `comp=none`,
  `wsize == size`. For `comp=zstd`, `wsize` is the compressed-frame length.
- `comp` — `none` or `zstd`. Each `zstd` frame is independent and
  self-contained; concatenated wire payloads form a multi-frame zstd
  archive that decodes to the full FM/1.
- `hash` in the header — xxh128 of this chunk's logical bytes.
- `hash` at the end of the trailer — xxh64 of the line (header + payload
  + trailer prefix), for wire/control corruption detection.
- `file-hash` on the trailer — cumulative xxh128 of the entire manifest's
  logical bytes; **present only on the terminal trailer** (the one with
  `next=0`). Clients validate this once at end-of-stream.

Manifest clients validate all three integrity layers: the per-chunk logical
hash, the full frame/control checksum, and the terminal cumulative logical
hash. This is intentionally stricter than SEND file-frame validation because
the manifest is control data for the rest of the transfer.

Chunks are produced by the server on a streaming basis: a frame flushes
when either ~4 MiB of logical bytes have accumulated or ~1 s has passed
since the last frame, whichever comes first. This lets clients display
real-time progress even when walking large directory trees.

A successful empty-manifest response still contains exactly one terminal
FX/1+FXT/1 pair (`size=0 next=0`; `wsize` follows `comp`), so the protocol
is uniformly self-delimiting via the `OK\r\n` line.

The wire payloads (concatenated, without the FX/1/FXT/1 headers) form a
standalone multi-frame zstd archive when `comp=zstd` — clients may tee
those bytes directly to a `.zst` file on disk while a parallel decoder
parses the manifest in memory. This `RawSink` path is the compressed
persistence path; clients do not need to retain both compressed and decoded
manifest copies in memory.

## SYNC

Resume-aware manifest refresh. The client sends a prior FM/1 manifest as the
request body, and the server returns a fresh FM/1 manifest plus `RM` lines
naming files that existed in the prior manifest but are no longer on disk.
The server retains no state across SYNC calls — all knowledge of the prior
manifest comes from the client-supplied body.

### Request

`SYNC <path> mode=<fast|gentle> link-mbps=<int> concurrency=<int> [deadline-ms=<int>] [comp=none|zstd]`

- `<path>` must be quoted or length-prefixed.
- directory must be absolute, existing, and readable.
- `mode`, `link-mbps`, and `concurrency` are required.
- `link-mbps` must be `>= 0`.
- `concurrency` must be `> 0`.
- `comp` selects the per-frame wire compression for the **response** stream.
  Supported values are `none` and `zstd`; default is `zstd`. The request
  body's framing is unconditional, and each request frame is self-describing
  via its `comp=` header — the request body may use any supported codec
  independently of the request-line `comp`.

### Request body

The request line is followed by an FX/1 + FXT/1 framed stream carrying the
prior FM/1 manifest (`file_id=0`, same framing rules as TXFER's response
described below). The terminal frame (`next=0` with the cumulative
`file-hash`) signals end-of-body — there is no separate terminator.

The server hashes each path in the supplied manifest with xxh3-128 and
indexes the entries by that hash. The actual path strings are discarded;
only `(size, mtime, mode, fileID)` are retained per hash.

### Response

The response is a sequence of paired FX/1 + FXT/1 frames carrying `file_id=0`,
followed by the verb-level `OK\r\n` line. Each frame's logical payload is a
mix of FM/1 lines and `RM <fileID>` lines (one per byte stream — they are
interleaved at line granularity but each line is whole).

```
FX/1 0 offset=<chunk-start> size=<logical> wsize=<wire> comp=<none|zstd> hash=xxh128:<chunk-content> ts=<ms>\n
<wire bytes>
FXT/1 0 status=ok ts=<ms> [file-hash=xxh128:<full-body>] next=<next-offset|0> hash=xxh64:<frame-line>\n
... repeat ...
OK\r\n
```

After decompression and concatenation, the logical body has the structure:

```
FM/1 <txferid> mode=<mode> link-mbps=<n> concurrency=<n> [deadline-ms=<n>]\n
D0 ... <root entry>\n
F1 ... <file entry>\n
...
RM <old-fileID>\n
RM <old-fileID>\n
...
```

The integrity model is identical to TXFER (per-chunk xxh128, per-frame xxh64,
cumulative xxh128 on the terminal trailer). See [TXFER](#txfer) for the
per-chunk details.

The `<fileID>` in `RM` lines refers to the **prior manifest's** file IDs (the
ones the client supplied in the request body). Because the server retains no
path strings from the request, clients must keep their own `fileID → path`
index from the manifest they uploaded and resolve `RM` IDs locally.

A successful response always contains at least one terminal frame, even when
the on-disk tree is empty (single frame with `size=0 next=0`).

## SEND

Streams one or more file windows as `FX/1` frames. Directory entries use the
same command for metadata-only responses.

### Request

`SEND <txferid> fd=<fid> <path> [offset=<n>] [size=<n>] [comp=<name>] [mode=<fast|gentle>] [<unknown key=value>...] [fd=<fid> <path> ...]`

- each `fd=` starts a new file block.
- required per block: `fd`, `path`.
- `offset` defaults to `0`.
- `size` defaults to `0` (means "from offset to EOF").
- `comp` defaults to `adapt`.
- `mode` defaults to `fast`.
- accepted compression values: `adapt`, `none`, `identity`, `lz4`, `zstd`.
- accepted load strategy values: `fast`, `gentle`.
- `identity` is normalized to `none`.
- in `adapt`, server may emit different per-frame `comp` values as it adjusts compression.
- unknown compression values are rejected with `ERR UNSUPPORTED_COMP ...`.
- each `<path>` is quoted or length-prefixed.
- directory entries must request `offset=0` and omit `size`; they return one
  empty frame plus terminal metadata trailer.
- the transfer root is manifest entry `D0` and may be requested for metadata
  with `fid=0` and the root path itself.
- unknown `key=value` fields are ignored.

### Response

- Continuous `FX/1` stream for all tuples, in request order (see [FRAMING.md](./FRAMING.md)).
- Terminal status line after stream: `OK` or `ERR ...`.

## ACK

Acknowledges file progress/window completion.

### Request

`ACK <txferid> fd=<fid> <path> ack-token=<token> [delta-bytes=<n>] [recv-ms=<n>] [sync-ms=<n>] [<unknown key=value>...] [fd=<fid> <path> ...]`

- each `fd=` starts a new ack block.
- required per block: `fd`, `path`, `ack-token`.
- telemetry fields default to `0` when omitted.
- unknown `key=value` fields are ignored.

`ack-token` forms:

- missing file: `-1`
- positive progress: `<ack-bytes>@<server-ts>@<hash-token>`

Rules:

- non-`-1` ack must include hash token.
- hash is validated against stored window hash at ack offset.

### Response

- `OK` / `OK <message>` / `ERR ...`

## CXSUM

Streams checksum frames for one or more requested file ranges.

### Request

`CXSUM <txferid> fd=<fid> <path> [offset=<n>] [size=<n>] [algo=xxh128|xxh64] ...`

- `<path>` is quoted or length-prefixed.
- `fd=<fid>` may be repeated to request multiple ranges, including multiple ranges for the same file id.
- `offset` defaults to `0`.
- omitted `size` means checksum through EOF.
- `algo` defaults to `xxh128`.

### Response

- `FX/1` frame stream (one zero-payload frame per requested range, in request order)
- terminal status line: `OK` or `ERR ...`

## STATUS

Returns transfer status JSON. Two forms:

- `STATUS <txferid>` — single transfer lookup
- `STATUS` (no argument) — list all active transfers

### Request

`STATUS [<txferid>]`

### Response

- failure: `ERR <code> <message>`

**Single transfer** (`STATUS <txferid>`):

- success: `OK <json>`

```json
{
  "transfer_id": "string",
  "directory": "string",
  "num_files": 0,
  "total_size": 0,
  "done": 0,
  "done_size": 0,
  "percent_files": 0,
  "percent_bytes": 0,
  "download_status": {
    "started": 0,
    "running": 0,
    "done": 0,
    "missing": 0
  }
}
```

**List all** (`STATUS`):

- success: `OK <count>` followed by `<count>` lines, each containing one transfer status JSON object (same schema above). Count may be `0` with no following lines.

## PROBE

Latency/throughput probe used before `TXFER` so the client can send transfer hints.
Clients may continue probing every 10s during an active transfer to refresh the
server's observed link estimate for gentle limiting.

### Request

`PROBE cpu=<client-cpu> probe-bytes=<n> cts0=<unix-ms> [txferid=<id>] [obs-link-mbps=<int>]`

- request line is followed by exactly `probe-bytes` raw bytes.
- `probe-bytes` must be `<= 32 MiB`.
- `txferid` is optional and scopes periodic reprobe updates to an existing transfer.
- `obs-link-mbps` is optional and reports the client's last measured link bandwidth.
  When provided, the server uses it to refresh the per-transfer observed link estimate
  for gentle limiting.

### Response

- first line:
  - `PROBE cpu=<server-cpu> cts0=<echo-client-cts0> sts0=<unix-ms> sts1=<unix-ms> probe-bytes=<n> [io-depth=<int>] [wmem=<bytes>] gentle-cpu-pct=<int> gentle-bw-pct=<int> [limiter-bps=<bytes/sec>]`
- then exactly `probe-bytes` raw bytes.
- terminal status line: `OK` or `ERR ...`.
- `gentle-cpu-pct` is the server-advertised CPU budget clients use when computing
  gentle suggested concurrency.
- `gentle-bw-pct` is the server-advertised share of observed link bandwidth used
  to derive gentle transfer rate limits.
- `PROBE` traffic itself is not rate-limited.

Clients typically run 3 probes, compute a rounded link estimate, choose mode/concurrency, then issue `TXFER` with those required hints.
