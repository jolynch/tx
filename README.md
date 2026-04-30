# `tx`: Fast file transfer for the cloud

`tx` copies directories and files at line rate by fully saturating modern cloud
hardware - SSD, network, and CPU - simultaneously. On large servers it achieves
up to 10x the throughput of a simple `rsync`.

Three capabilities make this possible:

- **Adaptive concurrency.** A probe measures link speed and server capacity,
  then opens a pool of pre-authenticated TCP connections sized to keep every
  core and disk busy. Work is packed into fixed-size batches - many small files
  are bundled into one request, and large files are split into parallel
  chunks - so every connection does roughly equal work with no head-of-line
  blocking.

- **Adaptive compression.** The server monitors read vs. write latency per 4 MiB
  frame and walks a `none → lz4 → zstd` ladder. Compressible files effectively
  exceed line rate; incompressible files (media, archives) stream with zero CPU
  overhead.

- **Lightweight verification.** Metadata checks (size, mtime, permissions) run
  by default after every copy. Sampled data verification (`--verify 5%data`)
  reads 5% of bytes and catches corruption without a full re-read — practical
  on expensive cloud storage.

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for a deeper dive.

## Quick start

### Serve a directory

```bash
# Serve the current directory on the default port (127.0.0.1:3453)
tx send serve

# Serve a specific directory on all interfaces
tx send 0.0.0.0:3453 serve /srv/data
```

### Copy a directory

```bash
# Copy /srv/data from a remote server to a local directory
tx recv 10.0.0.1:3453 copy /srv/data /var/lib/data
```

Rerun the same command to sync deltas — `copy` detects an existing destination
and only transfers new or changed files. Metadata verification runs by default;
add `--verify 5%data` for sampled data checks.

### With encryption

```bash
# Server — keys are generated automatically on first run
tx send 0.0.0.0:3453 serve -k /var/lib/tx/keys /srv/data

# Client — ephemeral keys by default, auto-negotiates cipher
tx recv 10.0.0.1:3453 copy --encrypt auto /srv/data /var/lib/data
```

### With authorization

Shared-secret tokens restrict who can download. Generate a token securely and
share it out-of-band (e.g. a secrets manager or `scp`):

```bash
# Generate a token
TOKEN=$(head -c 16 /dev/random | xxd -p)
echo "$TOKEN"  # share this with authorized clients

# Server — only clients presenting this token are allowed
tx send 0.0.0.0:3453 serve -k /var/lib/tx/keys \
  --require-auth-token "$TOKEN" /srv/data

# Client — present the token inside the encrypted AUTH blob
tx recv 10.0.0.1:3453 copy --encrypt auto -t "$TOKEN" \
  /srv/data /var/lib/data
```

Tokens are sent inside the encrypted AUTH blob, so they are never visible on
the wire. The server validates them with constant-time comparison.

## Full CLI reference

See [docs/CLI.md](docs/CLI.md) for the complete flag reference including
`--mode gentle`, `--verify`, `--deadline`, progress reporting, and
the `get` / `status` subcommands.

## Building and testing

```bash
go build ./cmd/tx
go test ./...
```
