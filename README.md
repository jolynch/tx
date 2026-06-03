# `tx`: Fast file transfer for the cloud

`tx` copies directories and files at line rate by fully saturating modern cloud
hardware - SSD, NIC, and CPU - simultaneously. On large servers it achieves
up to 10x the throughput of a simple `rsync --fsync` + [`happycache`](https://github.com/hashbrowncipher/happycache) while providing above line-rate encryption, compression,
and page cache transfer.

Four capabilities make this possible:

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

- **Background durability.** Per-file `fdatasync` is moved off the download
  critical path into a bounded background batcher that deduplicates by inode.
  It starts with a 512 MiB flush target, grows the batch size under backlog
  pressure, and never lets fsync work fan out into unbounded goroutines. A
  final `syncfs` ensures the entire filesystem is durable before the transfer
  reports success.

- **Lightweight verification.** Metadata checks (size, mtime, permissions) run
  by default after every copy. Sampled data verification (`--verify 5%data`)
  reads 5% of bytes and catches corruption without a full re-read *or* time
  budget verification (`--verify 30s`) verifies all metadata and as much
  sampled data as it can in the time budget - practical on slow network
  attached drives such as EBS.

See [docs/README.md](docs/README.md) for the documentation index and
[docs/arch/OVERVIEW.md](docs/arch/OVERVIEW.md) for a deeper dive. If you
are curious about benchmarking on your own setup check out
[bench/README.md](bench/README.md) which has a data generator that can both
generate incompressible (random) and
compressible ([Selesia](https://sun.aei.polsl.pl/~sdeor/index.php?page=silesia))
test datasets as well as run `tx` or alternative `rsync` commands.

## Quick start

### Serve a directory

```bash
# Serve the current directory on the default port (127.0.0.1:3453)
tx send tree

# Serve a specific directory on all interfaces
tx send tree --listen 0.0.0.0:3453 /srv/data
```

### Copy a directory

```bash
# Copy /srv/data from a remote server to a local directory
tx recv copy tx://10.0.0.1:3453/srv/data /var/lib/data
```

Rerun the same command to sync deltas — `copy` detects an existing destination
and only transfers new or changed files. Metadata verification runs by default;
add `--verify 5%data` for sampled data checks or `--verify 30s` to verify as
much as you can in a 30s budget.

### With encryption

```bash
# Server — keys are generated automatically on first run
tx send tree --listen 0.0.0.0:3453 -k /var/lib/tx/keys /srv/data

# Client — ephemeral keys by default, auto-negotiates optimal cipher
tx recv copy --encrypt auto tx://10.0.0.1:3453/srv/data /var/lib/data
```

### With authorization

Shared-secret tokens restrict who can download from the server.
Generate a token securely and share it out-of-band via invoking the
command. Treat these tokens as short lived session credentials, not
long lived keys (for long lived just use an `age` public key as
the token).

```bash
# Generate a token
TOKEN=$(head -c 16 /dev/random | xxd -p)
echo "$TOKEN"  # share this with authorized clients

# Server — only clients presenting this token are allowed
tx send tree --listen 0.0.0.0:3453 -k /var/lib/tx/keys \
  --require-auth-token "$TOKEN" /srv/data

# Client — present the token inside the encrypted AUTH blob
tx recv copy --encrypt auto -t "$TOKEN" \
  tx://10.0.0.1:3453/srv/data /var/lib/data
```

Tokens are sent inside the encrypted AUTH blob, so they are never visible on
the wire. The server validates them with constant-time comparison.

## Full CLI reference

See [docs/pub/CLI.md](docs/pub/CLI.md) for the complete flag reference
including `--mode gentle`, `--verify`, `--deadline`, progress reporting, and
the `get` / `status` subcommands. FTCP wire-format details live in
[docs/ftcp/OVERVIEW.md](docs/ftcp/OVERVIEW.md).

## Building and testing

```bash
go build ./cmd/tx
go test ./...
```
