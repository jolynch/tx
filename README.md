<picture>
  <source media="(prefers-color-scheme: dark)" srcset="imgs/logo_dark_2048w.png">
  <source media="(prefers-color-scheme: light)" srcset="imgs/logo_light_2048w.png">
  <img alt="tx logo: two boxes connected by a transfer arrow" src="imgs/logo_light_2048w.png" width="100%">
</picture>

# `tx`: Fast file transfer for the cloud

`tx` copies directories and files between Linux servers in the cloud at line
rate by fully saturating modern cloud hardware - SSD, NIC, and CPU -
simultaneously. On large servers it achieves up to 10x the throughput of a simple `rsync --fsync` + [`happycache`](https://github.com/hashbrowncipher/happycache) while providing above line-rate encryption, compression,
and page cache transfer.

Four capabilities make this possible:

- **Adaptive concurrency.** A probe measures link speed and server capacity,
  then opens a pool of pre-authenticated TCP connections sized to keep every
  core and disk busy. Work is packed into fixed-size batches - many small files
  are bundled into one request, and large files are split into parallel
  chunks - so every connection does roughly equal work with no head-of-line
  blocking.

- **Adaptive compression.** The server measures each 4 MiB frame's throughput
  and samples lz4 and zstd level 1 on bounded background workers. It switches
  after sustained evidence, validates the result on normal frames, and skips
  sampling when a file has too little work left. Upward comparisons stay off
  the wire; only bounded checks of cheaper codecs use real frames.

- **Background durability.** Per-file `fdatasync` is kept off the download
  critical path in a background batcher that deduplicates by inode. It
  starts with a 512 MiB flush target and grows the batch size under backlog
  pressure. A final `syncfs` backstops the whole filesystem, and a failed
  `fdatasync` fails the copy instead of reporting success.

- **Lightweight verification.** Every transferred window is hashed, and the
  receiver's acknowledgment verifies the hash. Metadata checks (size, mtime,
  permissions) run by default after every copy. Sampled data verification
  (`--verify 5%data`) reads a ~5% sample of bytes, or a time budget
  (`--verify 30s`) checks as much data as it can in that time, which is
  practical on network-attached drives such as EBS. A file that fails
  verification or its `fdatasync` is fetched again on the next run.
  Fault-injection tests in CI drive whole transfers through network, disk,
  and syscall failures to check that a copy ends with the right bytes or
  fails loudly.

`tx` builds and runs only on Linux. These optimizations use Linux-only system
calls: zero-copy sends (`splice`, `tee`), read-ahead hints (`posix_fadvise`,
`readahead`), background durability (`fdatasync`, `syncfs`), page cache
transfer (`mincore`), local copies (`copy_file_range`), and the TCP delivery
rate that adaptive compression reads from `TCP_INFO`.

See [docs/README.md](docs/README.md) for the documentation index and
[docs/arch/OVERVIEW.md](docs/arch/OVERVIEW.md) for a deeper dive.
[docs/arch/CLOUD.md](docs/arch/CLOUD.md) shows why adapting matters when
cloud bottlenecks move mid-copy. If you are curious about benchmarking on
your own setup check out
[bench/README.md](bench/README.md) which has a data generator that can both
generate incompressible (random) and
compressible ([Silesia](https://sun.aei.polsl.pl/~sdeor/index.php?page=silesia))
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
# Server — require encryption and proof of the client's private key
tx send tree --require-auth --listen 0.0.0.0:3453 /srv/data

# Client — generates an in-memory key
tx recv copy --encrypt auto tx://10.0.0.1:3453/srv/data /var/lib/data
```

This accepts any client with a key pair. For a long-running server with a known
age public key, the client can also authenticate the server with
`--require-server-recipient age1...`. Obtain that key through a trusted channel.
Each connection runs one Noise handshake, then encrypts every command and
response. Pooled connections reuse the session.

### With a token from the environment

Allowlist entries imply `--require-auth` and restrict which clients can connect.

```bash
# The token is secret: don't log it! Share it with the client out of band.
export TX_AUTH_TOKEN=$(openssl rand -hex 32)

# Server
tx send tree --require-auth-token "$TX_AUTH_TOKEN" --listen 0.0.0.0:3453 /srv/data

# Client — reads TX_AUTH_TOKEN automatically; no key files needed
tx recv copy tx://10.0.0.1:3453/srv/data /var/lib/data
```

### With a token file

```bash
# Generate a secret token; copy the file to the client securely
umask 077
openssl rand -hex 32 > client.token

# Server
tx send tree --require-auth-token-file client.token --listen 0.0.0.0:3453 /srv/data

# Client
tx recv copy --auth-token-file client.token tx://10.0.0.1:3453/srv/data /var/lib/data
```

Tokens never cross the wire: each side proves it holds the token inside the
handshake. Use one token per client so each can be revoked alone.

### With a generated age key

```bash
# Client — generate a private key and share only its public recipient
mkdir -p client-keys
age-keygen -o client-keys/key
age-keygen -y client-keys/key

# Server — allow the recipient printed above
tx send tree --require-client-recipient age1... --listen 0.0.0.0:3453 /srv/data

# Client — proves it holds the corresponding private key
tx recv copy --keys ./client-keys tx://10.0.0.1:3453/srv/data /var/lib/data
```

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
