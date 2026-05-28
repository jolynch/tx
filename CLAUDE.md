# tx — Architecture Guide

This document covers the protocol, server, client library, and testing conventions for the tx file transfer subsystem.

## Directory layout

```
./                                 # Public client library (package tx)
  client.go                        # Client type, options, request/response types, ClientMetrics
  client_tcp.go                    # TCP transport + async-refilled connection pool
  client_test.go                   # Integration tests against a real server
  docs/
    README.md                      # Docs index
    arch/
      OVERVIEW.md                  # High-level system architecture
      VERIFICATION.md              # Integrity model and deterministic sampling
    ftcp/
      OVERVIEW.md                  # FTCP line protocol (AUTH, TXFER, SEND, ACK, CXSUM, STATUS, PROBE)
      MANIFEST.md                  # FM/1 manifest wire format
      FRAMING.md                   # FX/1 frame wire format
    pub/
      CLI.md                       # CLI usage reference

cmd/tx/
  main.go                          # Binary entrypoint: send and recv subcommands
  main_test.go                     # Binary-level smoke tests

internal/filexfer/
  ftcp/                            # Server-side FTCP command handlers
    server.go                      # Listener loop and connection dispatch
    verb.go                        # Verb enum
    request.go                     # Protocol line parser (ParseRequest)
    auth.go                        # AUTH handler + age encryption + auth-token validation
    txfer.go                       # TXFER handler — manifest generation
    send.go                        # SEND handler — file streaming
    ack.go                         # ACK handler — progress acknowledgment
    cxsum.go                       # CXSUM handler — checksum streaming
    status.go                      # STATUS handler — transfer status/list
    probe.go                       # PROBE handler — latency/bandwidth probe
    sync.go                        # SYNC handler
    deps.go                        # Deps interface + runtimeDeps (thin wrapper over store)
    errors.go                      # protocolErr, writeOKLine, writeErrFrame helpers
    *_test.go                      # per-handler tests with mockDeps
  encoding/
    manifest.go                    # FM/1 marshal/parse (front-coded paths + mtimes)
    frame.go                       # FX/1 frame marshal/parse
    codec.go                       # Compression codec pools (zstd, lz4, identity)
    format.go                      # Byte/duration string parsing
  store/
    store.go                       # In-memory transfer state (global map, TTL eviction)
  policy/
    policy.go                      # Adaptive compression policy
  limit/
    limit.go                       # Rate-limited io.Writer
  progress.go                      # Background progress-file writer
  progress.go                      # Background progress-file writer

internal/cliflags/
  cliflags.go                      # Shared CLI flag helpers: Flags (short/long builder),
                                   # StringSliceFlag, ResolveProgressTargets

internal/cmd/filexfercli/
  cli.go                           # CLI commands: copy, get, status, verify
  cli_test.go                      # End-to-end CLI tests with fake TCP servers

internal/aead/
  aead.go                          # Streaming AEAD (AES-GCM, ChaCha20-Poly1305)
  token.go                         # Auth token generate/validate/redact/compare
  keyfile.go                       # Age identity file load/persist

internal/utils/
  addr.go                          # Host:port validation (IsHostPort, ValidateHostPort)
  socket.go                        # Socket tuning (SO_SNDBUF, TCP_NODELAY, etc.)
  strings.go                       # String helpers
  timeout.go                       # Timeout helpers

internal/metrics/
  metrics.go                       # ClientMetrics holder + ClientMetricsSnapshot value type

internal/bench/                    # Benchmark binary + regression benchmarks
  main.go                          # Benchmark runner entrypoint
  generate.go                      # Synthetic dataset generation
  report.go                        # Result formatting
  *_test.go                        # aead / codec pool / common-prefix / store benchmarks
```

## Protocol

Full specification lives in `docs/ftcp/OVERVIEW.md`. Key points:

- **Transport**: one TCP connection per command, server closes after completion.
- **Line format**: `VERB args...\r\n` → optional streaming payload → `OK [msg]\r\n` or `ERR <code> <msg>\r\n`.
- **AUTH**: optional first command; supports age encryption for both the command line and response stream, and carries bearer tokens validated server-side inside the encrypted blob.
- **Token encoding**: path/blob args are quoted (`"..."`) or length-prefixed (`<len>:<bytes>`).

### Command sequence for a typical download

```
PROBE cpu=<n> probe-bytes=<n> cts0=<ms>        → measure link
TXFER "<dir>" mode=fast link-mbps=<n> ...       → get FM/1 manifest stream
SEND <tid> fd=0 "<path>" [fd=1 "<path>" ...]    → receive FX/1 frame stream
ACK  <tid> fd=0 "<path>" ack-token=<tok> ...    → confirm windows received
STATUS <tid>                                    → poll progress JSON
STATUS                                          → list all active transfers (count + N JSON lines)
```

## Server internals (`internal/filexfer/`)

### `ftcp/` — command handlers

`Serve()` in `server.go` accepts connections and dispatches on the parsed verb. Each handler receives `(ctx, Request, io.Writer, Deps)`.

**`Deps` interface** (`deps.go`) abstracts all state mutations so handlers are testable without a real store. `runtimeDeps` is a thin pass-through to the `store` package. Tests construct a `mockDeps` implementing the same interface.

**`txfer.go`** handles both directory TXFER (walks the tree with `WalkDir`) and single-file TXFER (`encodeSingleFileManifest`). After writing the manifest it calls `deps.ClipTransfer` to seal the file count.

**`send.go`** is the hot path. It:
1. Opens files through `deps.GetFile` (validates transfer + path).
2. Streams windows as FX/1 frames with per-frame adaptive compression (`policy.CompressionPolicy`).
3. Uses zero-copy sendfile when available, falls back to buffered read.
4. Sets `SetTransferFileWindowHash` so subsequent ACKs can be verified.

**`status.go`** — two modes:
- `STATUS <txferid>`: returns `OK <json>` with a single `TransferStatus`.
- `STATUS` (no args): returns `OK <count>\r\n` followed by `<count>` JSON lines, one per active transfer.

Completed transfers remain in the store until TTL expiry (default 10 min), so `ListTransfers()` returns them.

### `encoding/` — wire formats

- **FM/1 manifest**: header line + one entry line per file. Paths and mtimes use front-coding (delta from previous entry) to compress the manifest without a compression codec.
- **FX/1 frames**: fixed header with file ID, compression name, byte offsets, wire size, and a checksum token. Optional trailer with aggregate metadata.
- **Codec pools**: zstd and lz4 decoder instances are pooled by `sync.Pool` to avoid per-frame allocations.

### `store/` — transfer state

A global `sync.Map`-backed store keyed by random hex transfer ID. `Transfer` holds per-file arrays (`State []uint8`, `FileSize []int64`, `AckedSize []int64`, `PathHash []xxh3.Uint128`). TTL is enforced lazily on reads.

State transitions per file: `Started → Running → Done` (or `Missing` for 404s).

`ClipTransfer` seals the file count after the manifest is written; calls before that can still update `NumFiles`.

### `policy/` — adaptive compression

`CompressionPolicy.Decide(metrics)` returns the next compression mode based on measured read vs. write latency ratios. It upgrades (to zstd) when writes are cheap relative to reads, and downgrades (to lz4 or none) when compression becomes the bottleneck.

## Client library (`package tx` at repo root)

`Client` is a config struct (not an interface). All public operations are methods on `*Client`:

| Method | Protocol command |
|--------|-----------------|
| `GetManifest` | `TXFER` |
| `GetFiles` | `SEND` + `ACK` |
| `GetStatus` | `STATUS <tid>` |
| `ListStatuses` | `STATUS` (list all) |
| `GetChecksum` | `CXSUM` |
| `ProbeLink` | `PROBE` |
| `SyncManifest` | `SYNC` |
| `StartFromManifest` | orchestrates `SEND` + `ACK` in batches |
| `AcknowledgeFileProgress` | `ACK` |
| `Close` | — (releases warmed TCP pool state) |
| `MetricSnapshot` | — (returns a `ClientMetrics` snapshot) |

The `Client` struct holds connection config (`ServerAddr`, `ServerAgePublicKey`) and client-side encryption keys (`ClientAgePublicKey`, `ClientAgeIdentity`). Age keys on the client struct are used automatically by all methods that need encryption, so request structs do not carry them.

**Auth tokens.** `ClientAuthTokens []string` + `WithClientAuthTokens(...)` attach bearer tokens that are sent inside the encrypted AUTH blob. The server validates them using helpers in `internal/aead/token.go` (`NewAuthToken`, `ValidateAuthToken`, `MatchAuthToken`, `RedactAuthToken`).

**Custom dialer.** `WithContextDialer` lets callers substitute the net.Conn source (e.g., for TLS or test harnesses). The Client also pools hot-path allocations via `bufferPool`, `lineReaderPool`, and `scratchBufferPool`.

**TCP connection pool.** `client_tcp.go` maintains an async-refilled pool (`tcpConnPool`) sized to the configured concurrency plus a 25% headroom (`warmTCPPoolTarget`). Borrowed connections are wrapped in a `managedTCPConnCloser` that returns them to the pool on close. When the pool is empty the client falls back to a synchronous `dial` and calls `clientMetrics.IncSyncConnectionFallback()`, exposed via `MetricSnapshot().SyncConnectionCount`. `readTCPStatus` reads the `OK`/`ERR` terminal line; `readTCPLine` reads an arbitrary line up to `maxTCPLineBytes`.

**Metrics.** All client-side counters live in `internal/metrics`. `metrics.ClientMetrics` is the holder (atomic counters, mutated via methods like `IncSyncConnectionFallback`); `metrics.ClientMetricsSnapshot` is the value type returned by its `Snapshot()` method. The exported `tx.ClientMetrics` is a type alias for `metrics.ClientMetricsSnapshot`, so callers see one name. `Client` embeds a `clientMetrics metrics.ClientMetrics` field; new counters are added by extending the metrics package, with no change to `Client` shape.

`TransferStatus` in `client.go` mirrors the JSON schema returned by the server's STATUS command.

## CLI

Both sides take the command first: `tx send <command> [options]` and `tx recv <command> [options]`. The server address is no longer a leading positional — `tx send tree` takes a `--listen <addr>` flag, and `tx recv copy/get` embed it in a `REMOTE_SRC` URL of the form `tx://host:port/abs/path` (host:port may be omitted to use `127.0.0.1:3453`). The `file://` scheme and bare/schemeless (local daemonless) sources are reserved but not yet implemented — `parseRemoteSrc` in `cli.go` rejects them with an instructive error.

Full flag reference with `--help` output lives in `docs/pub/CLI.md`. **Any time you change flags in either CLI or `internal/cliflags/cliflags.go`, update `docs/pub/CLI.md` to match.**

### `tx send` (`cmd/tx/main.go`)

- **`tree`**: starts the file transfer TCP server. Binds the `--listen <addr>` flag (default `127.0.0.1:3453`) and takes an optional positional `CHROOT` (defaults to cwd).

### `tx recv` (`internal/cmd/filexfercli/cli.go`)

- **`copy`**: full directory download with probes, manifest fetch, parallel SEND batches, ACK, optional verify. Writes `.tx/` state (manifest + progress file) for resume.
- **`get`**: single-file download. Skips probes and `.tx/` state.
- **`status [REMOTE_HOST] [LOCAL_DST]`**: monitors a transfer. `REMOTE_HOST` defaults to `127.0.0.1:3453`, `LOCAL_DST` to cwd (a single positional is auto-detected as host:port vs path). Reads `.tx/manifest.server.zst` from `LOCAL_DST` for the transfer ID and polls combined server+client progress; `--tid` polls server only; `--all` lists all active transfers on `REMOTE_HOST`.

### Shared flag helpers (`internal/cliflags/`)

`cliflags.Flags` wraps `flag.FlagSet` to register short/long pairs and print combined help. `StringSliceFlag` implements repeatable string flags. `ResolveProgressTargets` pairs `--progress-path` with `--progress-format` values.

## Testing

### Unit tests (per package)

Each `ftcp/` handler has a `*_test.go` with a `mockDeps` that implements `Deps`. Tests construct a fake `Request`, pass a `bytes.Buffer` as the writer, and assert the response bytes.

`request_test.go` covers the protocol parser extensively including edge cases (quoted paths, length-prefixed blobs, missing args).

### End-to-end CLI tests (`cli_test.go`)

`TestRunCLITransferAndGet` and friends spin up a real `net.Listener` in-process, implement a minimal FTCP server responding to PROBE / TXFER / SEND / ACK, and invoke `RunCLI` against it. This validates the full client+CLI stack without requiring real files.

Pattern for a test server:
```go
ln, _ := net.Listen("tcp", "127.0.0.1:0")
go func() {
    conn, _ := ln.Accept()
    // read/write raw FTCP protocol
}()
RunCLI([]string{"copy", "--server", ln.Addr().String(), ...})
```

When extending `Deps`, add the new method to `mockDeps` in `cli_test.go` (and any other test files that define their own mock) before running `go test ./...`.

### Benchmarks (`internal/bench/`)

Regression benchmarks for AEAD, codec pools, manifest common-prefix encoding, and store operations live alongside a small runner binary (`main.go`, `generate.go`, `report.go`) that generates synthetic datasets and formats results. Run with:

```sh
go test -bench=. ./internal/bench/
go run ./internal/bench                                    # runner binary
```

### Running tests

```sh
go test ./...                                              # all packages
go test ./internal/filexfer/...                            # server packages only
go test -run TestRunCLI ./internal/cmd/filexfercli/        # CLI tests
go test -bench=. ./internal/filexfer/encoding/             # codec benchmarks
go test -bench=. ./internal/bench/                         # regression benchmarks
```
