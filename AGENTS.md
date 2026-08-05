# tx — Agent Guide

Canonical working rules and architecture summary. `CLAUDE.md` points here.

## Documentation map

Start at the [docs index](docs/README.md), then open only the reference needed:

- [Architecture](docs/arch/OVERVIEW.md): concurrency, connection pooling,
  compression, verification, and durability.
- [Transfer modes](docs/arch/TRANSFER.md): fast/gentle copy and sync behavior.
- [Verification](docs/arch/VERIFICATION.md): metadata/data integrity, sampling,
  ACK validation, and failure semantics.
- [FTCP protocol](docs/ftcp/OVERVIEW.md): transport, AUTH, commands, and responses.
- [FM/1 manifests](docs/ftcp/MANIFEST.md) and
  [FX/1 frames](docs/ftcp/FRAMING.md): exact wire formats and parsing rules.
- [CLI reference](docs/pub/CLI.md): commands, flags, workflows, and `.tx/` state.

## Code standards

### Testing

- Prefer Go fuzz tests (`FuzzX`) for invariants and input variation; use unit
  tests for awkward properties, known regressions, exact protocol errors,
  timing/expiry, and goroutine lifecycle. Models: `FuzzCommonPrefixLen` in
  `internal/utils/strings_test.go` and `FuzzSync` in
  `internal/filexfer/ftcp/sync_test.go`.
- **Register every fuzz test in the Makefile, in `fuzz-short` or `fuzz-long`.**
  Verify its package path and confirm `go test -fuzz=X` reports a real `execs:`
  count; unmatched targets exit successfully without fuzzing.
  - `fuzz-short` — property tests standing in for unit tests: one function or
    invariant over a small input space. Everything interesting turns up in the
    first few seconds. Models: `FuzzCommonPrefixLen`, `FuzzRoundTrip`.
  - `fuzz-long` — end-to-end properties driving the whole system, which keep
    reaching new states the longer they run. Model: `FuzzSync`, which walks
    TXFER → modify → SYNC and checks the result against the real filesystem.
  - Classify a new fuzz test by measuring, not guessing. Probe it for 10s:
    `go test -run=^$ -fuzz=FuzzX -fuzztime=10s ./that/pkg`, and watch
    `new interesting:`. Still finding new coverage at 10s → `fuzz-long`, where
    CI gives it 30s to keep exploring. Saturated well before 10s → leave it in
    `fuzz-short` at 5s, since more time buys nothing. Re-probe when a test's
    scope changes.
  - The tiers are a budget, not a ranking. Fuzzing exists to discover
    interesting behavior, not to finish quickly: a test that keeps finding new
    states is the better test. Never narrow one to make it fit `fuzz-short`.
- Cover key flows with real-listener integration tests. Models:
  `client_test.go` and `internal/cmd/filexfercli/cli_test.go`.
- Add tests to an existing package test file rather than opening a new one per
  feature; a module should not accumulate a dozen small `x_feature_test.go`
  files. Past ~1000 lines, first consolidate overlapping cases into fuzz tests.
  If it is still oversized, split along functional boundaries — one file per
  coherent area of behavior — never one per feature.
- Keep one shared fake per package. FTCP uses `mockDeps` in
  `internal/filexfer/ftcp/mockdeps_test.go`; update it whenever `Deps` changes.

### Go and repository conventions

- Completion requires clean `gofmt`, `go vet ./...`, and `go test ./...`; run
  `-race` for packages involving goroutines.
- Prefer option structs or functional options to long positional argument
  lists, especially repeated types.
- Keep unrelated constants out of `iota` blocks to avoid shifting values.
- Keep source files below ~1000 lines; split by responsibility.
- Mirror every CLI flag change in the [CLI reference](docs/pub/CLI.md).
- Update this architecture summary when layout, protocol, or package ownership
  changes.

```sh
make test         # build + unit + acceptance
make unit         # go test -race ./...
make fuzz-short   # unit-test replacements; 5s each
make fuzz-long    # whole-system properties; 30s each
make acceptance   # fuzz-short, then fuzz-long
make bench        # regression benchmarks + report

go test ./...
go test ./internal/filexfer/...
go test -run TestRunCLI ./internal/cmd/filexfercli/
go test -bench=. ./internal/bench/
go run ./internal/bench
```

## Architecture

### Map

- Root package (`client.go`, `client_tcp.go`): public `tx.Client`, requests,
  responses, metrics, TCP transport, and keep-alive pool.
- `cmd/tx`: binary entry point for `send` and `recv`.
- `internal/cmd/filexfercli`: recv-side copy/get/status orchestration, remote
  transfers, resume/sync, and daemonless local copies.
- `internal/cliflags`: shared flags, short/long aliases, repeatable strings,
  and progress target resolution.
- `internal/filexfer/ftcp`: server, parser, FTCP handlers, and `Deps` boundary.
- `internal/filexfer/encoding`: FM/1 manifests, FX/1 frames, codecs, formats,
  page-cache hints, and shared transfer types.
- `internal/filexfer/store`: transfer state and TTL reaping; `interface.go`
  holds the `Interface` contract consumers depend on.
- `internal/filexfer/{policy,limit}`: adaptive compression and rate limiting.
- `internal/{aead,pagecache,fsync,sampler,utils,metrics}`: encryption/auth,
  cache restore, fsync batching, sampling, network/string helpers, and metrics.
- `internal/bench`: benchmark runner and regression benchmarks.
- The [documentation map](#documentation-map) links the authoritative design,
  wire-format, and CLI references.

### Protocol

FTCP is a line protocol documented in the
[protocol reference](docs/ftcp/OVERVIEW.md):

```text
VERB args...\r\n -> optional stream -> OK [msg]\r\n | ERR <code> <msg>\r\n
PROBE -> TXFER (FM/1 manifest) -> SEND (FX/1 frames) -> ACK -> STATUS
```

- Paths/blobs are quoted or length-prefixed (`<len>:<bytes>`).
- Optional first-command `AUTH` age-encrypts request/response data and carries
  bearer tokens inside the encrypted blob.
- Connections are normally single-command. `PROBE keep-alive=auto` can grant
  `keep-alive-ms`; the client then pools sessions, heartbeats at one quarter of
  the window, checks pooled connections before reuse, and evicts dead ones.
  Server idle timeout defaults to 60s (`0` disables); heartbeats do not count
  toward `--exit-after` activity.

### Server and state

`ftcp.Serve` dispatches parsed requests to handlers shaped like
`(ctx, Request, io.Writer, Deps)`. `Deps` embeds `store.Interface` and adds the
two concerns that are not the store's business (`Root`,
`EnqueueCacheRestoreBatch`); `runtimeDeps` embeds the store, so store methods
are promoted rather than forwarded. Build it with
`NewRuntimeDeps(st, WithRoot(...), WithPool(...))` — the store is a required
argument because there is no process-wide fallback. `Serve` creates and closes
one when `ServerOptions.Deps` is nil, exactly as it does the restore pool.

- `TXFER` emits a directory or single-file manifest, then `ClipTransfer` seals
  the file count.
- `SEND` validates files through `Deps`, streams adaptively compressed FX/1
  windows (sendfile when possible), and records window hashes for ACK checks.
- `STATUS <tid>` returns one JSON status; bare `STATUS` returns a count followed
  by that many JSON lines. Completed transfers remain listed until TTL expiry.
- FM/1 front-codes paths/mtimes. FX/1 frames carry file ID, codec, offsets,
  sizes, and checksum; zstd/lz4 decoders are pooled.
- `CompressionPolicy` selects zstd, lz4, or identity from read/write latency.

`Store` is an RWMutex-protected transfer map with **no process-wide instance**:
whoever runs a server owns one and closes it. Per-file state is
`Started -> Running -> Done` (or `Missing` for 404). `NewStore` owns a reap
goroutine and **must be closed**. TTL expiry happens only in that goroutine,
not lazily during reads; `WithTTL` shortens the window so tests can reach it.
`store.Interface` is the consumer contract — methods on `*Store` outside it
have no production caller and exist for the store's own tests or benchmarks.

### Client

`Client` is a config struct, not an interface. Principal mappings:

| Method | FTCP operation |
|---|---|
| `GetManifest` | `TXFER` |
| `GetFiles` / `AcknowledgeFileProgress` | `SEND` / `ACK` |
| `GetStatus` / `ListStatuses` | `STATUS <tid>` / `STATUS` |
| `GetChecksum`, `ProbeLink`, `SyncManifest` | `CXSUM`, `PROBE`, `SYNC` |
| `StartFromManifest` | batched `SEND` + `ACK` orchestration |
| `Close`, `MetricSnapshot` | pool release, metrics snapshot |

Client age keys apply automatically. `WithClientAuthTokens` sends bearer
tokens inside AUTH; `WithContextDialer` supplies custom connections. The TCP
pool refills asynchronously to concurrency +25%; without keep-alive or when
empty, connections are single-use and synchronous fallbacks increment
`SyncConnectionCount`. Add counters in `internal/metrics`; exported
`tx.ClientMetrics` aliases the snapshot type. `TransferStatus` mirrors STATUS
JSON.

### CLI

- `tx send tree [--listen addr] [CHROOT]` starts the server. Listen defaults to
  `127.0.0.1:3453`; chroot defaults to cwd.
- `tx recv copy REMOTE_SRC DST` downloads directories with probe, manifest,
  parallel SEND/ACK, optional verification, and resumable `.tx/` state.
- `tx recv get REMOTE_SRC DST` downloads one file without probe or `.tx/` state.
- Remote sources use `tx://[host:port]/abs/path`; omitted host uses the default.
  `file://`, absolute, and relative local sources use daemonless
  `copy_file_range`: no `.tx/`, fast mode only. New directory destinations are
  staged then renamed; existing ones receive an in-place delta. Additions
  proceed automatically; overwrite/removal prompts unless `-y`.
- `tx recv status [REMOTE_HOST] [LOCAL_DST]` reads the local server manifest and
  combines server/client progress. `--tid` is server-only; `--all` lists active
  transfers. Host and destination have the defaults above and cwd respectively.

### Test-specific guidance

- FTCP handler tests pass a fake `Request`, `bytes.Buffer`, and shared
  `mockDeps`. Use `realDeps` whenever asserting store state (including ACK hash
  validation or server-loop behavior), not merely recorded calls.
- CLI end-to-end tests run `RunCLI` against a minimal in-process
  `net.Listener` FTCP server; extend that pattern for complete flows.
- Benchmarks cover AEAD, codec pools, manifest prefix encoding, and store
  operations under `internal/bench`.
