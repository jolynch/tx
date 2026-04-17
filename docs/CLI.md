# File Transfer CLI

## Quick Reference

```text
$ tx --help
usage: tx <command> [options]

Commands:
  send       File transfer server
  recv       File transfer CLI client

Run 'tx <command> --help' for command-specific options.
```

### `tx send`

```text
$ tx send --help
usage: tx send [<addr>] <command> [options]

Commands:
  serve      Start the file transfer TCP server

Default listen address: 127.0.0.1:3453
Run 'tx send <command> --help' for command-specific options.
```

#### `tx send serve`

```text
$ tx send serve --help
usage: tx send [addr] serve [options] [CHROOT]

Start the file transfer TCP server.

  addr      listen address (default "127.0.0.1:3453")
  CHROOT    server root directory (default: current working directory)

Options:
  -b, --bwlimit string             Response rate limit for gentle transfers only; fast
                                   transfers do not respect it (e.g. 100MiB, 1000mbps)
      --bwlimit-burst string       Rate limit burst size (default "1MiB")
      --gentle-cpu string          Percent of server CPUs advertised for gentle
                                   concurrency (default "25%")
      --gentle-bw string           Percent of observed link bandwidth used for gentle
                                   limiting (default "25%")
  -k, --keys string                Age keys directory (default "/var/lib/pinch/keys")
      --require-auth               Require AUTH before commands
      --require-auth-token string  Allowlisted auth token (opaque string >8 bytes,
                                   repeatable); implies --require-auth
      --target-io-depth int        Target IO depth per CPU advertised in PROBE (default 4)
      --disable-zero-copy          Force buffered send path (for benchmarking)
      --trace string               Write runtime/trace output to this file
  -p, --progress-path string       Progress output target; repeatable, use - for stdout
  -f, --progress-format string     Progress format: json|int; 1 applies to all targets,
                                   or one per target (default json)
      --progress-interval string   Progress write interval (e.g. 500ms, 10s) (default "1s")
```

### `tx recv`

```text
$ tx recv --help
usage: tx recv [<addr>] <command> [options]

Commands:
  copy       Copy REMOTE_SRC to LOCAL_DST
  status     Query and monitor transfer progress
  get        Download a single remote file

State is stored in <local-dst>/../.tx/ (manifest, progress, staging).
Default server address: 127.0.0.1:3453
Run 'tx recv <command> --help' for command-specific options.
```

#### `tx recv copy`

```text
$ tx recv copy --help
usage: tx recv [addr] copy [flags] REMOTE_SRC LOCAL_DST

Copy REMOTE_SRC from the remote to LOCAL_DST on the local machine.

Behavior:
  - If LOCAL_DST does not exist: full transfer
  - If LOCAL_DST exists: diff remote and send deltas
  - --clean removes LOCAL_DST first and forces a clean transfer
  - --skip-fetch fetches and writes manifest state only; no start/sync
  - --skip-write fetches bodies to a discard sink and never mutates LOCAL_DST
  - --verify-meta reruns read-only metadata verification after copy
  - --verify-data-sample=N implies --verify-meta and verifies N percent of data

Options:
      --clean                     Remove LOCAL_DST first, then force a clean transfer
      --skip-fetch                Fetch and persist remote manifest state only; do not
                                  start or sync files
      --skip-write                Do not mutate LOCAL_DST; fetch file bodies to discard
                                  instead of writing them
      --skip-fsync                Acknowledge writes without fdatasync
      --verify-meta               Run read-only metadata verification after copy; with
                                  --skip-fetch this is allowed only if LOCAL_DST already
                                  exists
      --verify-data-sample int    Percent of frame slots to sample per file for data
                                  verification (0-100); implies --verify-meta; not
                                  allowed with --skip-fetch or --skip-write (default 0)
      --mode string               Server read strategy: fast|gentle (default "fast")
      --encrypt string            Encryption algorithm: none|auto|aes|chacha20
                                  (default: none)
  -k, --keys string               Persistent age keys directory (default: ephemeral)
  -t, --auth-token string         Client auth token presented in encrypted AUTH blob;
                                  repeatable
      --compress string           Compression algorithm: adapt|none|lz4|zstd
                                  (default: adapt)
      --concurrency int           Parallel download / verification workers
                                  (0=adapt from server) (default 0)
      --progress                  Show transfer progress every 2s (default true)
  -v, --verbose                   Per-file progress output
  -p, --progress-path string      Progress output target; repeatable, use - for stdout
  -f, --progress-format string    Progress format: json|int; 1 applies to all targets,
                                  or one per target (default json)
      --progress-interval string  Progress write interval (e.g. 500ms, 10s) (default "1s")
  -y, --yes                       Skip confirmation prompt on sync paths
  -a, --ack-every string          Bytes between progress acks; e.g. 1B, 4KiB, 8MiB
                                  (default "128.00 MiB")
      --probe-size string         Probe payload size; e.g. 1B, 4KiB, 8MiB
                                  (default "1.00 MiB")
      --deadline string           Transfer deadline (e.g. 60s, 5m)
      --trace string              Write runtime/trace output to this file
```

#### `tx recv get`

```text
$ tx recv get --help
usage: tx recv [addr] get [flags] REMOTE_PATH

Download a single remote file. REMOTE_PATH must be an absolute path to a file
on the server. Output defaults to the file's basename in the current directory.

Options:
  -o string                       Output file path, or '-' for stdout
      --encrypt string            Encryption algorithm: none|auto|aes|chacha20
                                  (default: none)
  -k, --keys string               Persistent age keys directory (default: ephemeral)
  -t, --auth-token string         Client auth token presented in encrypted AUTH blob;
                                  repeatable
      --compress string           Compression algorithm: adapt|none|lz4|zstd
                                  (default: adapt)
      --concurrency int           Parallel download workers (0=auto) (default 0)
      --skip-write                Do not write the file; fetch to discard instead
      --skip-fsync                Acknowledge writes without fdatasync
      --progress                  Show transfer progress every 2s (default true)
  -v, --verbose                   Per-file progress output
  -p, --progress-path string      Progress output target; repeatable, use - for stdout
  -f, --progress-format string    Progress format: json|int; 1 applies to all targets,
                                  or one per target (default json)
      --progress-interval string  Progress write interval (e.g. 500ms, 10s) (default "1s")
  -a, --ack-every string          Bytes between progress acks; e.g. 1B, 4KiB, 8MiB
                                  (default "128.00 MiB")
      --deadline string           Transfer deadline (e.g. 60s, 5m)
      --trace string              Write runtime/trace output to this file
```

#### `tx recv status`

```text
$ tx recv status --help
usage: tx recv [addr] status [--tid <id>] [LOCAL_DST]

Query and monitor transfer progress.

Modes:
  status LOCAL_DST       Discover transfer from .tx/ state and poll until complete
  status --tid <id>      Poll a transfer by ID (server-side progress only)
  status                 List all active transfers on the server

Options:
      --tid string         Transfer ID
      --encrypt string     Encryption algorithm: none|auto|aes|chacha20 (default: none)
  -k, --keys string        Persistent age keys directory (default: ephemeral)
  -t, --auth-token string  Client auth token presented in encrypted AUTH blob; repeatable
```

---

## Detailed Behavior

### Copy Workflow

`copy` is the main entry point for directory transfers.

Behavior:

- if `LOCAL_DST` does not exist, `copy` performs a full transfer into a staging
  directory and then renames it into place
- if `LOCAL_DST` already exists, `copy` switches to sync mode and applies the
  delta needed to converge the local tree to the remote tree
- successful non-`--skip-fetch` runs clean up `.tx` after they finish

Common examples:

```bash
tx recv copy /srv/data /var/lib/pinch/data
tx recv copy --clean /srv/data /var/lib/pinch/data
tx recv copy --mode gentle /srv/data /var/lib/pinch/data
tx recv copy --verify-meta /srv/data /var/lib/pinch/data
tx recv copy --verify-data-sample 5 /srv/data /var/lib/pinch/data
tx recv copy --deadline 30m /srv/data /var/lib/pinch/data
```

### Convergence Workflow

`copy` is designed so you can rerun the same command until the local tree is
fully consistent with the remote tree.

Typical pattern:

1. Run a bounded first pass:

   ```bash
   tx recv copy --deadline 30m --mode gentle /srv/data /var/lib/pinch/data
   ```

2. Run the same command again in fast mode to get deltas:

   ```bash
   tx recv copy /srv/data /var/lib/pinch/data
   ```

3. Keep rerunning until the sync phase reports:

   ```text
   sync: remote and local converged, nothing to do
   ```

Why this works:

- the first run creates `LOCAL_DST`
- later runs see an existing destination and take the sync path instead of the
  clean start path
- sync only downloads new or stale files and removes remote-missing files when
  needed

If you want an explicit final check, add `--verify-meta` to the last run.

### Send Server Examples

```bash
tx send serve                              # serve cwd on default addr
tx send serve /srv/data                    # serve /srv/data
tx send 0.0.0.0:4000 serve /srv/data      # custom addr + chroot
tx send serve --require-auth /srv/data     # auto-generate token
tx send serve --bwlimit 100MiB /srv/data   # rate limit gentle transfers
```

### Transfer Strategies: `fast` and `gentle`

The transfer layer supports two load strategies:

- `fast`: maximize throughput and finish as quickly as possible
- `gentle`: reduce pressure on the source side and trade peak throughput for
  lower impact

Use `fast` when:

- the source host is dedicated to the transfer
- you want the shortest wall-clock time
- transient read or CPU pressure is acceptable

Use `gentle` when:

- the source host is shared with other work
- you want a lower-impact first pass
- you expect to converge over multiple runs instead of finishing in one shot

Both `copy` (`--mode fast|gentle`) and the lower-level transfer phase support
strategy selection.

## State Directory

State is stored in `<LOCAL_DST>/../.tx/`:

- `manifest.server`: the last remote manifest snapshot
- `manifest`: the local manifest after a successful write
- `manifest.progress`: resumable progress state
- `remote/`: start-phase staging directory

## Notes

- `REMOTE_SRC` must be an absolute path on the remote server
- `LOCAL_DST` is local filesystem state on the client machine
- human-readable byte sizes are accepted for flags such as `--ack-every`,
  `--probe-size`, `--bwlimit`
- a successful sync prompt is skipped automatically when no local mutations are
  needed
