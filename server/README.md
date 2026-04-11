tx server
=========

Fast file transfer server and client. Ported from
[jolynch/pinch](https://github.com/jolynch/pinch)'s `server/` tree.

Subcommands:

- `tx filesrv` — TCP file transfer server
- `tx filecli` — file transfer CLI client

See [filexfer/docs/CLI.md](filexfer/docs/CLI.md) and
[filexfer/docs/PROTOCOL.md](filexfer/docs/PROTOCOL.md) for usage and the
wire protocol; [filexfer/docs/FRAMING.md](filexfer/docs/FRAMING.md) and
[filexfer/docs/MANIFEST.md](filexfer/docs/MANIFEST.md) cover the framing
and manifest formats.

Build and test from the tx repo root:

```bash
go build ./...
go test ./... -race
```
