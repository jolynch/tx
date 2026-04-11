# tx - Copy directories and files at line rate in the Cloud

Modern cloud servers have capability far exceeding what single threaded and
single-tcp connection tools can achieve. In order to fully saturate cloud hardware you
need:

1. Auto-tuning concurrency > #cores
2. Multiple TCP connections > #cores
3. Adaptive bottleneck detection.

Fundamentally `tx` is like `rsync`, except it fully saturates modern cloud hardware, achieving up to 10x performance over rsync on large servers. This performance is
achieved by using:

* [Fast adaptive data algorithms](https://jolynch.github.io/posts/use_fast_data_algorithms/) -
  per 4MiB frame adaptation between no/lz4/zstd compression and only xxh3
  checksums which proceed at `GiB/s`.
* [Zero copy streaming](./docs/FRAMING.md) when possible (when not encrypting),
* [Optimized crypto](./internal/aead/aead.go) which proceeds at 2-4 `GiB/s`
  (when encrypting)
* A [_highly_ parallelized TCP protocol](./docs/PROTOCOL.md) which sidesteps head-of-line blocking and keeps
  disks busy on both ends. 

## Using the Tool

### Hosting a Dataset

TODO

### Pulling a Dataset

TODO

## Building and Testing

