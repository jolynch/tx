# tx performance history

Latest: [e694c0c](https://github.com/jolynch/tx/commit/e694c0c5d4f1d6a0a0f3e3f2c653d58224ad3acb) (2026-11-26).

One point per `main` merge: the median, with repeats as dots and min–max whiskers. Red and green lines mark sustained shifts. A dashed line means the benchmark changed; dotted, the machine.

## Shifts

| Commits | Worse | Better |
|---|---|---|
| [aeb3456…e41b895](https://github.com/jolynch/tx/compare/aeb3456be5c1c8744cfee8437b6a7e18c0650d73...e41b89511b79e11f6e37f787f1bd0a2a8983b4c5) |  | -6% [Handshake/token/chacha20](#handshake) time/op |
| [1dc72fa…aeb3456](https://github.com/jolynch/tx/compare/1dc72fa086e85cdfe39c6359bd1ce4463800e7db...aeb3456be5c1c8744cfee8437b6a7e18c0650d73) | +9% [RequestRecords/long-paths](#control-path) B/op | +297% [WrapDecompressedReader/lz4](#ciphers-and-codecs) MiB/s<br>-33% [WrapDecompressedReader/lz4](#ciphers-and-codecs) allocs/op |

## End-to-End Copy Suites

Median and range across measured copies.

| Workload | Throughput (MiB/s) | Copy time | Verification time | Most-used codec by frame count |
| --- | --- | --- | --- | --- |
| Compressible files | 71.6 (57.4–73.9) | 172 s (166–214) | 0.174 s (0.174–0.178) | none (61.2%) |
| Large files | 3,116 (3,111–3,138) | 13.1 s (13.1–13.2) | 6.83 s (6.8–6.87) | none (100.0%) |
| Mixed files | 1,494 (1,287–1,512) | 21.9 s (21.7–25.5) | 5.84 s (5.65–5.99) | none (100.0%) |
| Small files | 308 (305–310) | 6.64 s (6.61–6.7) | 0.55 s (0.547–0.57) | none (100.0%) |

## End-to-End Copy Suites, throughput

![End-to-End Copy Suites, throughput](ssd-throughput.png)

<details><summary>Commands</summary>

~~~bash
git checkout e694c0c5d4f1d6a0a0f3e3f2c653d58224ad3acb
GOFLAGS= GOWORK=off go build -o ./tx ./cmd/tx
GOFLAGS= GOWORK=off go build -o ./tx-bench ./cmd/tx-bench
DATA=/path/on/the/drive/under/test  # remove the -dst trees between runs

# ssd-compressible: Compressible files
./tx-bench prep --size 12GiB --mix silesia=100%@128MiB..1GiB --seed 1 --check "$DATA/ssd-compressible-src"
./tx-bench local --tx ./tx --auth off --cache-warm 0% --require-cold-cache 10% --expect 97fe88a9a8b1a364 --oracle full -w 0 -n 3 --send-arg=--bwlimit --send-arg=50MiB "$DATA/ssd-compressible-src" "$DATA/ssd-compressible-dst"

# ssd-large: Large files
./tx-bench prep --size 40GiB --mix rand=100%@2GiB --seed 1 --check "$DATA/ssd-large-src"
./tx-bench local --tx ./tx --auth off --cache-warm 0% --require-cold-cache 10% --expect 1a2eca0ea833af19 --oracle full -w 0 -n 3 "$DATA/ssd-large-src" "$DATA/ssd-large-dst"

# ssd-mixed: Mixed files
./tx-bench prep --size 32GiB --mix rand=25%@4KiB..256KiB,rand=25%@1GiB,silesia=50%@128MiB..1GiB --seed 1 --check "$DATA/ssd-mixed-src"
./tx-bench local --tx ./tx --auth off --cache-warm 0% --require-cold-cache 10% --expect 641292361748a31d --oracle full -w 0 -n 3 "$DATA/ssd-mixed-src" "$DATA/ssd-mixed-dst"

# ssd-small: Small files
./tx-bench prep --size 2GiB --mix rand=100%@16KiB --seed 1 --check "$DATA/ssd-small-src"
./tx-bench local --tx ./tx --auth off --cache-warm 0% --require-cold-cache 10% --expect 44c4cc0c5b8e9dc2 --oracle full -w 0 -n 3 "$DATA/ssd-small-src" "$DATA/ssd-small-dst"
~~~

</details>

## End-to-End Copy Suites, copy and verification times

![End-to-End Copy Suites, copy and verification times](ssd-timings.png)

Same commands as [End-to-End Copy Suites, throughput](#end-to-end-copy-suites-throughput).

## Transfer

![Transfer](transfer-throughput.png)
![Transfer](transfer-cpu.png)
![Transfer](transfer-ratio.png)

<details><summary>Commands</summary>

~~~bash
git checkout e694c0c5d4f1d6a0a0f3e3f2c653d58224ad3acb

# transfer
TX_BENCH_THROUGHPUT_MIN_WALL=5s make bench-throughput THROUGHPUT_SIZE=256MiB THROUGHPUT_CPUS=4,16 THROUGHPUT_ENCRYPT=none,aes,chacha20
~~~

</details>

## Ciphers and codecs

![Ciphers and codecs](codecs.png)

<details><summary>Commands</summary>

~~~bash
git checkout e694c0c5d4f1d6a0a0f3e3f2c653d58224ad3acb

# cipher
go test '-run=^$' -cpu=8 -benchmem -benchtime=1s -count=3 '-bench=^Benchmark(Encrypt|Decrypt)Throughput$/^1MiB$/^(aesgcm|chacha20poly1305)$' ./internal/aead

# codec
go test '-run=^$' -cpu=8 -benchmem -benchtime=1s -count=3 '-bench=^BenchmarkWrapDecompressedReader$/^(zstd|lz4)$' ./internal/bench

# frame-codec
go test '-run=^$' -cpu=8 -benchmem -benchtime=1s -count=3 '-bench=^BenchmarkAppendCompressedFrame$/^(lz4|zstd)$/^(c1|auto)$' ./internal/bench
~~~

</details>

## Control path

![Control path](control-path.png)

<details><summary>Commands</summary>

~~~bash
git checkout e694c0c5d4f1d6a0a0f3e3f2c653d58224ad3acb

# ack
go test '-run=^$' -cpu=8 -benchmem -benchtime=1s -count=3 '-bench=^BenchmarkServePlaintextACKBody$' ./internal/filexfer/ftcp

# ack-store
go test '-run=^$' -cpu=8 -benchmem -benchtime=1s -count=3 '-bench=^BenchmarkTransferStoreAckBatch$' ./internal/bench

# request
go test '-run=^$' -cpu=8 -benchmem -benchtime=1s -count=3 '-bench=^BenchmarkRequestRecords$/^(short-paths|long-paths)$' ./internal/filexfer/ftcp

# split
go test '-run=^$' -cpu=8 -benchmem -benchtime=1s -count=3 '-bench=^BenchmarkSplitEncodedRequests$/^(short-paths|long-paths)$' .
~~~

</details>

## Handshake

![Handshake](handshake.png)

<details><summary>Commands</summary>

~~~bash
git checkout e694c0c5d4f1d6a0a0f3e3f2c653d58224ad3acb

# handshake
go test '-run=^$' -cpu=8 -benchmem -benchtime=1s -count=3 '-bench=^BenchmarkHandshake$/^(token|age)$/^(aes|chacha20)$' ./internal/aead
~~~

</details>

## Sender frames

![Sender frames](sender-frames.png)

<details><summary>Commands</summary>

~~~bash
git checkout e694c0c5d4f1d6a0a0f3e3f2c653d58224ad3acb

# frames
go test '-run=^$' -cpu=8 -benchmem -benchtime=1s -count=3 '-bench=^BenchmarkSendFramePayload$/^(zerocopy|buffered)$/^(4KiB|1MiB)$' ./internal/filexfer/ftcp
~~~

</details>

## Compressed transfer

![Compressed transfer](transfer-compressed.png)

Commands for transfer-compressed were not recorded; see [Reproduce the workloads](#reproduce-the-workloads).

## Verification

![Verification](verification.png)

<details><summary>Commands</summary>

~~~bash
git checkout e694c0c5d4f1d6a0a0f3e3f2c653d58224ad3acb

# verify
TX_VERIFY_BYTES=268435456 TX_VERIFY_CONCURRENCY=4 TX_VERIFY_IO_DEPTH=4 TX_VERIFY_IO_LATENCY='' go test '-run=^$' -cpu=8 -benchmem -benchtime=1s -count=3 '-bench=^BenchmarkVerifyInMemory$/^(4KiB|1MiB|16MiB)$/^cpus=8$/^c=4$' ./internal/cmd/filexfercli
~~~

</details>

## Reproduce the workloads

Print the commands from the measured checkout.

~~~bash
git checkout e694c0c5d4f1d6a0a0f3e3f2c653d58224ad3acb
make bench-suite-commands BENCH_SUITE=full
~~~

<details><summary>How shifts are detected</summary>

Flagged when the 8 measured commits before and after don't overlap and their medians differ by at least 5% and 4× the median absolute deviation. Needs three commits after; never spans a dashed or dotted line.

</details>

Not measured: b85f279.

