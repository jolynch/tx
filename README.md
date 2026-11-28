# tx performance history

Latest: [c9ce3f6](https://github.com/jolynch/tx/commit/c9ce3f61ee2333d4aba5f1af6d3c143a39912ca6) (2026-11-27).

One point per `main` merge: the median, with repeats as dots and min–max whiskers. Red and green lines mark sustained shifts. A dotted line means the machine changed. A changed benchmark starts a new series, so only its current definition is plotted.

## Shifts

| Commits | Worse | Better |
|---|---|---|
| [e950e02…f3baa70](https://github.com/jolynch/tx/compare/e950e02191b33e21aea343138a4e22df4c6ceb0a...f3baa709d02e5dd64216f71138fd674a87df2375) | -9% [TransferInMemoryCompressed/adapt/16MiB](#compressed-transfer) MiB/s | +142% [AppendCompressedFrame/zstd/auto](#ciphers-and-codecs) MiB/s<br>-81% [AppendCompressedFrame/zstd/auto](#ciphers-and-codecs) allocs/op<br>-74% [TransferInMemoryCompressedFew/zstd](#compressed-transfer) allocs/op<br>+1 more |
| [b85f279…e950e02](https://github.com/jolynch/tx/compare/b85f279acae5f7268e10a2eb3a08603afee646c7...e950e02191b33e21aea343138a4e22df4c6ceb0a) |  | +45% [WrapDecompressedReader/zstd](#ciphers-and-codecs) MiB/s<br>+17% [TransferInMemoryCompressed/zstd/1MiB](#compressed-transfer) MiB/s<br>-16% [TransferInMemoryCompressed/zstd/1MiB](#compressed-transfer) core-s/GiB<br>+3 more |
| [aeb3456…e41b895](https://github.com/jolynch/tx/compare/aeb3456be5c1c8744cfee8437b6a7e18c0650d73...e41b89511b79e11f6e37f787f1bd0a2a8983b4c5) | +5% [ServePlaintextACKBody](#control-path) B/op |  |
| [1dc72fa…aeb3456](https://github.com/jolynch/tx/compare/1dc72fa086e85cdfe39c6359bd1ce4463800e7db...aeb3456be5c1c8744cfee8437b6a7e18c0650d73) |  | +297% [WrapDecompressedReader/lz4](#ciphers-and-codecs) MiB/s<br>-33% [WrapDecompressedReader/lz4](#ciphers-and-codecs) allocs/op |

## End-to-End Copy Suites

Median and range across measured copies.

| Workload | Throughput (MiB/s) | Copy time | Verification time | Most-used codec by frame count |
| --- | --- | --- | --- | --- |
| Compressible files | 65.5 (62.6–66.1) | 188 s (186–196) | 0.184 s (0.175–0.185) | none (64.8%) |
| Large files | 3,100 (3,085–3,118) | 13.2 s (13.1–13.3) | 6.82 s (6.78–6.84) | none (100.0%) |
| Mixed files | 1,412 (1,407–1,437) | 23.2 s (22.8–23.3) | 5.45 s (4.9–6.09) | none (100.0%) |
| Small files | 308 (306–308) | 6.65 s (6.64–6.7) | 0.567 s (0.551–0.577) | none (100.0%) |

## End-to-End Copy Suites, throughput

![End-to-End Copy Suites, throughput](ssd-throughput.png)

<details><summary>Commands</summary>

~~~bash
git checkout c9ce3f61ee2333d4aba5f1af6d3c143a39912ca6
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
git checkout c9ce3f61ee2333d4aba5f1af6d3c143a39912ca6

# transfer
TX_BENCH_THROUGHPUT_MIN_WALL=5s make bench-throughput THROUGHPUT_SIZE=256MiB THROUGHPUT_CPUS=4,16 THROUGHPUT_ENCRYPT=none,aes,chacha20
~~~

</details>

## Ciphers and codecs

![Ciphers and codecs](codecs.png)

<details><summary>Commands</summary>

~~~bash
git checkout c9ce3f61ee2333d4aba5f1af6d3c143a39912ca6

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
git checkout c9ce3f61ee2333d4aba5f1af6d3c143a39912ca6

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
git checkout c9ce3f61ee2333d4aba5f1af6d3c143a39912ca6

# handshake
go test '-run=^$' -cpu=8 -benchmem -benchtime=1s -count=3 '-bench=^BenchmarkHandshake$/^(token|age)$/^(aes|chacha20)$' ./internal/aead
~~~

</details>

## Sender frames

![Sender frames](sender-frames.png)

<details><summary>Commands</summary>

~~~bash
git checkout c9ce3f61ee2333d4aba5f1af6d3c143a39912ca6

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
git checkout c9ce3f61ee2333d4aba5f1af6d3c143a39912ca6

# verify
TX_VERIFY_BYTES=268435456 TX_VERIFY_CONCURRENCY=4 TX_VERIFY_IO_DEPTH=4 TX_VERIFY_IO_LATENCY='' go test '-run=^$' -cpu=8 -benchmem -benchtime=1s -count=3 '-bench=^BenchmarkVerifyInMemory$/^(4KiB|1MiB|16MiB)$/^cpus=8$/^c=4$' ./internal/cmd/filexfercli
~~~

</details>

## Reproduce the workloads

Print the commands from the measured checkout.

~~~bash
git checkout c9ce3f61ee2333d4aba5f1af6d3c143a39912ca6
make bench-suite-commands BENCH_SUITE=full
~~~

<details><summary>How shifts are detected</summary>

Flagged when the 8 measured commits before and after don't overlap and their medians differ by at least 5% and 4× the median absolute deviation. Needs three commits after; never spans a dotted line.

</details>

