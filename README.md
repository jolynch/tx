# tx performance history

Latest: [01f4d50](https://github.com/jolynch/tx/commit/01f4d5000cb786d0394056842e45dc699db65e97) (2026-11-23).

One point per `main` merge: the median, with repeats as dots and min–max whiskers. Red and green lines mark sustained shifts. A dashed line means the benchmark changed; dotted, the machine.

## Shifts

None detected.

## Latest SSD copies

Median and range across measured copies.

| Workload | Throughput (MiB/s) | Copy time | Verification time | Most-used codec by frame count |
| --- | --- | --- | --- | --- |
| Compressible files | 2,884 (1,525–3,086) | 4.26 s (3.98–8.06) | 0.181 s (0.179–0.201) | none (100.0%) |
| Large files | 3,092 (3,082–3,109) | 13.2 s (13.2–13.3) | 6.7 s (6.69–6.75) | none (100.0%) |
| Mixed files | 1,466 (1,296–1,488) | 22.3 s (22–25.3) | 6.04 s (6.04–6.08) | none (100.0%) |
| Small files | 305 (304–308) | 6.71 s (6.64–6.74) | 0.577 s (0.561–0.587) | none (100.0%) |

## Ciphers and codecs

![Ciphers and codecs](codecs.png)

## Control path

![Control path](control-path.png)

## Handshake

![Handshake](handshake.png)

## Sender frames

![Sender frames](sender-frames.png)

## SSD copies

![SSD copies](ssd-throughput.png)

## SSD copy and verification times

![SSD copy and verification times](ssd-timings.png)

## Transfer, AES-GCM

![Transfer, AES-GCM](transfer-aes.png)

## Transfer, ChaCha20-Poly1305

![Transfer, ChaCha20-Poly1305](transfer-chacha20.png)

## Transfer, plaintext

![Transfer, plaintext](transfer-none.png)

## Verification

![Verification](verification.png)

## Reproduce the workloads

Print the commands from the measured checkout.

~~~bash
git checkout 01f4d5000cb786d0394056842e45dc699db65e97
make bench-suite-commands BENCH_SUITE=full
~~~

<details><summary>How shifts are detected</summary>

Flagged when the 8 measured commits before and after don't overlap and their medians differ by at least 5% and 4× the median absolute deviation. Needs three commits after; never spans a dashed or dotted line.

</details>

