# tx performance history

Latest: [54b716c](https://github.com/jolynch/tx/commit/54b716c330483d1d8c007e7c6483d1db04998ffb) (2026-11-23).

One point per `main` merge: the median, with repeats as dots and min–max whiskers. Red and green lines mark sustained shifts. A dashed line means the benchmark changed; dotted, the machine.

## Shifts

None detected.

## Latest SSD copies

Median and range across measured copies.

| Workload | Throughput (MiB/s) | Copy time | Verification time | Most-used codec by frame count |
| --- | --- | --- | --- | --- |
| Compressible files | 1,964 (1,617–3,075) | 6.26 s (4–7.6) | 0.185 s (0.171–0.191) | none (100.0%) |
| Large files | 3,154 (3,097–3,175) | 13 s (12.9–13.2) | 6.86 s (6.82–6.88) | none (100.0%) |
| Mixed files | 1,472 (1,272–1,479) | 22.3 s (22.2–25.8) | 5.98 s (5.97–6) | none (100.0%) |
| Small files | 302 (302–307) | 6.78 s (6.67–6.79) | 0.577 s (0.566–0.583) | none (100.0%) |

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
git checkout 54b716c330483d1d8c007e7c6483d1db04998ffb
make bench-suite-commands BENCH_SUITE=full
~~~

<details><summary>How shifts are detected</summary>

Flagged when the 8 measured commits before and after don't overlap and their medians differ by at least 5% and 4× the median absolute deviation. Needs three commits after; never spans a dashed or dotted line.

</details>

Not measured: ec8b389.

