# tx performance history

Latest: [ec8b389](https://github.com/jolynch/tx/commit/ec8b38996aa25ff710a80af510c80ae3aaa23ce3) (2026-11-23).

One point per `main` merge: the median, with repeats as dots and min–max whiskers. Red and green lines mark sustained shifts. A dashed line means the benchmark changed; dotted, the machine.

## Shifts

None detected.

## Latest SSD copies

Median and range across measured copies.

| Workload | Throughput (MiB/s) | Copy time | Verification time | Most-used codec by frame count |
| --- | --- | --- | --- | --- |
| Compressible files | 2,060 (1,672–3,051) | 5.97 s (4.03–7.35) | 0.188 s (0.176–0.192) | none (100.0%) |
| Large files | 3,112 (3,110–3,147) | 13.2 s (13–13.2) | 6.79 s (6.77–6.82) | none (100.0%) |
| Mixed files | 1,472 (1,447–1,494) | 22.3 s (21.9–22.7) | 5.77 s (5.69–5.91) | none (100.0%) |
| Small files | 307 (306–307) | 6.68 s (6.66–6.7) | 0.566 s (0.556–0.574) | none (100.0%) |

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
git checkout ec8b38996aa25ff710a80af510c80ae3aaa23ce3
make bench-suite-commands BENCH_SUITE=full
~~~

<details><summary>How shifts are detected</summary>

Flagged when the 8 measured commits before and after don't overlap and their medians differ by at least 5% and 4× the median absolute deviation. Needs three commits after; never spans a dashed or dotted line.

</details>

