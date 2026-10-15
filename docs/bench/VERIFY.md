# Verification throughput

## Proposal

Run each batch’s local hashing and server CXSUM request concurrently. Use one
verification worker per requested data connection, capped by the negotiated pool
size. Each side hashes its items sequentially within a batch or request.

Reduce the serial work as well: compute only requested hash algorithms, reuse
server hashers within each request, size reusable read buffers to the requested
ranges, and format zero-payload checksum frames into a pooled buffer. Preserve
client file grouping, request-order frames, error ordering, and exact encoding.
The first failure cancels the peer operation; the worker joins both operations
before returning and compares hashes after successful response validation.

The hypothesis is that overlapping the two read streams hides their combined
latency, while reducing per-file CPU and allocation costs helps cached workloads.
These experiments test that hypothesis without adding hashing workers inside a
batch or request.

## Experiment

`BenchmarkVerifyInMemory` runs the real verifier against a loopback FTCP server.
Client and server share one process on an AMD Ryzen AI 9 HX 370. Both GOMAXPROCS
and OS affinity are pinned to 2, 4, 8, or 12 distinct physical cores, without SMT
siblings. Sets above four include lower-clock cores. The connection plan uses
`cpus/2` as a nominal server CPU count; it does not divide the CPU capacity
between the two sides.

Each cached operation verifies a 256 MiB logical tree backed by at most 64 MiB of
memfds. Both sides read the same memfd pool: the server duplicates a descriptor
after store lookup; the receiver opens `/proc/self/fd/N`. This excludes on-disk directory traversal. Files are 4 KiB,
16 KiB, 64 KiB, 1 MiB, 4 MiB, or 16 MiB. Requested concurrency **c** is one or
planned: 4/8/16/24 for 2/4/8/12 CPUs. Advertised I/O depth remains four. The client
opens two pooled connections at c=1, but the proposed verifier uses one worker.
Connections are probed and warmed before measurement.

Four implementations distinguish the effects:

| Implementation | Verification workers | Work within one batch | Checksum computation and frames | CSV build |
|---|---:|---|---|---|
| Sequential | c | Local hashing, then CXSUM | Both hash algorithms; per-item hashers and frame formatting | `sequential` |
| Overlap | c | Local hashing alongside CXSUM | Same as sequential | `overlap` |
| Optimized overlap | c | Local hashing alongside CXSUM | Requested algorithms, reused hashers/buffers, pooled frame formatting | `optimized` |
| Batch pipeline | 2c | Local hashing, then CXSUM; other batches can overlap | Same as sequential | `pipeline` |

The 48-case cached matrix compares sequential, overlap, and optimized overlap
across all sizes, CPU budgets, and connection counts. There are six alternating
runs per implementation, using 200 ms minimum subbenchmarks, in sequential /
overlap / optimized order and its reverse. An operation can exceed 200 ms.

A separate eight-core comparison alternates the batch pipeline and optimized
overlap six times, covering all six sizes at c=1/16. It tests the combined
proposal against the existing 2c-worker pipeline. These measurements are
independent of the 48-case matrix, whose sequential control isolates overlap
under the same worker cap.

Aggregate changes are equal-weight geometric means of paired case ratios.
The 95% intervals resample six whole paired runs, 10,000 times. Graph cells and
table rates use medians of six runs; their ratios can differ from the paired
aggregate. Local-only controls remain available in the benchmark but are
excluded from these comparisons.

## Results

Optimized overlap improves cached throughput **15.8%** against the batch pipeline
at eight cores (95% interval **10.2% to 22.4%**), while using c workers instead of
2c. All 12 case medians improve. CPU per GiB falls 17.3%, allocation count falls
43.8%, and allocated bytes fall 21.0% in that comparison.

With the same worker cap in both implementations, overlap alone improves the
cached matrix **7.1%** overall. It helps at one connection (**19.8%**) but regresses
at planned concurrency (**−4.3%**). Adding the serial checksum optimizations
raises the 48-case improvement to **32.7%**. The two comparisons use different
controls; the combined gain cannot be attributed to overlap alone.

![Throughput with overlapped hashing and serial checksum optimizations](plots/verify-simple.png)

| Comparison | Scope | Throughput change, 95% interval | CPU per GiB change |
|---|---|---:|---:|
| Overlap / sequential | All 48 cases | +7.1% (+4.3% to +10.6%) | +17.9% |
| Overlap / sequential | c=1 | +19.8% (+15.3% to +24.0%) | +32.7% |
| Overlap / sequential | Planned c | −4.3% (−7.2% to −0.9%) | +4.9% |
| Optimized overlap / sequential | All 48 cases | +32.7% (+25.4% to +41.1%) | −1.0% |
| Optimized overlap / sequential | c=1 | +53.0% (+44.1% to +64.5%) | +13.9% |
| Optimized overlap / sequential | Planned c | +15.1% (+9.2% to +21.4%) | −14.0% |
| Optimized overlap / batch pipeline | Eight cores, 12 cases | +15.8% (+10.2% to +22.4%) | −17.3% |

Against the sequential control, optimized overlap reduces allocation count by
40.7% and allocated bytes by 23.9%. Its overall CPU per GiB change is within
noise: −1.0%, with an interval of −6.1% to +3.7%. Overlap alone raises CPU per GiB
17.9%; more concurrent execution does not guarantee better cached throughput.

The weakest optimized-overlap median against the sequential control is
64 KiB / 8 cores / c=16: 4,900 → 4,855 MiB/s (−0.9%). The next weakest gains are
+1.9% and +2.4% at 12 cores / c=24 for 16/64 KiB. Overlap alone has larger
regressions: −15.2% for 1 MiB / 4 cores / c=8, −13.9% for 16 KiB / 2 cores / c=4,
and −13.6% for 64 KiB / 2 cores / c=4. The serial optimizations offset these in
the combined proposal.

Eight-core throughput against the batch pipeline, in MiB/s:

| File size | c=1 pipeline (2 workers) | c=1 optimized overlap (1 worker) | c=16 pipeline (32 workers) | c=16 optimized overlap (16 workers) |
|---|---:|---:|---:|---:|
| 4 KiB | 340 | 400 | 693 | 812 |
| 16 KiB | 1,180 | 1,309 | 2,041 | 2,595 |
| 64 KiB | 2,446 | 2,778 | 4,995 | 5,606 |
| 1 MiB | 3,884 | 4,242 | 7,942 | 9,180 |
| 4 MiB | 3,704 | 4,075 | 7,553 | 9,207 |
| 16 MiB | 3,456 | 4,233 | 7,634 | 8,906 |

## Read-latency sensitivity

The benchmark also injects independent waits before content reads, using the
Netflix gp3 and i4i beta fits at service-capacity-modeling revision
`c56c0dd7861a`. Larger reads include an assumed size term; open/stat work is real
and undelayed. The models have no shared device queue, aggregate bandwidth or
IOPS limit, cache model, or correlated stalls. They test service latency rather
than predict cloud-volume throughput.

Both sides share eight pinned cores, c=2, and advertised I/O depth four. Each
operation verifies 16 MiB to keep slow 4 KiB cases practical. Each implementation
has six alternating one-operation measurements per case, after pool warmup.
This smaller dataset is analyzed separately from the cached matrix.

Across three sizes per model, overlap alone improves throughput **98.1%** for
gp3 (interval 96.2% to 100.4%) and **96.0%** for i4i (89.2% to 102.1%). These gains
come from overlapping the two serial read streams.

| Model | File size | Sequential MiB/s | Overlap MiB/s | Optimized overlap MiB/s |
|---|---|---:|---:|---:|
| gp3 | 4 KiB | 3.50 | 6.97 | 6.98 |
| gp3 | 64 KiB | 52.74 | 104.13 | 104.05 |
| gp3 | 4 MiB | 420.60 | 821.70 | 836.10 |
| i4i | 4 KiB | 21.09 | 41.16 | 41.62 |
| i4i | 64 KiB | 295.60 | 575.55 | 564.41 |
| i4i | 4 MiB | 1,621.03 | 3,249.11 | 3,108.47 |

## Evidence and limits

The [measurements CSV](data/verify-simple.csv) preserves all 1,116 per-run results:
throughput in MiB/s, CPU in core-s/GiB, CPU capacity used, allocation count/bytes,
hash/server busy, pool size, and requested/delivered read waits. The `cached`
series identifies the 48-case matrix; `pipeline` identifies the independent
2c-worker comparison. Both CPU and allocation metrics include client and server.
Busy metrics include waits, descheduling, parsing, and output; they do not
measure CPU utilization or storage queue depth.

The [analysis script](verify_results.py) recomputes aggregate changes and the
graph from the CSV. Run it from any directory; plotting requires matplotlib:

```sh
python docs/bench/verify_results.py
python docs/bench/verify_results.py --plot
```

The [benchmark guide](OVERVIEW.md#verification-microbenchmark) lists workload
controls and an invocation. Preserved binaries, hashing sources, raw benchmark
output, and validation logs are under `/tmp/tx-checksum-simple`.

Formatting, full vet/tests, and affected race tests pass. Integration tests
cover overlap, cancellation and errors in either direction, worker caps,
budget behavior, split requests, range validation, ordered error prefixes, and
descriptor cleanup. The frame encoder fuzz target checks byte-identical encoding
against `encoding.WriteFrame` and is registered in `fuzz-long` after its coverage
continued growing beyond ten seconds.

These measurements support overlapping verification and reducing serial checksum
costs. They do not establish physical-storage throughput, full CPU saturation,
or the best storage concurrency.
