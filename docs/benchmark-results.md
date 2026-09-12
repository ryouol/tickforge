# Benchmark results

## Complete hosted experiment — 2026-09-12

[Verified workflow run](https://github.com/ryouol/tickforge/actions/runs/34723522223): **22 tests passed, zero failures**, Docker replay passed, and all **22 replay runs** (one warmup plus seven settings repeated three times) produced the same canonical ledger checksum. Both JMH benchmarks completed with their documented default warmup, measurement and fork settings.

This is an **exploratory GitHub-hosted runner experiment**, not a stable-machine performance certification. The code head was 548d24bc396f7952326a92e0d884ae0deb08b61d; GitHub built the PR merge commit recorded in [metadata](benchmarks/hosted/metadata.json). The local interrupted run below is retained separately and must not be used as a controlled before/after comparison.

Conditions: AMD EPYC 9V74 host, **two allocated logical CPUs**, about 7.75 GiB RAM, Ubuntu-based Linux 6.17 Azure runner, Temurin 21.0.9+10, G1, 256 MiB fixed Java heap. PostgreSQL 17.6 ran in Docker on the same runner. Each replay used the same 3,000 synthetic events, seed 42, two fictional symbols, strict validation and synchronous durable commits.

| Batch | Queue | Speed | Median committed records/s | Median actual offered records/s | Median p99 admission-to-commit (ms) |
|---:|---:|---:|---:|---:|---:|
| 1 | 8192 | 0 | 438 | 14337 | 6547.3 |
| 100 | 1024 | 0 | 3034 | 4236 | 399.5 |
| 100 | 8192 | 0 | 2980 | 14008 | 761.3 |
| 100 | 8192 | 20 | 1929 | 2000 | 105.0 |
| 100 | 8192 | 100 | 2980 | 9960 | 688.9 |
| 100 | 65536 | 0 | 2878 | 13620 | 812.6 |
| 1000 | 8192 | 0 | 3261 | 14769 | 716.2 |

The 100-record batch delivered about **6.8 times** the durable throughput of single-record commits on this run, supporting the demonstration default. Queue capacity 1,024 had similar throughput and a smaller observed p99 than 8,192; use --queue-capacity 1024 when demonstrating that measured tradeoff. Neither this short dataset nor three repeats establishes a universal optimum. At speed 100, the producer offered roughly 10,000 records/s but the engine completed roughly 2,980 records/s over the replay, with backlog visible in commit latency.

JMH isolated results (operations/s; error is the JMH-reported confidence interval):

| Isolated operation | Mean ops/s | Reported error |
|---|---:|---:|
| parser | 691745 | ±30832 |
| quoteSignalAndPartialFill | 3494730 | ±534579 |

The core operation processes **three events** per invocation; it is not a records/s metric. In-memory figures are not durable throughput. These experiments do not justify a low-latency or production-readiness claim.

Inspect [all replay distributions/checksums](benchmarks/hosted/results.jsonl), [maximum memory and process resource records](benchmarks/hosted/resource-usage.txt), [JMH JSON](benchmarks/hosted/jmh.json), [JMH log](benchmarks/hosted/jmh.log), and [EXPLAIN ANALYZE](benchmarks/hosted/explain-orders.txt). The query plan used the existing orders_symbol_history index; no additional index was added. The hosted checksum is `4fd11d0685f894a1e1c544d988788a3b20ca850a5f35e9856f9923dbbf823d06`. It covers the streaming report including its final newline and normalized account/position rows; older exploratory checksums used the earlier report format.

## Initial exploratory run — 2026-09-12

These are development measurements on a memory- and disk-constrained Mac, **not stable performance claims**. The matrix stopped during its third repetition because the host ran out of disk space and PostgreSQL reported I/O failures. Completed runs below had matching canonical business checksums. Do not extrapolate these figures to production or describe this engine as low latency.

Environment: Apple M1 Pro, macOS-26.6.2-arm64-arm-64bit-Mach-O, Homebrew OpenJDK 21.0.9, G1, 256 MiB fixed heap, PostgreSQL 17.6 in local Docker. Input: 3,000 seeded synthetic records (seed 42), two fictional symbols, strict mode, one full warmup replay. Exact build hash, input hash and working-tree metadata are preserved in [metadata](benchmarks/exploratory/metadata.json).

| Batch | Queue | Speed | Completed repetitions | Median records/s | Median p99 admission-to-commit (ms) |
|---:|---:|---:|---:|---:|---:|
| 1 | 8192 | 0 | 3 | 196 | 15091.1 |
| 100 | 1024 | 0 | 3 | 696 | 1782.6 |
| 100 | 8192 | 0 | 3 | 729 | 4055.9 |
| 100 | 8192 | 20 | 2 | 752 | 2515.5 |
| 100 | 8192 | 100 | 2 | 762 | 3631.2 |
| 100 | 65536 | 0 | 2 | 738 | 4008.7 |
| 1000 | 8192 | 0 | 3 | 780 | 3781.2 |

The observed batching improvement supports keeping 100 records per transaction for this demo instead of one. It does not justify a general optimum: batches of 1,000 and larger queues did not consistently improve these constrained runs. The offered paced targets were 2,000 and 10,000 records/s; completed throughput was lower, so neither target was sustained.

Raw [results](benchmarks/exploratory/results.jsonl) include p50/p95/p99/max for queue wait, engine work, commit, admission-to-commit and scheduled-arrival-to-commit, producer blocked duration, and output checksums. [Resource records](benchmarks/exploratory/resource-usage.txt) include per-process maximum resident memory. [Run log](benchmarks/exploratory/run-log.txt) preserves the interrupted experiment.

## Reproduction

`./scripts/benchmark.sh --events 3000 --repeats 3 --output data/benchmark` runs a warmup and varies batch size, queue capacity, then pacing one factor at a time. Every completed run must match the same business checksum. The Java summary measures replay through final commit; hash validation, connection setup and report generation are excluded from this elapsed time, while process resource measurements include startup. Admission begins before a potentially blocked offer. Latencies include batch-flush delay and saturate at one hour.

JMH isolates parser work and a three-event quote/signal/partial-fill cycle: `java -jar benchmarks/target/benchmarks-1.0.0-SNAPSHOT.jar -rf json -rff data/jmh.json`. Defaults: three one-second warmup iterations, five one-second measurement iterations, two forks, 256 MiB heap. Returned outputs prevent dead-code elimination. Core state is reset outside each measured invocation; interpret its allocation/GC effects separately. In-memory throughput is not durable throughput.
