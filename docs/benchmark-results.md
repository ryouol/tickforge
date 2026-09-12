# Benchmark results

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
