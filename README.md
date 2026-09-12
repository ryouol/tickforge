# TickForge

A Java 21 market-data replay and paper-execution engine with transactionally checkpointed PostgreSQL recovery.

**Synthetic data. Simulated orders. No live trading, account credentials, or investment claims.** The project demonstrates deterministic state transitions, bounded concurrency, database failure handling, and reproducible measurement.

## Quick start

Requires Java 21 (CI/container: Temurin 21.0.9+10), Docker with Compose, and Python 3 for the benchmark harness. Maven 3.9.11 is supplied by the wrapper.

```sh
export TICKFORGE_DB_PASSWORD="$(openssl rand -hex 24)"
docker compose up -d --wait db
./mvnw verify
./scripts/demo.sh
```

Keep that password in your shell or an ignored `.env` file for Compose; Java reads environment variables, so export it before CLI commands. PostgreSQL binds only to localhost (`55432` by default). Set `TICKFORGE_DB_PORT` and the matching `TICKFORGE_DB_URL` if that port is occupied. Existing volumes retain their original password. The scripts do not delete your database.

```sh
mkdir -p data
java -jar engine/target/engine-1.0.0-SNAPSHOT.jar generate --seed 42 --events 100000 --output data/demo.csv
java -jar engine/target/engine-1.0.0-SNAPSHOT.jar replay --input data/demo.csv --config config/demo.yaml --run-id demo-001 --port 8080
java -jar engine/target/engine-1.0.0-SNAPSHOT.jar resume --run-id demo-001
java -jar engine/target/engine-1.0.0-SNAPSHOT.jar report --run-id demo-001 --format json
./scripts/verify-recovery.sh
./scripts/benchmark.sh
```

`replay` creates a new run and refuses an existing ID. `resume` reads the original input path and configuration from PostgreSQL, checks dataset/configuration/build compatibility, then restores all state. Keep the input, its `.manifest.json`, and the original jar. `report` emits the full deterministic business ledger; it requires the run writer to be stopped. Status endpoints are available while replay runs; the process exits at EOF.

## Behavior

- Trade prices at configured entry/exit thresholds create fixed-size long-only order intents.
- Risk checks enforce quote freshness/liquidity, order and position limits, cash including fees, and a buy-price cap.
- A later same-symbol quote fills at ask for buys or bid for sells, capped by displayed size and current risk allowance. IOC remainders and EOF pending orders are cancelled.
- Physical rows checkpoint independently of sequence numbers and timestamps. Duplicates are audited; gaps and malformed records halt by default; older timestamps are quarantined.
- A single engine thread owns mutable trading state. A bounded queue blocks the producer during slow commits.
- PostgreSQL atomically stores ledger changes and a complete versioned snapshot. A session advisory lock excludes concurrent writers for a run.

Configuration is in `config/demo.yaml`; all money is integer ticks at 10,000 ticks/USD. `--batch-size` defaults to 100, `--queue-capacity` to 8192, and `--speed 0` means maximum speed. A positive speed scales event-time pacing. These operational controls do not alter trading configuration or deterministic results.

Database variables: `TICKFORGE_DB_URL` (default `jdbc:postgresql://localhost:55432/tickforge`), `TICKFORGE_DB_USER` (default `tickforge`), and `TICKFORGE_DB_PASSWORD` (required for the Compose setup). Unknown CLI options and invalid configurations fail clearly.

## Inspect and reproduce

With `--port 8080`, inspect localhost `/health/live`, `/health/ready`, `/status`, `/metrics`, and `/metrics.json`. `/status` contains only committed financial state; counters and timings are live operational observations. Both metrics endpoints expose histogram statistics and committed outcome counters. Live `processedIndex` may be ahead of `lastCommittedIndex`; `actualOfferedPerSecond` measures the producer's admission span. Histograms report p50/p95/p99/max in nanoseconds, capped at one hour. Admission starts before a potentially blocked queue offer, so latency includes backpressure and batch flush waits. Paced scheduled-arrival latency separately includes missed schedule time.

Example local alert: investigate any increase in `tickforge_commitFailures`, or a running, ready replay whose committed count remains unchanged for 30 seconds while the queue is nonempty. This is a documented example, not a production monitoring claim.

- [Architecture and data contract](docs/architecture.md)
- [Recovery tests and fault boundaries](docs/recovery.md)
- [Benchmark method and measured results](docs/benchmark-results.md)
- [Original proposed design](docs/design.md)

## Limits

One ordered input file, one process, USD fictional equities, top-of-book quotes, integer shares and market IOC orders. No short selling, leverage, impact, queue-position model, full order book, live connectivity, corporate actions, distributed failover, or automatic liquidation. Trade prints never supply executable liquidity. The final cash is not mark-to-market P&L. Performance results describe the measured machine and durable path only.

Development is recorded in focused stacked PRs using actual commit dates. The design's effort estimates are estimates, not claims of elapsed development time.
