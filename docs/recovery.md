# Recovery contract and reproduction

Run `./scripts/verify-recovery.sh` with Java 21 and Docker available. Tests launch the packaged application in child JVMs against an isolated PostgreSQL 17.6 container. Baseline and recovered canonical reports compare complete snapshots, outcomes, orders, transition sequences and fills.

The suite places child processes at barriers before commit, after commit/before publication, and before/after EOF finalization, then calls forcible process termination (SIGKILL on Linux/macOS). Resume uses a fresh connection and session lock and reloads the database checkpoint. Repeated resume of a complete run is a no-op.

A PostgreSQL wire-protocol test proxy drops the COMMIT response *after the database sent its completion*. The original process fails; the checkpoint proves that the batch committed. Resume matches baseline without duplicate effects. This verifies a lost acknowledgement rather than treating an ordinary exception as evidence of rollback.

The engine never retries from provisionally mutated memory. A database failure exits the process. Re-run `resume --run-id ID`; do not use `replay` with the same ID. Batch size, queue capacity and pacing may change; risk configuration, dataset bytes and the actual engine artifact hash must match. Keep the original jar for recovery. New jars require independent runs; there is no compatibility shim.

SIGTERM sets a stop flag, interrupts admission after a bounded wait, commits the currently processed partial batch, and discards queued uncommitted records. SIGKILL loses only uncommitted effects. Source records may be reread; committed effects remain unique within the tested single-database run. This is not end-to-end exactly-once trading or production failover.

Fault hooks (`TICKFORGE_FAULT`, `TICKFORGE_FAULT_INDEX`, `TICKFORGE_FAULT_MODE`, `TICKFORGE_FAULT_MARKER`) are opt-in test controls. Never set them during benchmarks. `barrier` mode waits for the test driver to terminate the child; otherwise the process immediately halts at the selected boundary.
