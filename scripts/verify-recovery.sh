#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
./mvnw -B -ntp -pl engine -am -Dtest=CsvEventReaderTest,TradingEngineTest,ReplayRunnerTest -Dsurefire.failIfNoSpecifiedTests=false -Dit.test=ProcessRecoveryIT -Dfailsafe.failIfNoSpecifiedTests=false verify
