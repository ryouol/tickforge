#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
: "${TICKFORGE_DB_PASSWORD:?Set TICKFORGE_DB_PASSWORD before benchmarking}"
./mvnw -B -ntp -DskipTests -DskipITs package
python3 scripts/benchmark.py "$@"
