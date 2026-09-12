#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
: "${TICKFORGE_DB_PASSWORD:?Set TICKFORGE_DB_PASSWORD before running}"
./mvnw -B -ntp -DskipTests -DskipITs package
mkdir -p data
run_id="demo-$(date +%s)-$$"
java -jar engine/target/engine-1.0.0-SNAPSHOT.jar generate --seed 42 --events 1000 --output "data/$run_id.csv"
java -jar engine/target/engine-1.0.0-SNAPSHOT.jar replay --input "data/$run_id.csv" --config config/demo.yaml --run-id "$run_id"
java -jar engine/target/engine-1.0.0-SNAPSHOT.jar report --run-id "$run_id" --format json > "data/$run_id.report.json"
printf 'Canonical report: data/%s.report.json\n' "$run_id"
