#!/usr/bin/env python3
"""Durable replay experiment; all rows must match a canonical business checksum."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import tempfile
import time
import uuid

parser = argparse.ArgumentParser()
parser.add_argument("--events", type=int, default=3000)
parser.add_argument("--repeats", type=int, default=3)
parser.add_argument("--output", type=Path, default=Path("data/benchmark"))
parser.add_argument("--quick", action="store_true", help="only batch sizes 1/100/1000")
args = parser.parse_args()
if args.events < 1 or args.repeats < 1:
    parser.error("events and repeats must be positive")
args.output.mkdir(parents=True, exist_ok=True)
java = str(Path(os.environ["JAVA_HOME"]) / "bin/java") if "JAVA_HOME" in os.environ else "java"
jar = Path("engine/target/engine-1.0.0-SNAPSHOT.jar")
base = [java, "-Xms256m", "-Xmx256m", "-jar", str(jar)]
input_path = args.output / "events.csv"
subprocess.run(base + ["generate", "--seed", "42", "--events", str(args.events), "--output", str(input_path)], check=True)

def output(command):
    return subprocess.run(command, check=True, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT).stdout.strip()

metadata = {
    "os": platform.platform(), "architecture": platform.machine(),
    "cpu": output(["sysctl", "-n", "machdep.cpu.brand_string"]) if platform.system() == "Darwin" else platform.processor(),
    "java": output([java, "-version"]), "gitCommit": output(["git", "rev-parse", "HEAD"]),
    "workingTree": output(["git", "status", "--porcelain"]),
    "buildSha256": hashlib.sha256(jar.read_bytes()).hexdigest(),
    "manifest": json.loads(Path(str(input_path) + ".manifest.json").read_text()),
    "heap": "-Xms256m -Xmx256m", "gc": "JDK default (G1)",
    "database": "PostgreSQL 17.6 in local Docker; synchronous JDBC commits",
    "strictness": "strict gaps and malformed rows", "warmup": "one full unreported replay",
    "repeats": args.repeats, "dateUtc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
}
(args.output / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
cases = [(1, 8192, 0), (100, 8192, 0), (1000, 8192, 0)]
if not args.quick:
    cases += [(100, 1024, 0), (100, 65536, 0), (100, 8192, 20), (100, 8192, 100)]
expected = None
results = []
for ordinal, (batch, queue, speed) in enumerate([cases[1]] + cases * args.repeats):
    run_id = "bench-" + uuid.uuid4().hex[:16]
    command = base + ["replay", "--input", str(input_path), "--run-id", run_id, "--batch-size", str(batch), "--queue-capacity", str(queue), "--speed", str(speed)]
    time_path = args.output / f"{ordinal:02d}.time.txt"
    command = ["/usr/bin/time", "-l" if platform.system() == "Darwin" else "-v", "-o", str(time_path)] + command
    process = subprocess.run(command, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    (args.output / f"{ordinal:02d}.stdout.json").write_text(process.stdout)
    (args.output / f"{ordinal:02d}.stderr.jsonl").write_text(process.stderr)
    process.check_returncode()
    result = json.loads(process.stdout)
    with tempfile.TemporaryFile() as report:
        subprocess.run(base + ["report", "--run-id", run_id], check=True, stdout=report)
        report.seek(0)
        digest = hashlib.sha256()
        while chunk := report.read(65536):
            digest.update(chunk)
        checksum = digest.hexdigest()
    if expected is None:
        expected = checksum
    if checksum != expected:
        raise RuntimeError(f"business output mismatch for {run_id}")
    seconds = result["elapsedNs"] / 1e9
    row = {"ordinal": ordinal, "warmup": ordinal == 0, "runId": run_id, "batchSize": batch, "queueCapacity": queue, "speed": speed, "offeredTargetPerSecond": speed * 100 if speed else None, "committedPerSecond": args.events / seconds, "businessSha256": checksum, "summary": result}
    results.append(row)
    (args.output / "results.json").write_text(json.dumps(results, indent=2) + "\n")
    print(f"batch={batch} queue={queue} speed={speed}: {row['committedPerSecond']:.0f} committed records/s; checksum OK", flush=True)
