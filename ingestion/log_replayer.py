#!/usr/bin/env python3
"""
Log Replayer — Reads CLF log files and replays them as a file-based stream
for Spark Structured Streaming consumption.

Streaming approach: FILE-BASED
  The replayer writes small JSON files into an output directory. Spark's
  readStream.format("json").option("path", ...) picks them up automatically.
  This is more reliable than socket streaming (survives restarts, no ordering
  issues, supports backpressure naturally via file accumulation).

Usage:
  python log_replayer.py --input data/raw/access_log \\
                         --output-dir /tmp/log_stream \\
                         --speed 500 --batch-size 100

  --speed N     : Compress real time by factor N (500 = 1 real hour → 7.2 sec)
  --batch-size  : Number of log lines per output file
  --inject-anomaly       : Enable anomaly injection
  --anomaly-type ddos|5xx: Type of anomaly to inject
  --anomaly-start N      : Start injecting after N lines
  --anomaly-duration N   : Inject for N lines
"""

import argparse
import json
import os
import random
import re
import sys
import time
from datetime import datetime
from pathlib import Path


# ─── CLF Regex ───────────────────────────────────────────────────────────────
# Common Log Format: host ident authuser [date] "request" status bytes
# Example: crl5.crl.com - - [28/Aug/1995:00:00:23 -0400] "GET /Software.html HTTP/1.0" 200 1204
CLF_PATTERN = re.compile(
    r'^(\S+)'                        # host
    r' \S+'                          # ident (ignored)
    r' \S+'                          # authuser (ignored)
    r' \[([^\]]+)\]'                 # timestamp
    r' "(\S+)'                       # method
    r' (\S+)'                        # path
    r'(?: (\S+))?'                   # protocol (optional)
    r'"'
    r' (\d{3})'                      # status code
    r' (\d+|-)'                      # bytes (or -)
)

# Fallback pattern for malformed request lines
CLF_PATTERN_FALLBACK = re.compile(
    r'^(\S+)'                        # host
    r' \S+'                          # ident
    r' \S+'                          # authuser
    r' \[([^\]]+)\]'                 # timestamp
    r' "([^"]*)"'                    # full request (may be malformed)
    r' (\d{3})'                      # status
    r' (\d+|-)'                      # bytes
)

# DDoS injection: a small pool of "attacker" IPs
DDOS_IPS = [
    "10.0.0.101", "10.0.0.102", "10.0.0.103",
    "10.0.0.104", "10.0.0.105"
]


def parse_clf_line(line: str) -> dict | None:
    """Parse a single CLF log line into a structured dict."""
    line = line.strip()
    if not line:
        return None

    match = CLF_PATTERN.match(line)
    if match:
        host, timestamp, method, path, protocol, status, size = match.groups()
        return {
            "host": host,
            "timestamp": timestamp,
            "method": method,
            "path": path,
            "protocol": protocol or "HTTP/1.0",
            "status": int(status),
            "bytes": int(size) if size != "-" else 0,
            "raw": line,
        }

    # Try fallback for malformed request lines
    match = CLF_PATTERN_FALLBACK.match(line)
    if match:
        host, timestamp, request, status, size = match.groups()
        parts = request.split() if request else []
        return {
            "host": host,
            "timestamp": timestamp,
            "method": parts[0] if len(parts) >= 1 else "UNKNOWN",
            "path": parts[1] if len(parts) >= 2 else "/",
            "protocol": parts[2] if len(parts) >= 3 else "HTTP/1.0",
            "status": int(status),
            "bytes": int(size) if size != "-" else 0,
            "raw": line,
        }

    return None


def inject_anomaly(record: dict, anomaly_type: str) -> dict:
    """Modify a record to inject anomalous behaviour."""
    if anomaly_type == "ddos":
        # Replace host with one from the attacker IP pool
        record["host"] = random.choice(DDOS_IPS)
        # Rapid-fire GET requests to common paths
        record["method"] = "GET"
        record["path"] = random.choice(["/", "/index.html", "/login", "/api/data"])
    elif anomaly_type == "5xx":
        # Override status code to a 5xx error
        record["status"] = random.choice([500, 502, 503, 504])
    return record


def write_batch(records: list[dict], output_dir: str, batch_num: int):
    """Write a batch of records as a JSON-lines file."""
    filename = f"batch_{batch_num:08d}_{int(time.time()*1000)}.json"
    filepath = os.path.join(output_dir, filename)
    # Write to temp file first, then rename (atomic on Linux) so Spark
    # doesn't read a partially-written file
    tmp_path = filepath + ".tmp"
    with open(tmp_path, "w") as f:
        for record in records:
            f.write(json.dumps(record) + "\n")
    os.rename(tmp_path, filepath)


def replay(args):
    """Main replay loop: read log file, parse, optionally inject anomalies,
    write batches at a controllable rate."""
    output_dir = args.output_dir
    os.makedirs(output_dir, exist_ok=True)

    # Read all lines
    print(f"[Replayer] Reading log file: {args.input}")
    with open(args.input, "r", errors="replace") as f:
        lines = f.readlines()

    total_lines = len(lines)
    print(f"[Replayer] Total lines: {total_lines}")
    print(f"[Replayer] Speed factor: {args.speed}x")
    print(f"[Replayer] Batch size: {args.batch_size}")
    print(f"[Replayer] Output dir: {output_dir}")
    if args.inject_anomaly:
        print(f"[Replayer] Anomaly injection: {args.anomaly_type} "
              f"from line {args.anomaly_start} for {args.anomaly_duration} lines")

    batch = []
    batch_num = 0
    parsed_count = 0
    skipped_count = 0

    for i, line in enumerate(lines):
        record = parse_clf_line(line)
        if record is None:
            skipped_count += 1
            continue

        # Inject anomaly if in the anomaly window
        if args.inject_anomaly:
            if args.anomaly_start <= i < args.anomaly_start + args.anomaly_duration:
                record = inject_anomaly(record, args.anomaly_type)

        # Add replay timestamp (wall-clock time for the dashboard)
        record["replay_timestamp"] = datetime.utcnow().isoformat()

        batch.append(record)
        parsed_count += 1

        if len(batch) >= args.batch_size:
            write_batch(batch, output_dir, batch_num)
            batch_num += 1
            batch = []

            # Rate control: sleep to simulate compressed real-time
            # With speed=500 and batch_size=100, we produce 100 records
            # then sleep briefly so Spark can keep up
            sleep_time = args.batch_size / (args.speed * 10)
            if sleep_time > 0:
                time.sleep(sleep_time)

            if batch_num % 50 == 0:
                print(f"[Replayer] Written {batch_num} batches "
                      f"({parsed_count} records, {skipped_count} skipped)")

    # Flush remaining
    if batch:
        write_batch(batch, output_dir, batch_num)
        batch_num += 1

    print(f"[Replayer] Done! {batch_num} batches, "
          f"{parsed_count} records parsed, {skipped_count} skipped")


def main():
    parser = argparse.ArgumentParser(description="CLF Log Replayer for Spark Streaming")
    parser.add_argument("--input", required=True, help="Path to CLF log file")
    parser.add_argument("--output-dir", required=True,
                        help="Directory to write JSON stream files")
    parser.add_argument("--speed", type=float, default=500,
                        help="Time compression factor (default: 500)")
    parser.add_argument("--batch-size", type=int, default=100,
                        help="Records per output file (default: 100)")
    parser.add_argument("--inject-anomaly", action="store_true",
                        help="Enable anomaly injection")
    parser.add_argument("--anomaly-type", choices=["ddos", "5xx"], default="ddos",
                        help="Type of anomaly: ddos or 5xx (default: ddos)")
    parser.add_argument("--anomaly-start", type=int, default=5000,
                        help="Line number to start injecting anomalies (default: 5000)")
    parser.add_argument("--anomaly-duration", type=int, default=2000,
                        help="Number of lines to inject anomalies for (default: 2000)")
    args = parser.parse_args()
    replay(args)


if __name__ == "__main__":
    main()
