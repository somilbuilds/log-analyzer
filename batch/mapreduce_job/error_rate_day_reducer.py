#!/usr/bin/env python3
"""
Reducer for error_rate_day task.
Input keys are like: DD/Mon/YYYY:total and DD/Mon/YYYY:error
Aggregates counts per composite key, then outputs: DD/Mon/YYYY\ttotal\terror\tpercent
"""
import sys
from collections import defaultdict

counts = defaultdict(lambda: {"total": 0, "error": 0})

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    try:
        key, value = line.split("\t", 1)
        count = int(value)
    except ValueError:
        continue

    # Key format: DD/Mon/YYYY:total or DD/Mon/YYYY:error
    if ":total" in key:
        day = key.replace(":total", "")
        counts[day]["total"] += count
    elif ":error" in key:
        day = key.replace(":error", "")
        counts[day]["error"] += count

# Output sorted by date key
for day in sorted(counts.keys()):
    total = counts[day]["total"]
    error = counts[day]["error"]
    pct = round((error / total) * 100, 2) if total > 0 else 0.0
    print("{}\t{}\t{}\t{}".format(day, total, error, pct))
