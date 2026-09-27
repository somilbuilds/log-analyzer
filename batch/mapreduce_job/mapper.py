#!/usr/bin/env python3
"""
Hadoop Streaming Mapper — Status Code Frequency Counter
========================================================

Reads CLF log lines from stdin, extracts the HTTP status code,
and emits: status_code\t1

This is used with Hadoop Streaming to do a MapReduce word-count
style aggregation of status codes directly on raw log files in HDFS.

Usage with Hadoop Streaming:
  hadoop jar /opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-*.jar \
    -input /user/data/raw_logs \
    -output /user/data/status_counts \
    -mapper mapper.py \
    -reducer reducer.py \
    -file mapper.py \
    -file reducer.py
"""

import sys
import re

# Regex to extract just the status code from a CLF line
# Looks for the pattern: "..." NNN  (status code after closing quote)
STATUS_PATTERN = re.compile(r'" (\d{3}) ')


def mapper():
    """Read CLF lines from stdin, emit status_code<TAB>1 for each."""
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        match = STATUS_PATTERN.search(line)
        if match:
            status_code = match.group(1)
            # Emit key-value pair: status_code \t 1
            print(f"{status_code}\t1")


if __name__ == "__main__":
    mapper()
