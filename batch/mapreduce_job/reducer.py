#!/usr/bin/env python3
"""
Hadoop Streaming Reducer — Status Code Frequency Counter
=========================================================

Reads sorted key-value pairs from stdin (output of mapper),
aggregates counts per status code.

Input format (from mapper, sorted by Hadoop):
  200\t1
  200\t1
  404\t1
  200\t1
  500\t1

Output:
  200\t3
  404\t1
  500\t1
"""

import sys


def reducer():
    """Sum up counts for each status code."""
    current_key = None
    current_count = 0

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        try:
            key, value = line.split("\t", 1)
            count = int(value)
        except ValueError:
            # Skip malformed lines
            continue

        if key == current_key:
            current_count += count
        else:
            # Emit previous key's total
            if current_key is not None:
                print(f"{current_key}\t{current_count}")
            current_key = key
            current_count = count

    # Don't forget the last key
    if current_key is not None:
        print(f"{current_key}\t{current_count}")


if __name__ == "__main__":
    reducer()
