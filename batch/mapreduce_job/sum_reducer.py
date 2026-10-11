#!/usr/bin/env python3
"""Hadoop Streaming Reducer — Sum values (for bytes aggregation)"""
import sys
current_key = None
current_sum = 0
for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    try:
        key, value = line.split("\t", 1)
        val = int(value)
    except ValueError:
        continue
    if key == current_key:
        current_sum += val
    else:
        if current_key is not None:
            print("{}\t{}".format(current_key, current_sum))
        current_key = key
        current_sum = val
if current_key is not None:
    print("{}\t{}".format(current_key, current_sum))
