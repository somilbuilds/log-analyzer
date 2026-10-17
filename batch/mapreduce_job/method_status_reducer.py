#!/usr/bin/env python3
"""
Reducer for method_status task.
Input keys: METHOD:STATUS\t1  (e.g., GET:200\t1)
Output: METHOD\tSTATUS\tCOUNT (for proper stacked chart rendering)
"""
import sys

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
        continue

    if key == current_key:
        current_count += count
    else:
        if current_key is not None:
            # Split composite key into method and status
            if ":" in current_key:
                method, status = current_key.split(":", 1)
                print("{}\t{}\t{}".format(method, status, current_count))
            else:
                print("{}\t{}\t{}".format(current_key, "unknown", current_count))
        current_key = key
        current_count = count

if current_key is not None:
    if ":" in current_key:
        method, status = current_key.split(":", 1)
        print("{}\t{}\t{}".format(method, status, current_count))
    else:
        print("{}\t{}\t{}".format(current_key, "unknown", current_count))
