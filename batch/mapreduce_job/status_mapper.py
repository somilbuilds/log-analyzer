#!/usr/bin/env python3
import re
import sys
STATUS_PATTERN = re.compile(r'" (\d{3}) ')
for line in sys.stdin:
    match = STATUS_PATTERN.search(line)
    if match:
        print(f"{match.group(1)}\t1")
