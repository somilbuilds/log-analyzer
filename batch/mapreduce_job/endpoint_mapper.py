#!/usr/bin/env python3
import re
import sys
REQUEST_PATTERN = re.compile(r'"\S+\s+(\S+)')
for line in sys.stdin:
    match = REQUEST_PATTERN.search(line)
    if match:
        print(f"{match.group(1)}\t1")
