#!/usr/bin/env python3
import sys
for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    host = line.split()[0]
    if host:
        print(f"{host}\t1")
