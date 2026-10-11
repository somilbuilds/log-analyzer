#!/usr/bin/env python3
"""Emit paths that returned 4xx/5xx errors: PATH\t1"""
import re, sys
P = re.compile(r'"\S+\s+(\S+)\s+\S+"\s+([45]\d{2})')
for line in sys.stdin:
    m = P.search(line)
    if m:
        print("{}\t1".format(m.group(1)))
