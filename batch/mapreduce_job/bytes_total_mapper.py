#!/usr/bin/env python3
"""Emit path and its response bytes: PATH\tBYTES"""
import re, sys
P = re.compile(r'"\S+\s+(\S+)\s+\S+"\s+\d{3}\s+(\d+|-)')
for line in sys.stdin:
    m = P.search(line)
    if m:
        path, b = m.group(1), m.group(2)
        if b != '-':
            print("{}\t{}".format(path, b))
