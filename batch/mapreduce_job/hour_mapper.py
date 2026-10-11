#!/usr/bin/env python3
"""Emit hour-of-day from CLF timestamp: HH\t1"""
import re, sys
P = re.compile(r'\[(\d{2})/\w+/\d{4}:(\d{2})')
for line in sys.stdin:
    m = P.search(line)
    if m:
        print("{}\t1".format(m.group(2)))
