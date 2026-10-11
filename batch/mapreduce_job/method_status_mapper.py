#!/usr/bin/env python3
"""Emit METHOD:STATUS pair: GET:200\t1"""
import re, sys
P = re.compile(r'"(\S+)\s+\S+\s+\S+"\s+(\d{3})')
for line in sys.stdin:
    m = P.search(line)
    if m:
        print("{}:{}\t1".format(m.group(1), m.group(2)))
