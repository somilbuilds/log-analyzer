#!/usr/bin/env python3
"""Emit host and response bytes: HOST\tBYTES"""
import re, sys
P = re.compile(r'^(\S+)\s+\S+\s+\S+\s+\[.*?\]\s+".*?"\s+\d{3}\s+(\d+|-)')
for line in sys.stdin:
    m = P.match(line)
    if m:
        host, b = m.group(1), m.group(2)
        if b != '-':
            print("{}\t{}".format(host, b))
