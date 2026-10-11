#!/usr/bin/env python3
"""Emit hosts with 4xx/5xx errors: HOST\t1"""
import re, sys
P = re.compile(r'^(\S+)\s+\S+\s+\S+\s+\[.*?\]\s+".*?"\s+([45]\d{2})')
for line in sys.stdin:
    m = P.match(line)
    if m:
        print("{}\t1".format(m.group(1)))
