#!/usr/bin/env python3
"""Emit HTTP method from CLF lines: METHOD\t1"""
import re, sys
P = re.compile(r'"(\S+)\s+\S+')
for line in sys.stdin:
    m = P.search(line)
    if m:
        print("{}\t1".format(m.group(1)))
