#!/usr/bin/env python3
"""Emit day from CLF timestamp: DD/Mon/YYYY\t1"""
import re, sys
P = re.compile(r'\[(\d{2}/\w+/\d{4})')
for line in sys.stdin:
    m = P.search(line)
    if m:
        print("{}\t1".format(m.group(1)))
