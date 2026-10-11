#!/usr/bin/env python3
"""Emit day:category for error rate over time: DD/Mon/YYYY:error\t1 or DD/Mon/YYYY:total\t1"""
import re, sys
P = re.compile(r'\[(\d{2}/\w+/\d{4}).*?" (\d{3}) ')
for line in sys.stdin:
    m = P.search(line)
    if m:
        day, status = m.group(1), int(m.group(2))
        print("{}:total\t1".format(day))
        if status >= 400:
            print("{}:error\t1".format(day))
