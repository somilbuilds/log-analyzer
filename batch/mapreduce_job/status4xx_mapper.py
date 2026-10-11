#!/usr/bin/env python3
"""Emit 4xx status codes only: STATUS\t1"""
import re, sys
P = re.compile(r'" (4\d{2}) ')
for line in sys.stdin:
    m = P.search(line)
    if m:
        print("{}\t1".format(m.group(1)))
