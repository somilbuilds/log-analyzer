#!/usr/bin/env python3
"""Emit file extension from requested path: .ext\t1"""
import re, sys
P = re.compile(r'"\S+\s+(\S+)')
for line in sys.stdin:
    m = P.search(line)
    if m:
        path = m.group(1)
        idx = path.rfind('.')
        if idx > 0 and '/' not in path[idx:]:
            ext = path[idx:].split('?')[0].lower()
            print("{}\t1".format(ext))
        else:
            print("[no-ext]\t1")
