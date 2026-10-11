#!/usr/bin/env python3
"""Emit response bytes bucket: BUCKET\t1 (for histogram)"""
import re, sys
P = re.compile(r'" \d{3} (\d+|-)')
BUCKETS = [0, 100, 500, 1000, 5000, 10000, 50000, 100000, 500000, 1000000]
def bucket_label(b):
    if b == 0: return "0B"
    if b < 1000: return "{}B".format(b)
    if b < 1000000: return "{}KB".format(b//1000)
    return "{}MB".format(b//1000000)
for line in sys.stdin:
    m = P.search(line)
    if m:
        val = m.group(1)
        if val == '-':
            print("0B\t1")
            continue
        v = int(val)
        label = ">{}".format(bucket_label(BUCKETS[-1]))
        for i in range(len(BUCKETS)-1):
            if BUCKETS[i] <= v < BUCKETS[i+1]:
                label = "{}-{}".format(bucket_label(BUCKETS[i]), bucket_label(BUCKETS[i+1]))
                break
        print("{}\t1".format(label))
