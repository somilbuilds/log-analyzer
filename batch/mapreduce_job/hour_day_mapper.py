#!/usr/bin/env python3
"""Emit DayOfWeek:Hour heatmap key: DOW:HH\t1"""
import re, sys
from datetime import datetime
MONTHS = {'Jan':1,'Feb':2,'Mar':3,'Apr':4,'May':5,'Jun':6,'Jul':7,'Aug':8,'Sep':9,'Oct':10,'Nov':11,'Dec':12}
DOW = ['Mon','Tue','Wed','Thu','Fri','Sat','Sun']
P = re.compile(r'\[(\d{2})/(\w+)/(\d{4}):(\d{2})')
for line in sys.stdin:
    m = P.search(line)
    if m:
        d, mon, y, h = int(m.group(1)), m.group(2), int(m.group(3)), m.group(4)
        mo = MONTHS.get(mon, 1)
        try:
            dow = DOW[datetime(y, mo, d).weekday()]
            print("{}:{}\t1".format(dow, h))
        except ValueError:
            pass
