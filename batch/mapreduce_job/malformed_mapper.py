#!/usr/bin/env python3
"""Emit malformed log records that fail CLF parsing: REASON\t1"""
import re, sys

# Full CLF pattern: host ident authuser [timestamp] "request" status bytes
CLF = re.compile(
    r'^(\S+)\s+\S+\s+\S+\s+'           # host ident authuser
    r'\[\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2}\s+[^\]]*\]\s+'  # [timestamp]
    r'"[^"]*"\s+'                        # "request"
    r'\d{3}\s+'                          # status
    r'(\d+|-)$'                          # bytes
)

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    if not CLF.match(line):
        # Categorize the malformation
        if '[' not in line:
            print("missing_timestamp\t1")
        elif '"' not in line:
            print("missing_request\t1")
        else:
            print("unparseable\t1")
