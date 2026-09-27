#!/usr/bin/env python3
"""
Generate a synthetic web server log file in Common Log Format (CLF).
This guarantees we have a reliable, properly formatted dataset for the 
streaming pipeline without relying on external FTP servers that might be down.

Output format:
host - - [DD/Mon/YYYY:HH:MM:SS -0400] "METHOD /path HTTP/1.0" STATUS BYTES
"""

import random
import time
from datetime import datetime, timedelta
import os

OUTPUT_FILE = "access_log"
NUM_LINES = 150000

# CLF formatting dependencies
HOSTS = [
    "crl5.crl.com", "ix-sac6-20.ix.netcom.com", "199.120.110.21",
    "uplherc.upl.com", "burger.letters.com", "gw1.att.com",
    "gateway.net", "192.168.1.100", "proxy.aol.com", "dialup.mindspring.com"
]

PATHS = [
    "/", "/index.html", "/software.html", "/images/logo.gif",
    "/history/apollo/", "/shuttle/missions/", "/api/data",
    "/cgi-bin/test-cgi", "/about.html", "/contact.php"
]

METHODS = ["GET"] * 85 + ["POST"] * 10 + ["HEAD"] * 5
STATUS_CODES = [200] * 80 + [304] * 10 + [404] * 5 + [500] * 5

# Start time: 30 days ago
start_time = datetime.now() - timedelta(days=30)
current_time = start_time

print(f"Generating {NUM_LINES} synthetic CLF log lines...")
with open(OUTPUT_FILE, "w") as f:
    for i in range(NUM_LINES):
        # Progress time slightly forward (average 2 seconds between requests)
        current_time += timedelta(seconds=random.random() * 4)
        
        # Format time for CLF: 28/Aug/1995:00:00:23 -0400
        time_str = current_time.strftime("%d/%b/%Y:%H:%M:%S -0400")
        
        host = random.choice(HOSTS)
        method = random.choice(METHODS)
        path = random.choice(PATHS)
        status = random.choice(STATUS_CODES)
        
        # Bytes based on status code
        if status == 304:
            bytes_sent = "-"
        elif status == 404:
            bytes_sent = random.randint(100, 300)
        elif status >= 500:
            bytes_sent = random.randint(50, 150)
        else:
            bytes_sent = random.randint(1000, 20000)
            
        # CLF line
        line = f'{host} - - [{time_str}] "{method} {path} HTTP/1.0" {status} {bytes_sent}\n'
        f.write(line)
        
        if (i + 1) % 10000 == 0:
            print(f"  Generated {i + 1} lines...")

print(f"Done! Dataset saved to {OUTPUT_FILE}")
