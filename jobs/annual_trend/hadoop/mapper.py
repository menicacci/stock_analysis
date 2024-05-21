#!/home/matteo/anaconda3/envs/dev/bin/python
"""mapper.py"""

import sys
from datetime import datetime

for line in sys.stdin:
    fields = line.strip().split(',')

    ticker = fields[0]
    date = fields[7]
    year = datetime.strptime(date, '%Y-%m-%d').year
    close = fields[2]
    low = fields[4]
    high = fields[5]
    volume = fields[6]
    name = fields[9]

    try:
        print(f"{ticker},{year}\t{date},{close},{low},{high},{volume},{name}")
    except ValueError:
        continue
