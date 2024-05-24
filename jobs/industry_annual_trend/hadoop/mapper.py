#!/home/matteo/anaconda3/envs/dev/bin/python
"""mapper.py"""

import sys
from datetime import datetime

for line in sys.stdin:
    fields = line.strip().split(',')

    if len(fields) != 12:
        continue

    ticker = fields[0]
    date = fields[7]
    year = datetime.strptime(date, '%Y-%m-%d').year
    close = fields[2]
    volume = fields[6]
    sector = fields[10]
    industry = fields[11]

    try:
        print(f"{ticker},{year},{industry}\t{date},{close},{volume},{sector}")
    except ValueError:
        continue