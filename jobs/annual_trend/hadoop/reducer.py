#!/home/matteo/anaconda3/envs/dev/bin/python
"""reducer.py"""

import sys
import numpy as np
from collections import defaultdict
from datetime import datetime

ticker_data = defaultdict(lambda: {
    "dates": [],
    "close": [],
    "low": [],
    "high": [],
    "volume": [],
    "name": None
})


for line in sys.stdin:
    key, value = line.strip().split('\t')

    ticker, year = key.split(',')
    date, close, low, high, volume, name = value.split(',')

    year = int(year)
    close = float(close)
    low = float(low)
    high = float(high)
    volume = float(volume)
    date = datetime.strptime(date, '%Y-%m-%d')

    ticker_data[(ticker, year)]["dates"].append(date)
    ticker_data[(ticker, year)]["close"].append(close)
    ticker_data[(ticker, year)]["low"].append(low)
    ticker_data[(ticker, year)]["high"].append(high)
    ticker_data[(ticker, year)]["volume"].append(volume)
    ticker_data[(ticker, year)]["name"] = name


for (ticker, year), values in ticker_data.items():
    dates = values["dates"]
    close_prices = values["close"]
    low_prices = values["low"]
    high_prices = values["high"]
    volumes = values["volume"]
    name = values["name"]

    sorted_indices = np.argsort(dates)
    first_close = close_prices[sorted_indices[0]]
    last_close = close_prices[sorted_indices[-1]]
    percentual_variation_rounded = round(((last_close - first_close) / first_close) * 100, 2)

    max_high = max(high_prices)
    min_low = min(low_prices)
    mean_volume = np.mean(volumes)

    print(f"{ticker},{name},{year},{percentual_variation_rounded},{min_low},{max_high},{mean_volume}")
