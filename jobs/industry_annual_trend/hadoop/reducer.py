#!/home/matteo/anaconda3/envs/dev/bin/python
"""reducer.py"""

import sys
import numpy as np
from collections import defaultdict
from datetime import datetime
from scripts import utils

ticker_data = defaultdict(lambda: {
    "dates": [],
    "close": [],
    "volume": [],
})

industry_data = defaultdict(lambda: {
    "first_close": 0,
    "last_close": 0,
    "ticker_max_increment": None,
    "max_increment": -100000,
    "ticker_max_volume": None,
    "max_volume": -100000,
    "sector": None,
    "variation": None
})

ticker_to_industry = {}
industry_to_sector = {}


for line in sys.stdin:
    key, value = line.strip().split("\t")

    ticker, year, industry = key.split(",")
    date, close, volume, sector = value.split(',')

    year = int(year)
    close = float(close)
    volume = float(volume)
    date = datetime.strptime(date, '%Y-%m-%d')

    industry_to_sector[industry] = sector
    ticker_to_industry[ticker] = industry

    ticker_data[(ticker, year)]["dates"].append(date)
    ticker_data[(ticker, year)]["close"].append(close)
    ticker_data[(ticker, year)]["volume"].append(volume)


for (ticker, year), values in ticker_data.items():
    industry = ticker_to_industry[ticker]
    
    dates = values["dates"]
    close_prices = values["close"]
    volumes = values["volume"]

    sorted_indices = np.argsort(dates)
    first_close = close_prices[sorted_indices[0]]
    last_close = close_prices[sorted_indices[-1]]

    industry_data[(industry, year)]["first_close"] += first_close
    industry_data[(industry, year)]["last_close"] += last_close

    percentual_variation_rounded = utils.calculate_percentual_variation(first_close, last_close)

    if industry_data[(industry, year)]["max_increment"] < percentual_variation_rounded:
        industry_data[(industry, year)]["max_increment"] = percentual_variation_rounded
        industry_data[(industry, year)]["ticker_max_increment"] = ticker

    ovr_volume = np.sum(volumes)
    if industry_data[(industry, year)]["max_volume"] < ovr_volume:
        industry_data[(industry, year)]["max_volume"] = ovr_volume
        industry_data[(industry, year)]["ticker_max_volume"] = ticker

    industry_data[(industry, year)]["max_volume"] = utils.round_val(industry_data[(industry, year)]["max_volume"])
    industry_data[(industry, year)]["sector"] = industry_to_sector[industry]


for values in industry_data.values():
    values["variation"] = utils.calculate_percentual_variation(values["first_close"], values["last_close"])


output = sorted(list(industry_data.items()), key=lambda item: (item[0][1], item[1]["sector"], -item[1]["variation"]))

for item in output:
    print(f"{item[1]['sector']},{item[0][0]},{item[0][1]},{item[1]['variation']},{item[1]['ticker_max_increment']},{item[1]['max_increment']},{item[1]['ticker_max_volume']},{item[1]['max_volume']}")
