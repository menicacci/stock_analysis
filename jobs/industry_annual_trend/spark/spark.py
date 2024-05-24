#!/home/matteo/anaconda3/envs/dev/bin/python
"""spark application"""

import argparse
from pyspark.sql import SparkSession
import numpy as np
from datetime import datetime

from scripts import utils


def parse_line(line):
    fields = line.split(',')

    ticker = fields[0]
    date = datetime.strptime(fields[7], '%Y-%m-%d')
    year = date.year
    close = float(fields[2])
    volume = float(fields[6])
    sector = fields[10]
    industry = fields[11]

    return ((ticker, year), (date, close, volume, sector, industry))


def ticker_stats(values):
    dates, close_prices, volumes, sector, industry = zip(*values)
    
    first_close = close_prices[np.argmin(dates)]
    last_close = close_prices[np.argmax(dates)]
    
    ovr_volume = sum(volumes)

    return (sector[0], industry[0], first_close, last_close, ovr_volume)


def reformat_to_industry(ticker_values):
    (ticker, year), (sector, industry, first_close, last_close, ovr_volume) = ticker_values
    return ((year, sector, industry), (ticker, first_close, last_close, ovr_volume))


def industry_stats(values):
    ticker, first_close, last_close, ovr_volume = zip(*values)

    percentual_variation_industry = utils.calculate_percentual_variation(sum(first_close), sum(last_close))

    percentual_variation = [utils.calculate_percentual_variation(first, last) for first, last in zip(first_close, last_close)]

    max_variation = np.argmax(percentual_variation)
    max_volume = utils.round_val(np.argmax(ovr_volume))

    return (
        percentual_variation_industry,
        ticker[max_variation],
        percentual_variation[max_variation],
        ticker[max_volume],
        ovr_volume[max_volume]
    )
    

parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input dataset path")
parser.add_argument("--output_path", type=str, help="Output folder path")

args = parser.parse_args()
dataset_filepath, output_filepath = args.input_path, args.output_path

spark = SparkSession \
    .builder \
    .appName("Industry Annual Trend") \
    .getOrCreate()

lines = spark.sparkContext.textFile(dataset_filepath).cache()
data = lines.map(parse_line).groupByKey()

stats_per_ticker = data.mapValues(ticker_stats)
stats_per_industry = stats_per_ticker.map(reformat_to_industry).groupByKey()

stats = stats_per_industry.mapValues(industry_stats)
sorted_stats = stats.sortBy(lambda item: (item[0][0], item[0][1], -item[1][0]))

flattened_output = utils.flat_keys_values(sorted_stats)

utils.array_to_csv(flattened_output).saveAsTextFile(output_filepath)

spark.stop()