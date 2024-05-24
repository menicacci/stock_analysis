#!/home/matteo/anaconda3/envs/dev/bin/python
"""spark application"""

import argparse
from pyspark.sql import SparkSession
import numpy as np
from datetime import datetime
import scripts.utils as utils 


def parse_line(line):
    fields = line.split(',')
    ticker = fields[0]
    date = datetime.strptime(fields[7], '%Y-%m-%d')
    year = date.year
    close, low, high, volume = map(float, (fields[2], fields[4], fields[5], fields[6]))
    name = fields[9]
    return (ticker, year), (date, close, low, high, volume, name)


def map_ticker_stats(values):
    sorted_values = sorted(values, key=lambda x: x[0])
    dates, close_prices, low_prices, high_prices, volumes, name = zip(*sorted_values)
    
    first_close = close_prices[0]
    last_close = close_prices[-1]
    percentual_variation = utils.calculate_percentual_variation(first_close, last_close)
    
    max_high = utils.round_val(max(high_prices))
    min_low = utils.round_val(min(low_prices))
    
    mean_volume = utils.round_val(np.mean(volumes))
    
    return name[0], percentual_variation, min_low, max_high, mean_volume


parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input dataset path")
parser.add_argument("--output_path", type=str, help="Output folder path")

args = parser.parse_args()
dataset_filepath, output_filepath = args.input_path, args.output_path

spark = SparkSession \
    .builder \
    .appName("Stock Annual Trend") \
    .getOrCreate()


lines = spark.sparkContext.textFile(dataset_filepath).cache()
data = lines.map(parse_line).groupByKey()

stats_per_stock_per_year = utils.flat_keys_values(data.mapValues(map_ticker_stats))
stats_per_stock_per_year = stats_per_stock_per_year.sortBy(lambda item: (item[1], item[0]))

utils.array_to_csv(stats_per_stock_per_year).saveAsTextFile(output_filepath)

spark.stop()