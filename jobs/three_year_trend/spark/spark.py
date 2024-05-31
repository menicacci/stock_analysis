#!/home/matteo/anaconda3/envs/dev/bin/python
"""spark application"""

import argparse
from pyspark.sql import SparkSession
import numpy as np
from datetime import datetime
import scripts.utils as utils 
import json


def parse_line(line):
    fields = line.split(',')
    
    ticker = fields[0]
    date = datetime.strptime(fields[7], '%Y-%m-%d')
    year = date.year
    close = float(fields[2])
    name = fields[9]

    return (ticker, year), (date, close, name)


def ticker_stats(values):
    dates, closes, name = zip(*values)
    
    first_close = closes[np.argmin(dates)]
    last_close = closes[np.argmax(dates)]

    percentual_variation = utils.calculate_percentual_variation(first_close, last_close)
    
    return name[0], int(round(percentual_variation))


def refactor_ticker_stats(key_values):
    (ticker, year), (name, percentual_variation) = key_values
    return ((ticker, name), (year, percentual_variation))


def three_years_trend(ticker_trends):
    (ticker_name, year_trends) = ticker_trends
    (ticker, name) = ticker_name
    years, trends = zip(*year_trends)

    sorted_indices = np.argsort(years)

    return [((years[sorted_indices[i]], trends[sorted_indices[i]], 
              years[sorted_indices[i + 1]], trends[sorted_indices[i + 1]], 
              years[sorted_indices[i + 2]], trends[sorted_indices[i + 2]]), 
             (ticker, name)) 
            for i in range(len(sorted_indices) - 2)]


parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input dataset path")
parser.add_argument("--output_path", type=str, help="Output folder path")

args = parser.parse_args()
dataset_filepath, output_filepath = args.input_path, args.output_path

spark = SparkSession \
    .builder \
    .appName("Three Year Same Trend") \
    .getOrCreate()


lines = spark.sparkContext.textFile(dataset_filepath).cache()
data = lines.map(parse_line).groupByKey()

stats_per_ticker = data.mapValues(ticker_stats)

ticker_trends = stats_per_ticker.map(refactor_ticker_stats).groupByKey()

three_years_trends = ticker_trends.flatMap(three_years_trend).groupByKey()

same_three_years_trends = three_years_trends.filter(lambda x: len(x[1]) > 1).collect()

output_sorted = sorted(same_three_years_trends, key=lambda x: (x[0][0], -x[0][1]))


columns = ["Year1", "Trend1", "Year2", "Trend2", "Year3", "Trend3", "Tickers", "Names"]
rows = [(k[0], k[1], k[2], k[3], k[4], k[5], [v[0] for v in vs], [v[1] for v in vs]) for k, vs in output_sorted]
df = spark.createDataFrame(rows, columns)

df.write.csv(output_filepath, header=True)
