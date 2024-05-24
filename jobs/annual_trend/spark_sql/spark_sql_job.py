#!/home/matteo/anaconda3/envs/dev/bin/python
"""spark application"""

import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, min, max, avg, first, last, col, round as spark_round
from pyspark.sql.window import Window

parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input dataset path")
parser.add_argument("--output_path", type=str, help="Output folder path")

args = parser.parse_args()
dataset_filepath, output_filepath = args.input_path, args.output_path

spark = SparkSession \
    .builder \
    .appName("Stock Annual Trend") \
    .getOrCreate()


df = spark.read.csv(dataset_filepath, inferSchema=True, header=False)
columns = ["ticker", "open", "close", "adj_close", "low", "high", "volume", "date", "exchange", "name", "sector", "industry"]
df = df.toDF(*columns)
df = df.withColumn("year", year("date"))

window_spec = Window.partitionBy("ticker", "year").orderBy("date")
df = df.withColumn("first_close", first("close").over(window_spec)) \
       .withColumn("last_close", last("close").over(window_spec))

stock_stats = df.groupBy("ticker", "name", "year").agg(
    spark_round(((last("last_close") - first("first_close")) / first("first_close")) * 100, 2).alias("percentual_variation"),
    spark_round(min("low"), 2).alias("min_low"),
    spark_round(max("high"), 2).alias("max_high"),
    spark_round(avg("volume"), 2).alias("mean_volume")
)

stock_stats.write.csv(f"{output_filepath}/stock_stats.csv", header=True, mode="overwrite")

spark.stop()
