import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, first, last, sum as spark_sum, col, round as spark_round
from pyspark.sql.window import Window
from pyspark.sql.functions import max as spark_max

parser = argparse.ArgumentParser()
parser.add_argument("--input_path", type=str, help="Input dataset path")
parser.add_argument("--output_path", type=str, help="Output folder path")
args = parser.parse_args()
dataset_filepath, output_filepath = args.input_path, args.output_path

spark = SparkSession.builder.appName("Industry Annual Trend").getOrCreate()


df = spark.read.csv(dataset_filepath, inferSchema=True, header=False)

columns = [
    "ticker", "open", "close", "adj_close", "low", "high",
    "volume", "date", "exchange", "name", "sector", "industry"
]

df = df.toDF(*columns)
df = df.withColumn("year", year("date"))
df = df.withColumn("volume", df["volume"].cast("double"))

industry_prices_df = df.groupBy("sector", "industry", "year", "ticker") \
    .agg(
        first("close").alias("first_close"),
        last("close").alias("last_close"),
        spark_sum("volume").alias("total_volume")
    )

industry_prices_df.createOrReplaceTempView("industry_prices")

percent_change_df = spark.sql("""
    SELECT 
        sector,
        industry,
        year,
        ticker,
        first_close,
        last_close,
        total_volume,
        ROUND(((last_close - first_close) / first_close) * 100, 2) AS percent_change
    FROM industry_prices
""")


industry_variation_df = percent_change_df.groupBy("sector", "industry", "year") \
    .agg(
        spark_sum("first_close").alias("sum_first_close"),
        spark_sum("last_close").alias("sum_last_close")
    ) \
    .withColumn("percentual_variation_industry",
                spark_round(((col("sum_last_close") - col("sum_first_close")) / col("sum_first_close")) * 100, 2)
                )


window_spec = Window.partitionBy("sector", "industry", "year")

max_var_perc_df = percent_change_df.withColumn(
    "max_var_perc",
    spark_round(spark_max("percent_change").over(window_spec), 2)
).filter(col("percent_change") == col("max_var_perc")) \
 .select("sector", "industry", "year", col("ticker").alias("ticker_max_var"), "percent_change")

max_vol_df = percent_change_df.withColumn(
    "max_vol",
    spark_max("total_volume").over(window_spec)
).filter(col("total_volume") == col("max_vol")) \
 .select("sector", "industry", "year", col("ticker").alias("ticker_max_vol"), "total_volume")


result_df = max_var_perc_df.join(
    max_vol_df,
    ["sector", "industry", "year"]
)

result_df = result_df.join(
    industry_variation_df,
    ["sector", "industry", "year"]
)

ordered_result_df = result_df.orderBy("year", "sector", "percentual_variation_industry", ascending=[True, True, False])
ordered_result_df.select(
    "sector", 
    "industry", 
    "year", 
    "percentual_variation_industry",
    "ticker_max_var", 
    "percent_change", 
    "total_volume", 
    "ticker_max_vol"
    ).write.csv(output_filepath, mode='overwrite', header=True)

spark.stop()
