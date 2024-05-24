SET mapreduce.map.memory.mb=4096;
SET mapreduce.reduce.memory.mb=4096;
SET mapreduce.map.java.opts=-Xmx3072m;
SET mapreduce.reduce.java.opts=-Xmx3072m;
SET mapreduce.job.reduces=20;
SET hive.exec.reducers.bytes.per.reducer=67108864;
SET hive.exec.reducers.max=50;
SET hive.auto.convert.join=true;

CREATE TABLE stock_data (
    `ticker` STRING,
    `open` DOUBLE,
    `close` DOUBLE,
    `adj_close` DOUBLE,
    `low` DOUBLE,
    `high` DOUBLE,
    `volume` INT,
    `stock_date` STRING,
    `exchange` STRING,
    `name` STRING,
    `sector` STRING,
    `industry` STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

LOAD DATA INPATH 'input/annual_trend/merged_historical_stock_data.csv' INTO TABLE stock_data;

CREATE TABLE stock_data_with_year AS
SELECT
    ticker,
    name,
    year(FROM_UNIXTIME(UNIX_TIMESTAMP(stock_date, 'yyyy-MM-dd'))) AS year,
    close,
    low,
    high,
    volume,
    stock_date
FROM
    stock_data;


CREATE TABLE stock_yearly_stats_with_window_functions AS
SELECT
    ticker,
    name,
    year,
    low,
    high,
    volume,
    stock_date,
    FIRST_VALUE(close) OVER (PARTITION BY ticker, year ORDER BY stock_date) AS first_close,
    LAST_VALUE(close) OVER (PARTITION BY ticker, year ORDER BY stock_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_close
FROM
    stock_data_with_year;


CREATE TABLE final_stock_yearly_stats AS
SELECT
    ticker,
    name,
    year,
    ROUND(((last_close - first_close) / first_close) * 100, 2) AS percentual_variation_rounded,
    ROUND(MIN(low), 2) AS min_low,
    ROUND(MAX(high), 2) AS max_high,
    ROUND(AVG(volume), 2) AS mean_volume
FROM
    stock_yearly_stats_with_window_functions
GROUP BY
    ticker,
    name,
    year,
    first_close,
    last_close
ORDER BY
    ticker,
    year;


SELECT * FROM final_stock_yearly_stats;


-- Drop the intermediate tables if they are no longer needed
DROP TABLE stock_data_with_year;
DROP TABLE final_stock_yearly_stats;
DROP TABLE stock_yearly_stats_with_window_functions;
DROP TABLE stock_data;
