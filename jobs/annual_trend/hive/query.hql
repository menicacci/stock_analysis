CREATE TABLE IF NOT EXISTS stock_data (
    `ticker` string,
    `open` double,
    `close` double,
    `adj_close` double,
    `low` double,
    `high` double,
    `volume` int,
    `stock_date` string,
    `exchange` string,
    `name` string,
    `sector` string,
    `industry` string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

LOAD DATA INPATH 'input/merged_historical_stock_data.csv' INTO TABLE stock_data;


CREATE TABLE IF NOT EXISTS stock_with_closes AS
SELECT 
    ticker,
    name,
    YEAR(TO_DATE(stock_date)) AS year,
    FIRST_VALUE(close) OVER (PARTITION BY ticker, YEAR(TO_DATE(stock_date)) ORDER BY TO_DATE(stock_date) ASC) AS first_close,
    FIRST_VALUE(close) OVER (PARTITION BY ticker, YEAR(TO_DATE(stock_date)) ORDER BY TO_DATE(stock_date) DESC) AS last_close,
    high,
    low,
    volume
FROM stock_data;


CREATE TABLE IF NOT EXISTS ticker_yearly_stats AS
SELECT
    ticker,
    name,
    year,
    ROUND(((last_close - first_close) / first_close) * 100, 2) AS perc_variation,
    ROUND(MAX(high), 2) AS max_high,
    ROUND(MIN(low), 2) AS min_low,
    ROUND(AVG(volume), 2) AS avg_volume
FROM stock_with_closes
GROUP BY ticker, name, year
ORDER BY ticker;


SELECT * FROM ticker_yearly_stats;


DROP TABLE IF EXISTS stock_data;
DROP TABLE IF EXISTS stock_with_closes;
DROP TABLE IF EXISTS ticker_yearly_stats;
