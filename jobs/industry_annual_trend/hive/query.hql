
CREATE TEMPORARY TABLE stock_data_temp (
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

LOAD DATA INPATH 'input/merged_historical_stock_data.csv' INTO TABLE stock_data_temp;

CREATE TABLE stock_data_year AS
SELECT
    ticker,
    open,
    close,
    adj_close,
    low,
    high,
    volume,
    stock_date,
    `exchange`,
    name,
    sector,
    industry,
    YEAR(TO_DATE(stock_date)) AS year
FROM
    stock_data_temp;


CREATE TABLE stock_first_last_close AS
SELECT
    ticker,
    year,
    FIRST_VALUE(close) OVER (PARTITION BY ticker, year ORDER BY stock_date) AS first_close,
    LAST_VALUE(close) OVER (PARTITION BY ticker, year ORDER BY stock_date ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_close
FROM
    stock_data_year;


CREATE TABLE stock_stats AS
SELECT
    sd.ticker,
    sd.name,
    sd.year,
    sd.sector,
    sd.industry,
    ROUND(((fl.last_close - fl.first_close) / fl.first_close) * 100, 2) AS percentual_variation,
    ROUND(MIN(sd.low), 2) AS min_low,
    ROUND(MAX(sd.high), 2) AS max_high,
    ROUND(SUM(sd.volume), 2) AS ovr_volume
FROM
    stock_data_year sd
JOIN
    stock_first_last_close fl
ON
    sd.ticker = fl.ticker AND sd.year = fl.year
GROUP BY
    sd.ticker, sd.name, sd.year, sd.sector, sd.industry, fl.first_close, fl.last_close;


CREATE TABLE industry_annual_variation AS
SELECT
    sector,
    industry,
    year,
    ROUND(((SUM(last_close) - SUM(first_close)) / SUM(first_close)) * 100, 2) AS percentual_variation_industry
FROM
    stock_stats
GROUP BY
    sector, industry, year;


CREATE TABLE industry_highest_increase AS
SELECT
    sector,
    industry,
    year,
    ticker,
    name,
    MAX(percentual_variation) AS highest_percentual_increase
FROM
    stock_stats
GROUP BY
    sector, industry, year, ticker, name;


CREATE TABLE industry_highest_volume AS
SELECT
    sector,
    industry,
    year,
    ticker,
    name,
    MAX(ovr_volume) AS highest_volume
FROM
    stock_stats
GROUP BY
    sector, industry, year, ticker, name;


CREATE TABLE final_report AS
SELECT
    ih.sector,
    ih.industry,
    ih.year,
    iav.percentual_variation_industry,
    ih.highest_percentual_increase,
    ih.name AS highest_increase_stock,
    hv.highest_volume,
    hv.name AS highest_volume_stock
FROM
    industry_highest_increase ih
JOIN
    industry_highest_volume hv
ON
    ih.sector = hv.sector AND ih.industry = hv.industry AND ih.year = hv.year
JOIN
    industry_annual_variation iav
ON
    ih.sector = iav.sector AND ih.industry = iav.industry AND ih.year = iav.year
ORDER BY
    iav.percentual_variation_industry DESC,
    ih.sector, ih.industry, ih.year DESC;


SELECT * FROM final_report;


DROP TABLE stock_data_temp;
DROP TABLE stock_data_year;
DROP TABLE stock_first_last_close;
DROP TABLE stock_stats;
DROP TABLE industry_annual_variation;
DROP TABLE industry_highest_increase;
DROP TABLE industry_highest_volume;
DROP TABLE final_report;
