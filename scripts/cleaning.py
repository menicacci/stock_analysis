import argparse
import pandas as pd


parser = argparse.ArgumentParser()
parser.add_argument("--data_dir", type=str, help="Data directory path")

args = parser.parse_args()
data_dir = args.data_dir

stock_prices_path = f'{data_dir}/historical_stock_prices.csv'
stocks_path = f'{data_dir}/historical_stocks.csv'

historical_stock_prices = pd.read_csv(stock_prices_path)
historical_stocks = pd.read_csv(stocks_path)

# Display some rows to understand the structure of the data
print("historical_stock_prices:")
print(historical_stock_prices.head())
print("\n\nhistorical_stocks:")
print(historical_stocks.head())

original_stock_prices_count = len(historical_stock_prices)
original_stocks_count = len(historical_stocks)

# Remove rows with missing values
historical_stock_prices_cleaned = historical_stock_prices.dropna()

# Data types check
historical_stock_prices_cleaned['date'] = pd.to_datetime(historical_stock_prices_cleaned['date'])
historical_stock_prices_cleaned['open'] = pd.to_numeric(historical_stock_prices_cleaned['open'], errors='coerce')
historical_stock_prices_cleaned['close'] = pd.to_numeric(historical_stock_prices_cleaned['close'], errors='coerce')
historical_stock_prices_cleaned['low'] = pd.to_numeric(historical_stock_prices_cleaned['low'], errors='coerce')
historical_stock_prices_cleaned['high'] = pd.to_numeric(historical_stock_prices_cleaned['high'], errors='coerce')
historical_stock_prices_cleaned['volume'] = pd.to_numeric(historical_stock_prices_cleaned['volume'], errors='coerce')

historical_stock_prices_cleaned.dropna(subset=['open', 'close', 'low', 'high', 'volume'], inplace=True)

# Remove anomalous values
historical_stock_prices_cleaned = historical_stock_prices_cleaned[
    (historical_stock_prices_cleaned['open'] >= 0) & 
    (historical_stock_prices_cleaned['close'] >= 0) & 
    (historical_stock_prices_cleaned['low'] >= 0) & 
    (historical_stock_prices_cleaned['high'] >= 0) & 
    (historical_stock_prices_cleaned['volume'] >= 0)
]

# Remove rows with missing values
historical_stocks_cleaned = historical_stocks.dropna(subset=['ticker', 'exchange', 'name'])

# Number of records after cleaning
cleaned_stock_prices_count = len(historical_stock_prices_cleaned)
cleaned_stocks_count = len(historical_stocks_cleaned)


print(f"Records removed in historical_stock_prices: {original_stock_prices_count - cleaned_stock_prices_count}/{original_stock_prices_count}")
print(f"Records removed in historical_stocks: {original_stocks_count - cleaned_stocks_count}/{original_stocks_count}")

# Save the cleaned datasets to new CSV files
# historical_stock_prices_cleaned.to_csv(stock_prices_path, index=False, header=False)
# historical_stocks_cleaned.to_csv(stocks_path, index=False, header=False)

tickers_in_stocks = set(historical_stocks_cleaned['ticker'])
historical_stock_prices_cleaned = historical_stock_prices_cleaned[historical_stock_prices_cleaned['ticker'].isin(tickers_in_stocks)]

# Merge dataset
merged_dataset = pd.merge(historical_stock_prices_cleaned, historical_stocks_cleaned, on='ticker')

print("\n\nMerged dataset:")
print(merged_dataset.head())

merged_dataset.to_csv(f'{data_dir}/merged_historical_stock_data.csv', index=False, header=False)