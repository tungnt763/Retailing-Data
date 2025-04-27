import pandas as pd
import os

# Read transactions
transactions = pd.read_csv('/Users/tungnt763/Documents/Retailing-Data/dags/data/transactions.csv')

# Convert the 'date' column to datetime format
trans_date = pd.to_datetime(transactions['Date'])

# Define the date range for filtering
start_date = trans_date.min().normalize()
end_date = trans_date.max().normalize()

# Ensure output directory exists
output_dir = '/Users/tungnt763/Documents/Retailing-Data/dags/data'
os.makedirs(output_dir, exist_ok=True)

# Filter and save transactions for each date in the range
for date in pd.date_range(start_date, end_date):
    # Filter rows where 'date' matches the current date (by date only)
    filtered = transactions[trans_date.dt.normalize() == date]
    # Only save if there is data for this date
    if not filtered.empty:
        out_path = os.path.join(output_dir, f'transactions_{date.strftime("%Y-%m-%d")}.csv')
        filtered.to_csv(out_path, index=False)
