import pandas as pd

# Thay ten file parquet o day
df = pd.read_parquet('data/parquet/parquet_example.parquet')
df.to_csv('data/csv/parquet_example.csv', index=False)