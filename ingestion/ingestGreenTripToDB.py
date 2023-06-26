import pandas as pd
from sqlalchemy import create_engine
from config.default import POSTGRES_CONN_STRING
import pyarrow.parquet as pq
from time import time


def main():

    engine = create_engine(POSTGRES_CONN_STRING)

    parquet_file = pq.ParquetFile('data/green_tripdata_2023-01.parquet')

    for batch in parquet_file.iter_batches(batch_size=batch_size):
        print("RecordBatch")
        df = batch.to_pandas()
        print(len(df))
        # df = df.append(batch_df)

        df.to_sql(name=tablename, con=engine, if_exists='append')

        t_start = time()

        df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
        df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

        t_end = time()

        print('Inserted another batch, took %.3f second' % (t_end - t_start))

    print("Finished ingesting data into the postgres database")

if __name__ == '__main__':
    tablename = "green_tripdata_2023Jan"
    batch_size = 10000
    main()