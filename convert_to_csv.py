import pandas as pd
import os
import time

MONTHS = ["2024-01", "2024-02", "2024-03"]

COLUMNS = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "PULocationID",
    "DOLocationID",
    "payment_type",
    "fare_amount",
    "tip_amount",
    "total_amount",
]

total_rows = 0
total_parquet_mb = 0
total_csv_mb = 0

for month in MONTHS:
    src = f"yellow_tripdata_{month}.parquet"
    dst = f"yellow_tripdata_{month}.csv"

    start = time.time()
    df = pd.read_parquet(src, columns=COLUMNS)

    # write with no header so every HDFS split is self-describing
    df.to_csv(dst, index=False, header=False)

    parquet_mb = os.path.getsize(src) / (1024 * 1024)
    csv_mb = os.path.getsize(dst) / (1024 * 1024)
    elapsed = time.time() - start

    total_rows += len(df)
    total_parquet_mb += parquet_mb
    total_csv_mb += csv_mb

    print(f"{month}: {len(df):,} rows | "
          f"parquet {parquet_mb:.1f} MB -> csv {csv_mb:.1f} MB "
          f"({csv_mb/parquet_mb:.1f}x) | {elapsed:.1f}s")

print()
print(f"TOTAL rows      : {total_rows:,}")
print(f"TOTAL parquet   : {total_parquet_mb:.1f} MB")
print(f"TOTAL csv       : {total_csv_mb:.1f} MB")
print(f"Expansion factor: {total_csv_mb/total_parquet_mb:.1f}x")