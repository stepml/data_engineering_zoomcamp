#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm


# --- Data types for each column ---
dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

# --- Columns to parse as dates ---
parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]


def run():
    # --- Database connection settings ---
    pg_user = 'root'
    pg_pass = 'root'
    pg_host = 'localhost'
    pg_port = 5432
    pg_db = 'ny_taxi'

    # --- Data settings ---
    year = 2021
    month = 1
    chunksize = 100000
    target_table = 'yellow_taxi_data'

    # --- Build the URL ---
    prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'
    url = f'{prefix}yellow_tripdata_{year}-{month:02d}.csv.gz'

    print(f"Downloading data from: {url}")

    # --- Create database engine ---
    engine = create_engine(
        f'postgresql+psycopg://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}'
    )

    # --- Preview: read first 100 rows to inspect the data ---
    df_preview = pd.read_csv(
        url,
        nrows=100,
        dtype=dtype,
        parse_dates=parse_dates
    )

    print(f"Preview shape: {df_preview.shape}")
    print(df_preview.head())

    # --- Print the SQL schema that will be created ---
    print("\nSQL Schema:")
    print(pd.io.sql.get_schema(df_preview, name=target_table, con=engine))

    # --- Read the full file in chunks ---
    df_iter = pd.read_csv(
        url,
        dtype=dtype,
        parse_dates=parse_dates,
        iterator=True,
        chunksize=chunksize
    )

    # --- Insert chunks into PostgreSQL ---
    first = True
    for df_chunk in tqdm(df_iter, desc="Inserting chunks"):

        if first:
            # First chunk: create the table (or replace if exists)
            df_chunk.to_sql(
                name=target_table,
                con=engine,
                if_exists="replace",  # replace so we don't duplicate on rerun
                index=False
            )
            first = False
        else:
            # All subsequent chunks: append to existing table
            df_chunk.to_sql(
                name=target_table,
                con=engine,
                if_exists="append",
                index=False
            )

    print(f"\nDone! Data loaded into table: {target_table}")


if __name__ == '__main__':
    run()