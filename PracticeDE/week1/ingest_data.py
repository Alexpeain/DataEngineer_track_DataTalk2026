#!/usr/bin/env python
# coding: utf-8

import click
import pandas as pd
from sqlalchemy import create_engine
from tqdm import tqdm

@click.command()
@click.option('--pg-user', '-u', default='postgres', help='Postgres username')
@click.option('--pg-password', '-p', default='root', help='Postgres password')
@click.option('--pg-host', '-h', default='localhost', help='Postgres host')
@click.option('--pg-port', default=5433, type=int, help='Postgres port')
@click.option('--pg-db', '-d', default='ny_taxi', help='Postgres database')
@click.option('--year', '-y', default=2021, type=int, help='Data year')
@click.option('--month', '-m', default=1, type=int, help='Data month (1-12)')
@click.option('--target-table', '-t', default='yellow_taxi_data', help='Target table name')
@click.option('--chunksize', '-c', default=100000, type=int, help='CSV chunk size')
def load_taxi_data(pg_user, pg_password, pg_host, pg_port, pg_db, year, month, target_table, chunksize):
    """Load NYC yellow taxi data to Postgres with chunking."""
    
    # File path
    filepath = f"./yellow_tripdata_{year:04d}-{month:02d}.csv.gz"
    
    # Create engine
    engine = create_engine(f'postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}')
    
    # Preview data
    print("Previewing first 100 rows...")
    df = pd.read_csv(filepath, nrows=100)
    print("Data shape:", df.shape)
    print("\nData types:")
    print(df.dtypes)
    print("\nFirst 5 rows:")
    print(df.head())
    
    # Print table schema
    print("\nTable schema:")
    print(pd.io.sql.get_schema(df, name=target_table, con=engine))
    
    # Create empty table
    print("\nCreating empty table...")
    df.head(n=0).to_sql(name=target_table, con=engine, if_exists='replace')
    print("Empty table created.")
    
    # Define dtypes and parse dates
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
    
    parse_dates = [
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime"
    ]
    
    # Load in chunks
    print("\nStarting chunked data load...")
    df_iter = pd.read_csv(
        filepath,  
        dtype=dtype,
        parse_dates=parse_dates,
        iterator=True,
        chunksize=chunksize
    )
    
    # Load chunks
    for df_chunk in tqdm(df_iter):
        df_chunk['pickup_year'] = df_chunk['tpep_pickup_datetime'].dt.year
        df_chunk['pickup_month'] = df_chunk['tpep_pickup_datetime'].dt.month
        df_chunk.to_sql(
            name=target_table, 
            con=engine, 
            if_exists='append',
            index=False,
            method='multi',
            chunksize=500
        )
    
    print("\nData loading complete!")
    print("Verify row count and new columns:")
    print(pd.read_sql(f"SELECT COUNT(*) as total_rows, MIN(pickup_year), MAX(pickup_year), MIN(pickup_month), MAX(pickup_month) FROM {target_table}", con=engine))

if __name__ == '__main__':
    load_taxi_data()
