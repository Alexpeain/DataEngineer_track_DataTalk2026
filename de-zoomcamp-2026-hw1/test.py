import pandas as pd
from sqlalchemy import create_engine

# Connection string: from host machine to Postgres in Docker
engine = create_engine(
    "postgresql://postgres:postgres@localhost:5433/ny_taxi"
)

def ingest_green_trips():
    df = pd.read_parquet("green_tripdata_2025-11.parquet")

    # Optional: inspect columns
    print(df.head())

    # Write to Postgres (let pandas create the table)
    df.to_sql(
        name="green_taxi_trips",
        con=engine,
        if_exists="replace",  # recreates table each run
        index=False,
    )

def ingest_zones():
    df_zones = pd.read_csv("taxi_zone_lookup.csv")
    print(df_zones.head())

    df_zones.to_sql(
        name="taxi_zone_lookup",
        con=engine,
        if_exists="replace",
        index=False,
    )

if __name__ == "__main__":
    ingest_green_trips()
    ingest_zones()
