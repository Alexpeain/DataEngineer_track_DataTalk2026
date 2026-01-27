#!/usr/bin/env python
# coding: utf-8
import pandas as pd
from sqlalchemy import create_engine

engine = create_engine(
    "postgresql://postgres:postgres@localhost:5433/ny_taxi"
)

green_path = "../green_tripdata_2025-11.parquet"

df_green = pd.read_parquet(green_path)

df_green.head()

df_green.to_sql(
    name= "green_taxi_trips",
    con =engine,
    if_exists ="replace",
    index =False,
)



with engine.connect() as conn:
    row_count = pd.read_sql("SELECT COUNT(*) AS n FROM green_taxi_trips;", conn)
row_count

zones_path = "../taxi_zone_lookup.csv"

df_zones = pd.read_csv(zones_path)

df_zones.head()

df_zones.to_sql(
    name="taxi_zone_lookup",
    con=engine,
    if_exists="replace",
    index=False,
)

with engine.connect() as conn:
    row_count_zones = pd.read_sql("SELECT COUNT(*) AS n FROM taxi_zone_lookup;", conn)
row_count_zones

#Question 3. For the trips in November 2025, how many trips had a trip_distance of less than or equal to 1 mile? 

query_q3 = """
SELECT
    COUNT(*) AS num_short_trips
FROM green_taxi_trips
WHERE
    lpep_pickup_datetime >= '2025-11-01'
    AND lpep_pickup_datetime < '2025-12-01'
    AND trip_distance <= 1;
"""

with engine.connect() as conn:
    df_q3 = pd.read_sql(query_q3,conn)
df_q3



#Question 4. Which was the pick up day with the longest trip distance? Only consider trips with trip_distance less than 100 miles (to exclude data errors).

query_q4 = """
SELECT
    DATE(lpep_pickup_datetime) AS pickup_date,
    MAX(trip_distance) AS max_distance
FROM green_taxi_trips
WHERE
    lpep_pickup_datetime >= '2025-11-01'
    AND lpep_pickup_datetime < '2025-12-01'
    AND trip_distance < 100
GROUP BY
    DATE(lpep_pickup_datetime)
ORDER BY
    max_distance DESC
LIMIT 1;
"""

with engine.connect() as conn:
    df_q4 = pd.read_sql(query_q4, conn)

df_q4

#Question 5. Which was the pickup zone with the largest total_amount (sum of all trips) on November 18th, 2025?

query_q5 = """
SELECT
    z."Zone" AS pickup_zone,
    SUM(t.total_amount) AS total_revenue
FROM green_taxi_trips t
JOIN taxi_zone_lookup z
    ON t."PULocationID" = z."LocationID"
WHERE
    DATE(t.lpep_pickup_datetime) = '2025-11-18'
GROUP BY
    z."Zone"
ORDER BY
    total_revenue DESC
LIMIT 1;
"""

with engine.connect() as conn:
    df_q5 = pd.read_sql(query_q5, conn)

df_q5


#Question 6. For the passengers picked up in the zone named "East Harlem North" in November 2025, which was the drop off zone that had the largest tip?

query_q6 = """
SELECT
    dz."Zone" AS dropoff_zone,
    MAX(t.tip_amount) AS max_tip
FROM green_taxi_trips t
JOIN taxi_zone_lookup pz
    ON t."PULocationID" = pz."LocationID"
JOIN taxi_zone_lookup dz
    ON t."DOLocationID" = dz."LocationID"
WHERE
    pz."Zone" = 'East Harlem North'
    AND t.lpep_pickup_datetime >= '2025-11-01'
    AND t.lpep_pickup_datetime < '2025-12-01'
GROUP BY
    dz."Zone"
ORDER BY
    max_tip DESC
LIMIT 1;
"""


with engine.connect() as conn:
    df_q6 = pd.read_sql(query_q6, conn)

df_q6

#Question 7. Which of the following sequences describes the Terraform workflow for: 1) Downloading plugins and setting up backend, 2) Generating and executing changes, 3) Removing all resources? 

#For the Terraform part of the homework, I used a separate `terraform/` folder.

### Files

#- `providers.tf` – configures the Google provider.
#- `variables.tf` – defines `project_id`, `region`, `gcs_bucket_name`, `bq_dataset_name`.
#- `main.tf` – creates one GCS bucket (`google_storage_bucket`) and one BigQuery dataset (`google_bigquery_dataset`) in my GCP project.
#- `terraform.tfvars` (local only, gitignored) – contains my real values for `project_id`, region, bucket name, and dataset name.

