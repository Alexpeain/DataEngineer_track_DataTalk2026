# Homework 1

# Data ingestion (Jupyter notebook)

Prereqsites:

- Postgres running via `docker compose up -d` (db: ny_taxi, user/pass: postgres/postgres, port: 5433 on host)
- Jupyter and dependencies managed by uv

Steps:

1. Install deps:

   ```bash
   uv add pandas pyarrow sqlalchemy psycopg2-binary
   uv add --dev jupyter ipykernel

   ```

2. Start Jupyter:

```bash
    uv run jupyter notebook
```

#Q3

```sql
SELECT
COUNT(\*) AS num_short_trips
FROM green_taxi_trips
WHERE
lpep_pickup_datetime >= '2025-11-01'
AND lpep_pickup_datetime < '2025-12-01'
AND trip_distance <= 1;
-- Result: 8007
```

#Q4

```sql
SELECT
DATE(lpep_pickup_datetime) AS pickup_date,
MAX(trip_distance) AS max_distance
FROM green_taxi_trips
WHERE
lpep_pickup_datetime >= '2025-11-01'
AND lpep_pickup_datetime < '2025-12-01'
AND trip_distance < 100
GROUP BY DATE(lpep_pickup_datetime)
ORDER BY max_distance DESC
LIMIT 1;
-- Result: 2025-11-14, 88.03
```

#Q5

```sql
SELECT
z."Zone" AS pickup_zone,
SUM(t.total_amount) AS total_revenue
FROM green_taxi_trips t
JOIN taxi_zone_lookup z
ON t."PULocationID" = z."LocationID"
WHERE
DATE(t.lpep_pickup_datetime) = '2025-11-18'
GROUP BY z."Zone"
ORDER BY total_revenue DESC
LIMIT 1;
-- Result: East Harlem North, 9281.92
```

#Q6

```sql
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

-- Result: Yorkville West 81.89
```

## Q7 – Terraform workflow and resources

For the Terraform part of the homework, I used a separate `terraform/` folder.

### Files

- `providers.tf` – configures the Google provider.
- `variables.tf` – defines `project_id`, `region`, `gcs_bucket_name`, `bq_dataset_name`.
- `main.tf` – creates one GCS bucket (`google_storage_bucket`) and one BigQuery dataset (`google_bigquery_dataset`) in my GCP project.
- `terraform.tfvars` (local only, gitignored) – contains my real values for `project_id`, region, bucket name, and dataset name.

### Commands

From the `terraform/` directory:

```bash
terraform init
terraform apply -auto-approve
terraform destroy -auto-approve
```
