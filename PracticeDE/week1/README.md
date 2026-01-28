docker run -d \
 --name pgdatabase \
 --network pg-network \
 -p 5433:5432 \
 -v ny_taxi_pg18_data:/var/lib/postgresql \
 -e POSTGRES_USER=postgres \
 -e POSTGRES_PASSWORD=root \
 -e POSTGRES_DB=ny_taxi \
 postgres:18

1. Ingest the NY Taxi data
   Run your ingestion script so the Docker Postgres gets real data (you may already have a version of this):

uv run python ingest_data.py \
 --pg-user=postgres \
 --pg-password=root \
 --pg-host=localhost \
 --pg-port=5433 \
 --pg-db=ny_taxi \
 --target-table=yellow_taxi_trips \
 --year=2021 \
 --month=1 \
 --chunksize=100000

# connect and verify postgres has the data

uv run pgcli -h localhost -p 5433 -U postgres -d ny_taxi
ny_taxi> \dt
ny_taxi> SELECT COUNT(\*) FROM yellow_taxi_trips;
