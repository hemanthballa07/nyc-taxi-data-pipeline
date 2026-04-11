---
name: ingest-data
description: Ingest NYC taxi data from TLC. Use when adding new months of data, fixing ingestion issues, or modifying the download/load pipeline.
---

# Data Ingestion Skill

## Data Source
NYC TLC Yellow Taxi Trip Records in Parquet format.
- Base URL: `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{YYYY}-{MM}.parquet`
- Zone lookup: `https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv`
- Data available from 2009 to present (use 2023-2024 for this project)

## Ingestion Pattern
1. Download Parquet file to `data/raw/`
2. Validate: check row count > 0, expected columns exist, no schema drift
3. Load into `raw.yellow_taxi_trips` in PostgreSQL using COPY or batch INSERT
4. Log: record ingestion metadata (file, rows loaded, timestamp) to `raw.ingestion_log`

## Expected Columns in Source
`VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, RatecodeID, store_and_fwd_flag, PULocationID, DOLocationID, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, airport_fee`

## Validation Rules
- `tpep_pickup_datetime` must be within the expected month/year
- `fare_amount` should not be negative (flag but don't drop — handle in dbt)
- `trip_distance` should not be negative
- Row count should be > 100,000 for any month (sanity check)
- File size should be > 10MB (sanity check)

## Key Libraries
- `requests` or `urllib` for download
- `pyarrow` or `pandas` for Parquet reading
- `psycopg2` for PostgreSQL loading (use `copy_expert` for bulk loads — much faster than INSERT)

## Anti-Patterns to Avoid
- Never use `pandas.to_sql()` for large datasets — it's extremely slow
- Never load directly from Parquet to Postgres without validation
- Never hardcode file paths — use CLI arguments or config
- Don't download if the file already exists locally (add `--force` flag to override)

## Script Location
`scripts/ingest.py` — main ingestion script
`scripts/download.py` — download-only utility (if separated)
