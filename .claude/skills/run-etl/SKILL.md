---
name: run-etl
description: Run or debug the ETL pipeline. Use when executing the pipeline, troubleshooting failures, or modifying Airflow DAGs.
---

# ETL Pipeline Skill

## Pipeline Flow
```
download_parquet → validate_raw → load_to_postgres → dbt_run_staging → dbt_run_marts → dbt_test → log_success
```

## Airflow DAG Location
`dags/nyc_taxi_monthly.py`

## DAG Design Rules
- DAG must be **idempotent**: running twice for the same month must not duplicate data
- Use `DELETE WHERE` before INSERT, or use a staging-then-swap pattern
- Tasks should be **atomic**: each task does one thing
- Use `PythonOperator` for ingestion tasks, `BashOperator` for dbt commands
- Set `retries=2` and `retry_delay=timedelta(minutes=5)` on each task
- Set `catchup=False` on the DAG to avoid backfilling accidentally

## Idempotency Pattern
```python
# In the load task, always delete before inserting
def load_to_postgres(year, month, **kwargs):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        "DELETE FROM raw.yellow_taxi_trips WHERE date_trunc('month', tpep_pickup_datetime) = %s",
        (f"{year}-{month:02d}-01",)
    )
    # Then bulk load the new data
    ...
```

## Running Locally Without Airflow
For development and testing, you can run the pipeline steps manually:
```bash
# Step 1: Download
python scripts/ingest.py --year 2024 --month 1 --download-only

# Step 2: Load
python scripts/ingest.py --year 2024 --month 1 --load-only

# Step 3: Transform
cd dbt && dbt run

# Step 4: Test
cd dbt && dbt test
```

## Debugging
- Check Airflow logs: `docker compose logs airflow-worker`
- Check Postgres connection: `docker compose exec postgres psql -U nyctaxi -d nyctaxi`
- Check dbt logs: `dbt/logs/dbt.log`
