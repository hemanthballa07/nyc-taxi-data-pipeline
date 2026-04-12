"""
NYC Taxi Monthly Pipeline DAG

Orchestrates the full pipeline for a given year/month:
  1. ingest_trips  — download Parquet from TLC + bulk-load into raw.yellow_taxi_trips
  2. dbt_seed      — reload static dimension seeds (payment type, rate code)
  3. dbt_run       — rebuild all dbt models: staging → dims → facts
  4. dbt_test      — run all 33 schema tests + 2 singular tests

Trigger manually:
  airflow dags trigger nyc_taxi_monthly --conf '{"year": 2024, "month": 1}'
Or via the Airflow UI: Trigger DAG w/ config.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator

DBT = "/home/airflow/.local/bin/dbt"
DBT_DIR = "/opt/airflow/dbt"
SCRIPTS_DIR = "/opt/airflow/scripts"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="nyc_taxi_monthly",
    description="Download, load, and transform one month of NYC Yellow Taxi data.",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,  # prevent concurrent runs for the same month corrupting raw data
    tags=["nyc-taxi", "ingestion", "dbt"],
    params={
        "year": Param(
            default=2024,
            type="integer",
            description="4-digit year of the data to load (e.g. 2024)",
        ),
        "month": Param(
            default=1,
            type="integer",
            description="Month number 1–12",
            minimum=1,
            maximum=12,
        ),
    },
    default_args=default_args,
) as dag:

    ingest_trips = BashOperator(
        task_id="ingest_trips",
        bash_command=(
            f"python {SCRIPTS_DIR}/ingest.py "
            "--year {{ params.year }} --month {{ params.month }}"
        ),
        doc_md=(
            "Download the monthly TLC Parquet file and bulk-load into "
            "`raw.yellow_taxi_trips`. Idempotent — delete-and-reload for the target month."
        ),
    )

    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=f"cd {DBT_DIR} && {DBT} seed --profiles-dir .",
        doc_md=(
            "Reload static dimension seeds (`dim_payment_type`, `dim_rate_code`) "
            "into the `marts` schema. Safe to re-run."
        ),
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_DIR} && {DBT} run --profiles-dir .",
        doc_md=(
            "Rebuild all dbt models in dependency order: "
            "`stg_yellow_taxi_trips` → dims → `fact_trips` → `fact_hourly_summary`."
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_DIR} && {DBT} test --profiles-dir .",
        retries=0,  # test failures are data issues, not transient errors
        doc_md=(
            "Run all 33 schema tests and 2 singular tests. "
            "A failure here means bad data reached the marts — do not retry automatically."
        ),
    )

    ingest_trips >> dbt_seed >> dbt_run >> dbt_test
