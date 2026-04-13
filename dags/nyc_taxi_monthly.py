"""
NYC Taxi Monthly Pipeline DAG

Orchestrates the full pipeline for a given year/month:
  1. ingest_trips  — download Parquet from TLC + bulk-load into raw.yellow_taxi_trips
  2. ge_validate   — run 10 Great Expectations checks; save HTML report to docs/ge_report/
  3. dbt_seed      — reload static dimension seeds (payment type, rate code)
  4. dbt_run       — rebuild all dbt models: staging → dims → facts
  5. dbt_test      — run all 33 schema tests + 2 singular tests

ge_validate is a soft-fail step: it always exits 0 so the pipeline continues
even when checks fail. Failures are visible in the task logs and the HTML report.

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
        # --vars passes year/month so incremental models filter to the target month.
        # Month boundaries are computed in SQL via make_date() + interval '1 month'.
        bash_command=(
            f"cd {DBT_DIR} && {DBT} run --profiles-dir . "
            '--vars \'{"year": {{ params.year }}, "month": {{ params.month }}}\''
        ),
        doc_md=(
            "Run all dbt models with year/month vars. `stg_yellow_taxi_trips` and `fact_trips` "
            "are incremental — only the target month's rows are processed via delete+insert. "
            "Dims and `fact_hourly_summary` rebuild in full."
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

    ge_validate = BashOperator(
        task_id="ge_validate",
        bash_command=(
            f"python {SCRIPTS_DIR}/run_ge.py "
            "--year {{ params.year }} --month {{ params.month }}"
        ),
        retries=0,  # GE failures are data issues, not transient — don't retry
        doc_md=(
            "Run 10 Great Expectations checks on `raw.yellow_taxi_trips` for the "
            "target month. Saves an HTML report to `docs/ge_report/YYYY-MM.html`. "
            "Soft fail — always exits 0 so the pipeline continues."
        ),
    )

    ingest_trips >> ge_validate >> dbt_seed >> dbt_run >> dbt_test
