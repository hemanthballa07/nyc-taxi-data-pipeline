#!/usr/bin/env bash
# Load all 12 months of 2024: make ingest → trigger DAG → wait for green
set -euo pipefail

YEAR=2024
LOG_PREFIX="[load_all_2024]"

wait_for_dag() {
    local run_id="$1"
    local month="$2"
    local attempt=0
    local max_attempts=60  # 30s each = 30 min max per month

    echo "$LOG_PREFIX  Polling DAG run: $run_id"
    while [ $attempt -lt $max_attempts ]; do
        sleep 30
        attempt=$((attempt + 1))

        local states
        states=$(docker compose exec -T airflow-scheduler \
            airflow tasks states-for-dag-run nyc_taxi_monthly "$run_id" 2>/dev/null \
            | grep -E "ingest|dbt_" | awk '{print $7}' | sort -u | tr '\n' ',')

        echo "$LOG_PREFIX  Month $month check $attempt: $states"

        if echo "$states" | grep -q "failed"; then
            echo "$LOG_PREFIX  ERROR: a task failed on month $month"
            docker compose exec -T airflow-scheduler \
                airflow tasks states-for-dag-run nyc_taxi_monthly "$run_id" 2>/dev/null
            return 1
        fi

        if ! echo "$states" | grep -qE "running|None|^,*$"; then
            echo "$LOG_PREFIX  Month $month DONE: $states"
            return 0
        fi
    done

    echo "$LOG_PREFIX  ERROR: timed out waiting for month $month"
    return 1
}

for MONTH in $(seq 1 12); do
    echo ""
    echo "========================================"
    echo "$LOG_PREFIX  Starting YEAR=$YEAR MONTH=$MONTH"
    echo "========================================"

    # Step 1: make ingest
    echo "$LOG_PREFIX  Running make ingest YEAR=$YEAR MONTH=$MONTH"
    make ingest YEAR=$YEAR MONTH=$MONTH

    # Step 2: Trigger DAG
    echo "$LOG_PREFIX  Triggering DAG for month $MONTH"
    docker compose exec -T airflow-scheduler \
        airflow dags trigger nyc_taxi_monthly \
        --conf "{\"year\": $YEAR, \"month\": $MONTH}" 2>/dev/null | tail -3

    # Get the latest run_id via list-runs (avoids truncation in trigger output)
    sleep 3
    RUN_ID=$(docker compose exec -T airflow-scheduler \
        airflow dags list-runs -d nyc_taxi_monthly 2>/dev/null \
        | grep "^nyc_taxi_monthly" | head -1 | awk '{print $3}')

    if [ -z "$RUN_ID" ]; then
        echo "$LOG_PREFIX  ERROR: could not retrieve run_id"
        exit 1
    fi
    echo "$LOG_PREFIX  DAG run_id: $RUN_ID"

    # Step 3: Wait for all 4 tasks to go green
    wait_for_dag "$RUN_ID" "$MONTH"

    echo "$LOG_PREFIX  Month $MONTH complete."
done

echo ""
echo "========================================"
echo "$LOG_PREFIX  All 12 months loaded. Querying row counts..."
echo "========================================"
docker compose exec -T postgres psql -U nyctaxi -d nyctaxi -c "
SELECT
    'raw.yellow_taxi_trips' AS table_name,
    COUNT(*) AS row_count,
    MIN(tpep_pickup_datetime)::date AS earliest,
    MAX(tpep_pickup_datetime)::date AS latest
FROM raw.yellow_taxi_trips
UNION ALL
SELECT 'marts.fact_trips', COUNT(*), NULL, NULL
FROM marts.fact_trips;
"
