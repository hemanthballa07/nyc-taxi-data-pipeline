# Plan: Airflow DAG — NYC Taxi Monthly Pipeline

**Status:** Approved — ready to implement
**Last updated:** 2026-04-11
**Scope:** One DAG orchestrating ingest → dbt seed → dbt run → dbt test.
           No dashboard work. No schedule automation yet (manual trigger only).

---

## Goal

Write `dags/nyc_taxi_monthly.py` — an Airflow DAG that runs the full pipeline
for a given year/month on demand. The DAG is the authoritative way to run the
pipeline; `make ingest` and `make dbt-run` remain available for local dev only.

---

## Approved Design Decisions (from brainstorm)

| Decision                         | Choice                                                                                   |
|----------------------------------|------------------------------------------------------------------------------------------|
| Install dbt in Airflow container | `_PIP_ADDITIONAL_REQUIREMENTS` in docker-compose                                         |
| dbt profiles.yml connection      | `env_var()` interpolation; container sets `POSTGRES_HOST=postgres`, `POSTGRES_PORT=5432` |
| Run ingest.py                    | `BashOperator` calling the script as subprocess                                          |
| Task granularity                 | 4 tasks: `ingest_trips` → `dbt_seed` → `dbt_run` → `dbt_test`                            |
| Schedule & params                | `schedule=None`, explicit `year`/`month` Params                                          |

---

## Files to Create or Modify

| File                       | Action | Purpose                                                                  |
|----------------------------|--------|--------------------------------------------------------------------------|
| `docker-compose.yml`       | Modify | Add `_PIP_ADDITIONAL_REQUIREMENTS` + `POSTGRES_HOST/PORT` to airflow env |
| `dbt/profiles.yml`         | Modify | Replace hardcoded host/port with `env_var()` calls                       |
| `dags/nyc_taxi_monthly.py` | Create | The DAG                                                                  |

---

## Step 1 — Install dbt in the Airflow Container

Add to the `x-airflow-common` environment block in `docker-compose.yml`:

```yaml
_PIP_ADDITIONAL_REQUIREMENTS: "dbt-core dbt-postgres"
POSTGRES_HOST: postgres
POSTGRES_PORT: "5432"
```

`_PIP_ADDITIONAL_REQUIREMENTS` is an official Airflow mechanism — it runs
`pip install` during container startup before the scheduler/webserver starts.
`POSTGRES_HOST` and `POSTGRES_PORT` override the dbt profile values so dbt
inside the container reaches Postgres via the internal Docker network
(`postgres:5432`) instead of the host machine (`localhost:5433`).

**After editing docker-compose.yml, restart only the Airflow services:**
```bash
docker compose up -d --no-deps airflow-scheduler airflow-webserver
```

**Verification step (mandatory before building the DAG):**
```bash
docker compose exec airflow-scheduler bash -c "dbt --version"
```
Must print dbt-core and dbt-postgres versions. If `dbt` is not on `$PATH`,
try the full path:
```bash
docker compose exec airflow-scheduler bash -c \
  "python -c 'import dbt; print(dbt.__file__)'"
```
If dbt is installed but not on PATH, find the binary and note the path —
all BashOperator commands must use the full path to `dbt` (e.g.
`/home/airflow/.local/bin/dbt`). Capture the exact path in this plan
before writing the DAG.

---

## Step 2 — Update dbt profiles.yml

Replace the hardcoded `host` and `port` with `env_var()` so the same
`profiles.yml` works for both local dev and the Airflow container:

```yaml
nyc_taxi:
  target: dev
  outputs:
    dev:
      type: postgres
      host: "{{ env_var('POSTGRES_HOST', 'localhost') }}"
      port: "{{ env_var('POSTGRES_PORT', '5433') | int }}"
      dbname: nyctaxi
      user: nyctaxi
      password: nyctaxi
      schema: staging
      threads: 4
```

Local dev behavior is unchanged — env vars default to `localhost:5433`
when `POSTGRES_HOST`/`POSTGRES_PORT` are not set in the shell.

**Verify local dbt still works after this change:**
```bash
cd dbt && .venv/bin/dbt debug --profiles-dir .
```

---

## Step 3 — Write the DAG (`dags/nyc_taxi_monthly.py`)

### DAG configuration

```
dag_id:         nyc_taxi_monthly
schedule:       None  (manual trigger only)
start_date:     2024-01-01
catchup:        False
tags:           ['nyc-taxi', 'ingestion', 'dbt']
params:
  year:  Param(default=2024, type='integer', description='4-digit year')
  month: Param(default=1,    type='integer', description='Month 1-12',
               minimum=1, maximum=12)
default_args:
  retries:        1
  retry_delay:    timedelta(minutes=5)
  on_failure_callback: (none for V1)
```

### Task definitions

**Task 1: `ingest_trips`**

```python
BashOperator(
    task_id='ingest_trips',
    bash_command=(
        'python /opt/airflow/scripts/ingest.py '
        '--year {{ params.year }} --month {{ params.month }}'
    ),
)
```

- Runs `ingest.py` as a subprocess — download, validate, bulk-load
- `ingest.py` reads `POSTGRES_HOST`/`POSTGRES_PORT` from the container env
- Already idempotent — safe to retry
- `retries=1` inherited from `default_args`

**Task 2: `dbt_seed`**

```python
BashOperator(
    task_id='dbt_seed',
    bash_command='cd /opt/airflow/dbt && {dbt_bin} seed --profiles-dir .',
)
```

- Reloads `dim_payment_type` and `dim_rate_code` seeds into `marts` schema
- Seeds are static reference data — re-seeding is always safe
- `{dbt_bin}` is the verified path from Step 1 (e.g. `dbt` or full path)

**Task 3: `dbt_run`**

```python
BashOperator(
    task_id='dbt_run',
    bash_command='cd /opt/airflow/dbt && {dbt_bin} run --profiles-dir .',
)
```

- Rebuilds all models: staging → dims → facts
- Full-refresh tables; idempotent by design
- `retries=1` inherited from `default_args`

**Task 4: `dbt_test`**

```python
BashOperator(
    task_id='dbt_test',
    bash_command='cd /opt/airflow/dbt && {dbt_bin} test --profiles-dir .',
    retries=0,    # test failures are data issues, not transient errors
)
```

- Runs all 33 schema tests + 2 singular tests
- `retries=0` — a test failure means bad data, not a flaky process

### Dependency chain

```python
ingest_trips >> dbt_seed >> dbt_run >> dbt_test
```

### Template and import notes

- Use `from airflow.models.param import Param` for typed DAG params
- Use `from datetime import datetime, timedelta` — not `pendulum` for simplicity
- Use the `@dag` decorator pattern (Airflow 2.x TaskFlow style) OR classic
  `with DAG(...)` block — either is fine; use `with DAG(...)` for clarity
- Reference `{{ params.year }}` and `{{ params.month }}` via Jinja in
  `bash_command` strings — these are available because `render_template_as_native_obj=False` (default)

---

## Step 4 — Verify the DAG in Airflow UI

After creating the DAG file:

1. Check it appears in the Airflow UI at `http://localhost:8080` (airflow/airflow)
2. Confirm no import errors in the DAG detail view
3. Trigger manually: **Trigger DAG w/ config** → `{"year": 2024, "month": 1}`
4. Watch all 4 tasks turn green in the Graph view
5. Verify in Postgres:
   ```sql
   SELECT * FROM raw.ingestion_log ORDER BY loaded_at DESC LIMIT 3;
   SELECT COUNT(*) FROM marts.fact_trips;
   ```
6. Trigger a second time for the same month — confirm row counts are unchanged (idempotency)

---

## Definition of Done

- [ ] `docker compose exec airflow-scheduler dbt --version` prints dbt version
- [ ] `cd dbt && .venv/bin/dbt debug --profiles-dir .` still passes locally
- [ ] DAG appears in Airflow UI with no import errors
- [ ] Manual trigger for year=2024, month=1 completes with all 4 tasks green
- [ ] `raw.ingestion_log` has a new `success` row after the run
- [ ] Re-triggering for the same month leaves `fact_trips` row count unchanged
