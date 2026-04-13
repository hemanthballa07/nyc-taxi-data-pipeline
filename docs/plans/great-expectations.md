# Implementation Plan: Great Expectations Data Quality Layer

**Status:** Approved — ready to implement  
**Approved:** 2026-04-12  
**Approach:** Option 1 — standalone script, EphemeralDataContext, custom HTML report  
**Fail mode:** Soft fail (saves report and logs warnings; pipeline continues)

---

## Goal

Automated data quality checks on `raw.yellow_taxi_trips` after each monthly ingestion.
A per-month HTML report is saved to `docs/ge_report/YYYY-MM.html`.
The check runs as a new `ge_validate` task in the Airflow DAG, inserted between
`ingest_trips` and `dbt_seed`.

---

## Files to Create or Modify

| File | Action | Notes |
|------|--------|-------|
| `scripts/run_ge.py` | **Create** | Main GE script; accepts `--year` / `--month` args |
| `tests/test_run_ge.py` | **Create** | Unit tests for the script |
| `dags/nyc_taxi_monthly.py` | **Modify** | Add `ge_validate` task after `ingest_trips` |
| `requirements.txt` | **Modify** | Add `great-expectations>=1.0`, `sqlalchemy>=2.0` |
| `.gitignore` | **Modify** | Add `docs/ge_report/` (generated at runtime) |
| `docs/architecture.md` | **Modify** | Update pipeline diagram and component table |
| `docs/changelog.md` | **Modify** | Log the change |
| `docs/plan.md` | **Modify** | Add GE tasks under a new Phase 7 |

---

## Script Design: `scripts/run_ge.py`

### Entry point
```
python scripts/run_ge.py --year 2024 --month 1
```
Exits 0 always (soft fail). Logs a warning if any expectations fail.

### Connection
Uses `sqlalchemy` to connect to Postgres via the same env vars as `ingest.py`:
`PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD`, `PGDATABASE` (defaulting to the
values in `.env`).

### GE setup
- `EphemeralDataContext` — no files written to disk by GE itself
- `SqlAlchemyExecutionEngine` — all expectations run server-side; no full table
  scan into memory
- One `ExpectationSuite` named `raw_yellow_taxi_trips`
- One `Validator` scoped to the target month via a `query` asset:
  ```sql
  SELECT * FROM raw.yellow_taxi_trips
  WHERE date_trunc('month', pickup_datetime) = '2024-01-01'
  ```

### The 10 Expectations

| # | Expectation | Parameters | Column |
|---|-------------|------------|--------|
| 1 | `expect_table_row_count_to_be_between` | min=1_000_000, max=6_000_000 | — |
| 2 | `expect_column_values_to_not_be_null` | — | `pickup_datetime` |
| 3 | `expect_column_values_to_not_be_null` | — | `dropoff_datetime` |
| 4 | `expect_column_values_to_be_between` | min=0 | `fare_amount` |
| 5 | `expect_column_values_to_be_between` | min=0 | `trip_distance` |
| 6 | `expect_column_values_to_be_between` | min=1, max=265 | `pickup_location_id` |
| 7 | `expect_column_values_to_be_between` | min=1, max=265 | `dropoff_location_id` |
| 8 | `expect_column_values_to_be_in_set` | value_set={1,2,3,4,5,6} | `payment_type` |
| 9 | `expect_column_values_to_be_between` | min=0 | `total_amount` |
| 10 | `expect_column_values_to_be_between` | min=YYYY-MM-01, max=YYYY-MM-last-day | `pickup_datetime` |

Expectation 10 uses the `--year` / `--month` args to compute the date bounds
dynamically, catching spillover rows that fall outside the target month.

### Validation result handling
- If all 10 pass: log `INFO "GE validation passed: 10/10"`, exit 0
- If any fail: log `WARNING "GE validation failed: N/10 — see report"`, exit 0
  (soft fail — pipeline continues; Airflow task stays green)
- Either way: render and save the HTML report before exiting

### HTML report
Hand-rolled Jinja2 template (single `.html` file, no external assets).
Saved to `docs/ge_report/YYYY-MM.html`. The script creates the directory if
it doesn't exist.

Report sections:
1. **Header** — "NYC Taxi Data Quality Report — January 2024"
2. **Summary banner** — "10/10 checks passed" (green) or "8/10 checks passed" (red)
3. **Run metadata table** — timestamp, year, month, total row count validated
4. **Expectations table** — one row per check:
   - Column name
   - Description (human-readable, not the GE method name)
   - Result: PASS (green) or FAIL (red)
   - Observed value (e.g. actual row count, null %, min value found)
   - Threshold (e.g. "between 1M and 6M")

No JavaScript, no external CSS — pure inline styles so the file is
self-contained and renders anywhere.

---

## DAG Change: `dags/nyc_taxi_monthly.py`

New task inserted between `ingest_trips` and `dbt_seed`:

```python
ge_validate = BashOperator(
    task_id="ge_validate",
    bash_command=(
        f"python {SCRIPTS_DIR}/scripts/run_ge.py "
        "--year {{ params.year }} --month {{ params.month }}"
    ),
)

ingest_trips >> ge_validate >> dbt_seed >> dbt_run >> dbt_test
```

No `soft_fail=True` needed on the operator itself — the script always exits 0.
Failures are visible in the Airflow logs, not in the task state.

---

## Tests: `tests/test_run_ge.py`

Three unit tests (no live database required — mock the SQLAlchemy connection):

1. **`test_all_pass`** — mock validation result where all 10 expectations pass;
   assert exit code 0 and HTML file written
2. **`test_some_fail`** — mock result where 2 expectations fail;
   assert exit code still 0 (soft fail) and report shows FAIL rows
3. **`test_report_content`** — parse the generated HTML and assert the summary
   banner text, table row count (10), and metadata fields are present

---

## Dependency Pinning

Add to `requirements.txt`:
```
great-expectations>=1.0,<2.0
sqlalchemy>=2.0,<3.0
jinja2>=3.1.0        # likely already pulled in transitively; pin explicitly
```

GE 1.x requires SQLAlchemy 2.x. Verify no conflict with existing `psycopg2-binary`.

---

## Risks and Mitigations

| Risk | Mitigation |
|------|-----------|
| GE 1.x API changed significantly from 0.18 — online examples are mostly old | Use only the Fluent API (`context.sources.add_sql(...)`) and GE 1.x docs |
| `SqlAlchemyExecutionEngine` may pull full table into memory on some expectations | Scope the asset to a single month via a query; test row count before building |
| SQLAlchemy 2.x + psycopg2 dialect changes | Use `postgresql+psycopg2://` connection string; test connection before adding GE |
| `docs/ge_report/` grows unbounded over months | Out of scope for now; note in docs that reports are gitignored and local-only |
| Jinja2 already in venv (pulled by dbt) but version may conflict | Pin explicitly; run `pip check` after install |

---

## Definition of Done

- [ ] `scripts/run_ge.py` runs locally: `python scripts/run_ge.py --year 2024 --month 1`
- [ ] HTML report appears at `docs/ge_report/2024-01.html` and opens correctly in a browser
- [ ] All 10 expectations evaluate (pass or fail) without crashing
- [ ] DAG `nyc_taxi_monthly` runs end-to-end with the new `ge_validate` task green
- [ ] `tests/test_run_ge.py` — all 3 tests pass via `make test`
- [ ] `docs/ge_report/` is gitignored
- [ ] `docs/architecture.md`, `docs/changelog.md`, `docs/plan.md` updated
