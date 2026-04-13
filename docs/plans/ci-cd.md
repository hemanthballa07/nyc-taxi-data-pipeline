# CI/CD with GitHub Actions — Implementation Plan

**Approved:** 2026-04-13
**Approach:** Option 1 — pytest + dbt compile

---

## Problem

`pytest tests/` can run in CI with zero infrastructure (both test files are fully mocked).
`dbt test` cannot — it queries 41M rows of live data that only exists locally.
The right CI split: Python unit tests in GitHub Actions, full dbt data tests in Airflow.

---

## What Gets Built

One new file: `.github/workflows/ci.yml`

Nothing else changes.

---

## Workflow Design

**Triggers:** `push` to any branch, `pull_request` to any branch.

**Steps (single job, `ubuntu-latest`):**

1. `actions/checkout@v4` — check out the repo
2. `actions/setup-python@v5` with `python-version: '3.11'` — matches dbt's supported range
3. `pip install -r requirements.txt` — installs pytest, pyarrow, psycopg2-binary, etc.
4. `pytest tests/ -v` — runs all unit tests; step fails the job on any failure
5. `pip install dbt-core dbt-postgres` — minimal dbt install, no version pin needed for compile
6. `dbt compile --profiles-dir dbt/ --project-dir dbt/` — renders all Jinja2 and validates SQL syntax; no database connection required for compile

**No Postgres service container needed.** `dbt compile` is a pure parse/render step.

---

## What This Catches

| Check | Caught by |
|---|---|
| Broken Python imports | pytest |
| Logic bugs in ingest.py / run_ge.py | pytest |
| SQL syntax errors in dbt models | dbt compile |
| Bad Jinja2 in models (e.g. undefined var) | dbt compile |
| Data constraint violations | dbt test (runs in Airflow, not CI) |
| Wrong row counts | dbt test (runs in Airflow, not CI) |

---

## What Could Go Wrong

- **`dbt compile` needs a profiles.yml target** — the existing `dbt/profiles.yml` uses `env_var()` for credentials. Since compile doesn't connect to a DB, it will still try to parse profiles.yml. Solution: pass `--profiles-dir dbt/` and ensure the profile parses cleanly even with missing env vars. If it errors, add a minimal `profiles.yml` override or use `--no-version-check`.
- **`requirements.txt` installs `great-expectations==0.18.22`** — GE has heavy dependencies and may be slow to install. Not a blocker, just adds ~60s to CI. Accept the slowness for now.
- **`psycopg2-binary` on Linux** — the binary wheel should work on `ubuntu-latest` without issue.

---

## Files

| File | Action |
|---|---|
| `.github/workflows/ci.yml` | Create |

---

## Success Criteria

- Workflow appears in GitHub Actions tab on next push
- `pytest tests/` step shows all tests green in CI logs
- `dbt compile` step completes without errors
- Any future push that breaks a test or dbt model causes the workflow to fail
