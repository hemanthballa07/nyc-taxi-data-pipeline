---
name: status
description: Check the current state of the project — what's built, what's running, what's left.
disable-model-invocation: true
---

Check the project status by:

1. **Read living docs first** (this is the fastest way to get context):
   - `docs/plan.md` — current phase, completed tasks
   - `docs/architecture.md` — Current State tables
   - `docs/changelog.md` — last 5 entries

2. **Verify against reality** (only if services are running):
   - `docker compose ps` — which services are up
   - Check `data/raw/` for downloaded Parquet files
   - If Postgres is running, verify table row counts:
     ```sql
     SELECT count(*) FROM raw.yellow_taxi_trips;
     ```
   - Check if dbt models exist: `ls dbt/models/staging/ dbt/models/marts/ 2>/dev/null`

3. **Report** in this format:
   - Current phase and progress
   - What's working
   - What's next (the specific next task from plan.md)
   - Any discrepancies between docs and reality (e.g., docs say a table exists but it doesn't)

Keep the report short — 10-15 lines max.
