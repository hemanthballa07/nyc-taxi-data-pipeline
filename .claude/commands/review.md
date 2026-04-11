---
name: review
description: Review recent changes for quality before committing.
disable-model-invocation: true
---

Review the changes for: $ARGUMENTS

1. Run `git diff` to see what's changed
2. Check against project standards in CLAUDE.md:
   - Python: type hints present? `logging` instead of `print`? `ruff` clean?
   - SQL/dbt: lowercase keywords? CTEs used? `schema.yml` entry exists?
   - All new files: do they belong in the right directory per project structure?
3. Check for common mistakes:
   - Hardcoded credentials or paths
   - Missing error handling
   - Missing docstrings on functions
   - Data files accidentally staged for commit
4. Run tests if applicable:
   - `python -m pytest tests/` for Python
   - `cd dbt && dbt test` for dbt models
5. Summarize: what's good, what needs fixing before commit
