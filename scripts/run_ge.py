"""
Great Expectations data quality validation for raw.yellow_taxi_trips.

Runs 10 expectations against the target month's rows in Postgres and saves
a self-contained HTML report to docs/ge_report/YYYY-MM.html.

Always exits 0 (soft fail) — the Airflow pipeline continues even when checks
fail. Failures are visible in the HTML report and in the Airflow task logs.
A hard-fail mode can be added later by changing the exit code.

Usage:
    python scripts/run_ge.py --year 2024 --month 1
"""

import calendar
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import click
from dotenv import load_dotenv
from jinja2 import Template

# great_expectations is imported lazily inside run_validation() so this module
# can be imported (and unit-tested) on Python 3.14, where GE's Pydantic V1
# dependency is incompatible. GE runs correctly in the Airflow container (Python 3.11).

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger(__name__)

# HTML reports land here; directory is gitignored (generated at runtime)
REPORT_DIR = Path(__file__).parent.parent / "docs" / "ge_report"

# ── HTML report template (inline so the script is self-contained) ─────────────

HTML_TEMPLATE = """\
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Data Quality Report — {{ month_name }}</title>
  <style>
    * { box-sizing: border-box; }
    body {
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, sans-serif;
      margin: 0; padding: 2rem; color: #1f2937; background: #f3f4f6;
    }
    .card {
      max-width: 940px; margin: 0 auto; background: #fff;
      border-radius: 10px; padding: 2rem 2.5rem;
      box-shadow: 0 1px 6px rgba(0,0,0,.08);
    }
    h1 { font-size: 1.4rem; margin: 0 0 .2rem; }
    .subtitle { font-size: .95rem; color: #6b7280; margin: 0 0 1.5rem; }
    h3 {
      font-size: .78rem; font-weight: 700; text-transform: uppercase;
      letter-spacing: .07em; color: #9ca3af; margin: 1.75rem 0 .6rem;
    }
    .banner {
      padding: .9rem 1.3rem; border-radius: 6px;
      font-size: 1.2rem; font-weight: 700; margin-bottom: 1.5rem;
    }
    .banner.pass { background: #d1fae5; color: #065f46; border-left: 5px solid #10b981; }
    .banner.fail { background: #fee2e2; color: #7f1d1d; border-left: 5px solid #ef4444; }
    table { width: 100%; border-collapse: collapse; font-size: .875rem; }
    th {
      background: #f8fafc; text-align: left; padding: .55rem .9rem;
      font-size: .72rem; text-transform: uppercase; letter-spacing: .06em;
      color: #6b7280; border-bottom: 2px solid #e5e7eb;
    }
    td { padding: .65rem .9rem; border-bottom: 1px solid #f3f4f6; vertical-align: middle; }
    tr:last-child td { border-bottom: none; }
    tr.fail-row { background: #fff7f7; }
    .meta td:first-child { font-weight: 500; color: #374151; width: 155px; }
    .badge {
      display: inline-block; padding: 2px 9px; border-radius: 999px;
      font-size: .72rem; font-weight: 700;
    }
    .pass-badge { background: #d1fae5; color: #065f46; }
    .fail-badge { background: #fee2e2; color: #b91c1c; }
    .mono { font-family: "SFMono-Regular", Consolas, monospace; font-size: .85em; }
    .footer { margin-top: 2rem; font-size: .7rem; color: #d1d5db; text-align: right; }
  </style>
</head>
<body>
  <div class="card">
    <h1>NYC Taxi — Data Quality Report</h1>
    <p class="subtitle">{{ month_name }}</p>

    <div class="banner {{ 'pass' if all_passed else 'fail' }}">
      {{ passed_count }}/{{ total_count }} checks passed
    </div>

    <h3>Run Details</h3>
    <table class="meta">
      <tr><td>Period</td><td>{{ month_name }}</td></tr>
      <tr><td>Validated at</td><td>{{ run_timestamp }}</td></tr>
      <tr><td>Rows validated</td><td class="mono">{{ row_count }}</td></tr>
      <tr><td>Fail mode</td><td>Soft fail — pipeline continues regardless</td></tr>
    </table>

    <h3>Expectation Results</h3>
    <table>
      <thead>
        <tr>
          <th>#</th>
          <th>Column</th>
          <th>Check</th>
          <th>Threshold</th>
          <th>Observed</th>
          <th>Result</th>
        </tr>
      </thead>
      <tbody>
        {% for r in results %}
        <tr class="{{ 'fail-row' if not r.passed else '' }}">
          <td>{{ loop.index }}</td>
          <td class="mono">{{ r.column }}</td>
          <td>{{ r.description }}</td>
          <td class="mono">{{ r.threshold }}</td>
          <td class="mono">{{ r.observed }}</td>
          <td>
            {% if r.passed %}
            <span class="badge pass-badge">PASS</span>
            {% else %}
            <span class="badge fail-badge">FAIL</span>
            {% endif %}
          </td>
        </tr>
        {% endfor %}
      </tbody>
    </table>

    <p class="footer">Generated by scripts/run_ge.py &middot; Great Expectations</p>
  </div>
</body>
</html>
"""

# ── Expectation definitions ───────────────────────────────────────────────────


def get_expectation_specs(year: int, month: int) -> list[dict[str, Any]]:
    """
    Return the 10 expectation specs as plain dicts.

    Each dict has:
      type        — GE expectation class name (in gx.expectations)
      kwargs      — passed to the expectation constructor
      description — human-readable label for the HTML report
      column      — column being checked ("(table)" for table-level checks)
      threshold   — human-readable threshold string for the report
    """
    last_day = calendar.monthrange(year, month)[1]
    month_label = datetime(year, month, 1).strftime("%B %Y")

    return [
        {
            "type": "ExpectTableRowCountToBeBetween",
            "kwargs": {"min_value": 1_000_000, "max_value": 6_000_000},
            "description": "Row count in expected range",
            "column": "(table)",
            "threshold": "1,000,000 – 6,000,000",
        },
        {
            "type": "ExpectColumnValuesToNotBeNull",
            "kwargs": {"column": "pickup_datetime"},
            "description": "No null pickup datetimes",
            "column": "pickup_datetime",
            "threshold": "0 nulls",
        },
        {
            "type": "ExpectColumnValuesToNotBeNull",
            "kwargs": {"column": "dropoff_datetime"},
            "description": "No null dropoff datetimes",
            "column": "dropoff_datetime",
            "threshold": "0 nulls",
        },
        {
            "type": "ExpectColumnValuesToBeBetween",
            "kwargs": {"column": "fare_amount", "min_value": 0},
            "description": "Fare amount ≥ 0",
            "column": "fare_amount",
            "threshold": "≥ 0",
        },
        {
            "type": "ExpectColumnValuesToBeBetween",
            "kwargs": {"column": "trip_distance", "min_value": 0},
            "description": "Trip distance ≥ 0",
            "column": "trip_distance",
            "threshold": "≥ 0",
        },
        {
            "type": "ExpectColumnValuesToBeBetween",
            "kwargs": {"column": "pickup_location_id", "min_value": 1, "max_value": 265},
            "description": "Pickup zone ID in valid range",
            "column": "pickup_location_id",
            "threshold": "1 – 265",
        },
        {
            "type": "ExpectColumnValuesToBeBetween",
            "kwargs": {"column": "dropoff_location_id", "min_value": 1, "max_value": 265},
            "description": "Dropoff zone ID in valid range",
            "column": "dropoff_location_id",
            "threshold": "1 – 265",
        },
        {
            "type": "ExpectColumnValuesToBeInSet",
            "kwargs": {"column": "payment_type", "value_set": [1, 2, 3, 4, 5, 6]},
            "description": "Payment type is a known code (1–6)",
            "column": "payment_type",
            "threshold": "{1, 2, 3, 4, 5, 6}",
        },
        {
            "type": "ExpectColumnValuesToBeBetween",
            "kwargs": {"column": "total_amount", "min_value": 0},
            "description": "Total amount ≥ 0",
            "column": "total_amount",
            "threshold": "≥ 0",
        },
        {
            "type": "ExpectColumnValuesToBeBetween",
            "kwargs": {
                "column": "pickup_datetime",
                "min_value": f"{year}-{month:02d}-01",
                "max_value": f"{year}-{month:02d}-{last_day}",
            },
            "description": f"Pickup datetime within {month_label}",
            "column": "pickup_datetime",
            "threshold": f"{year}-{month:02d}-01 – {year}-{month:02d}-{last_day}",
        },
    ]


# ── Validation ────────────────────────────────────────────────────────────────


def _build_connection_string() -> str:
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "nyctaxi")
    user = os.getenv("POSTGRES_USER", "nyctaxi")
    password = os.getenv("POSTGRES_PASSWORD", "nyctaxi")
    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"


def _month_query(year: int, month: int) -> str:
    """SQL that scopes the asset to the target month only (server-side filter)."""
    last_day = calendar.monthrange(year, month)[1]
    return (
        "SELECT * FROM raw.yellow_taxi_trips "
        f"WHERE pickup_datetime >= '{year}-{month:02d}-01' "
        f"  AND pickup_datetime <= '{year}-{month:02d}-{last_day} 23:59:59'"
    )


def _format_observed(result_dict: dict) -> str:
    """Extract a human-readable observed value from a GE ExpectationValidationResult."""
    if "observed_value" in result_dict:
        val = result_dict["observed_value"]
        # Row count comes back as a large int — format with commas
        if isinstance(val, int) and abs(val) >= 10_000:
            return f"{val:,}"
        return str(val)
    if "unexpected_percent" in result_dict:
        return f"{result_dict['unexpected_percent']:.2f}% unexpected"
    return "—"


def run_validation(year: int, month: int) -> list[dict[str, Any]]:
    """
    Run GE against raw.yellow_taxi_trips for the given month.

    Uses GE 0.18.x Fluent API with SqlAlchemyExecutionEngine so all
    expectations execute server-side — no full table scan into Python memory.
    Returns a list of result dicts (one per expectation) with keys from
    get_expectation_specs() plus:
      passed   — bool
      observed — human-readable observed value string
    """
    import great_expectations as gx  # lazy: avoids Python 3.14 import error in tests

    specs = get_expectation_specs(year, month)

    log.info("Setting up GE EphemeralDataContext for %d-%02d", year, month)
    context = gx.get_context(mode="ephemeral")

    # Postgres Fluent datasource — expectations run via SQLAlchemy in the DB
    datasource = context.sources.add_postgres(
        name="taxi_postgres",
        connection_string=_build_connection_string(),
    )

    # Query asset scoped to the target month (avoids scanning all 41M rows)
    asset = datasource.add_query_asset(
        name=f"trips_{year}_{month:02d}",
        query=_month_query(year, month),
    )

    batch_request = asset.build_batch_request()

    # GE 0.18.x pattern: expectation suite + validator
    suite_name = "raw_trips_suite"
    context.add_or_update_expectation_suite(expectation_suite_name=suite_name)
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=suite_name,
    )

    # Expectation class names map to snake_case validator methods
    _method = {
        "ExpectTableRowCountToBeBetween": "expect_table_row_count_to_be_between",
        "ExpectColumnValuesToNotBeNull": "expect_column_values_to_not_be_null",
        "ExpectColumnValuesToBeBetween": "expect_column_values_to_be_between",
        "ExpectColumnValuesToBeInSet": "expect_column_values_to_be_in_set",
    }

    for spec in specs:
        getattr(validator, _method[spec["type"]])(**spec["kwargs"])

    log.info("Running %d expectations", len(specs))
    validation_result = validator.validate()

    results: list[dict[str, Any]] = []
    for spec, exp_result in zip(specs, validation_result.results):
        result_dict: dict = exp_result.result if hasattr(exp_result, "result") else {}
        results.append(
            {
                **spec,
                "passed": bool(exp_result.success),
                "observed": _format_observed(result_dict),
            }
        )

    return results


def _error_results(year: int, month: int) -> list[dict[str, Any]]:
    """Placeholder results when GE crashes — ensures a report is always written."""
    return [
        {**spec, "passed": False, "observed": "Error — see Airflow logs"}
        for spec in get_expectation_specs(year, month)
    ]


# ── Reporting ─────────────────────────────────────────────────────────────────


def render_report(year: int, month: int, results: list[dict[str, Any]]) -> str:
    """Render the HTML report from validation results."""
    passed = sum(1 for r in results if r["passed"])
    total = len(results)

    # Row count comes from the first expectation (ExpectTableRowCountToBeBetween)
    row_count = results[0]["observed"] if results else "—"

    return Template(HTML_TEMPLATE).render(
        month_name=datetime(year, month, 1).strftime("%B %Y"),
        run_timestamp=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        row_count=row_count,
        passed_count=passed,
        total_count=total,
        all_passed=(passed == total),
        results=results,
    )


def save_report(year: int, month: int, html: str) -> Path:
    """Write the HTML report to docs/ge_report/YYYY-MM.html."""
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    path = REPORT_DIR / f"{year}-{month:02d}.html"
    path.write_text(html, encoding="utf-8")
    return path


# ── CLI entry point ───────────────────────────────────────────────────────────


@click.command()
@click.option("--year", required=True, type=int, help="4-digit year (e.g. 2024)")
@click.option("--month", required=True, type=int, help="Month number 1–12")
def main(year: int, month: int) -> None:
    log.info("Starting GE validation for %d-%02d", year, month)

    try:
        results = run_validation(year, month)
    except Exception:
        log.exception("GE validation crashed — saving error report and continuing")
        results = _error_results(year, month)

    passed = sum(1 for r in results if r["passed"])
    total = len(results)

    html = render_report(year, month, results)
    report_path = save_report(year, month, html)
    log.info("Report saved: %s", report_path)

    if passed == total:
        log.info("GE validation passed: %d/%d checks passed", passed, total)
    else:
        log.warning(
            "GE validation: %d/%d checks failed — see report at %s",
            total - passed,
            total,
            report_path,
        )
    # Always exit 0 — soft fail, pipeline continues


if __name__ == "__main__":
    main()
