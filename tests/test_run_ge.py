"""
Unit tests for scripts/run_ge.py.

No database or network required — run_validation() is mocked so all tests
run purely against the render/save/CLI logic.
"""

import sys
from pathlib import Path
from unittest.mock import patch

import pytest
from click.testing import CliRunner

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

import run_ge


# ── Helpers ───────────────────────────────────────────────────────────────────


def _make_results(num_pass: int = 10) -> list[dict]:
    """
    Build a fake result list using the real spec definitions.
    The first result (row count) always gets a plausible observed value.
    """
    specs = run_ge.get_expectation_specs(2024, 1)
    results = []
    for i, spec in enumerate(specs):
        passed = i < num_pass
        if i == 0:
            observed = "2,964,606"  # row count for Jan 2024
        elif passed:
            observed = "0.00% unexpected"
        else:
            observed = "2.34% unexpected"
        results.append({**spec, "passed": passed, "observed": observed})
    return results


# ── Tests ─────────────────────────────────────────────────────────────────────


def test_all_pass_exits_zero_and_writes_report(tmp_path: Path) -> None:
    """All 10 checks pass → exit 0, report written, summary shows 10/10."""
    results = _make_results(num_pass=10)

    with (
        patch("run_ge.run_validation", return_value=results),
        patch("run_ge.REPORT_DIR", tmp_path),
    ):
        outcome = CliRunner().invoke(run_ge.main, ["--year", "2024", "--month", "1"])

    assert outcome.exit_code == 0
    report = tmp_path / "2024-01.html"
    assert report.exists(), "report file should be created"
    assert "10/10 checks passed" in report.read_text()


def test_some_fail_still_exits_zero(tmp_path: Path) -> None:
    """Soft fail: 2 checks fail → exit 0 (pipeline continues), report shows FAIL rows."""
    results = _make_results(num_pass=8)

    with (
        patch("run_ge.run_validation", return_value=results),
        patch("run_ge.REPORT_DIR", tmp_path),
    ):
        outcome = CliRunner().invoke(run_ge.main, ["--year", "2024", "--month", "1"])

    assert outcome.exit_code == 0
    content = (tmp_path / "2024-01.html").read_text()
    assert "8/10 checks passed" in content
    assert "fail-badge" in content  # at least one FAIL badge rendered


def test_ge_crash_still_exits_zero(tmp_path: Path) -> None:
    """If GE crashes entirely, an error report is written and exit code is still 0."""
    with (
        patch("run_ge.run_validation", side_effect=RuntimeError("DB connection refused")),
        patch("run_ge.REPORT_DIR", tmp_path),
    ):
        outcome = CliRunner().invoke(run_ge.main, ["--year", "2024", "--month", "1"])

    assert outcome.exit_code == 0
    report = tmp_path / "2024-01.html"
    assert report.exists(), "error report should still be written"
    assert "0/10 checks passed" in report.read_text()


def test_report_html_structure() -> None:
    """render_report produces valid HTML with 10 rows, metadata, and summary banner."""
    results = _make_results(num_pass=10)
    html = run_ge.render_report(2024, 1, results)

    assert "January 2024" in html
    assert "10/10 checks passed" in html
    # Banner should have the pass class, not fail
    assert 'class="banner pass"' in html
    # 10 PASS badges, 0 FAIL badge spans (CSS class definition doesn't count)
    assert html.count('class="badge pass-badge"') == 10
    assert 'class="badge fail-badge"' not in html
    # Row count from first expectation is shown
    assert "2,964,606" in html
    # Timestamp present
    assert "UTC" in html


def test_report_html_fail_structure() -> None:
    """When checks fail, report banner has fail class and FAIL badges appear."""
    results = _make_results(num_pass=7)
    html = run_ge.render_report(2024, 1, results)

    assert "7/10 checks passed" in html
    assert 'class="banner fail"' in html
    assert html.count('class="badge fail-badge"') == 3
    assert html.count('class="badge pass-badge"') == 7


def test_get_expectation_specs_count() -> None:
    """get_expectation_specs returns exactly 10 specs for any month."""
    assert len(run_ge.get_expectation_specs(2024, 1)) == 10
    assert len(run_ge.get_expectation_specs(2024, 12)) == 10


def test_get_expectation_specs_last_day() -> None:
    """Expectation #10 (pickup_datetime range) uses the correct last day of the month."""
    specs = run_ge.get_expectation_specs(2024, 2)  # Feb 2024 (leap year)
    last_spec = specs[-1]
    assert "2024-02-29" in last_spec["threshold"]
    assert last_spec["kwargs"]["max_value"] == "2024-02-29"
