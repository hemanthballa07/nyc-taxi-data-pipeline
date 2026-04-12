"""Unit tests for scripts/ingest.py — no DB, no network calls."""

import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

# Allow importing from scripts/ without installing as a package
sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from ingest import (
    COLUMN_MAP,
    DB_COLUMNS,
    REQUIRED_COLUMNS,
    build_url,
    validate,
)

# ── Helpers ───────────────────────────────────────────────────────────────────


def make_parquet(tmp_path: Path, df: pd.DataFrame) -> Path:
    path = tmp_path / "trips.parquet"
    pq.write_table(pa.Table.from_pandas(df, preserve_index=False), path)
    return path


def make_valid_df(year: int = 2024, month: int = 1, rows: int = 500_000) -> pd.DataFrame:
    """Return a minimal DataFrame that passes all validation checks."""
    pickup = datetime(year, month, 15, 12, 0, 0)
    data: dict = {col: [None] * rows for col in REQUIRED_COLUMNS}
    data["tpep_pickup_datetime"] = [pickup] * rows
    return pd.DataFrame(data)


# ── Column validation ─────────────────────────────────────────────────────────


def test_validate_columns_ok(tmp_path):
    """A file with all required columns passes without error."""
    path = make_parquet(tmp_path, make_valid_df())
    count = validate(path, 2024, 1)
    assert count == 500_000


def test_validate_columns_missing(tmp_path):
    """A file missing a required column raises ValueError."""
    df = make_valid_df().drop(columns=["fare_amount"])
    path = make_parquet(tmp_path, df)
    with pytest.raises(ValueError, match="Missing required columns"):
        validate(path, 2024, 1)


# ── Row count validation ───────────────────────────────────────────────────────


def test_validate_empty_file_raises(tmp_path):
    """A file with 0 rows raises ValueError."""
    path = make_parquet(tmp_path, make_valid_df(rows=0))
    with pytest.raises(ValueError, match="0 rows"):
        validate(path, 2024, 1)


def test_validate_small_file_warns(tmp_path, caplog):
    """A file with <10k rows logs a warning but does not raise."""
    import logging

    path = make_parquet(tmp_path, make_valid_df(rows=5_000))
    with caplog.at_level(logging.WARNING):
        count = validate(path, 2024, 1)

    assert count == 5_000
    assert any("unusually low" in r.message for r in caplog.records)


# ── Date range validation ─────────────────────────────────────────────────────


def test_validate_date_range_warn(tmp_path, caplog):
    """When <80% of pickups are in the expected month, a warning is logged but no exception raised."""
    import logging

    rows = 500_000
    df = make_valid_df(rows=rows)
    wrong_month = datetime(2023, 12, 15, 12, 0, 0)
    right_month = datetime(2024, 1, 15, 12, 0, 0)
    cutoff = int(rows * 0.9)
    df["tpep_pickup_datetime"] = [wrong_month] * cutoff + [right_month] * (rows - cutoff)

    path = make_parquet(tmp_path, df)
    with caplog.at_level(logging.WARNING):
        count = validate(path, 2024, 1)

    assert count == rows
    assert any("spillover" in r.message for r in caplog.records)


# ── Column rename mapping ─────────────────────────────────────────────────────


def test_column_rename():
    """After applying COLUMN_MAP, the DataFrame has DB column names and no Parquet names."""
    df = pd.DataFrame({col: [1] for col in REQUIRED_COLUMNS})
    renamed = df.rename(columns=COLUMN_MAP)

    assert list(renamed.columns) == DB_COLUMNS
    parquet_only = set(REQUIRED_COLUMNS) - set(DB_COLUMNS)
    for col in parquet_only:
        assert col not in renamed.columns


# ── URL construction ──────────────────────────────────────────────────────────


def test_build_url():
    assert build_url(2024, 1) == (
        "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
    )


def test_build_url_zero_pads_month():
    assert build_url(2024, 9).endswith("2024-09.parquet")
