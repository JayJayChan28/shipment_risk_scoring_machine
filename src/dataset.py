"""Lightweight helpers for loading the Silver datasets.

Keep this module side-effect free so importing it does not immediately read
the full parquet tables into memory.
"""

from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SILVER_POSITION = ROOT / "data" / "processed" / "silver" / "position"
SILVER_STATIC = ROOT / "data" / "processed" / "silver" / "static"

POSITION_COLUMNS = [
    "timestamp",
    "mmsi",
    "latitude",
    "longitude",
    "sog",
    "cog",
    "true_heading",
    "navigational_status",
    "event_date",
]

STATIC_COLUMNS = [
    "timestamp",
    "mmsi",
    "ship_name",
    "ship_type",
    "call_sign",
    "imo_number",
    "destination",
    "max_draught",
    "event_date",
]


def _date_filters(
    start_date: str | None = None,
    end_date: str | None = None,
) -> list[tuple[str, str, str]] | None:
    """Build parquet partition filters for the event_date column."""

    filters: list[tuple[str, str, str]] = []

    if start_date is not None:
        filters.append(("event_date", ">=", start_date))

    if end_date is not None:
        filters.append(("event_date", "<=", end_date))

    return filters or None


def load_position(
    columns: list[str] | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
) -> pd.DataFrame:
    """Load the Silver position table.

    By default, only the columns needed for feature engineering are read.
    """

    selected = columns or POSITION_COLUMNS
    return pd.read_parquet(
        SILVER_POSITION,
        columns=selected,
        filters=_date_filters(start_date, end_date),
    )


def load_static(
    columns: list[str] | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
) -> pd.DataFrame:
    """Load the Silver static table.

    By default, only the columns needed for joining vessel metadata are read.
    """

    selected = columns or STATIC_COLUMNS
    return pd.read_parquet(
        SILVER_STATIC,
        columns=selected,
        filters=_date_filters(start_date, end_date),
    )


def load_silver(
    columns_position: list[str] | None = None,
    columns_static: list[str] | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load both Silver tables with narrow column selection."""

    pos = load_position(columns_position, start_date=start_date, end_date=end_date)
    sta = load_static(columns_static, start_date=start_date, end_date=end_date)
    return pos, sta


def build_training_frame(
    start_date: str | None = None,
    end_date: str | None = None,
    sample_n: int | None = None,
) -> pd.DataFrame:
    """Return a basic joined frame for notebook exploration.

    This keeps the join on MMSI only; static rows are reduced to the latest
    record per vessel before the merge so the frame stays compact.
    """

    pos, sta = load_silver(start_date=start_date, end_date=end_date)

    if sample_n is not None and len(pos) > sample_n:
        pos = pos.sample(sample_n, random_state=42)

    latest_static = (
        sta.sort_values("timestamp")
        .groupby("mmsi", as_index=False)
        .tail(1)
        .drop(columns=["timestamp", "event_date"], errors="ignore")
    )

    return pos.merge(latest_static, on="mmsi", how="left")
