import math

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.fs as pafs

from src.ETL.Silver_Layer import _build_s3_filesystem, _parse_s3_uri

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# navigational_status == 1  → "At anchor"
# low SOG is a common secondary anchor indicator
_ANCHOR_STATUS = 1
_ANCHOR_SOG_THRESHOLD = 0.5  # knots


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _haversine_nm(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in nautical miles between two (lat, lon) points."""
    R_nm = 3440.065  # Earth radius in nautical miles
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return 2 * R_nm * math.asin(math.sqrt(a))


def _total_distance_nm(lats: pd.Series, lons: pd.Series) -> float:
    """Sum of consecutive Haversine distances for an ordered track."""
    total = 0.0
    lat_vals = lats.values
    lon_vals = lons.values
    for i in range(1, len(lat_vals)):
        try:
            total += _haversine_nm(lat_vals[i - 1], lon_vals[i - 1], lat_vals[i], lon_vals[i])
        except Exception:
            pass
    return total


def _largest_gap_minutes(timestamps: pd.Series) -> float:
    """Largest gap in minutes between consecutive timestamps (sorted)."""
    ts = timestamps.sort_values().dropna()
    if len(ts) < 2:
        return float("nan")
    gaps = ts.diff().dropna().dt.total_seconds() / 60.0
    return float(gaps.max())


# ---------------------------------------------------------------------------
# Reading Silver data
# ---------------------------------------------------------------------------


def _read_silver_dataset(path: str, columns: list[str] | None = None) -> pd.DataFrame:
    """Read a partitioned Silver parquet dataset from S3."""
    bucket, prefix = _parse_s3_uri(path)
    filesystem = _build_s3_filesystem()
    dataset = ds.dataset(
        f"{bucket}/{prefix}",
        filesystem=filesystem,
        format="parquet",
        partitioning="hive",
    )
    return dataset.to_table(columns=columns).to_pandas()


# ---------------------------------------------------------------------------
# Gold table 1 — vessel_day_summary
# ---------------------------------------------------------------------------


def build_vessel_day_summary(position_path: str) -> pd.DataFrame:
    """Aggregate per-(mmsi, event_date) KPIs from Silver position data.

    Columns produced
    ----------------
    mmsi, event_date,
    ping_count, avg_sog, std_sog, max_sog,
    avg_cog, std_cog,
    lat_min, lat_max, lon_min, lon_max,
    total_distance_nm,
    hours_active,
    anchor_fraction,
    largest_gap_minutes
    """
    cols = [
        "mmsi", "event_date", "timestamp",
        "latitude", "longitude",
        "sog", "cog",
        "navigational_status",
    ]
    df = _read_silver_dataset(position_path, columns=cols)
    if df.empty:
        return pd.DataFrame()

    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    df = df.dropna(subset=["mmsi", "timestamp", "latitude", "longitude"])

    # Anchor flag: official status OR very low speed
    df["is_anchor"] = (df["navigational_status"] == _ANCHOR_STATUS) | (
        df["sog"].fillna(999) < _ANCHOR_SOG_THRESHOLD
    )

    records = []
    for (mmsi, event_date), grp in df.groupby(["mmsi", "event_date"]):
        grp = grp.sort_values("timestamp")
        ts = grp["timestamp"]
        hours_active = (ts.max() - ts.min()).total_seconds() / 3600.0

        records.append(
            {
                "mmsi": mmsi,
                "event_date": event_date,
                "ping_count": len(grp),
                "avg_sog": grp["sog"].mean(),
                "std_sog": grp["sog"].std(),
                "max_sog": grp["sog"].max(),
                "avg_cog": grp["cog"].mean(),
                "std_cog": grp["cog"].std(),
                "lat_min": grp["latitude"].min(),
                "lat_max": grp["latitude"].max(),
                "lon_min": grp["longitude"].min(),
                "lon_max": grp["longitude"].max(),
                "total_distance_nm": _total_distance_nm(grp["latitude"], grp["longitude"]),
                "hours_active": hours_active,
                "anchor_fraction": grp["is_anchor"].mean(),
                "largest_gap_minutes": _largest_gap_minutes(ts),
            }
        )

    return pd.DataFrame(records)


# ---------------------------------------------------------------------------
# Gold table 2 — vessel_static_latest
# ---------------------------------------------------------------------------


def build_vessel_static_latest(static_path: str) -> pd.DataFrame:
    """Keep the most-recent static record per MMSI across all dates.

    Columns produced
    ----------------
    mmsi, ship_name, ship_type, call_sign, imo_number,
    destination, max_draught, latest_seen
    """
    cols = [
        "mmsi", "timestamp",
        "ship_name", "ship_type", "call_sign",
        "imo_number", "destination", "max_draught",
    ]
    df = _read_silver_dataset(static_path, columns=cols)
    if df.empty:
        return pd.DataFrame()

    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    df = df.dropna(subset=["mmsi", "timestamp"])

    # Keep the row with the newest timestamp per MMSI
    latest = df.sort_values("timestamp").groupby("mmsi", as_index=False).last()
    latest = latest.rename(columns={"timestamp": "latest_seen"})
    return latest


# ---------------------------------------------------------------------------
# Gold table 3 — enriched_positions
# ---------------------------------------------------------------------------


def build_enriched_positions(
    position_path: str, vessel_static_latest: pd.DataFrame
) -> pd.DataFrame:
    """Join position pings with latest static vessel metadata.

    Adds ship_name, ship_type, call_sign, imo_number, destination,
    max_draught to every position ping row.
    """
    pos = _read_silver_dataset(position_path)
    if pos.empty or vessel_static_latest.empty:
        return pos

    static_cols = [
        "mmsi", "ship_name", "ship_type", "call_sign",
        "imo_number", "destination", "max_draught",
    ]
    meta = vessel_static_latest[static_cols].copy()

    enriched = pos.merge(meta, on="mmsi", how="left")
    return enriched


# ---------------------------------------------------------------------------
# Writing Gold data
# ---------------------------------------------------------------------------


def _write_gold_table(
    df: pd.DataFrame,
    destination: str,
    partition_cols: list[str] | None = None,
) -> None:
    """Write a Gold DataFrame to S3 as parquet."""
    if df.empty:
        print(f"[warn] skipping empty table → {destination}")
        return

    bucket, prefix = _parse_s3_uri(destination)
    filesystem = _build_s3_filesystem()
    table = pa.Table.from_pandas(df, preserve_index=False)
    write_kwargs: dict = dict(
        base_dir=f"{bucket}/{prefix}",
        filesystem=filesystem,
        format="parquet",
        existing_data_behavior="overwrite_or_ignore",
    )
    if partition_cols:
        write_kwargs["partitioning"] = partition_cols
    ds.write_dataset(table, **write_kwargs)

    rows = len(df)
    print(f"[info] wrote {rows:,} rows → {destination}")


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def run_gold_build(
    position_path: str,
    static_path: str,
    gold_summary_path: str,
    gold_static_path: str,
    gold_enriched_path: str,
) -> dict:
    """Build all three Gold tables and write them to their destinations.

    Parameters
    ----------
    position_path : str
        Silver position dataset root (s3://).
    static_path : str
        Silver static dataset root (s3://).
    gold_summary_path : str
        Destination for vessel_day_summary (partitioned by event_date).
    gold_static_path : str
        Destination for vessel_static_latest (single parquet file).
    gold_enriched_path : str
        Destination for enriched_positions (partitioned by event_date).

    Returns
    -------
    dict with row counts for each output table.
    """
    print("[gold] building vessel_day_summary …")
    summary_df = build_vessel_day_summary(position_path)
    _write_gold_table(summary_df, gold_summary_path, partition_cols=["event_date"])

    print("[gold] building vessel_static_latest …")
    static_latest_df = build_vessel_static_latest(static_path)
    _write_gold_table(static_latest_df, gold_static_path)

    print("[gold] building enriched_positions …")
    enriched_df = build_enriched_positions(position_path, static_latest_df)
    _write_gold_table(enriched_df, gold_enriched_path, partition_cols=["event_date"])

    return {
        "vessel_day_summary_rows": len(summary_df),
        "vessel_static_latest_rows": len(static_latest_df),
        "enriched_positions_rows": len(enriched_df),
    }
