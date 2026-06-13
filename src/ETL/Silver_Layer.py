import json
import os
import re
from collections import defaultdict
from urllib.parse import urlparse

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.fs as pafs
from dotenv import load_dotenv

load_dotenv()

POSITION_TYPES = {
    "PositionReport",
    "StandardClassBPositionReport",
    "ExtendedClassBPositionReport",
}


def get_s3_client(region: str | None = None):
    return boto3.client(
        "s3", region_name=region or os.getenv("AWS_REGION", "us-east-2")
    )


def normalize_prefix(prefix: str) -> str:
    return prefix.strip("/") + "/"


def _build_s3_filesystem() -> pafs.S3FileSystem:
    return pafs.S3FileSystem(region=os.getenv("AWS_REGION", "us-east-2"))


def _parse_s3_uri(uri: str) -> tuple[str, str]:
    parsed = urlparse(uri)
    if parsed.scheme != "s3" or not parsed.netloc:
        raise ValueError(f"Expected an s3:// URI, got: {uri}")
    return parsed.netloc, parsed.path.lstrip("/").rstrip("/")


def iter_raw_keys(s3_client, bucket: str, prefix: str):
    """Yield all non-empty object keys under a prefix, handling pagination."""
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=normalize_prefix(prefix)):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            size = obj.get("Size", 0)
            if key.endswith("/") or size == 0:
                continue
            yield key


def read_one_jsonl_file(s3_client, bucket: str, key: str) -> pd.DataFrame:
    """Read one raw JSONL object and return normalized rows as a DataFrame."""
    response = s3_client.get_object(Bucket=bucket, Key=key)
    rows = []

    for line in response["Body"].iter_lines():
        if not line:
            continue
        raw = json.loads(line.decode("utf-8"))
        rows.append(
            {
                "message_type": raw.get("message_type"),
                "mmsi": raw.get("mmsi"),
                "timestamp": raw.get("timestamp"),
                "latitude": raw.get("latitude"),
                "longitude": raw.get("longitude"),
                "sog": raw.get("sog"),
                "cog": raw.get("cog"),
                "true_heading": raw.get("true_heading"),
                "navigational_status": raw.get("navigational_status"),
                "rate_of_turn": raw.get("rate_of_turn"),
                "timestamp_ais": raw.get("timestamp_ais"),
                "ship_name": raw.get("ship_name"),
                "ship_type": raw.get("ship_type"),
                "call_sign": raw.get("call_sign"),
                "imo_number": raw.get("imo_number"),
                "destination": raw.get("destination"),
                "max_draught": raw.get("max_draught"),
                "source_key": key,
            }
        )

    return pd.DataFrame(rows)


def clean_and_split(df: pd.DataFrame):
    """Clean unified rows and split into movement/static Silver tables."""
    if df.empty:
        return df.copy(), df.copy()

    expected_cols = [
        "message_type",
        "mmsi",
        "timestamp",
        "latitude",
        "longitude",
        "sog",
        "cog",
        "true_heading",
        "navigational_status",
        "rate_of_turn",
        "timestamp_ais",
        "ship_name",
        "ship_type",
        "call_sign",
        "imo_number",
        "destination",
        "max_draught",
        "source_key",
    ]
    for col in expected_cols:
        if col not in df.columns:
            df[col] = pd.NA

    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)

    numeric_cols = [
        "mmsi",
        "latitude",
        "longitude",
        "sog",
        "cog",
        "true_heading",
        "navigational_status",
        "rate_of_turn",
        "timestamp_ais",
        "ship_type",
        "imo_number",
        "max_draught",
    ]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Shared quality checks.
    df = df[df["mmsi"].notna()]
    df = df[(df["latitude"].isna()) | (df["latitude"].between(-90, 90))]
    df = df[(df["longitude"].isna()) | (df["longitude"].between(-180, 180))]
    df = df.drop_duplicates(
        subset=["mmsi", "timestamp", "message_type", "latitude", "longitude"]
    )

    df["event_date"] = df["timestamp"].dt.strftime("%Y-%m-%d")

    movement_df = df[df["message_type"].isin(POSITION_TYPES)].copy()
    movement_df = movement_df.dropna(subset=["timestamp", "latitude", "longitude"])

    static_df = df[df["message_type"].eq("ShipStaticData")].copy()
    static_df = static_df.dropna(subset=["timestamp"])

    return movement_df, static_df


def write_partitioned_parquet(
    movement_df: pd.DataFrame,
    static_df: pd.DataFrame,
    movement_path: str,
    static_path: str,
):
    """Write movement/static Silver outputs as partitioned parquet on S3."""

    def _write_df(df: pd.DataFrame, destination: str) -> None:
        if df.empty:
            return

        bucket, prefix = _parse_s3_uri(destination)
        filesystem = _build_s3_filesystem()
        table = pa.Table.from_pandas(df, preserve_index=False)
        ds.write_dataset(
            table,
            base_dir=f"{bucket}/{prefix}",
            filesystem=filesystem,
            format="parquet",
            partitioning=["event_date"],
            existing_data_behavior="overwrite_or_ignore",
        )

    _write_df(movement_df, movement_path)
    _write_df(static_df, static_path)


def _date_from_key(key: str) -> str:
    """Extract YYYY-MM-DD from a raw key like raw/ais/ais_20260525T123456Z.jsonl."""
    m = re.search(r"(\d{4})(\d{2})(\d{2})T", key)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    return "unknown"


_DATE_RE = re.compile(r"(\d{4}-\d{2}-\d{2})")


def _existing_partition_dates(s3_client, path: str) -> set[str]:
    """Return event_date partitions already written under an S3 dataset path.

    Handles both directory-style (``2026-05-25/``) and hive-style
    (``event_date=2026-05-25/``) partition folders. Missing destinations
    return an empty set.
    """
    dates: set[str] = set()
    bucket, prefix = _parse_s3_uri(path)
    prefix = normalize_prefix(prefix)
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
        for common in page.get("CommonPrefixes", []):
            folder = common["Prefix"][len(prefix):].strip("/")
            m = _DATE_RE.search(folder)
            if m:
                dates.add(m.group(1))
    return dates


def run_silver_backfill(
    s3_client,
    bucket: str,
    raw_prefix: str,
    movement_path: str,
    static_path: str,
    max_files: int | None = None,
):
    """Backfill Silver movement/static datasets from raw JSONL objects.

    Keys are grouped by date extracted from the filename. All files for a
    given date are read into memory together, cleaned as one batch, then
    written to S3 in a single write per date. This keeps S3 PUT calls equal
    to the number of unique dates rather than the number of raw files.
    """
    # Group all raw keys by date without reading any data yet.
    keys_by_date: dict[str, list[str]] = defaultdict(list)
    for key in iter_raw_keys(s3_client, bucket, raw_prefix):
        keys_by_date[_date_from_key(key)].append(key)

    total_dates = len(keys_by_date)
    files_processed = 0
    movement_rows = 0
    static_rows = 0

    for date_idx, (date, keys) in enumerate(sorted(keys_by_date.items()), start=1):
        print(
            f"[info] processing date {date} ({date_idx}/{total_dates}) — {len(keys)} files"
        )

        raw_frames: list[pd.DataFrame] = []
        for key in keys:
            raw_frames.append(read_one_jsonl_file(s3_client, bucket, key))
            files_processed += 1
            if max_files is not None and files_processed >= max_files:
                break

        # Combine all raw data for this date, clean once, write once.
        raw_day = pd.concat(raw_frames, ignore_index=True)
        movement_df, static_df = clean_and_split(raw_day)
        write_partitioned_parquet(movement_df, static_df, movement_path, static_path)

        movement_rows += len(movement_df)
        static_rows += len(static_df)
        print(
            f"[info] wrote date {date} | movement_rows={len(movement_df)} | static_rows={len(static_df)}"
        )

        if max_files is not None and files_processed >= max_files:
            break

    return {
        "files_processed": files_processed,
        "movement_rows": movement_rows,
        "static_rows": static_rows,
    }
