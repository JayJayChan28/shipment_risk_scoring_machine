import json
import os

import boto3
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

POSITION_TYPES = {
    "PositionReport",
    "StandardClassBPositionReport",
    "ExtendedClassBPositionReport",
}


def get_s3_client(region: str | None = None):
    return boto3.client("s3", region_name=region or os.getenv("AWS_REGION", "us-east-2"))


def normalize_prefix(prefix: str) -> str:
    return prefix.strip("/") + "/"


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
    """Write movement/static Silver outputs as partitioned parquet datasets."""
    os.makedirs(movement_path, exist_ok=True)
    os.makedirs(static_path, exist_ok=True)

    if not movement_df.empty:
        movement_df.to_parquet(
            movement_path,
            index=False,
            engine="pyarrow",
            partition_cols=["event_date"],
        )

    if not static_df.empty:
        static_df.to_parquet(
            static_path,
            index=False,
            engine="pyarrow",
            partition_cols=["event_date"],
        )


def run_silver_backfill(
    s3_client,
    bucket: str,
    raw_prefix: str,
    movement_path: str,
    static_path: str,
    max_files: int | None = None,
):
    """Backfill Silver movement/static datasets from raw JSONL objects."""
    files_processed = 0
    movement_rows = 0
    static_rows = 0

    for key in iter_raw_keys(s3_client, bucket, raw_prefix):
        raw_df = read_one_jsonl_file(s3_client, bucket, key)
        movement_df, static_df = clean_and_split(raw_df)

        write_partitioned_parquet(movement_df, static_df, movement_path, static_path)

        files_processed += 1
        movement_rows += len(movement_df)
        static_rows += len(static_df)

        if files_processed % 50 == 0:
            print(
                f"[info] processed {files_processed} files | movement_rows={movement_rows} | static_rows={static_rows}"
            )

        if max_files is not None and files_processed >= max_files:
            break

    return {
        "files_processed": files_processed,
        "movement_rows": movement_rows,
        "static_rows": static_rows,
    }





