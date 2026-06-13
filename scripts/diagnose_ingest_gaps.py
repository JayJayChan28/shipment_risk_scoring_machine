"""diagnose_ingest_gaps.py — find ingestion downtime from the raw S3 layer.

The ingest client writes one NDJSON file roughly every flush interval
(default 60s). When the collector dies, the steady ~1 file/min cadence
breaks. This script reads the raw object listing straight from S3 and
reports:

  * total files and the first/last upload time
  * per-day coverage (% of an ideal 1-file-per-minute day)
  * every downtime window where uploads stalled longer than --gap-min

Run:
    python scripts/diagnose_ingest_gaps.py
    python scripts/diagnose_ingest_gaps.py --gap-min 10

It only needs S3 read access (the same creds the pipeline already uses).
"""

import argparse
import os
from collections import defaultdict
from datetime import datetime, timedelta

import boto3
from dotenv import load_dotenv

FLUSH_INTERVAL_SEC = 60
FILES_PER_DAY = 24 * 60 * 60 // FLUSH_INTERVAL_SEC  # ideal cadence -> 1440


def _iter_objects(s3_client, bucket: str, prefix: str):
    """Yield (last_modified, key) for every object under the prefix."""
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".jsonl"):
                yield obj["LastModified"], obj["Key"]


def main() -> None:
    load_dotenv()

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bucket", default=os.getenv("S3_BUCKET", "shipment-risk-scoring"))
    parser.add_argument("--prefix", default=os.getenv("S3_PREFIX", "raw/ais"))
    parser.add_argument("--region", default=os.getenv("AWS_REGION", "us-east-2"))
    parser.add_argument(
        "--gap-min",
        type=float,
        default=5.0,
        help="Report a downtime window when uploads stall longer than this (minutes).",
    )
    args = parser.parse_args()

    s3_client = boto3.client("s3", region_name=args.region)

    # Upload time is the wall-clock truth of when data actually landed; the
    # filename timestamp can drift if the collector host clock is skewed.
    uploads = sorted(
        ts for ts, _ in _iter_objects(s3_client, args.bucket, args.prefix)
    )

    if not uploads:
        print(f"No .jsonl objects under s3://{args.bucket}/{args.prefix}")
        return

    first, last = uploads[0], uploads[-1]
    now = datetime.now(first.tzinfo)

    print(f"Bucket           : s3://{args.bucket}/{args.prefix}")
    print(f"Total raw files  : {len(uploads):,}")
    print(f"First upload     : {first:%Y-%m-%d %H:%M:%S %Z}")
    print(f"Last  upload     : {last:%Y-%m-%d %H:%M:%S %Z}")
    print(f"Calendar span    : {last - first}")
    print(f"Since last file  : {now - last}  (now {now:%Y-%m-%d %H:%M:%S %Z})")

    # Is the collector alive right now? If the newest file is older than a few
    # flush intervals, ingestion has stopped.
    stale_after = timedelta(seconds=FLUSH_INTERVAL_SEC * 5)
    status = "ONLINE" if (now - last) <= stale_after else "STOPPED"
    print(f"Collector status : {status}")

    print("\n=== Per-day coverage (by upload time) ===")
    by_day: dict[str, list[datetime]] = defaultdict(list)
    for ts in uploads:
        by_day[ts.strftime("%Y-%m-%d")].append(ts)
    for day in sorted(by_day):
        times = sorted(by_day[day])
        n = len(times)
        pct = 100 * n / FILES_PER_DAY
        print(
            f"  {day}: {n:5d} files | {times[0]:%H:%M} -> {times[-1]:%H:%M} | ~{pct:5.1f}% of day"
        )

    print(f"\n=== Downtime windows (stall > {args.gap_min:g} min) ===")
    threshold = timedelta(minutes=args.gap_min)
    windows = []
    for prev, cur in zip(uploads, uploads[1:]):
        gap = cur - prev
        if gap > threshold:
            windows.append((prev, cur, gap))

    if not windows:
        print("  None — uploads were continuous within the threshold.")
    else:
        total_down = timedelta()
        for prev, cur, gap in windows:
            total_down += gap
            print(f"  STOPPED {prev:%Y-%m-%d %H:%M:%S}  ->  RESUMED {cur:%Y-%m-%d %H:%M:%S}   (down {gap})")
        print(f"\n  {len(windows)} outage(s), cumulative downtime {total_down}")


if __name__ == "__main__":
    main()
