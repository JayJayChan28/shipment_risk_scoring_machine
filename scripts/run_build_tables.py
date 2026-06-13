import os

from dotenv import load_dotenv

from src.ETL.Silver_Layer import get_s3_client, run_silver_backfill


def main() -> None:
    load_dotenv()

    bucket = os.getenv("S3_BUCKET", "shipment-risk-scoring")
    raw_prefix = os.getenv("S3_PREFIX", "raw/ais")
    silver_prefix = os.getenv("S3_SILVER_PREFIX", "silver/ais")

    movement_out = f"s3://{bucket}/{silver_prefix}/position"
    static_out = f"s3://{bucket}/{silver_prefix}/static"

    # One-time full rebuild: re-read all raw and replace existing silver partitions.
    # After this run completes, set skip_existing=True and overwrite_partitions=False.
    skip_existing = False
    overwrite_partitions = True

    client = get_s3_client()
    stats = run_silver_backfill(
        s3_client=client,
        bucket=bucket,
        raw_prefix=raw_prefix,
        movement_path=movement_out,
        static_path=static_out,
        max_files=None,
        skip_existing=skip_existing,
        overwrite_partitions=overwrite_partitions,
    )
    print(f"[done] {stats}")


if __name__ == "__main__":
    main()
