"""Entry point for the AIS stream ingestion pipeline.

Usage
-----
    python scripts/run_ingest_tracking.py

Reads all configuration from environment variables (see .env.example).
"""

import asyncio

from src.config import AISConfig
from src.ingest.client import AISStreamClient


def main() -> None:
    config = AISConfig()

    if not config.ais_api_key:
        raise ValueError("AIS_API_KEY is not set. Add it to your .env file.")
    if not config.s3_bucket:
        raise ValueError("S3_BUCKET is not set. Add it to your .env file.")

    client = AISStreamClient(
        ais_api_key=config.ais_api_key,
        ws_url=config.ws_url,
        batch_size=config.batch_size,
        flush_interval_sec=config.flush_interval_sec,
        s3_bucket=config.s3_bucket,
        s3_prefix=config.s3_prefix,
        aws_region=config.aws_region,
        reconnect_delay_sec=config.reconnect_delay_sec,
    )

    asyncio.run(client.run())


if __name__ == "__main__":
    main()
