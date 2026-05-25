# run_ingest_tracking.py
import os
import asyncio
from dotenv import load_dotenv
from src.ingest.fetch_tracking import AISStreamClient

load_dotenv()

create_ais_client = AISStreamClient(
    ais_api_key=os.getenv("AIS_API_KEY"),
    bounding_boxes=[
        [[49, -5], [52, 2]]  # English Channel
    ],
    message_types=[
        "PositionReport",
        "StandardClassBPositionReport",
        "ExtendedClassBPositionReport",
        "ShipStaticData",
    ],
    batch_size=500,
    flush_interval_sec=60,
    s3_bucket=os.getenv("S3_BUCKET"),
    s3_prefix="raw/ais",
    aws_region=os.getenv("AWS_REGION"),
    reconnect_delay_sec=5,
)

asyncio.run(create_ais_client.run())
