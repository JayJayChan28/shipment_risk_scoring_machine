import os
import json
import asyncio
from datetime import datetime, timezone
import boto3

import websockets
from dotenv import load_dotenv

load_dotenv()
WS_URL = "wss://stream.aisstream.io/v0/stream"

"""
sets up the endpoint for the websocket connection to the AIS stream. 
This URL is provided by the AIS stream service and is used to establish a connection to receive real-time AIS data.
"""

"""
This client will be for connecting to the AIS stream and handling AIS data.
key parameters:



Key --> You can customize the bounding boxes and message 
types to filter the AIS data you receive.

This client will also have a rate limit of around 500 messages per minute, 
so we will need to implement some logic to handle that if we want to store the data 
in a database or file.
"""


class AISStreamClient:
    # constructor for the AIS stream client
    def __init__(
        self,
        ais_api_key: str,
        ws_url: str = WS_URL,
        bounding_boxes: list | None = None,
        message_types: list | None = None,
        batch_size: int = 500,
        flush_interval_sec: int = 60,
        s3_bucket: str | None = None,
        s3_prefix: str = "raw/ais",
        aws_region: str | None = None,
        reconnect_delay_sec: int = 5,
    ):
        self.bounding_boxes = bounding_boxes or [[[-90, -180], [90, 180]]]
        self.message_types = message_types or [
            "PositionReport",
            "StandardClassBPositionReport",
            "ExtendedClassBPositionReport",
        ]
        self.api_key = ais_api_key
        self.ws_url = ws_url
        self.batch_size = batch_size
        self.flush_interval_sec = flush_interval_sec
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.aws_region = aws_region
        self.reconnect_delay_sec = reconnect_delay_sec

        self.boto3_client = boto3.client("s3", region_name=self.aws_region)

    def subscription_message(self):
        return {
            "APIKey": self.api_key,
            "BoundingBoxes": self.bounding_boxes,
            "FilterMessageTypes": self.message_types,
        }

    # flushes a batch of AIS messages to S3 bucket, this is used as a help function to our actual run
    def flush_batch(self, batch: list[dict]):
        """Flush batch to S3 or local fallback."""
        if not batch:
            return
        if self.s3_bucket:
            try:
                self._flush_to_s3(batch)
                return
            except Exception as e:
                print(f"[warn] S3 failed: {e}")
                return

    def _flush_to_s3(self, batch: list[dict]):
        """Flush batch to S3."""
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        key = f"{self.s3_prefix}/ais_{timestamp}.jsonl"
        payload = "".join(json.dumps(r) + "\n" for r in batch)
        self.boto3_client.put_object(
            Bucket=self.s3_bucket,
            Key=key,
            Body=payload.encode("utf-8"),
            ContentType="application/x-ndjson",
        )
        print(f"[info] Flushed {len(batch)} records to s3://{self.s3_bucket}/{key}")

    async def run(self) -> None:
        """Connect, subscribe, batch incoming messages, flush on size/time, and auto-reconnect."""
        while True:
            batch: list[dict] = []
            last_flush = asyncio.get_event_loop().time()
            try:
                async with websockets.connect(
                    self.ws_url, ping_interval=20, ping_timeout=20
                ) as ws:
                    await ws.send(json.dumps(self.subscription_message()))
                    print(f"[info] Connected to AIS stream at {self.ws_url}")

                    async for message_json in ws:
                        record = self.normalize_message(message_json)
                        if record is None:
                            continue

                        batch.append(record)

                        now = asyncio.get_event_loop().time()
                        if (
                            len(batch) >= self.batch_size
                            or (now - last_flush) >= self.flush_interval_sec
                        ):
                            self.flush_batch(batch)
                            batch = []
                            last_flush = now

            except (websockets.ConnectionClosed, OSError) as e:
                print(
                    f"[warn] Connection lost: {e}. Reconnecting in {self.reconnect_delay_sec}s..."
                )
                if batch:
                    self.flush_batch(batch)
                await asyncio.sleep(self.reconnect_delay_sec)

            except asyncio.CancelledError:
                print("[info] Shutting down — flushing remaining records...")
                if batch:
                    self.flush_batch(batch)
                raise

    def normalize_message(self, message_json: str) -> dict | None:
        """Normalize raw AIS message JSON into a consistent format."""
        try:
            message = json.loads(message_json)
            message_type = message.get("MessageType")
            ais_message = message.get("Message", {}).get(message_type, {})
            if not ais_message:
                return None
            metadata = message.get("MetaData", {})
            return {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "message_type": message_type,
                "mmsi": ais_message.get("UserID"),
                "latitude": ais_message.get("Latitude"),
                "longitude": ais_message.get("Longitude"),
                # position fields
                "sog": ais_message.get("Sog"),
                "cog": ais_message.get("Cog"),
                "true_heading": ais_message.get("TrueHeading"),
                "navigational_status": ais_message.get("NavigationalStatus"),
                "rate_of_turn": ais_message.get("RateOfTurn"),
                "timestamp_ais": ais_message.get("Timestamp"),
                # static data fields (populated for ShipStaticData messages)
                "ship_name": metadata.get("ShipName") or ais_message.get("Name"),
                "ship_type": ais_message.get("Type"),
                "call_sign": ais_message.get("CallSign"),
                "imo_number": ais_message.get("ImoNumber"),
                "destination": ais_message.get("Destination"),
                "max_draught": ais_message.get("MaximumStaticDraught"),
            }
        except json.JSONDecodeError:
            print(f"[error] Failed to decode JSON: {message_json}")
            return None
