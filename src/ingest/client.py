"""AIS stream ingestion client.

Connects to the AISStream WebSocket, normalises incoming position messages,
batches them, and flushes the batch to AWS S3 as NDJSON files.

Flush is triggered by whichever condition fires first:
  - the batch reaches ``batch_size`` records, or
  - ``flush_interval_sec`` seconds have elapsed since the last flush.

The client reconnects automatically after any connection error.
"""

import asyncio
import json
from datetime import datetime, timezone

import boto3
import websockets


class AISStreamClient:
    """WebSocket client for the AISStream real-time vessel position feed."""

    def __init__(
        self,
        ais_api_key: str,
        ws_url: str = "wss://stream.aisstream.io/v0/stream",
        bounding_boxes: list | None = None,
        message_types: list | None = None,
        batch_size: int = 500,
        flush_interval_sec: int = 60,
        s3_bucket: str | None = None,
        s3_prefix: str = "raw/ais",
        aws_region: str | None = None,
        reconnect_delay_sec: int = 5,
    ):
        self.api_key = ais_api_key
        self.ws_url = ws_url
        self.bounding_boxes = bounding_boxes or [[[-90, -180], [90, 180]]]
        self.message_types = message_types or [
            "PositionReport",
            "StandardClassBPositionReport",
            "ExtendedClassBPositionReport",
        ]
        self.batch_size = batch_size
        self.flush_interval_sec = flush_interval_sec
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.aws_region = aws_region
        self.reconnect_delay_sec = reconnect_delay_sec

        self._boto3_client = boto3.client("s3", region_name=self.aws_region)

    # ------------------------------------------------------------------
    # Public helpers
    # ------------------------------------------------------------------

    def subscription_message(self) -> dict:
        """Return the subscription payload sent to AISStream on connect."""
        return {
            "APIKey": self.api_key,
            "BoundingBoxes": self.bounding_boxes,
            "FilterMessageTypes": self.message_types,
        }

    def normalize_message(self, message_json: str) -> dict | None:
        """Parse and validate a raw AIS WebSocket message.

        Returns a flat dict with the essential fields, or ``None`` if the
        message is malformed or missing required position data.
        """
        try:
            message = json.loads(message_json)
        except json.JSONDecodeError:
            return None

        message_type = message.get("MessageType")
        if not message_type:
            return None

        ais_msg = message.get("Message", {}).get(message_type, {})
        if not ais_msg:
            return None

        mmsi = ais_msg.get("UserID")
        lat = ais_msg.get("Latitude")
        lon = ais_msg.get("Longitude")

        if mmsi is None or lat is None or lon is None:
            return None

        return {
            "mmsi": mmsi,
            "lat": lat,
            "lon": lon,
            "message_type": message_type,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "raw": ais_msg,
        }

    def flush_batch(self, batch: list[dict]) -> None:
        """Write *batch* to S3.  Logs a warning if the upload fails."""
        if not batch:
            return
        if self.s3_bucket:
            try:
                self._flush_to_s3(batch)
            except Exception as exc:
                print(f"[warn] S3 upload failed: {exc}")

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _flush_to_s3(self, batch: list[dict]) -> None:
        """Serialise *batch* as NDJSON and upload it to S3."""
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        key = f"{self.s3_prefix}/ais_{timestamp}.jsonl"
        payload = "".join(json.dumps(record) + "\n" for record in batch)
        self._boto3_client.put_object(
            Bucket=self.s3_bucket,
            Key=key,
            Body=payload.encode("utf-8"),
            ContentType="application/x-ndjson",
        )
        print(f"[info] Flushed {len(batch)} records to s3://{self.s3_bucket}/{key}")

    # ------------------------------------------------------------------
    # Main async loop
    # ------------------------------------------------------------------

    async def run(self) -> None:
        """Connect to the AIS stream and ingest messages indefinitely.

        Reconnects automatically after any connection error.
        """
        while True:
            batch: list[dict] = []
            last_flush = asyncio.get_running_loop().time()

            try:
                async with websockets.connect(
                    self.ws_url, ping_interval=20, ping_timeout=20
                ) as websocket:
                    await websocket.send(json.dumps(self.subscription_message()))
                    print("[info] Connected and subscribed to AIS stream.")

                    async for message_json in websocket:
                        record = self.normalize_message(message_json)
                        if record is None:
                            continue

                        batch.append(record)
                        now = asyncio.get_running_loop().time()

                        # Flush when batch is full or the time window has elapsed
                        if (
                            len(batch) >= self.batch_size
                            or (now - last_flush) >= self.flush_interval_sec
                        ):
                            self.flush_batch(batch)
                            batch = []
                            last_flush = now

            except Exception as exc:
                print(
                    f"[warn] Connection error: {exc}. "
                    f"Reconnecting in {self.reconnect_delay_sec}s…"
                )
                # Best-effort flush of any buffered records before reconnecting
                if batch:
                    self.flush_batch(batch)
                await asyncio.sleep(self.reconnect_delay_sec)
