import os
import json
import asyncio
from datetime import datetime, timezone
import boto3
import io
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
    #constructor for the AIS stream client
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
        self.api_key = ais_api_key
        self.ws_url = ws_url
        self.batch_size = batch_size
        self.flush_interval_sec = flush_interval_sec
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.aws_region = aws_region
        self.reconnect_delay_sec = reconnect_delay_sec
        
        #builds the payload for subscription to the AIS stream
    def subscription_message(self):
        return {
            "APIKey": self.api_key,
            "BoundingBoxes": self.bounding_boxes,
            "FilterMessageTypes": self.message_types,
        }
        
        
    #flushes a batch of AIS messages to S3 bucket, this is used as a help function to our actual run
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

        s3 = boto3.client("s3", region_name=self.aws_region)
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        key = f"{self.s3_prefix}/ais_{timestamp}.jsonl"
        buffer = io.StringIO()
        for record in batch:
            buffer.write(json.dumps(record) + "\n")
        buffer.seek(0)
        s3.upload_fileobj(buffer, self.s3_bucket, key)
        print(f"[info] Flushed {len(batch)} records to s3://{self.s3_bucket}/{key}")
  
    
        
        
            
        
        


async def connect_ais_stream():
    api_key = os.getenv("AIS_API_KEY")
    if not api_key:
        raise ValueError("Missing AIS_API_KEY in .env")

    subscribe_message = {
        "APIKey": api_key,
        "BoundingBoxes": [[[-90, -180], [90, 180]]], #bounding boxes for full world
        "FilterMessageTypes": [
            "PositionReport",
            "StandardClassBPositionReport",
            "ExtendedClassBPositionReport",
        ],
    }

    async with websockets.connect(WS_URL, ping_interval=20, ping_timeout=20) as websocket:
        await websocket.send(json.dumps(subscribe_message))
        print("Connected and subscribed to AIS stream.")

        async for message_json in websocket:
            message = json.loads(message_json)
            message_type = message.get("MessageType")
            ais_message = message.get("Message", {}).get(message_type, {})

            if not ais_message:
                continue

            mmsi = ais_message.get("UserID")
            lat = ais_message.get("Latitude")
            lon = ais_message.get("Longitude")

            print(f"[{datetime.now(timezone.utc).isoformat()}] type={message_type} mmsi={mmsi} lat={lat} lon={lon}")

if __name__ == "__main__":
    asyncio.run(connect_ais_stream())
    
# python src/ingest/fetch_tracking.py