import os
import json
import asyncio
from datetime import datetime, timezone

import websockets
from dotenv import load_dotenv

load_dotenv()

"""
sets up the endpoint for the websocket connection to the AIS stream. 
This URL is provided by the AIS stream service and is used to establish a connection to receive real-time AIS data.
"""

WS_URL = "wss://stream.aisstream.io/v0/stream"


async def connect_ais_stream():
    api_key = os.getenv("API_KEY")
    if not api_key:
        raise ValueError("Missing API_KEY in .env")

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