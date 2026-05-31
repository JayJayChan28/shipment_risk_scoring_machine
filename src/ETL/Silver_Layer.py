import os
import json
import asyncio
from datetime import datetime, timezone
import boto3
import pandas as pd
import websockets
from dotenv import load_dotenv

load_dotenv()

# creating the s3 client
s3_client_test = boto3.client(
    "s3", region_name=os.getenv("AWS_REGION")
)  # creating the s3 client


#this just here to check what is in the bucket
def iter_raw_keys(s3_client, bucket: str, prefix: str):
    response = s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    while True:
        for obj in response.get("Contents", []):
            key = obj["Key"]
            size = obj.get("Size", 0)

            # Skip folder markers and empty objects
            if key.endswith("/") or size == 0:
                continue

            yield key

        if not response.get("IsTruncated"):
            break

        token = response["NextContinuationToken"]
        response = s3_client.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix,
            ContinuationToken=token,
        )


### quick tests
generator_obj = iter_raw_keys(
    s3_client_test, os.getenv("S3_BUCKET"), os.getenv("S3_PREFIX")
)

for key in generator_obj:
    print(json)


def read_json_from_s3(s3_client, bucket: str, key: str):
    response = s3_client.get_object(Bucket=bucket, Key=key)

    body = response["Body"].read().decode("utf-8")

    if key.endswith(".jsonl"):
        data = [json.loads(line) for line in body.splitlines() if line.strip()]
    else:
        data = json.loads(body)

    return data


# testing read_json

read_json_from_s3(
    s3_client_test, os.getenv("S3_BUCKET"), "raw/ais/ais_20260525T213538Z.jsonl"
)

response = s3_client_test.get_object(
        Bucket=os.getenv("S3_BUCKET"),
        Key="raw/ais/ais_20260525T213538Z.jsonl"
    )
rows = []

for line in response["Body"].iter_lines():
    if not line:
        continue
    raw = json.loads(line.decode("utf-8"))
    rows.append({
            "mmsi": raw.get("mmsi") or raw.get("MMSI"),
            "timestamp": raw.get("timestamp") or raw.get("BaseDateTime"),
            "lat": raw.get("lat") or raw.get("LAT"),
            "lon": raw.get("lon") or raw.get("LON"),
            "speed": raw.get("speed") or raw.get("SOG"),
            "course": raw.get("course") or raw.get("COG"),
            "heading": raw.get("heading") or raw.get("Heading"),
        })
df = pd.DataFrame(rows)




##FUCK THIS 
def read_clean_write_one_file(s3_client, bucket: str, raw_key: str):
    response = s3_client.get_object(
        Bucket=bucket,
        Key=raw_key
    )

    rows = []

    for line in response["Body"].iter_lines():
        if not line:
            continue

        raw = json.loads(line.decode("utf-8"))

        rows.append({
            "mmsi": raw.get("mmsi") or raw.get("MMSI"),
            "timestamp": raw.get("timestamp") or raw.get("BaseDateTime"),
            "lat": raw.get("lat") or raw.get("LAT"),
            "lon": raw.get("lon") or raw.get("LON"),
            "speed": raw.get("speed") or raw.get("SOG"),
            "course": raw.get("course") or raw.get("COG"),
            "heading": raw.get("heading") or raw.get("Heading"),
        })

    df = pd.DataFrame(rows)

    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)
    df["lat"] = pd.to_numeric(df["lat"], errors="coerce")
    df["lon"] = pd.to_numeric(df["lon"], errors="coerce")
    df["speed"] = pd.to_numeric(df["speed"], errors="coerce")
    df["course"] = pd.to_numeric(df["course"], errors="coerce")
    df["heading"] = pd.to_numeric(df["heading"], errors="coerce")

    df = df.dropna(subset=["mmsi", "timestamp", "lat", "lon"])
    df = df[df["lat"].between(-90, 90) & df["lon"].between(-180, 180)]
    df = df.drop_duplicates()

    silver_key = (
        raw_key
        .replace("raw/ais/", "silver/ais/")
        .replace(".jsonl", ".parquet")
    )

    buffer = BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)

    s3_client.put_object(
        Bucket=bucket,
        Key=silver_key,
        Body=buffer.getvalue()
    )

    print(f"Wrote s3://{bucket}/{silver_key}")


