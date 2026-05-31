import os
import json
import asyncio
from datetime import datetime, timezone
import boto3
import pandas as pd
import websockets
from dotenv import load_dotenv
load_dotenv()

#creating the s3 client
s3_client_test = boto3.client("s3", region_name=os.getenv("AWS_REGION")) #creating the s3 client



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
generator_obj = iter_raw_keys(s3_client_test, os.getenv("S3_BUCKET"), os.getenv("S3_PREFIX"))

for key in generator_obj:
    print(json)


def read_json_from_s3(s3_client, bucket: str, key: str):
    response = s3_client.get_object(
        Bucket=bucket,
        Key=key
    )

    body = response["Body"].read().decode("utf-8")

    if key.endswith(".jsonl"):
        data = [
            json.loads(line)
            for line in body.splitlines()
            if line.strip()
        ]
    else:
        data = json.loads(body)

    return data

#testing read_json

read_json_from_s3(s3_client_test, os.getenv("S3_BUCKET"), "raw/ais/ais_20260525T213538Z.jsonl")

def build_silver_table(s3_client, bucket, prefix):
    rows = []

    for key in iter_raw_keys(s3_client, bucket, prefix):
        data = read_json_from_s3(
            s3_client,
            bucket,
            key
        )

        rows.append(data)

    return pd.DataFrame(rows)