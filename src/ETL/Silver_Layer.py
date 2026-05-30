import os
import json
import asyncio
from datetime import datetime, timezone
import boto3

import websockets
from dotenv import load_dotenv
load_dotenv()



s3 = boto3.client("s3", region_name=os.getenv("AWS_REGION")) #creating the s3 client

s3_bucket = os.getenv("S3_BUCKET") #getting the s3 bucket name from the environment variable
s3_prefix = "silver/ais" #setting the prefix for the silver layer in s3
response = s3.list_objects_v2(Bucket=s3_bucket, Prefix="raw/ais/")

while True:
    for obj in response.get("Contents", []):
        print(obj["Key"])

    if not response.get("IsTruncated"):
        break

    token = response["NextContinuationToken"]
    response = s3.list_objects_v2(
        Bucket=s3_bucket,
        Prefix="raw/ais/",
        ContinuationToken=token
    )

    