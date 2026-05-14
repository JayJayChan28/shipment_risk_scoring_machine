Shipment Risk Scoring Machine — Project Overview
What this project is
A machine learning pipeline that scores the risk of maritime shipments in real time. It ingests live vessel tracking data from the AIS (Automatic Identification System) stream, stores it in AWS S3, and will eventually train a model to detect vessels deviating from normal shipping lanes.

Architecture Overview
The project has four main stages:

Stage 1 — Ingestion (current stage)
A Python WebSocket client connects to AISStream and receives live vessel position messages. Every 500 messages or 60 seconds, the client batches the records and uploads them directly to an AWS S3 bucket as NDJSON files. The client handles reconnection automatically if the stream drops.

Stage 2 — Transformation (not built yet)
An AWS Glue ETL job will read the raw NDJSON files from S3, clean and deduplicate them, and write them back to S3 as Parquet files. Parquet is a compressed columnar format that is much cheaper and faster to query than raw JSONL.

Stage 3 — Machine Learning (not built yet)
Once enough data is collected (a few days to weeks), a DBSCAN clustering algorithm will identify normal shipping lanes from the position history. Then a classifier like XGBoost will be trained to detect when a vessel is deviating from its expected lane. The trained model gets saved back to S3.

Stage 4 — Dashboard (not built yet)
A Streamlit dashboard will read the latest data from S3 every 30 seconds, show live vessel positions on a map, and display risk scores for vessels currently being tracked.

Current File Structure
Recommended future structure
AWS Setup Required
S3 bucket created with folders: raw/ais, curated, features, models
AWS CLI configured on your machine with access key and secret key
Region set to us-east-1 or ap-southeast-1 (Singapore, good for AIS data)
Environment Variables Needed in .env
What is done in fetch_tracking.py
Constructor with all config params
subscription_message() — tells AISStream what data to send
normalize_message() — parses and validates each incoming message
flush_batch() — decides when and where to write data
_flush_to_s3() — writes batch to S3 using boto3
run() — main async loop with batching, flush trigger, and reconnect logic
Immediate next steps
Set up Python virtual environment and install dependencies: boto3, websockets, python-dotenv
Run aws configure on the machine to set credentials
Fill in the .env file with real API key and S3 bucket name
Run fetch_tracking.py and confirm records appear in S3
Let it collect data for a few days
Split fetch_tracking.py into config.py, client.py, and main.py
