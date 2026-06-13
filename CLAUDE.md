# Shipment Risk Scoring Machine

> **Always read this file fully before executing any task.** It defines the storage policy, S3 layout, and "ask before doing" rules that govern all work in this repo.

## Purpose
ML pipeline that scores maritime shipment risk from live AIS vessel tracking data.
Ingest → S3 (raw) → Silver (cleaned parquet) → Gold (aggregates) → features → model (planned).

## Pipeline stages
1. **Ingest** — `scripts/run_ingest_tracking.py` — WebSocket AIS stream → S3 JSONL (`raw/ais/`)
2. **Silver** — `scripts/run_build_tables.py` — raw JSONL → partitioned parquet on S3
3. **Gold** — `scripts/run_build_gold.py` — Silver → Gold tables on S3
4. **Features / train** — planned (`run_build_features`, `run_train`)
5. **Full pipeline** — `scripts/run_all.py` (can run subset: `python scripts/run_all.py run_build_tables run_build_gold`)

## Storage policy (critical)
- **All durable data lives on S3.** Do not add local disk fallbacks for Silver/Gold datasets.
- Destinations must be `s3://` URIs. Fail fast if a non-S3 path is passed.
- Local use is only for ad-hoc analysis pulls, not full dataset storage.

## S3 layout (defaults)
- Bucket: `shipment-risk-scoring` (`S3_BUCKET`)
- Raw: `s3://{bucket}/raw/ais/` (`S3_PREFIX`)
- Silver: `s3://{bucket}/silver/ais/position/` and `.../static/` (`S3_SILVER_PREFIX`)
- Gold: `s3://{bucket}/gold/ais/` (`S3_GOLD_PREFIX`)
- Region: `us-east-2` (`AWS_REGION`)

## Key modules
- `src/ingest/fetch_tracking.py` — AIS WebSocket client, batch flush to S3
- `src/ETL/Silver_Layer.py` — clean/split raw → movement + static parquet
- `src/ETL/Gold_Layer.py` — vessel_day_summary, vessel_static_latest, enriched_positions
- `tests/test_ingest_resilience.py` — ingest tests (mocked AWS/websocket)

## Running commands
- From repo root, with venv active: `python scripts/<script>.py`
- Tests: `pytest tests/`
- Diagnose ingest gaps: `scripts/diagnose_ingest_gaps.py`
- Load env via `python-dotenv` / `.env` (never commit `.env`)

## Coding conventions
- Minimize scope; match existing patterns in surrounding files
- Python 3.11+
- Prefer boto3 + pyarrow for S3 parquet reads/writes
- Silver partitions by `event_date`; Gold summary/enriched also partition by `event_date`
- Do not commit secrets, `.env`, or large data files

## Current focus
- Silver backfill complete for raw AIS through 2026-06-11
- Next: Gold build, features, model training
- Ingest bounding box: English Channel (configurable in `run_ingest_tracking.py`)

## Ask before doing
- Changing S3 bucket/prefix defaults
- Force-push or destructive git operations
- Adding local storage paths for ETL outputs