# Deploy — AIS ingest on EC2

The ingest (`scripts/run_ingest_tracking.py`) streams AIS over a WebSocket and
flushes batches to `s3://$S3_BUCKET/raw/ais/` every 60s. Run it under **systemd**
so it starts on boot and restarts on crash — never as a bare `python ...` in an
SSH shell (that dies on disconnect via SIGHUP, and does not come back after a
reboot).

## Install (`ais-ingest.service`)

1. Edit `deploy/ais-ingest.service` and fill in the three placeholders:
   - `<USER>` — the Linux user (output of `whoami`, e.g. `ubuntu`)
   - `<REPO_DIR>` — absolute repo path on the box (output of `pwd` in the repo,
     e.g. `/home/ubuntu/Shipment_risk_scoring_machine`); it appears in 3 spots
   - Fix the `.venv` path in `ExecStart` if your venv is named differently

2. Install, enable on boot, and start:
   ```bash
   sudo cp deploy/ais-ingest.service /etc/systemd/system/ais-ingest.service
   sudo systemctl daemon-reload
   sudo systemctl enable --now ais-ingest
   ```

3. Verify:
   ```bash
   systemctl status ais-ingest      # expect: active (running)
   journalctl -u ais-ingest -f      # live logs; watch for batch flushes
   ```
   Within ~1–2 min a fresh `raw/ais/ais_<today>T...jsonl` object should appear
   in S3.

## Operations

| Action | Command |
|--------|---------|
| Status | `systemctl status ais-ingest` |
| Live logs | `journalctl -u ais-ingest -f` |
| Restart | `sudo systemctl restart ais-ingest` |
| Stop (won't restart on boot) | `sudo systemctl disable --now ais-ingest` |
| Apply edits to the unit file | `sudo systemctl daemon-reload && sudo systemctl restart ais-ingest` |

## Requirements on the box

- A `.env` at the repo root (never committed) with at least `AIS_API_KEY`,
  `S3_BUCKET`, `AWS_REGION`. systemd loads it via `EnvironmentFile`.
- The instance's IAM role (or `.env` creds) must allow `s3:PutObject` to the
  raw prefix.

## After an outage: backfill silver

Raw is the source of truth, so once ingest is healthy, fold any newly ingested
dates into silver. `skip_existing=True` only writes dates not already present,
so it leaves rebuilt partitions untouched:

```bash
python -m scripts.run_build_tables   # with skip_existing=True in the script
```
