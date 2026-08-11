# Deployment helpers

## VM bootstrap
```bash
bash scripts/bootstrap_all.sh
```

## Production env
Copy `.env.production.example` to `.env` and edit the database credentials.

## systemd templates
Files are provided under `deploy/systemd/`.
Adjust the working directory if needed, then copy them to `/etc/systemd/system/`.
The units use `/opt/signalmaker/logs` for both worker output streams and configure
the API to read that same directory. Run `scripts/deploy_vm.sh` as the systemd
service user (root by default) first so the directory exists and is writable; if
you add a custom `User=`, change the directory ownership to that user.

Example:
```bash
sudo cp deploy/systemd/signalmaker-api.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable signalmaker-api
sudo systemctl start signalmaker-api
```

The stock/ETF deployment uses three stable worker identities: `ibkr_ingestion`,
`stock_etf_analysis`, and `scheduler`. Install `signalmaker-ibkr-ingestion.service`,
`signalmaker-market-analysis.service`, and `signalmaker-scheduler.service`. The
analysis unit consumes both `wyckoff_smc` and `momentum` jobs according to each
job payload; it is intentionally separate from crypto workers.

`IBKR_INGEST_INTERVAL_SECONDS` controls the ingestion cadence (default 86400),
and `MARKET_ANALYSIS_POLL_SECONDS` controls empty-queue polling (default 5).
`GET /api/v1/admin/workers` reports OS process state separately from queue state;
a running process does not imply that queued jobs are healthy.
