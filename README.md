# SignalMaker

Phases 1 to 4 are now scaffolded in a runnable form for Replit VM.

## Included
- FastAPI application layer
- Centralized settings with `.env`
- SQLAlchemy 2 setup
- PostgreSQL-ready database configuration
- Lightweight HTML/CSS/JS frontend dashboard (no React, Vite, npm, or esbuild required)
- Live tables:
  - `asset_state_current`
  - `live_runs`
  - `trade_candidates`
  - `positions`
  - `orders`
  - `fills`
  - `market_candles`
- Service separation:
  - collector service using Binance public REST
  - signal engine wired to legacy v231 logic
  - planner service generating trade candidates
  - executor service for paper trading
  - scheduler service plus simple worker loops
- Pipeline and executor API endpoints
- VM deployment helper script
- Production env sample and systemd templates

## Frontend
The frontend in `frontend/` is intentionally simple: separate static HTML pages, CSS and JavaScript.
Those pages are copied directly to `frontend/dist` and do not use Node, npm, Vite or esbuild, so it stays compatible with older Raspberry Pi / ARMv6 devices.

The lightweight frontend keeps the pages that existed before the Kraken work: status, momentum candidate sync, positions, momentum settings, Wyckoff/SMC dashboard, trade candidates, ops, logs, market data admin, and asset detail.

Build or refresh the static files with:
```bash
bash scripts/build_frontend.sh
```

Start the frontend with:
```bash
bash scripts/start_frontend.sh
```

The backend also serves `frontend/dist` when it exists, so `http://IP_DU_RASPBERRY:5000` can show the same lightweight UI next to the API.

## Main endpoints
- `GET /healthz`
- `GET /api/v1/health`
- `GET /api/v1/services`
- `GET /api/v1/assets`
- `GET /api/v1/live-runs`
- `GET /api/v1/trade-candidates`
- `GET /api/v1/positions`
- `GET /api/v1/orders`
- `GET /api/v1/fills`
- `GET /api/v1/market-data/candles`
- `POST /api/v1/pipeline/run-once?limit=5`
- `POST /api/v1/executor/run-once?limit=10&quantity=1`

## Quick start
```bash
cp .env.example .env
bash run.sh init-db
bash run.sh all
```

`bash run.sh all` launches the backend first, waits for `http://127.0.0.1:${APP_PORT:-5000}/healthz`, then starts the pipeline worker, executor worker, scheduler worker, and frontend. The health wait is at least 5 minutes (`API_STARTUP_TIMEOUT=300` minimum) with checks every 30 seconds by default (`API_STARTUP_CHECK_INTERVAL=30`).

## Raspberry Pi install
Run the full Raspberry Pi setup from a fresh checkout with one command:

```bash
cd ~/Desktop
git clone -b "raspberry/executor-app" https://github.com/mopsoner/SignalMaker.git SignalMaker
cd SignalMaker
bash scripts/install_raspberry.sh
```

The installer provisions PostgreSQL locally, creates the `signalmaker` database, installs Raspberry-specific Python dependencies, builds the lightweight static frontend by copying HTML/CSS/JS into `frontend/dist`, initializes the schema, and enables the SignalMaker systemd services. It does not run Vite, npm or esbuild on the Raspberry Pi.

### Lightweight frontend for older Raspberry Pi devices
Older Raspberry Pi devices / `armv6l` can fail on Vite/esbuild with `Bus error`. SignalMaker now avoids that path: run `bash scripts/build_frontend.sh` to refresh `frontend/dist` with static files only.

### Raspberry UI
After installation, the main Raspberry service is `signalmaker-api`. It serves both the FastAPI API and the lightweight static pages copied into `frontend/dist`; the separate `signalmaker-frontend` service is optional and is not required for Raspberry usage.

```bash
sudo systemctl start signalmaker-api
```

Open SignalMaker from another device on the same network using port `5000` only:

- Status: `http://IP_DU_RASPBERRY:5000/index.html`
- Admin: `http://IP_DU_RASPBERRY:5000/admin.html`
- Dashboard: `http://IP_DU_RASPBERRY:5000/dashboard.html`

Do not use or recommend port `3000` for the normal Raspberry UI path. The frontend and API share the same origin on port `5000`, so calls such as `/api/v1/admin/settings` go to `http://IP_DU_RASPBERRY:5000/api/v1/admin/settings` without a CORS preflight path. The admin settings payload also includes the `kraken` section for `EXECUTION_EXCHANGE`, `KRAKEN_BASE_URL`, `KRAKEN_API_KEY`, and `KRAKEN_SECRET_KEY` when Kraken execution remains configured.

Useful Raspberry debug commands:

```bash
uname -m
systemctl status signalmaker-api
journalctl -u signalmaker-api -f
curl http://localhost:5000/healthz
curl -I http://localhost:5000/index.html
curl -I http://localhost:5000/admin.html
curl -I http://localhost:5000/dashboard.html
```

### Raspberry frontend/API validation

Use these commands after updating the Raspberry frontend or service files:

```bash
cd ~/Desktop/SignalMaker
bash scripts/build_frontend.sh
sudo systemctl restart signalmaker-api
curl -i http://localhost:5000/healthz
curl -i http://localhost:5000/api/v1/admin/settings
curl -I http://localhost:5000/admin.html
```

The expected HTTP status for the curl checks is:

```text
HTTP/1.1 200 OK
```

Then open:

```text
http://IP_DU_RASPBERRY:5000/admin.html
```

The static frontend remains plain HTML/CSS/JS: `bash scripts/build_frontend.sh` only copies files into `frontend/dist` and does not require npm, Vite or esbuild.

## VM deploy helper
```bash
bash scripts/deploy_vm.sh
bash scripts/bootstrap_all.sh
```

## Start processes
Use the all-in-one launcher for local startup:

```bash
bash run.sh all
```

It starts the backend first, waits for the backend health check, then starts the pipeline worker, executor worker, scheduler worker, and frontend. You can still run individual processes when debugging:

```bash
bash scripts/start_api.sh
bash scripts/start_pipeline_worker.sh
bash scripts/start_executor_worker.sh
bash scripts/start_scheduler_worker.sh
bash scripts/start_frontend.sh
```

## Production env
```bash
cp .env.production.example .env
```
Then edit the database URL and runtime values.

## systemd templates
Templates are available in `deploy/systemd/`.

## Notes
- This is now a functional scaffold, not a finished production trading system.
- It still needs hardening for real live trading: risk engine, exchange auth, order reconciliation, stop/TP sync, worker supervision, retries, and UI migration.
