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

The backend also serves `frontend/dist` when it exists, so `http://IP_DU_RASPBERRY:8080` can show the same lightweight UI next to the API.

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
bash run.sh api
bash scripts/start_frontend.sh
```


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
After installation, start the backend API and frontend UI services:

```bash
sudo systemctl start signalmaker-api signalmaker-frontend
```

Open SignalMaker from another device on the same network:

- API: `http://IP_DU_RASPBERRY:8080`
- UI: `http://IP_DU_RASPBERRY:3000`

Useful Raspberry debug commands:

```bash
uname -m
systemctl status signalmaker-api
systemctl status signalmaker-frontend
journalctl -u signalmaker-frontend -f
curl http://localhost:8080/healthz
```

To keep the Raspberry Pi running without the UI temporarily, disable only the frontend service:

```bash
sudo systemctl stop signalmaker-frontend
sudo systemctl disable signalmaker-frontend
```

## VM deploy helper
```bash
bash scripts/deploy_vm.sh
bash scripts/bootstrap_all.sh
```

## Start processes
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
