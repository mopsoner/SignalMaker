# SignalMaker Raspberry Executor

This branch runs the Raspberry Executor device connected to a remote SignalMaker. Historical device mode pulls missing candles from the local exchange (Kraken/Kraken), pushes them to the remote SignalMaker, reads trade candidates/momentum decisions from that remote service, and executes orders locally.

Run modes:

```bash
./run.sh device       # historical device mode: candle feed + executor + supporting loops
./run.sh candle-feed  # only exchange -> remote SignalMaker candle sync
./run.sh backfill     # historical 4h exchange -> remote SignalMaker backfill
./run.sh executor     # only remote SignalMaker -> local exchange execution
./run.sh local-api    # only local Raspberry Executor monitoring UI/API
./run.sh all-local    # explicit local API + local pipeline + local executor + scheduler
```


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
  - collector service using Kraken public REST
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

The lightweight frontend keeps the pages that existed before the Kraken work: executor status, trade candidates, Momentum buy/sell tracking, positions, orders/fills, settings, admin, and logs.

Build or refresh the static files with:
```bash
bash scripts/build_frontend.sh
```

No separate frontend process is needed on Raspberry: the backend serves `frontend/dist`, so `http://IP_DU_RASPBERRY:8080` shows the same lightweight UI next to the API.

For development only, a standalone static server can still be started explicitly:
```bash
RUN_STANDALONE_FRONTEND=1 bash scripts/start_frontend.sh
```

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

`bash run.sh all` launches the backend first, waits for `http://127.0.0.1:${EXECUTOR_API_PORT:-${APP_PORT:-8080}}/healthz`, then starts the pipeline worker, executor worker, and scheduler worker. The frontend is served by the API. The health wait is at least 5 minutes (`API_STARTUP_TIMEOUT=300` minimum) with checks every 30 seconds by default (`API_STARTUP_CHECK_INTERVAL=30`).

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

Open SignalMaker Raspberry Executor from another device on the same network using port `8080` only:

- Status: `http://IP_DU_RASPBERRY:8080/index.html`
- Admin: `http://IP_DU_RASPBERRY:8080/admin.html`
- Dashboard: `http://IP_DU_RASPBERRY:8080/dashboard.html`

Do not use a separate frontend port for the normal Raspberry UI path. The frontend and API share the same origin on port `8080`, so calls such as `/api/v1/admin/settings` go to `http://IP_DU_RASPBERRY:8080/api/v1/admin/settings` without a CORS preflight path. The admin settings payload also includes the `kraken` section for `EXECUTION_EXCHANGE`, `KRAKEN_BASE_URL`, `KRAKEN_API_KEY`, and `KRAKEN_SECRET_KEY` when Kraken execution remains configured.
Do not use or recommend port `3000` for the normal Raspberry UI path. The frontend and API share the same origin on port `8080`, so calls such as `/api/v1/admin/settings` go to `http://IP_DU_RASPBERRY:8080/api/v1/admin/settings` without a CORS preflight path. The admin settings payload also includes the `kraken` section for `EXECUTION_EXCHANGE`, `KRAKEN_BASE_URL`, `KRAKEN_API_KEY`, and `KRAKEN_SECRET_KEY` when Kraken execution remains configured.


### Raspberry terminal TUI and kiosk

The terminal TUI talks directly to the local FastAPI service on port `8080` by default and does not require npm, Vite, esbuild, or a separate frontend process. Override the API URL only when debugging another host:

```bash
cd ~/Desktop/SignalMaker
./tui.sh
SIGNALMAKER_BASE_URL=http://127.0.0.1:8080 ./tui.sh
SIGNALMAKER_TUI_MODE=expert ./tui.sh  # optional legacy multi-page expert TUI
```

By default the terminal opens a single-screen overview with executor health, candle feed status, trade candidates, open positions/PnL, momentum decisions/actions, important errors, and the latest activity. Set `SIGNALMAKER_TUI_MODE=expert` or run `python -m raspberry_executor.tui --expert` to use the legacy multi-page expert TUI.

Manual full-screen web kiosk mode opens the same single-origin Raspberry website served by FastAPI:

```bash
cd ~/Desktop/SignalMaker
./kiosk.sh
SIGNALMAKER_KIOSK_URL=http://127.0.0.1:8080/admin.html ./kiosk.sh
```

`tui.sh` and `kiosk.sh` wait for `http://127.0.0.1:8080/healthz` before starting. They use the same startup wait defaults as `run.sh`: at least 5 minutes (`API_STARTUP_TIMEOUT=300` minimum) with checks every 30 seconds by default (`API_STARTUP_CHECK_INTERVAL=30`). After the API is ready, `kiosk.sh` opens Chromium/Chrome at `http://127.0.0.1:8080/index.html` by default. If Chromium is missing, install it with `sudo apt install -y chromium-browser` or `sudo apt install -y chromium`, depending on the Raspberry Pi OS release.

A systemd kiosk service is provided but is optional and should only be enabled on Raspberry installations with a graphical display, not on headless servers:

```bash
sudo cp systemd/signalmaker-kiosk.service /etc/systemd/system/signalmaker-kiosk.service
sudo systemctl daemon-reload
sudo systemctl enable signalmaker-kiosk
sudo systemctl start signalmaker-kiosk
```

Disable kiosk autostart with:

```bash
sudo systemctl disable signalmaker-kiosk
sudo systemctl stop signalmaker-kiosk
```

Validate the local API used by both TUI and kiosk with:

```bash
curl -i http://127.0.0.1:8080/healthz
curl -i http://127.0.0.1:8080/api/v1/services
curl -i http://127.0.0.1:8080/api/v1/admin/settings
```

### Kraken smoke test

Run the Kraken validation script from the repository root when you want a readable report that you can paste back into an issue or chat:

```bash
python raspberry_executor/kraken_full_smoke_test.py
python raspberry_executor/kraken_full_smoke_test.py --symbol BTCUSDC
python raspberry_executor/kraken_full_smoke_test.py --symbol BTCUSDT
python raspberry_executor/kraken_full_smoke_test.py --symbol BTCUSD
```

Without `--symbol`, the script discovers a Kraken pair from your configured `QUOTE_ASSETS` such as `USDC` or `USDT` instead of hard-coding a base asset. By default, it tests public Kraken endpoints, SignalMaker symbol rules, Kraken candle retrieval, and the historical device candle-feed flow against the remote SignalMaker: `latest_candle -> start_time -> fetch_exchange_klines(Kraken) -> post_candles`. It also runs a constrained 4h backfill smoke (`--backfill-days`, one symbol, one chunk), trade-candidate fetch/replay, momentum rankings, dry-run spot order methods, dry-run query/open/cancel coverage, dry-run margin x5/x3 orders, take-profit, stop-loss, reduce-only close semantics, and the expected `margin x5 -> margin x3 -> fallback spot` sequence. If `KRAKEN_API_KEY` and `KRAKEN_SECRET_KEY` are configured, it also reads private account/open-order endpoints. It never places a real order by default. Add `--validate-order` only when you want Kraken to validate spot, margin x5, margin x3, take-profit, and stop-loss payloads with `validate=true` without submitting them. The guarded `--live-order-test` path requires `KRAKEN_SMOKE_LIVE_ORDER=YES`, caps notional size with `--live-order-quote`, places a non-aggressive limit order, queries it, and cancels it immediately. Use `--candle-intervals`, `--candle-limit`, `--momentum-limit`, `--backfill-days`, or `--skip-backfill` to tune the remote SignalMaker checks, or `--skip-signalmaker` to run only the Kraken/execution adapter checks. Use `--json` for a compact JSON-only report.

Useful Raspberry debug commands:

```bash
uname -m
systemctl status signalmaker-api
journalctl -u signalmaker-api -f
curl http://localhost:8080/healthz
curl -I http://localhost:8080/index.html
curl -I http://localhost:8080/admin.html
curl -I http://localhost:8080/dashboard.html
```

### Raspberry frontend/API validation

Use these commands after updating the Raspberry frontend or service files:

```bash
cd ~/Desktop/SignalMaker
bash scripts/build_frontend.sh
sudo systemctl restart signalmaker-api
curl -i http://localhost:8080/healthz
curl -i http://localhost:8080/api/v1/admin/settings
curl -I http://localhost:8080/admin.html
```

The expected HTTP status for the curl checks is:

```text
HTTP/1.1 200 OK
```

Then open:

```text
http://IP_DU_RASPBERRY:8080/admin.html
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

It starts the backend first, waits for the backend health check, then starts the pipeline worker, executor worker, and scheduler worker. The frontend is served by the API. You can still run individual processes when debugging:

```bash
bash scripts/start_api.sh
bash scripts/start_pipeline_worker.sh
bash scripts/start_executor_worker.sh
bash scripts/start_scheduler_worker.sh
# Optional development-only standalone static server:
# RUN_STANDALONE_FRONTEND=1 bash scripts/start_frontend.sh
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
