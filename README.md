# SignalMaker

## IBKR Europe stock/ETF model

IBKR market data uses two primary universes: **Europe Stocks** and **Europe ETF**.
PEA eligibility and UCITS status are asset attributes rather than universes. Assets
also carry `provider`, `provider_symbol`, `asset_type`, `region`, `country`,
`currency`, `exchange_code`, `pea_eligible`, `ucits`, and free-form `metadata`.

Useful filter views are:

* **PEA Eligible:** `region=EU&pea_eligible=true`
* **ETF PEA:** `asset_type=ETF&region=EU&pea_eligible=true&ucits=true`
* **France Stocks:** `asset_type=STOCK&region=EU&country=FR`
* **Amsterdam**, **Xetra**, **London**, and **Switzerland:** select assets with
  exchange codes `AMS`, `XETRA`, `LSE`, and `SWX`, respectively.

The candle ingest endpoint continues to accept `Stocks Euronext Paris`, `Stocks
Europe`, `ETF PEA`, and `ETF Europe UCITS` from older executor-app releases, but
new integrations should send one of the two primary universe names. Legacy
`IBKR Imported` records are retained only when the payload lacks enough asset type
and region information to select a primary universe.

Phases 1 to 4 are now scaffolded in a runnable form for Replit VM.

## Included
- FastAPI application layer
- Centralized settings with `.env`
- SQLAlchemy 2 setup
- PostgreSQL-ready database configuration
- React + Vite frontend dashboard
- Live tables:
  - `asset_state_current`
  - `live_runs`
  - `trade_candidates`
  - `positions`
  - `orders`
  - `fills`
  - `market_candles`
- Service separation:
  - market-data ingestion API fed by the Raspberry Executor
  - signal engine wired to legacy v231 logic
  - planner service generating trade candidates
  - `wyckoff_paper` worker for simulated Wyckoff/SMC trading
  - scheduler service plus simple worker loops
- Pipeline and executor API endpoints
- VM deployment helper script
- Production env sample and systemd templates

## Frontend
The React/Vite app lives in `frontend/`.

Start it with:
```bash
bash scripts/start_frontend.sh
```

Leave `VITE_API_BASE` unset for the normal same-origin deployment. The frontend
server proxies `/api`, `/admin`, and `/healthz` to the backend. Set an alternate
base only when the browser can reach that backend URL (for example, during
development with the API on another host):
```bash
VITE_API_BASE=https://api.example.com npm run build
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
bash run.sh api
bash scripts/start_frontend.sh
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
bash scripts/start_wyckoff_paper_worker.sh
bash scripts/start_scheduler_worker.sh
bash scripts/start_frontend.sh
```

## Production env
```bash
cp .env.production.example .env
```
Then edit the database URL and runtime values.

### Préflight Kraken avant la production

Avant d'activer l'exécution réelle, lancez le préflight avec les mêmes clés API,
symboles et modes que la production :

```bash
# Le mode par défaut est MOMENTUM_LIVE_MODE.
bash run.sh kraken-preflight --symbol BTCUSD --quote-amount 50

# Vérifier explicitement les payloads spot et margin, achat et vente.
bash run.sh kraken-preflight --symbol BTCUSD --quote-amount 50 \
  --mode spot --mode margin
```

Le préflight appelle réellement `AssetPairs`, `Ticker`, `OHLC`, `Balance`,
`OpenOrders` et `OpenPositions`. Il envoie aussi chaque variante de `AddOrder`
avec `validate=true` : Kraken valide donc la signature, les permissions, la
paire, le volume et le levier **sans créer d'ordre**. La commande ne journalise
ni les clés ni les soldes et renvoie un code non nul dès qu'au moins un contrôle
échoue. `QueryOrders` et `CancelOrder` ne sont pas lancés : ils nécessitent un
identifiant d'ordre réel et ne disposent pas d'un mode de validation sans effet.

Ne passez `KRAKEN_DRY_RUN=false` et n'activez les workers d'exécution qu'après un
résultat global `"ok": true`. Les clés doivent autoriser la consultation des
fonds/ordres et la création d'ordres ; le préflight ne requiert aucun droit de
retrait.

### Démarrer le momentum live

Le moteur momentum est long-only ; commencez en spot. Après un préflight réussi,
configurez les valeurs suivantes dans le `.env` de production :

```dotenv
KRAKEN_EXECUTION_ENABLED=true
KRAKEN_DRY_RUN=false
MOMENTUM_LIVE_ENABLED=true
MOMENTUM_LIVE_MODE=spot
MOMENTUM_LIVE_INTERVAL_SECONDS=60
```

Puis installez et démarrez le worker dédié :

```bash
sudo cp deploy/systemd/signalmaker-momentum-live.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now signalmaker-momentum-live
sudo systemctl status signalmaker-momentum-live
```

Le worker refuse de démarrer si les clés, les garde-fous Kraken ou le mode live
ne sont pas cohérents. Chaque `decision_id` live est journalisé avant le prochain
cycle afin qu'un redémarrage ou une décision persistée ne soumette pas deux fois
le même ordre. Pour arrêter immédiatement les nouvelles soumissions :

```bash
sudo systemctl stop signalmaker-momentum-live
```

### Démarrer Wyckoff / SMC en live

Le worker `wyckoff_paper` reste dédié au portefeuille paper. L'exécution
réelle des candidats Wyckoff/SMC utilise un worker séparé afin qu'elle puisse
être arrêtée sans interrompre le moteur d'analyse ni le paper trading.

Après un préflight Kraken réussi, configurez :

```dotenv
KRAKEN_EXECUTION_ENABLED=true
KRAKEN_DRY_RUN=false
WYCKOFF_LIVE_ENABLED=true
WYCKOFF_LIVE_MODE=spot
WYCKOFF_LIVE_INTERVAL_SECONDS=60
WYCKOFF_LIVE_LIMIT=10
WYCKOFF_LIVE_QUANTITY=1
```

Puis démarrez le service dédié :

```bash
sudo cp deploy/systemd/signalmaker-wyckoff-live.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now signalmaker-wyckoff-live
```

Le spot accepte seulement les candidats haussiers. Pour ouvrir des positions
baissières, utilisez `WYCKOFF_LIVE_MODE=margin` et activez explicitement
`KRAKEN_MARGIN_EXECUTION_ENABLED` ainsi que `KRAKEN_MARGIN_SHORTS_ENABLED`.

## systemd templates
Templates are available in `deploy/systemd/`.

### Momentum cadence setting

The momentum worker cadence is stored in `AppSetting` with category `momentum`
and key `momentum_paper_cadence_hours`. Its default is one hour. Select 1, 4, 8,
or 24 hours on the Momentum page or in **Admin Settings → Bot runtime → Momentum
engine cadence**. The selection is persisted immediately and is used by the
momentum worker on its next tick.

## Notes
- This is now a functional scaffold, not a finished production trading system.
- It still needs hardening for real live trading: risk engine, exchange auth, order reconciliation, stop/TP sync, worker supervision, retries, and UI migration.
