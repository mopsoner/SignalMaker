# Runtime settings source of truth

SignalMaker now uses one explicit runtime source of truth:

1. **Canonical runtime store:** the API database table `app_settings`.
   - The API and admin UI load and persist runtime values through `app/services/runtime_settings.py`.
   - Canonical keys are lowercase section keys such as `executor.quote_assets`, `kraken.kraken_base_url`, and `notifications.telegram_secret`.
2. **Bootstrap/fallback store:** `.env`.
   - `app/core/config.py` still reads `.env` at process start so the application can boot before `app_settings` is available.
   - Raspberry-local fields that are not administered by the API may remain in `.env`.
   - `.env` must not be treated as authoritative after a canonical `app_settings` row exists.
3. **Deprecated local Raspberry store:** `raspberry_executor/settings_store.py` and its SQLite `settings` table.
   - This table is retained only as a compatibility source for migration/fallback during the cleanup window.
   - New runtime writes should go to the API/admin settings flow, not to the local Raspberry settings table.
   - Once deployed Raspberry executors read runtime settings exclusively from the API, this module can be removed.

## Runtime load flow

1. `app.core.config.Settings` reads environment variables and `.env` for bootstrap defaults.
2. `app.services.runtime_settings.load_runtime_settings()` starts with those defaults.
3. Existing canonical `app_settings` rows override bootstrap defaults.
4. Legacy admin rows and legacy Raspberry keys are migrated into canonical `app_settings` keys only when the canonical row is missing or empty.
5. The returned runtime payload is the authoritative settings payload for API services, the admin UI, and Raspberry runtime overrides.

## Migration rules

The cleanup migration copies useful old values into canonical `app_settings` rows without overwriting canonical runtime values:

| Legacy source | Canonical target |
| --- | --- |
| `EXECUTION_EXCHANGE` | `executor.execution_exchange` |
| `QUOTE_ASSETS`, `KRAKEN_QUOTE_ASSETS`, `ALLOWED_SYMBOLS`, `EXECUTION_QUOTE_ASSET`, `CANDLE_FEED_QUOTES`, `CANDLE_FEED_QUOTE_ASSETS` | `executor.quote_assets` |
| `KRAKEN_BASE_URL`, `KRAKEN_REST_BASE` | `kraken.kraken_base_url` |
| `KRAKEN_API_KEY` | `kraken.kraken_api_key` |
| `KRAKEN_SECRET_KEY` | `kraken.kraken_secret_key` |
| `TELEGRAM_BOT_TOKEN` | `notifications.telegram_secret` |
| `TELEGRAM_CHAT_ID` | `notifications.telegram_chat_id` |
| `DISCORD_WEBHOOK_URL` | `notifications.discord_url` |
| `LIVE_TRADING_ENABLED` | `live.live_trading_enabled` |
| `KRAKEN_USE_TESTNET` | `live.kraken_use_testnet` |
| `LIVE_MAX_OPEN_POSITIONS` | `live.live_max_open_positions` |
| `LIVE_MAX_NOTIONAL_PER_TRADE` | `live.live_max_notional_per_trade` |
| `SIGNALMAKER_BASE_URL` | `momentum.signalmaker_base_url` |
| `MOMENTUM_CANDIDATES_SYNC_ENABLED` | `momentum.momentum_candidates_sync_enabled` |
| `MOMENTUM_CANDIDATES_LIMIT` | `momentum.momentum_candidates_limit` |
| `MOMENTUM_CANDIDATES_MIN_SCORE` | `momentum.momentum_candidates_min_score` |

Legacy uppercase admin rows are also canonicalized and deleted after their values are copied.

## Removal plan for `settings_store.py`

1. Deploy this migration so old `.env` and Raspberry SQLite values are copied to `app_settings`.
2. Confirm Raspberry executors receive API/runtime overrides for exchange, quote assets, Kraken credentials, and notifier settings.
3. Stop calling `write_settings()` from Raspberry admin surfaces.
4. Remove `raspberry_executor/settings_store.py` and the SQLite `settings` table once no deployed executor depends on them.
