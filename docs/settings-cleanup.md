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
2. `app.services.runtime_settings.seed_app_settings_from_env()` parses `.env` with the same BaseSettings dotenv parser, applies the exhaustive `.env` -> `DEFAULT_SETTINGS` map, creates missing canonical `app_settings` rows, and fills empty rows.
3. `.env` is prioritized only during that initial seed (or during an explicit resync with `python -m scripts.seed_app_settings --overwrite`).
4. After installation, existing canonical `app_settings` rows override bootstrap defaults and are the admin/runtime source of truth.
5. Editing `.env` after installation requires a controlled resync command or deleting the intended `app_settings` rows before rerunning `python -m scripts.seed_app_settings`; otherwise populated DB rows are kept.
6. Legacy admin rows and legacy Raspberry keys are migrated into canonical `app_settings` keys only when the canonical row is missing or empty.
7. The returned runtime payload is the authoritative settings payload for API services, the admin UI, and Raspberry runtime overrides.

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

Legacy uppercase admin rows are also canonicalized and deleted after their values are copied.

## Removal plan for `settings_store.py`

1. Deploy this migration so old `.env` and Raspberry SQLite values are copied to `app_settings`.
2. Confirm Raspberry executors receive API/runtime overrides for exchange, quote assets, Kraken credentials, and notifier settings.
3. Stop calling `write_settings()` from Raspberry admin surfaces.
4. Remove `raspberry_executor/settings_store.py` and the SQLite `settings` table once no deployed executor depends on them.

## Initial seed and controlled resync

Run the seed explicitly after creating or editing `.env` during installation:

```bash
python -m scripts.seed_app_settings
```

The default seed creates missing `app_settings` rows and fills empty values only; it does not overwrite rows already configured by the admin UI or runtime DB. The command prints a JSON summary showing which settings came from `.env`, which remained from the DB, and which fell back to `DEFAULT_SETTINGS`.

For an intentional post-install resync from `.env`, use:

```bash
python -m scripts.seed_app_settings --overwrite
```

Use `--overwrite` carefully because it replaces populated DB rows with `.env`/default values. Without `--overwrite`, modify `.env` after installation only as a bootstrap fallback, then delete selected `app_settings` rows if you want the next seed to recreate them from `.env`.
