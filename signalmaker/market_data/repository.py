from __future__ import annotations

import json
import re
import threading
import weakref
from datetime import datetime, timedelta, timezone
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

from .models import ANALYSIS_PAYLOAD_VERSION, analysis_result_payload, legacy_analysis_result_payload


def _row(row: Any) -> dict[str, Any]:
    return dict(row._mapping) if hasattr(row, "_mapping") else dict(row)


class MarketDataRepository:
    # IBKR persists these bars verbatim.  In particular, there is deliberately no
    # "1d" fallback: an intraday workflow may only consume the matching interval.
    STOCK_ETF_TIMEFRAMES = {
        "15m": "15m",
        "1h": "1h",
        "4h": "4h",
        "1d": "1d",
    }
    TIMEFRAME_DURATIONS = {
        "15m": timedelta(minutes=15),
        "1h": timedelta(hours=1),
        "4h": timedelta(hours=4),
        "1d": timedelta(days=1),
    }
    _schema_ready: weakref.WeakSet = weakref.WeakSet()
    _schema_lock = threading.Lock()

    def __init__(self, db: Session):
        self.db = db

    @staticmethod
    def _asset_filters(query: str, params: dict[str, Any], filters: dict[str, Any]):
        for field in ("region", "country", "exchange_code", "provider", "pea_eligible", "ucits"):
            value = filters.get(field)
            if value is not None:
                query += f" AND a.{field} = :{field}"
                params[field] = value
        return query

    def ensure_schema(self) -> None:
        bind = self.db.get_bind()
        if bind in self._schema_ready:
            return
        with self._schema_lock:
            if bind in self._schema_ready:
                return
            self._ensure_schema(bind)
            self._schema_ready.add(bind)

    def _ensure_schema(self, bind) -> None:
        dialect = bind.dialect.name
        if dialect == "sqlite":
            stmts = _SQLITE_SCHEMA
        else:
            stmts = _POSTGRES_SCHEMA
        for stmt in stmts:
            self.db.execute(text(stmt))
        self._ensure_legacy_market_data_schema(dialect)
        self._ensure_market_assets_schema(dialect)
        self._normalize_ibkr_universes()
        self._ensure_stock_etf_candle_schema()
        self._ensure_analysis_result_indexes()
        self.db.commit()

    def _ensure_analysis_result_indexes(self) -> None:
        """Index dashboard filters; history itself remains addressable by run."""
        for statement in (
            "CREATE INDEX IF NOT EXISTS idx_market_analysis_results_latest ON market_analysis_results (asset_id, engine_name, timeframe, payload_version, created_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_market_analysis_results_filters ON market_analysis_results (engine_name, stage, signal, timeframe, created_at DESC)",
            "CREATE INDEX IF NOT EXISTS idx_market_analysis_results_run ON market_analysis_results (analysis_run_id)",
            "CREATE UNIQUE INDEX IF NOT EXISTS uq_market_analysis_results_idempotency ON market_analysis_results (idempotency_key)",
        ):
            self.db.execute(text(statement))

    def _ensure_market_assets_schema(self, dialect: str) -> None:
        """Add Europe-universe attributes safely to legacy installations."""
        definitions = {
            "provider": "TEXT NOT NULL DEFAULT 'IBKR'",
            "pea_eligible": "BOOLEAN NOT NULL DEFAULT FALSE",
            "ucits": "BOOLEAN NOT NULL DEFAULT FALSE",
            "metadata": "TEXT NULL" if dialect == "sqlite" else "JSONB NULL DEFAULT '{}'::jsonb",
        }
        if dialect == "sqlite":
            existing = {row.name for row in self.db.execute(text("PRAGMA table_info(market_assets)"))}
            for name, definition in definitions.items():
                if name not in existing:
                    self.db.execute(text(f"ALTER TABLE market_assets ADD COLUMN {name} {definition}"))
        else:
            for name, definition in definitions.items():
                self.db.execute(text(f"ALTER TABLE market_assets ADD COLUMN IF NOT EXISTS {name} {definition}"))

    def _normalize_ibkr_universes(self) -> None:
        """Idempotently collapse legacy IBKR universes into the two primary views."""
        rules = (
            ("Stocks Euronext Paris", "Europe Stocks", "STOCK", True, False, "FR", "PA"),
            ("Stocks Europe", "Europe Stocks", "STOCK", False, False, None, None),
            ("ETF PEA", "Europe ETF", "ETF", True, True, None, None),
            ("ETF Europe UCITS", "Europe ETF", "ETF", False, True, None, None),
        )
        for old, new, asset_type, pea, ucits, country, exchange in rules:
            old_id = self.db.execute(text("SELECT id FROM market_universes WHERE name=:name"), {"name": old}).scalar()
            if old_id is None:
                continue
            new_id = self.db.execute(text("SELECT id FROM market_universes WHERE name=:name"), {"name": new}).scalar()
            if new_id is None:
                new_id = self.db.execute(text("""
                    INSERT INTO market_universes (name, region, asset_type, provider)
                    VALUES (:name, 'EU', :asset_type, 'IBKR') RETURNING id
                """), {"name": new, "asset_type": asset_type}).scalar_one()
            self.db.execute(text("""
                UPDATE market_assets SET universe_id=:new_id, region='EU', asset_type=:asset_type,
                    pea_eligible=CASE WHEN :pea THEN TRUE ELSE COALESCE(pea_eligible, FALSE) END,
                    ucits=CASE WHEN :ucits THEN TRUE ELSE COALESCE(ucits, FALSE) END,
                    country=COALESCE(:country, country), exchange_code=COALESCE(:exchange, exchange_code),
                    provider='IBKR', updated_at=CURRENT_TIMESTAMP
                WHERE universe_id=:old_id AND (provider='IBKR' OR provider IS NULL)
            """), locals())

    def _ensure_legacy_market_data_schema(self, dialect: str) -> None:
        """Bring pre-existing market-data tables forward without replacing data."""
        self._ensure_job_requests_table()
        self._ensure_run_tables_schema()
        if dialect == "postgresql":
            for table_name in (
                "market_data_import_runs", "market_analysis_runs",
                "market_analysis_results", "market_data_job_requests",
            ):
                self._ensure_id_default(table_name)

    def _ensure_id_default(self, table_name: str, id_column: str = "id") -> None:
        """Install a PostgreSQL id generator matching a legacy column's type."""
        identifier = re.compile(r"^[A-Za-z0-9_]+$")
        if not identifier.fullmatch(table_name) or not identifier.fullmatch(id_column):
            raise ValueError("table and column names may contain only letters, digits, and underscores")
        if self.db.get_bind().dialect.name != "postgresql":
            return
        column = self.db.execute(text("""
            SELECT data_type, udt_name, column_default
            FROM information_schema.columns
            WHERE table_schema = current_schema()
              AND table_name = :table_name AND column_name = :id_column
        """), {"table_name": table_name, "id_column": id_column}).mappings().first()
        if not column or column["column_default"] is not None:
            return

        types = {str(column["data_type"]).lower(), str(column["udt_name"]).lower()}
        qualified_column = f'"{table_name}"."{id_column}"'
        if types & {"varchar", "text", "character varying"}:
            self.db.execute(text("CREATE EXTENSION IF NOT EXISTS pgcrypto"))
            self.db.execute(text(
                f'ALTER TABLE "{table_name}" ALTER COLUMN "{id_column}" '
                "SET DEFAULT gen_random_uuid()::text"
            ))
        elif "uuid" in types:
            self.db.execute(text("CREATE EXTENSION IF NOT EXISTS pgcrypto"))
            self.db.execute(text(
                f'ALTER TABLE "{table_name}" ALTER COLUMN "{id_column}" '
                "SET DEFAULT gen_random_uuid()"
            ))
        elif types & {"bigint", "integer", "int8", "int4"}:
            sequence = f"{table_name}_{id_column}_seq"
            self.db.execute(text(f'CREATE SEQUENCE IF NOT EXISTS "{sequence}"'))
            self.db.execute(text(
                f'ALTER TABLE "{table_name}" ALTER COLUMN "{id_column}" '
                f'''SET DEFAULT nextval('{sequence}')'''
            ))
            self.db.execute(text(
                f'ALTER SEQUENCE "{sequence}" OWNED BY {qualified_column}'
            ))
            self.db.execute(text(
                f'''SELECT setval('{sequence}', COALESCE((SELECT MAX("{id_column}")::bigint '''
                f'''FROM "{table_name}"), 0) + 1, false)'''
            ))

    def _ensure_run_tables_schema(self) -> None:
        """Upgrade legacy import/analysis run tables without rebuilding them."""
        dialect = self.db.get_bind().dialect.name
        definitions = (
            _SQLITE_RUN_TABLE_COLUMNS
            if dialect == "sqlite"
            else _POSTGRES_RUN_TABLE_COLUMNS
        )

        for table_name, columns in definitions.items():
            if dialect == "sqlite":
                existing = {
                    row.name
                    for row in self.db.execute(text(f"PRAGMA table_info({table_name})"))
                }
                for column_name, definition in columns.items():
                    if column_name not in existing:
                        self.db.execute(text(
                            f"ALTER TABLE {table_name} ADD COLUMN "
                            f"{column_name} {definition}"
                        ))
            else:
                for column_name, definition in columns.items():
                    self.db.execute(text(
                        f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS "
                        f"{column_name} {definition}"
                    ))

    def _ensure_stock_etf_candle_schema(self) -> None:
        """Create or upgrade only the STOCK/ETF candle table."""
        dialect = self.db.get_bind().dialect.name
        if dialect == "sqlite":
            exists = self.db.execute(text(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='stock_etf_candles'"
            )).first()
            if not exists:
                self.db.execute(text(_SQLITE_STOCK_ETF_CANDLES_TABLE))
            columns = {
                row.name for row in self.db.execute(text("PRAGMA table_info(stock_etf_candles)"))
            }
            column_definitions = _SQLITE_STOCK_ETF_CANDLE_COLUMNS
        else:
            exists = self.db.execute(text("""
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = current_schema()
                  AND table_name = 'stock_etf_candles'
            """)).first()
            if not exists:
                self.db.execute(text(_POSTGRES_STOCK_ETF_CANDLES_TABLE))
            columns = {
                row.column_name for row in self.db.execute(text("""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = current_schema()
                      AND table_name = 'stock_etf_candles'
                """))
            }
            column_definitions = _POSTGRES_STOCK_ETF_CANDLE_COLUMNS

        for name, definition in column_definitions.items():
            if name not in columns:
                self.db.execute(text(
                    f"ALTER TABLE stock_etf_candles ADD COLUMN {name} {definition}"
                ))

        self._ensure_stock_etf_candle_indexes(dialect)

    def _ensure_stock_etf_candle_indexes(self, dialect: str) -> None:
        expected = {
            "uq_stock_etf_candles_asset_provider_time": (
                True, (("asset_id", False), ("provider", False),
                       ("timeframe", False), ("timestamp", False))
            ),
            "idx_stock_etf_candles_asset_timeframe_timestamp": (
                False, (("asset_id", False), ("timeframe", False), ("timestamp", True))
            ),
            "idx_stock_etf_candles_symbol_timeframe_timestamp": (
                False, (("provider_symbol", False), ("timeframe", False), ("timestamp", True))
            ),
        }
        if dialect == "sqlite":
            rows = self.db.execute(text("PRAGMA index_list(stock_etf_candles)")).mappings()
            existing = {}
            for row in rows:
                details = self.db.execute(
                    text(f"PRAGMA index_xinfo('{row['name']}')")
                ).mappings()
                columns = tuple(
                    (detail["name"], bool(detail["desc"]))
                    for detail in details
                    if detail["key"] and detail["name"] is not None
                )
                existing[row["name"]] = (bool(row["unique"]), columns, bool(row["partial"]))
        else:
            rows = self.db.execute(text("""
                SELECT i.relname AS name, ix.indisunique AS is_unique,
                       ix.indpred IS NOT NULL AS is_partial,
                       array_agg(a.attname ORDER BY k.ordinality) AS columns,
                       array_agg((ix.indoption[k.ordinality - 1] & 1) = 1
                                 ORDER BY k.ordinality) AS descending
                FROM pg_class t
                JOIN pg_namespace n ON n.oid = t.relnamespace
                JOIN pg_index ix ON ix.indrelid = t.oid
                JOIN pg_class i ON i.oid = ix.indexrelid
                JOIN unnest(ix.indkey) WITH ORDINALITY AS k(attnum, ordinality) ON true
                JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = k.attnum
                WHERE n.nspname = current_schema() AND t.relname = 'stock_etf_candles'
                GROUP BY i.relname, ix.indisunique, ix.indpred
            """)).mappings()
            existing = {
                row["name"]: (
                    row["is_unique"],
                    tuple(zip(row["columns"], row["descending"])),
                    row["is_partial"],
                )
                for row in rows
            }

        for name, (unique, columns) in expected.items():
            definition = existing.get(name)
            if definition is not None and definition != (unique, columns, False):
                self.db.execute(text(f'DROP INDEX "{name}"'))
                definition = None
            if definition is None:
                unique_sql = "UNIQUE " if unique else ""
                columns_sql = ", ".join(
                    f'{column}{" DESC" if descending else ""}'
                    for column, descending in columns
                )
                self.db.execute(text(
                    f'CREATE {unique_sql}INDEX "{name}" '
                    f'ON stock_etf_candles ({columns_sql})'
                ))


    async def create_or_update_universe(self, name: str, description: str | None = None, region: str | None = None,
                                        asset_type: str | None = None, currency: str | None = None,
                                        provider: str = "IBKR", enabled: bool = True):
        q = text("""
        INSERT INTO market_universes (name, description, region, asset_type, currency, provider, enabled, updated_at)
        VALUES (:name, :description, :region, :asset_type, :currency, :provider, :enabled, CURRENT_TIMESTAMP)
        ON CONFLICT(name) DO UPDATE SET description=excluded.description, region=excluded.region,
          asset_type=excluded.asset_type, currency=excluded.currency, provider=excluded.provider,
          enabled=excluded.enabled, updated_at=CURRENT_TIMESTAMP
        RETURNING id
        """)
        return self.db.execute(q, locals()).scalar_one()

    async def upsert_market_asset(self, universe_id, symbol: str, provider_symbol: str, exchange_code: str | None,
                                  name: str | None, asset_type: str, region: str | None, country: str | None,
                                  currency: str | None, isin: str | None = None, mic: str | None = None,
                                  pea_eligible: bool | None = None, ucits: bool | None = None,
                                  enabled: bool = True, priority: int = 100, provider: str = "IBKR",
                                  metadata: dict | None = None):
        q = text("""
        INSERT INTO market_assets (universe_id,symbol,provider_symbol,exchange_code,name,asset_type,region,country,currency,isin,mic,pea_eligible,ucits,enabled,priority,provider,metadata,updated_at)
        VALUES (:universe_id,:symbol,:provider_symbol,:exchange_code,:name,:asset_type,:region,:country,:currency,:isin,:mic,:pea_eligible,:ucits,:enabled,:priority,:provider,:metadata,CURRENT_TIMESTAMP)
        ON CONFLICT(provider_symbol, asset_type) DO UPDATE SET universe_id=excluded.universe_id,symbol=excluded.symbol,exchange_code=excluded.exchange_code,name=excluded.name,region=excluded.region,country=excluded.country,currency=excluded.currency,isin=excluded.isin,mic=excluded.mic,pea_eligible=excluded.pea_eligible,ucits=excluded.ucits,enabled=excluded.enabled,priority=excluded.priority,provider=excluded.provider,metadata=excluded.metadata,updated_at=CURRENT_TIMESTAMP
        RETURNING id
        """)
        values = locals(); values["metadata"] = json.dumps(metadata or {})
        return self.db.execute(q, values).scalar_one()

    async def list_enabled_market_assets(self, asset_type: str | None = None, universe_name: str | None = None,
                                         limit: int | None = None, symbols: list[str] | None = None, **filters):
        query = """SELECT a.*, u.name AS universe_name, u.name AS universe FROM market_assets a LEFT JOIN market_universes u ON u.id = a.universe_id WHERE a.enabled = true"""
        params: dict[str, Any] = {}
        if asset_type:
            query += " AND a.asset_type = :asset_type"; params["asset_type"] = asset_type
        if universe_name:
            query += " AND u.name = :universe_name"; params["universe_name"] = universe_name
        query = self._asset_filters(query, params, filters)
        query += " ORDER BY a.priority ASC, a.symbol ASC"
        if limit:
            query += " LIMIT :limit"; params["limit"] = limit
        rows = [_row(r) for r in self.db.execute(text(query), params).all()]
        if symbols:
            wanted = set(symbols)
            rows = [r for r in rows if r.get("provider_symbol") in wanted]
        return rows


    async def find_market_asset_for_ingest(self, *, provider_symbol: str | None = None, symbol: str | None = None, asset_id=None, asset_type: str | None = None):
        query = "SELECT a.*, u.name AS universe_name FROM market_assets a LEFT JOIN market_universes u ON u.id = a.universe_id WHERE 1=1"
        params: dict[str, Any] = {}
        if asset_id is not None:
            query += " AND a.id = :asset_id"; params["asset_id"] = asset_id
        else:
            candidates = [value.upper() for value in (provider_symbol, symbol) if value]
            if not candidates:
                return None
            symbol_filters = []
            for index, candidate in enumerate(candidates):
                key = f"symbol_{index}"
                symbol_filters.append(f"upper(a.provider_symbol) = :{key} OR upper(a.symbol) = :{key}")
                params[key] = candidate
            query += " AND (" + " OR ".join(symbol_filters) + ")"
            if asset_type:
                query += " AND a.asset_type = :asset_type"; params["asset_type"] = asset_type
        query += " ORDER BY a.enabled DESC, a.priority ASC, a.symbol ASC LIMIT 1"
        row = self.db.execute(text(query), params).first()
        return _row(row) if row else None

    async def list_enabled_assets_by_universe(self, universe_name: str, limit: int | None = None):
        return await self.list_enabled_market_assets(universe_name=universe_name, limit=limit)

    async def upsert_stock_etf_candles(self, asset_id, provider: str, provider_symbol: str, timeframe: str, candles: list) -> int:
        """Upsert STOCK/ETF candles without touching the crypto candle table."""
        normalized_provider = provider.strip().upper()
        if normalized_provider != "IBKR":
            raise ValueError("IBKR is the only supported STOCK/ETF candle provider")

        def utc_timestamp(value: datetime | None) -> datetime:
            if value is None:
                raise ValueError("candle timestamp must not be null")
            # Both schemas use TIMESTAMP without a timezone. Persist a naive UTC
            # value so PostgreSQL and SQLite compare and return identical values.
            if value.tzinfo is not None:
                return value.astimezone(timezone.utc).replace(tzinfo=None)
            return value

        parameters = []
        for candle in candles:
            parameters.append({
                "asset_id": asset_id, "provider": normalized_provider,
                "provider_symbol": provider_symbol, "timeframe": timeframe,
                "timestamp": utc_timestamp(getattr(candle, "timestamp", None)),
                "open": candle.open, "high": candle.high, "low": candle.low,
                "close": candle.close,
                "adjusted_close": getattr(candle, "adjusted_close", None),
                "volume": getattr(candle, "volume", None),
            })
        sql = """
        INSERT INTO stock_etf_candles
          (asset_id, provider, provider_symbol, timeframe, timestamp, open, high, low, close, adjusted_close, volume, updated_at)
        VALUES
          (:asset_id, :provider, :provider_symbol, :timeframe, :timestamp, :open, :high, :low, :close, :adjusted_close, :volume, CURRENT_TIMESTAMP)
        ON CONFLICT (asset_id, provider, timeframe, timestamp) DO UPDATE SET
          provider_symbol=excluded.provider_symbol, open=excluded.open, high=excluded.high,
          low=excluded.low, close=excluded.close, adjusted_close=excluded.adjusted_close,
          volume=excluded.volume, updated_at=CURRENT_TIMESTAMP
        """
        for values in parameters:
            self.db.execute(text(sql), values)
        return len(candles)

    async def create_import_run(self, provider: str, run_type: str, status: str = "RUNNING", metadata: dict | None = None):
        return self.db.execute(text("INSERT INTO market_data_import_runs (provider,run_type,status,metadata) VALUES (:provider,:run_type,:status,:metadata) RETURNING id"), {"provider":provider,"run_type":run_type,"status":status,"metadata":json.dumps(metadata or {})}).scalar_one()

    async def finish_import_run(self, run_id, status: str, total_assets=0, success_count=0, failed_count=0, error_message=None):
        self.db.execute(text("UPDATE market_data_import_runs SET status=:status,finished_at=CURRENT_TIMESTAMP,total_assets=:total_assets,success_count=:success_count,failed_count=:failed_count,error_message=:error_message WHERE id=:run_id"), locals())

    async def create_analysis_run(self, engine_name: str, universe_id=None, timeframe="1d", status="RUNNING", metadata: dict | None = None):
        return self.db.execute(text("INSERT INTO market_analysis_runs (engine_name,universe_id,timeframe,status,metadata) VALUES (:engine_name,:universe_id,:timeframe,:status,:metadata) RETURNING id"), {"engine_name":engine_name,"universe_id":universe_id,"timeframe":timeframe,"status":status,"metadata":json.dumps(metadata or {})}).scalar_one()

    async def finish_analysis_run(self, run_id, status: str, total_assets=0, success_count=0, failed_count=0, error_message=None):
        self.db.execute(text("UPDATE market_analysis_runs SET status=:status,finished_at=CURRENT_TIMESTAMP,total_assets=:total_assets,success_count=:success_count,failed_count=:failed_count,error_message=:error_message WHERE id=:run_id"), locals())

    async def analysis_result_exists(self, idempotency_key: str) -> bool:
        return self.db.execute(text(
            "SELECT 1 FROM market_analysis_results WHERE idempotency_key=:key LIMIT 1"
        ), {"key": idempotency_key}).first() is not None

    async def insert_analysis_result(self, analysis_run_id, asset_id, engine_name: str, timeframe: str, result: dict, *, idempotency_key: str | None = None, workflow_version: str | None = None):
        payload = analysis_result_payload(result)
        self.db.execute(text("INSERT INTO market_analysis_results (analysis_run_id,asset_id,engine_name,timeframe,stage,signal,score,trend,confidence,payload_version,payload,idempotency_key,workflow_version) VALUES (:analysis_run_id,:asset_id,:engine_name,:timeframe,:stage,:signal,:score,:trend,:confidence,:payload_version,:payload,:idempotency_key,:workflow_version) ON CONFLICT (idempotency_key) DO NOTHING"), {"analysis_run_id":analysis_run_id,"asset_id":asset_id,"engine_name":engine_name,"timeframe":timeframe,"stage":result.get("stage") or payload.get("stage"),"signal":result.get("signal"),"score":result.get("score"),"trend":result.get("trend"),"confidence":result.get("confidence"),"payload_version":ANALYSIS_PAYLOAD_VERSION,"payload":json.dumps(payload, default=str),"idempotency_key":idempotency_key,"workflow_version":workflow_version})

    async def load_stock_etf_candles_for_asset(self, asset_id, timeframe="1d"):
        """Load one real, closed interval (never substitute a daily series)."""
        return (await self.load_stock_etf_candle_bundle(asset_id, (timeframe,)))[timeframe]

    async def load_stock_etf_candle_bundle(
        self, asset_id, timeframes=("15m", "1h", "4h"), *, as_of: datetime | None = None
    ):
        """Read a workflow's intervals in one snapshot and return closed bars only.

        The provider timestamps are UTC bar-open instants. Exchange holidays,
        daylight-saving transitions, regular-session boundaries and overnight
        gaps are therefore retained as gaps; this method never forward-fills or
        synthesizes bars. Corporate-action adjustment is also left untouched for
        the normalization layer to apply consistently.
        """
        requested = tuple(dict.fromkeys(timeframes))
        unsupported = [tf for tf in requested if tf not in self.STOCK_ETF_TIMEFRAMES]
        if unsupported:
            raise ValueError(f"unsupported_stock_etf_timeframes:{','.join(unsupported)}")
        if not requested:
            return {}
        storage = [self.STOCK_ETF_TIMEFRAMES[tf] for tf in requested]
        placeholders = ", ".join(f":tf_{index}" for index in range(len(storage)))
        params = {"asset_id": asset_id, **{f"tf_{i}": tf for i, tf in enumerate(storage)}}
        rows = self.db.execute(text(f"""
            SELECT * FROM stock_etf_candles
            WHERE asset_id=:asset_id AND timeframe IN ({placeholders})
            ORDER BY timeframe ASC, timestamp ASC, updated_at ASC, id ASC
        """), params).all()

        now = as_of or datetime.now(timezone.utc)
        if now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)
        else:
            now = now.astimezone(timezone.utc)
        bundle = {tf: [] for tf in requested}
        deduplicated: dict[tuple[str, datetime], dict[str, Any]] = {}
        reverse_mapping = {stored: requested_tf for requested_tf, stored in self.STOCK_ETF_TIMEFRAMES.items() if requested_tf in requested}
        for result in rows:
            candle = _row(result)
            tf = reverse_mapping[candle["timeframe"]]
            opened = candle["timestamp"]
            if isinstance(opened, str):
                opened = datetime.fromisoformat(opened.replace("Z", "+00:00"))
            opened_utc = opened.replace(tzinfo=timezone.utc) if opened.tzinfo is None else opened.astimezone(timezone.utc)
            if opened_utc + self.TIMEFRAME_DURATIONS[tf] > now:
                continue
            candle["timestamp"] = opened_utc
            # Last row wins if legacy data predates the current uniqueness rule.
            deduplicated[(tf, opened_utc)] = candle
        for (tf, _opened), candle in deduplicated.items():
            bundle[tf].append(candle)
        for candles in bundle.values():
            candles.sort(key=lambda candle: candle["timestamp"])
        return bundle


    async def list_market_universes(self):
        return [_row(r) for r in self.db.execute(text("SELECT * FROM market_universes ORDER BY name ASC")).all()]

    async def update_market_universe(self, universe_id, enabled: bool):
        self.db.execute(text("UPDATE market_universes SET enabled=:enabled, updated_at=CURRENT_TIMESTAMP WHERE id=:universe_id"), {"enabled": enabled, "universe_id": universe_id})

    async def update_market_asset(self, asset_id, enabled: bool | None = None, priority: int | None = None, universe_id=None):
        fields = []
        params = {"asset_id": asset_id}
        if enabled is not None:
            fields.append("enabled=:enabled"); params["enabled"] = enabled
        if priority is not None:
            fields.append("priority=:priority"); params["priority"] = priority
        if universe_id is not None:
            fields.append("universe_id=:universe_id"); params["universe_id"] = universe_id
        if not fields:
            return
        fields.append("updated_at=CURRENT_TIMESTAMP")
        self.db.execute(text(f"UPDATE market_assets SET {', '.join(fields)} WHERE id=:asset_id"), params)

    async def latest_analysis_results(self, engine_name: str | None = None, universe_name: str | None = None, asset_type: str | None = None, payload_version: int | None = None, limit: int = 200, **filters):
        query = """
        SELECT r.*, a.symbol, a.provider_symbol, a.name, a.asset_type, a.currency,
               a.country, a.exchange_code, a.region, a.pea_eligible, a.ucits, a.provider,
               a.enabled AS asset_enabled, u.name AS universe_name, u.name AS universe
        FROM market_analysis_results r
        JOIN market_assets a ON a.id = r.asset_id
        LEFT JOIN market_universes u ON u.id = a.universe_id
        WHERE r.id = (SELECT r2.id FROM market_analysis_results r2
          WHERE r2.asset_id=r.asset_id AND r2.engine_name=r.engine_name AND r2.timeframe=r.timeframe
            AND (:selected_payload_version IS NULL OR r2.payload_version=:selected_payload_version)
          ORDER BY r2.created_at DESC, r2.id DESC LIMIT 1)
          AND a.enabled = true
        """
        params: dict[str, Any] = {"limit": limit, "selected_payload_version": payload_version}
        if engine_name:
            query += " AND r.engine_name = :engine_name"; params["engine_name"] = engine_name
        if universe_name:
            query += " AND u.name = :universe_name"; params["universe_name"] = universe_name
        if asset_type:
            query += " AND a.asset_type = :asset_type"; params["asset_type"] = asset_type
        if payload_version is not None:
            query += " AND r.payload_version = :payload_version"; params["payload_version"] = payload_version
        query = self._asset_filters(query, params, filters)
        query += " ORDER BY r.created_at DESC, a.priority ASC, a.symbol ASC LIMIT :limit"
        rows = [_row(r) for r in self.db.execute(text(query), params).all()]
        for row in rows:
            if isinstance(row.get("payload"), str):
                row["payload"] = json.loads(row["payload"])
            row["payload"] = legacy_analysis_result_payload(row.get("payload"))
            row["payload_version"] = row.get("payload_version") or row["payload"]["schema_version"]
            row["schema_version"] = row["payload_version"]
            row["run_id"] = row.get("analysis_run_id")
            row["state_payload"] = row["payload"]
        return rows

    async def last_import_run(self):
        row = self.db.execute(text("SELECT * FROM market_data_import_runs ORDER BY started_at DESC LIMIT 1")).first()
        return _row(row) if row else None

    async def last_analysis_run(self):
        row = self.db.execute(text("SELECT * FROM market_analysis_runs ORDER BY started_at DESC LIMIT 1")).first()
        return _row(row) if row else None

    async def stock_etf_candle_quality(self, universe_name: str | None = None, asset_type: str | None = None, limit: int = 500, **filters):
        query = """
        SELECT a.id AS asset_id, a.symbol, a.provider_symbol, a.name, a.asset_type, a.currency,
               a.priority, u.name AS universe_name,
               COUNT(c.asset_id) AS candles_count, MIN(c.timestamp) AS first_candle_at, MAX(c.timestamp) AS last_candle_at,
               MAX(r.created_at) AS last_analysis_at,
               CASE
                 WHEN COUNT(c.asset_id) = 0 THEN 'MISSING'
                 WHEN MAX(c.timestamp) < CURRENT_TIMESTAMP - INTERVAL '7 days' THEN 'STALE'
                 ELSE 'OK'
               END AS data_status
        FROM market_assets a
        LEFT JOIN market_universes u ON u.id = a.universe_id
        LEFT JOIN stock_etf_candles c ON c.asset_id = a.id AND c.timeframe = '1d'
        LEFT JOIN market_analysis_results r ON r.asset_id = a.id
        WHERE a.enabled = true
        """
        if self.db.get_bind().dialect.name == "sqlite":
            query = query.replace("CURRENT_TIMESTAMP - INTERVAL '7 days'", "datetime('now', '-7 days')")
        params: dict[str, Any] = {"limit": limit}
        if universe_name:
            query += " AND u.name = :universe_name"; params["universe_name"] = universe_name
        if asset_type:
            query += " AND a.asset_type = :asset_type"; params["asset_type"] = asset_type
        query = self._asset_filters(query, params, filters)
        query += " GROUP BY a.id, a.symbol, a.provider_symbol, a.name, a.asset_type, a.currency, a.priority, u.name ORDER BY a.priority ASC, a.symbol ASC LIMIT :limit"
        return [_row(r) for r in self.db.execute(text(query), params).all()]

    async def analysis_freshness(self, universe_name: str | None = None, asset_type: str | None = None, limit: int = 500, **filters):
        rows = await self.stock_etf_candle_quality(universe_name=universe_name, asset_type=asset_type, limit=limit, **filters)
        for row in rows:
            row["analysis_status"] = "MISSING_ANALYSIS" if row.get("last_analysis_at") is None else "OK"
            if row.get("last_candle_at") and row.get("last_analysis_at") and str(row["last_candle_at"]) > str(row["last_analysis_at"]):
                row["analysis_status"] = "STALE_ANALYSIS"
        return rows

    async def feeder_status(self):
        """Return the compact coverage snapshot consumed by the admin page."""
        last_sync = self.db.execute(text("SELECT max(last_synced_at) FROM market_assets")).scalar()
        timeframes = [row[0] for row in self.db.execute(text(
            "SELECT DISTINCT timeframe FROM stock_etf_candles ORDER BY timeframe"
        )).all()]
        freshness = {}
        for row in self.db.execute(text("""
            SELECT coalesce(u.name, 'Sans univers') universe_name,
                   max(c.timestamp) last_candle_at, count(DISTINCT a.id) asset_count
            FROM market_assets a
            LEFT JOIN market_universes u ON u.id=a.universe_id
            LEFT JOIN stock_etf_candles c ON c.asset_id=a.id
            WHERE a.enabled=true
            GROUP BY u.name
        """)).mappings():
            freshness[row["universe_name"]] = {
                "last_candle_at": row["last_candle_at"], "asset_count": row["asset_count"],
                "status": "available" if row["last_candle_at"] else "missing",
            }
        return {"last_asset_sync_at": last_sync, "available_timeframes": timeframes,
                "freshness_by_universe": freshness}

    async def import_runs(self, limit: int = 50):
        return [_row(r) for r in self.db.execute(text("SELECT * FROM market_data_import_runs ORDER BY started_at DESC LIMIT :limit"), {"limit": limit}).all()]

    async def analysis_runs(self, limit: int = 50):
        return [_row(r) for r in self.db.execute(text("SELECT * FROM market_analysis_runs ORDER BY started_at DESC LIMIT :limit"), {"limit": limit}).all()]

    async def create_job_request(self, job_type: str, status: str = "QUEUED", payload: dict | None = None):
        self._ensure_job_requests_table()
        return self.db.execute(text("INSERT INTO market_data_job_requests (job_type,status,payload) VALUES (:job_type,:status,:payload) RETURNING id"), {"job_type": job_type, "status": status, "payload": json.dumps(payload or {})}).scalar_one()

    async def job_requests(self, limit: int = 50):
        self._ensure_job_requests_table()
        return [_row(r) for r in self.db.execute(text("SELECT * FROM market_data_job_requests ORDER BY created_at DESC LIMIT :limit"), {"limit": limit}).all()]

    async def claim_next_analysis_job(self, worker_id: str, *, max_attempts: int = 3):
        """Atomically claim one job; concurrent workers cannot receive the same row."""
        self._ensure_job_requests_table()
        dialect = self.db.get_bind().dialect.name
        suffix = " FOR UPDATE SKIP LOCKED" if dialect != "sqlite" else ""
        candidate = self.db.execute(text(
            "SELECT id FROM market_data_job_requests WHERE lower(status)='queued' AND job_type='analysis' "
            "AND attempts < :max_attempts ORDER BY created_at ASC LIMIT 1" + suffix
        ), {"max_attempts": max_attempts}).scalar()
        if candidate is None:
            return None
        row = self.db.execute(text(
            "UPDATE market_data_job_requests SET status='running', worker_id=:worker_id, attempts=attempts+1, "
            "started_at=COALESCE(started_at,CURRENT_TIMESTAMP), heartbeat_at=CURRENT_TIMESTAMP, updated_at=CURRENT_TIMESTAMP "
            "WHERE id=:id AND lower(status)='queued' RETURNING *"
        ), {"id": candidate, "worker_id": worker_id}).first()
        value = _row(row) if row else None
        if value and isinstance(value.get("payload"), str):
            value["payload"] = json.loads(value["payload"] or "{}")
        return value

    async def next_queued_analysis_job(self):
        """Compatibility read-only lookup; workers must use ``claim_next_analysis_job``."""
        self._ensure_job_requests_table()
        row = self.db.execute(text("SELECT * FROM market_data_job_requests WHERE lower(status)='queued' AND job_type='analysis' ORDER BY created_at ASC LIMIT 1")).first()
        value = _row(row) if row else None
        if value and isinstance(value.get("payload"), str): value["payload"] = json.loads(value["payload"] or "{}")
        return value

    async def heartbeat_job(self, job_id, worker_id: str):
        result = self.db.execute(text("UPDATE market_data_job_requests SET heartbeat_at=CURRENT_TIMESTAMP, updated_at=CURRENT_TIMESTAMP WHERE id=:id AND status='running' AND worker_id=:worker"), {"id": job_id, "worker": worker_id})
        return bool(result.rowcount)

    async def recover_abandoned_analysis_jobs(self, *, timeout_seconds: int = 900, max_attempts: int = 3):
        """Requeue stale claimed jobs, while preserving the bounded attempt count."""
        cutoff = datetime.now(timezone.utc) - timedelta(seconds=timeout_seconds)
        result = self.db.execute(text("""
            UPDATE market_data_job_requests SET status='queued', worker_id=NULL,
              last_error='worker heartbeat expired; controlled retry', updated_at=CURRENT_TIMESTAMP
            WHERE job_type='analysis' AND lower(status)='running' AND attempts < :max_attempts
              AND (heartbeat_at IS NULL OR heartbeat_at < :cutoff)
        """), {"cutoff": cutoff, "max_attempts": max_attempts})
        return result.rowcount

    async def update_job_request(self, job_id, status: str, *, result: dict | None = None):
        self._ensure_job_requests_table()
        finished = ",finished_at=CURRENT_TIMESTAMP" if status.lower() in {"completed", "insufficient_data", "skipped", "failed"} else ""
        self.db.execute(text("UPDATE market_data_job_requests SET status=:status,payload=:payload,last_error=:last_error,updated_at=CURRENT_TIMESTAMP" + finished + " WHERE id=:job_id"), {
            "job_id": job_id, "status": status, "payload": json.dumps(result or {}),
            "last_error": (result or {}).get("last_error"),
        })

    def _ensure_job_requests_table(self):
        dialect = self.db.get_bind().dialect.name
        if dialect == "sqlite":
            stmt = "CREATE TABLE IF NOT EXISTS market_data_job_requests (id INTEGER PRIMARY KEY AUTOINCREMENT, job_type TEXT NOT NULL, status TEXT NOT NULL, payload TEXT NULL, attempts INTEGER NOT NULL DEFAULT 0, worker_id TEXT NULL, started_at TIMESTAMP NULL, heartbeat_at TIMESTAMP NULL, finished_at TIMESTAMP NULL, last_error TEXT NULL, created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)"
        else:
            stmt = "CREATE TABLE IF NOT EXISTS market_data_job_requests (id BIGSERIAL PRIMARY KEY, job_type TEXT NOT NULL, status TEXT NOT NULL, payload JSONB NULL, attempts INTEGER NOT NULL DEFAULT 0, worker_id TEXT NULL, started_at TIMESTAMP NULL, heartbeat_at TIMESTAMP NULL, finished_at TIMESTAMP NULL, last_error TEXT NULL, created_at TIMESTAMP NOT NULL DEFAULT now(), updated_at TIMESTAMP NOT NULL DEFAULT now())"
        self.db.execute(text(stmt))
        columns = {"attempts": "INTEGER NOT NULL DEFAULT 0", "worker_id": "TEXT NULL", "started_at": "TIMESTAMP NULL",
                   "heartbeat_at": "TIMESTAMP NULL", "finished_at": "TIMESTAMP NULL", "last_error": "TEXT NULL"}
        existing = {row[1] for row in self.db.execute(text("PRAGMA table_info(market_data_job_requests)"))} if dialect == "sqlite" else {
            row[0] for row in self.db.execute(text("SELECT column_name FROM information_schema.columns WHERE table_name='market_data_job_requests'"))}
        for name, definition in columns.items():
            if name not in existing: self.db.execute(text(f"ALTER TABLE market_data_job_requests ADD COLUMN {name} {definition}"))

    def stats(self):
        def scalar(sql): return self.db.execute(text(sql)).scalar() or 0
        return {"total_universes": scalar("SELECT COUNT(*) FROM market_universes"), "total_assets": scalar("SELECT COUNT(*) FROM market_assets"), "total_candles": scalar("SELECT COUNT(*) FROM stock_etf_candles")}

_POSTGRES_SCHEMA = [
"CREATE EXTENSION IF NOT EXISTS pgcrypto",
"CREATE TABLE IF NOT EXISTS market_universes (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), name TEXT NOT NULL UNIQUE, description TEXT NULL, region TEXT NULL, asset_type TEXT NULL, currency TEXT NULL, provider TEXT NOT NULL DEFAULT 'IBKR', enabled BOOLEAN NOT NULL DEFAULT TRUE, created_at TIMESTAMP NOT NULL DEFAULT now(), updated_at TIMESTAMP NOT NULL DEFAULT now())",
"CREATE TABLE IF NOT EXISTS market_assets (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), universe_id UUID NULL REFERENCES market_universes(id), symbol TEXT NOT NULL, provider_symbol TEXT NOT NULL, exchange_code TEXT NULL, name TEXT NULL, asset_type TEXT NOT NULL, region TEXT NULL, country TEXT NULL, currency TEXT NULL, isin TEXT NULL, mic TEXT NULL, pea_eligible BOOLEAN NOT NULL DEFAULT FALSE, ucits BOOLEAN NOT NULL DEFAULT FALSE, provider TEXT NOT NULL DEFAULT 'IBKR', metadata JSONB NULL DEFAULT '{}'::jsonb, enabled BOOLEAN NOT NULL DEFAULT TRUE, priority INTEGER NOT NULL DEFAULT 100, last_synced_at TIMESTAMP NULL, last_error TEXT NULL, created_at TIMESTAMP NOT NULL DEFAULT now(), updated_at TIMESTAMP NOT NULL DEFAULT now(), UNIQUE(provider_symbol, asset_type))",
"CREATE TABLE IF NOT EXISTS market_data_import_runs (id BIGSERIAL PRIMARY KEY, provider TEXT NOT NULL, run_type TEXT NOT NULL, status TEXT NOT NULL, started_at TIMESTAMP NOT NULL DEFAULT now(), finished_at TIMESTAMP NULL, total_assets INTEGER DEFAULT 0, success_count INTEGER DEFAULT 0, failed_count INTEGER DEFAULT 0, error_message TEXT NULL, metadata JSONB NULL)",
"CREATE TABLE IF NOT EXISTS market_analysis_runs (id BIGSERIAL PRIMARY KEY, engine_name TEXT NOT NULL, universe_id UUID NULL REFERENCES market_universes(id), timeframe TEXT NOT NULL DEFAULT '1d', status TEXT NOT NULL, started_at TIMESTAMP NOT NULL DEFAULT now(), finished_at TIMESTAMP NULL, total_assets INTEGER DEFAULT 0, success_count INTEGER DEFAULT 0, failed_count INTEGER DEFAULT 0, metadata JSONB NULL, error_message TEXT NULL)",
"CREATE TABLE IF NOT EXISTS market_analysis_results (id BIGSERIAL PRIMARY KEY, analysis_run_id BIGINT NULL REFERENCES market_analysis_runs(id), asset_id UUID NOT NULL REFERENCES market_assets(id), engine_name TEXT NOT NULL, timeframe TEXT NOT NULL, stage TEXT NULL, signal TEXT NULL, score NUMERIC NULL, trend TEXT NULL, confidence NUMERIC NULL, payload_version INTEGER NOT NULL DEFAULT 1, payload JSONB NOT NULL DEFAULT '{}'::jsonb, idempotency_key TEXT NULL UNIQUE, workflow_version TEXT NULL, created_at TIMESTAMP NOT NULL DEFAULT now())",
]
_SQLITE_SCHEMA = [
"CREATE TABLE IF NOT EXISTS market_universes (id TEXT PRIMARY KEY DEFAULT (lower(hex(randomblob(16)))), name TEXT NOT NULL UNIQUE, description TEXT NULL, region TEXT NULL, asset_type TEXT NULL, currency TEXT NULL, provider TEXT NOT NULL DEFAULT 'IBKR', enabled BOOLEAN NOT NULL DEFAULT TRUE, created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)",
"CREATE TABLE IF NOT EXISTS market_assets (id TEXT PRIMARY KEY DEFAULT (lower(hex(randomblob(16)))), universe_id TEXT NULL REFERENCES market_universes(id), symbol TEXT NOT NULL, provider_symbol TEXT NOT NULL, exchange_code TEXT NULL, name TEXT NULL, asset_type TEXT NOT NULL, region TEXT NULL, country TEXT NULL, currency TEXT NULL, isin TEXT NULL, mic TEXT NULL, pea_eligible BOOLEAN NOT NULL DEFAULT FALSE, ucits BOOLEAN NOT NULL DEFAULT FALSE, provider TEXT NOT NULL DEFAULT 'IBKR', metadata TEXT NULL, enabled BOOLEAN NOT NULL DEFAULT TRUE, priority INTEGER NOT NULL DEFAULT 100, last_synced_at TIMESTAMP NULL, last_error TEXT NULL, created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, UNIQUE(provider_symbol, asset_type))",
"CREATE TABLE IF NOT EXISTS market_data_import_runs (id INTEGER PRIMARY KEY AUTOINCREMENT, provider TEXT NOT NULL, run_type TEXT NOT NULL, status TEXT NOT NULL, started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, finished_at TIMESTAMP NULL, total_assets INTEGER DEFAULT 0, success_count INTEGER DEFAULT 0, failed_count INTEGER DEFAULT 0, error_message TEXT NULL, metadata TEXT NULL)",
"CREATE TABLE IF NOT EXISTS market_analysis_runs (id INTEGER PRIMARY KEY AUTOINCREMENT, engine_name TEXT NOT NULL, universe_id TEXT NULL REFERENCES market_universes(id), timeframe TEXT NOT NULL DEFAULT '1d', status TEXT NOT NULL, started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, finished_at TIMESTAMP NULL, total_assets INTEGER DEFAULT 0, success_count INTEGER DEFAULT 0, failed_count INTEGER DEFAULT 0, metadata TEXT NULL, error_message TEXT NULL)",
"CREATE TABLE IF NOT EXISTS market_analysis_results (id INTEGER PRIMARY KEY AUTOINCREMENT, analysis_run_id BIGINT NULL REFERENCES market_analysis_runs(id), asset_id TEXT NOT NULL REFERENCES market_assets(id), engine_name TEXT NOT NULL, timeframe TEXT NOT NULL, stage TEXT NULL, signal TEXT NULL, score NUMERIC NULL, trend TEXT NULL, confidence NUMERIC NULL, payload_version INTEGER NOT NULL DEFAULT 1, payload TEXT NOT NULL DEFAULT '{}', idempotency_key TEXT NULL UNIQUE, workflow_version TEXT NULL, created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)",
]

# Columns are deliberately nullable when upgrading legacy tables if no reliable
# value can be inferred for existing rows. Defaults still apply to new rows.
_POSTGRES_RUN_TABLE_COLUMNS = {
    "market_data_import_runs": {
        "started_at": "TIMESTAMP NOT NULL DEFAULT now()",
        "metadata": "JSONB NULL",
        "total_assets": "INTEGER DEFAULT 0",
        "success_count": "INTEGER DEFAULT 0",
        "failed_count": "INTEGER DEFAULT 0",
        "error_message": "TEXT NULL",
        "finished_at": "TIMESTAMP NULL",
    },
    "market_analysis_runs": {
        "started_at": "TIMESTAMP NOT NULL DEFAULT now()",
        "universe_id": "UUID NULL REFERENCES market_universes(id)",
        "timeframe": "TEXT DEFAULT '1d'",
        "metadata": "JSONB NULL",
        "total_assets": "INTEGER DEFAULT 0",
        "success_count": "INTEGER DEFAULT 0",
        "failed_count": "INTEGER DEFAULT 0",
        "error_message": "TEXT NULL",
        "finished_at": "TIMESTAMP NULL",
    },
    "market_analysis_results": {
        "analysis_run_id": "BIGINT NULL REFERENCES market_analysis_runs(id)",
        "idempotency_key": "TEXT NULL", "workflow_version": "TEXT NULL",
        "asset_id": "UUID NULL REFERENCES market_assets(id)",
        "engine_name": "TEXT NULL",
        "timeframe": "TEXT NULL",
        "signal": "TEXT NULL",
        "stage": "TEXT NULL",
        "payload_version": "INTEGER DEFAULT 1",
        "score": "NUMERIC NULL",
        "trend": "TEXT NULL",
        "confidence": "NUMERIC NULL",
        "payload": "JSONB NULL DEFAULT '{}'::jsonb",
        "created_at": "TIMESTAMP NULL DEFAULT now()",
    },
    "market_data_job_requests": {
        "job_type": "TEXT NULL", "status": "TEXT NULL", "payload": "JSONB NULL",
        "created_at": "TIMESTAMP DEFAULT now()", "updated_at": "TIMESTAMP DEFAULT now()",
    },
}

_SQLITE_RUN_TABLE_COLUMNS = {
    "market_data_import_runs": {
        "started_at": "TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP",
        "metadata": "TEXT NULL",
        "total_assets": "INTEGER DEFAULT 0",
        "success_count": "INTEGER DEFAULT 0",
        "failed_count": "INTEGER DEFAULT 0",
        "error_message": "TEXT NULL",
        "finished_at": "TIMESTAMP NULL",
    },
    "market_analysis_runs": {
        "started_at": "TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP",
        "universe_id": "TEXT NULL REFERENCES market_universes(id)",
        "timeframe": "TEXT DEFAULT '1d'",
        "metadata": "TEXT NULL",
        "total_assets": "INTEGER DEFAULT 0",
        "success_count": "INTEGER DEFAULT 0",
        "failed_count": "INTEGER DEFAULT 0",
        "error_message": "TEXT NULL",
        "finished_at": "TIMESTAMP NULL",
    },
    "market_analysis_results": {
        "analysis_run_id": "BIGINT NULL REFERENCES market_analysis_runs(id)",
        "idempotency_key": "TEXT NULL", "workflow_version": "TEXT NULL",
        "asset_id": "TEXT NULL REFERENCES market_assets(id)",
        "engine_name": "TEXT NULL",
        "timeframe": "TEXT NULL",
        "signal": "TEXT NULL",
        "stage": "TEXT NULL",
        "payload_version": "INTEGER DEFAULT 1",
        "score": "NUMERIC NULL",
        "trend": "TEXT NULL",
        "confidence": "NUMERIC NULL",
        "payload": "TEXT NULL DEFAULT '{}'",
        "created_at": "TIMESTAMP NULL DEFAULT CURRENT_TIMESTAMP",
    },
    "market_data_job_requests": {
        "job_type": "TEXT NULL", "status": "TEXT NULL", "payload": "TEXT NULL",
        "created_at": "TIMESTAMP NULL", "updated_at": "TIMESTAMP NULL",
    },
}

_POSTGRES_STOCK_ETF_CANDLES_TABLE = "CREATE TABLE stock_etf_candles (id BIGSERIAL PRIMARY KEY, asset_id UUID NOT NULL REFERENCES market_assets(id) ON DELETE CASCADE, provider TEXT NOT NULL DEFAULT 'IBKR', provider_symbol TEXT NOT NULL, timeframe TEXT NOT NULL, timestamp TIMESTAMP NOT NULL, open NUMERIC NOT NULL, high NUMERIC NOT NULL, low NUMERIC NOT NULL, close NUMERIC NOT NULL, adjusted_close NUMERIC NULL, volume NUMERIC NULL, created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)"
_SQLITE_STOCK_ETF_CANDLES_TABLE = "CREATE TABLE stock_etf_candles (id INTEGER PRIMARY KEY AUTOINCREMENT, asset_id TEXT NOT NULL REFERENCES market_assets(id) ON DELETE CASCADE, provider TEXT NOT NULL DEFAULT 'IBKR', provider_symbol TEXT NOT NULL, timeframe TEXT NOT NULL, timestamp TIMESTAMP NOT NULL, open NUMERIC NOT NULL, high NUMERIC NOT NULL, low NUMERIC NOT NULL, close NUMERIC NOT NULL, adjusted_close NUMERIC NULL, volume NUMERIC NULL, created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)"

# Upgrades intentionally use nullable definitions: legacy tables can contain rows
# for which a safe value cannot be inferred. New tables retain the stricter schema.
_POSTGRES_STOCK_ETF_CANDLE_COLUMNS = {
    "asset_id": "UUID", "provider": "TEXT DEFAULT 'IBKR'", "provider_symbol": "TEXT",
    "timeframe": "TEXT", "timestamp": "TIMESTAMP", "open": "NUMERIC", "high": "NUMERIC",
    "low": "NUMERIC", "close": "NUMERIC", "adjusted_close": "NUMERIC", "volume": "NUMERIC",
    "created_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
    "updated_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
}
_SQLITE_STOCK_ETF_CANDLE_COLUMNS = {
    **_POSTGRES_STOCK_ETF_CANDLE_COLUMNS,
    "asset_id": "TEXT",
    # SQLite rejects non-constant defaults in ALTER TABLE ADD COLUMN.
    "created_at": "TIMESTAMP",
    "updated_at": "TIMESTAMP",
}
