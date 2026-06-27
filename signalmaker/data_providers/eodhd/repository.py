from __future__ import annotations

import json
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session


def _row(row: Any) -> dict[str, Any]:
    return dict(row._mapping) if hasattr(row, "_mapping") else dict(row)


class EODHDRepository:
    def __init__(self, db: Session):
        self.db = db

    def ensure_schema(self) -> None:
        bind = self.db.get_bind()
        dialect = bind.dialect.name
        if dialect == "sqlite":
            stmts = _SQLITE_SCHEMA
        else:
            stmts = _POSTGRES_SCHEMA
        for stmt in stmts:
            try:
                self.db.execute(text(stmt))
            except Exception:
                self.db.rollback()
        self._ensure_market_candle_columns(dialect)
        self._ensure_job_requests_table()
        self.db.commit()


    def _ensure_market_candle_columns(self, dialect: str) -> None:
        required = {
            "asset_id": "TEXT" if dialect == "sqlite" else "UUID",
            "provider": "TEXT",
            "provider_symbol": "TEXT",
            "timeframe": "TEXT",
            "timestamp": "TIMESTAMP",
            "adjusted_close": "NUMERIC",
            "created_at": "TIMESTAMP",
            "updated_at": "TIMESTAMP",
        }
        existing = set()
        if dialect == "sqlite":
            existing = {r[1] for r in self.db.execute(text("PRAGMA table_info(market_candles)")).all()}
        else:
            rows = self.db.execute(text("SELECT column_name FROM information_schema.columns WHERE table_name='market_candles'")).all()
            existing = {r[0] for r in rows}
        for name, typ in required.items():
            if name in existing:
                continue
            default = " DEFAULT CURRENT_TIMESTAMP" if name in {"created_at", "updated_at"} else ""
            try:
                self.db.execute(text(f"ALTER TABLE market_candles ADD COLUMN {name} {typ}{default}"))
            except Exception:
                self.db.rollback()
        try:
            self.db.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS uq_market_candles_asset_provider_time ON market_candles(asset_id, provider, timeframe, timestamp)"))
        except Exception:
            self.db.rollback()

    async def create_or_update_universe(self, name: str, description: str | None = None, region: str | None = None,
                                        asset_type: str | None = None, currency: str | None = None,
                                        provider: str = "EODHD", enabled: bool = True):
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
                                  enabled: bool = True, priority: int = 100):
        q = text("""
        INSERT INTO market_assets (universe_id,symbol,provider_symbol,exchange_code,name,asset_type,region,country,currency,isin,mic,pea_eligible,ucits,enabled,priority,updated_at)
        VALUES (:universe_id,:symbol,:provider_symbol,:exchange_code,:name,:asset_type,:region,:country,:currency,:isin,:mic,:pea_eligible,:ucits,:enabled,:priority,CURRENT_TIMESTAMP)
        ON CONFLICT(provider_symbol, asset_type) DO UPDATE SET universe_id=excluded.universe_id,symbol=excluded.symbol,exchange_code=excluded.exchange_code,name=excluded.name,region=excluded.region,country=excluded.country,currency=excluded.currency,isin=excluded.isin,mic=excluded.mic,pea_eligible=excluded.pea_eligible,ucits=excluded.ucits,enabled=excluded.enabled,priority=excluded.priority,updated_at=CURRENT_TIMESTAMP
        RETURNING id
        """)
        return self.db.execute(q, locals()).scalar_one()

    async def list_enabled_market_assets(self, asset_type: str | None = None, universe_name: str | None = None,
                                         limit: int | None = None, symbols: list[str] | None = None):
        query = """SELECT a.*, u.name AS universe_name FROM market_assets a LEFT JOIN market_universes u ON u.id = a.universe_id WHERE a.enabled = true"""
        params: dict[str, Any] = {}
        if asset_type:
            query += " AND a.asset_type = :asset_type"; params["asset_type"] = asset_type
        if universe_name:
            query += " AND u.name = :universe_name"; params["universe_name"] = universe_name
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

    async def upsert_market_candles(self, asset_id, provider: str, provider_symbol: str, timeframe: str, candles: list) -> int:
        q = text("""
        INSERT INTO market_candles (asset_id,provider,provider_symbol,timeframe,timestamp,open,high,low,close,adjusted_close,volume,updated_at)
        VALUES (:asset_id,:provider,:provider_symbol,:timeframe,:timestamp,:open,:high,:low,:close,:adjusted_close,:volume,CURRENT_TIMESTAMP)
        ON CONFLICT(asset_id, provider, timeframe, timestamp) DO UPDATE SET open=excluded.open,high=excluded.high,low=excluded.low,close=excluded.close,adjusted_close=excluded.adjusted_close,volume=excluded.volume,updated_at=CURRENT_TIMESTAMP
        """)
        count = 0
        for c in candles:
            self.db.execute(q, {"asset_id": asset_id, "provider": provider, "provider_symbol": provider_symbol, "timeframe": timeframe, "timestamp": c.timestamp, "open": c.open, "high": c.high, "low": c.low, "close": c.close, "adjusted_close": c.adjusted_close, "volume": c.volume})
            count += 1
        return count

    async def create_import_run(self, provider: str, run_type: str, status: str = "RUNNING", metadata: dict | None = None):
        return self.db.execute(text("INSERT INTO market_data_import_runs (provider,run_type,status,metadata) VALUES (:provider,:run_type,:status,:metadata) RETURNING id"), {"provider":provider,"run_type":run_type,"status":status,"metadata":json.dumps(metadata or {})}).scalar_one()

    async def finish_import_run(self, run_id, status: str, total_assets=0, success_count=0, failed_count=0, error_message=None):
        self.db.execute(text("UPDATE market_data_import_runs SET status=:status,finished_at=CURRENT_TIMESTAMP,total_assets=:total_assets,success_count=:success_count,failed_count=:failed_count,error_message=:error_message WHERE id=:run_id"), locals())

    async def create_analysis_run(self, engine_name: str, universe_id=None, timeframe="1d", status="RUNNING", metadata: dict | None = None):
        return self.db.execute(text("INSERT INTO market_analysis_runs (engine_name,universe_id,timeframe,status,metadata) VALUES (:engine_name,:universe_id,:timeframe,:status,:metadata) RETURNING id"), {"engine_name":engine_name,"universe_id":universe_id,"timeframe":timeframe,"status":status,"metadata":json.dumps(metadata or {})}).scalar_one()

    async def finish_analysis_run(self, run_id, status: str, total_assets=0, success_count=0, failed_count=0, error_message=None):
        self.db.execute(text("UPDATE market_analysis_runs SET status=:status,finished_at=CURRENT_TIMESTAMP,total_assets=:total_assets,success_count=:success_count,failed_count=:failed_count,error_message=:error_message WHERE id=:run_id"), locals())

    async def insert_analysis_result(self, analysis_run_id, asset_id, engine_name: str, timeframe: str, result: dict):
        self.db.execute(text("INSERT INTO market_analysis_results (analysis_run_id,asset_id,engine_name,timeframe,signal,score,trend,confidence,payload) VALUES (:analysis_run_id,:asset_id,:engine_name,:timeframe,:signal,:score,:trend,:confidence,:payload)"), {"analysis_run_id":analysis_run_id,"asset_id":asset_id,"engine_name":engine_name,"timeframe":timeframe,"signal":result.get("signal"),"score":result.get("score"),"trend":result.get("trend"),"confidence":result.get("confidence"),"payload":json.dumps(result.get("payload", {}))})

    async def load_candles_for_asset(self, asset_id, timeframe="1d"):
        return [_row(r) for r in self.db.execute(text("SELECT * FROM market_candles WHERE asset_id=:asset_id AND timeframe=:timeframe ORDER BY timestamp ASC"), {"asset_id": asset_id, "timeframe": timeframe}).all()]


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

    async def latest_analysis_results(self, engine_name: str | None = None, universe_name: str | None = None, asset_type: str | None = None, limit: int = 200):
        query = """
        SELECT r.*, a.symbol, a.provider_symbol, a.name, a.asset_type, a.currency, a.enabled AS asset_enabled, u.name AS universe_name
        FROM market_analysis_results r
        JOIN market_assets a ON a.id = r.asset_id
        LEFT JOIN market_universes u ON u.id = a.universe_id
        JOIN (SELECT asset_id, engine_name, timeframe, MAX(created_at) AS max_created_at FROM market_analysis_results GROUP BY asset_id, engine_name, timeframe) latest
          ON latest.asset_id = r.asset_id AND latest.engine_name = r.engine_name AND latest.timeframe = r.timeframe AND latest.max_created_at = r.created_at
        WHERE a.enabled = true
        """
        params: dict[str, Any] = {"limit": limit}
        if engine_name:
            query += " AND r.engine_name = :engine_name"; params["engine_name"] = engine_name
        if universe_name:
            query += " AND u.name = :universe_name"; params["universe_name"] = universe_name
        if asset_type:
            query += " AND a.asset_type = :asset_type"; params["asset_type"] = asset_type
        query += " ORDER BY r.created_at DESC, a.priority ASC, a.symbol ASC LIMIT :limit"
        return [_row(r) for r in self.db.execute(text(query), params).all()]

    async def last_import_run(self):
        row = self.db.execute(text("SELECT * FROM market_data_import_runs ORDER BY started_at DESC LIMIT 1")).first()
        return _row(row) if row else None

    async def last_analysis_run(self):
        row = self.db.execute(text("SELECT * FROM market_analysis_runs ORDER BY started_at DESC LIMIT 1")).first()
        return _row(row) if row else None

    async def candle_quality(self, universe_name: str | None = None, asset_type: str | None = None, limit: int = 500):
        query = """
        SELECT a.id AS asset_id, a.symbol, a.provider_symbol, a.name, a.asset_type, a.currency,
               a.priority, u.name AS universe_name,
               COUNT(c.id) AS candles_count, MIN(c.timestamp) AS first_candle_at, MAX(c.timestamp) AS last_candle_at,
               MAX(r.created_at) AS last_analysis_at,
               CASE
                 WHEN COUNT(c.id) = 0 THEN 'MISSING'
                 WHEN MAX(c.timestamp) < CURRENT_TIMESTAMP - INTERVAL '7 days' THEN 'STALE'
                 ELSE 'OK'
               END AS data_status
        FROM market_assets a
        LEFT JOIN market_universes u ON u.id = a.universe_id
        LEFT JOIN market_candles c ON c.asset_id = a.id AND c.timeframe = '1d'
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
        query += " GROUP BY a.id, a.symbol, a.provider_symbol, a.name, a.asset_type, a.currency, a.priority, u.name ORDER BY a.priority ASC, a.symbol ASC LIMIT :limit"
        return [_row(r) for r in self.db.execute(text(query), params).all()]

    async def analysis_freshness(self, universe_name: str | None = None, asset_type: str | None = None, limit: int = 500):
        rows = await self.candle_quality(universe_name=universe_name, asset_type=asset_type, limit=limit)
        for row in rows:
            row["analysis_status"] = "MISSING_ANALYSIS" if row.get("last_analysis_at") is None else "OK"
            if row.get("last_candle_at") and row.get("last_analysis_at") and str(row["last_candle_at"]) > str(row["last_analysis_at"]):
                row["analysis_status"] = "STALE_ANALYSIS"
        return rows

    async def import_runs(self, limit: int = 50):
        return [_row(r) for r in self.db.execute(text("SELECT * FROM market_data_import_runs ORDER BY started_at DESC LIMIT :limit"), {"limit": limit}).all()]

    async def analysis_runs(self, limit: int = 50):
        return [_row(r) for r in self.db.execute(text("SELECT * FROM market_analysis_runs ORDER BY started_at DESC LIMIT :limit"), {"limit": limit}).all()]

    async def confluence_results(self, universe_name: str | None = None, asset_type: str | None = None, limit: int = 300):
        query = """
        WITH latest AS (
          SELECT r.*, ROW_NUMBER() OVER (PARTITION BY r.asset_id, r.engine_name, r.timeframe ORDER BY r.created_at DESC, r.id DESC) AS rn
          FROM market_analysis_results r
        )
        SELECT a.id AS asset_id, a.symbol, a.provider_symbol, a.name, a.asset_type, u.name AS universe_name,
               m.signal AS momentum_signal, m.score AS momentum_score, m.trend AS momentum_trend, m.created_at AS momentum_at,
               w.signal AS wyckoff_signal, w.score AS wyckoff_score, w.trend AS wyckoff_trend, w.created_at AS wyckoff_at
        FROM market_assets a
        LEFT JOIN market_universes u ON u.id = a.universe_id
        LEFT JOIN latest m ON m.asset_id = a.id AND m.engine_name = 'momentum' AND m.rn = 1
        LEFT JOIN latest w ON w.asset_id = a.id AND w.engine_name = 'wyckoff_smc' AND w.rn = 1
        WHERE a.enabled = true
        """
        params: dict[str, Any] = {"limit": limit}
        if universe_name:
            query += " AND u.name = :universe_name"; params["universe_name"] = universe_name
        if asset_type:
            query += " AND a.asset_type = :asset_type"; params["asset_type"] = asset_type
        query += " ORDER BY a.priority ASC, a.symbol ASC LIMIT :limit"
        rows = [_row(r) for r in self.db.execute(text(query), params).all()]
        for row in rows:
            ms = str(row.get("momentum_signal") or "").upper(); ws = str(row.get("wyckoff_signal") or "").upper()
            if ms == "BUY" and ws == "BUY": label, rank = "STRONG_BUY", 1
            elif "BUY" in {ms, ws}: label, rank = "WATCH", 2
            elif ms == "SELL" and ws == "SELL": label, rank = "AVOID", 5
            elif "SELL" in {ms, ws}: label, rank = "CONFLICT", 4
            else: label, rank = "NEUTRAL", 3
            row["confluence"] = label; row["confluence_rank"] = rank
        return sorted(rows, key=lambda r: (r["confluence_rank"], r.get("provider_symbol") or ""))

    async def create_job_request(self, job_type: str, status: str = "QUEUED", payload: dict | None = None):
        self._ensure_job_requests_table()
        return self.db.execute(text("INSERT INTO market_data_job_requests (job_type,status,payload) VALUES (:job_type,:status,:payload) RETURNING id"), {"job_type": job_type, "status": status, "payload": json.dumps(payload or {})}).scalar_one()

    async def job_requests(self, limit: int = 50):
        self._ensure_job_requests_table()
        return [_row(r) for r in self.db.execute(text("SELECT * FROM market_data_job_requests ORDER BY created_at DESC LIMIT :limit"), {"limit": limit}).all()]

    def _ensure_job_requests_table(self):
        dialect = self.db.get_bind().dialect.name
        if dialect == "sqlite":
            stmt = "CREATE TABLE IF NOT EXISTS market_data_job_requests (id INTEGER PRIMARY KEY AUTOINCREMENT, job_type TEXT NOT NULL, status TEXT NOT NULL, payload TEXT NULL, created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP)"
        else:
            stmt = "CREATE TABLE IF NOT EXISTS market_data_job_requests (id BIGSERIAL PRIMARY KEY, job_type TEXT NOT NULL, status TEXT NOT NULL, payload JSONB NULL, created_at TIMESTAMP NOT NULL DEFAULT now(), updated_at TIMESTAMP NOT NULL DEFAULT now())"
        self.db.execute(text(stmt))

    def stats(self):
        def scalar(sql): return self.db.execute(text(sql)).scalar() or 0
        return {"total_universes": scalar("SELECT COUNT(*) FROM market_universes"), "total_assets": scalar("SELECT COUNT(*) FROM market_assets"), "total_candles": scalar("SELECT COUNT(*) FROM market_candles WHERE asset_id IS NOT NULL")}

_POSTGRES_SCHEMA = [
"CREATE EXTENSION IF NOT EXISTS pgcrypto",
"CREATE TABLE IF NOT EXISTS market_universes (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), name TEXT NOT NULL UNIQUE, description TEXT NULL, region TEXT NULL, asset_type TEXT NULL, currency TEXT NULL, provider TEXT NOT NULL DEFAULT 'EODHD', enabled BOOLEAN NOT NULL DEFAULT TRUE, created_at TIMESTAMP NOT NULL DEFAULT now(), updated_at TIMESTAMP NOT NULL DEFAULT now())",
"CREATE TABLE IF NOT EXISTS market_assets (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), universe_id UUID NULL REFERENCES market_universes(id), symbol TEXT NOT NULL, provider_symbol TEXT NOT NULL, exchange_code TEXT NULL, name TEXT NULL, asset_type TEXT NOT NULL, region TEXT NULL, country TEXT NULL, currency TEXT NULL, isin TEXT NULL, mic TEXT NULL, pea_eligible BOOLEAN NULL, ucits BOOLEAN NULL, enabled BOOLEAN NOT NULL DEFAULT TRUE, priority INTEGER NOT NULL DEFAULT 100, last_synced_at TIMESTAMP NULL, last_error TEXT NULL, created_at TIMESTAMP NOT NULL DEFAULT now(), updated_at TIMESTAMP NOT NULL DEFAULT now(), UNIQUE(provider_symbol, asset_type))",
"CREATE TABLE IF NOT EXISTS market_candles (id BIGSERIAL, asset_id UUID NULL REFERENCES market_assets(id), provider TEXT DEFAULT 'EODHD', provider_symbol TEXT, timeframe TEXT, timestamp TIMESTAMP, open NUMERIC NOT NULL, high NUMERIC NOT NULL, low NUMERIC NOT NULL, close NUMERIC NOT NULL, adjusted_close NUMERIC NULL, volume NUMERIC NULL, created_at TIMESTAMP NOT NULL DEFAULT now(), updated_at TIMESTAMP NOT NULL DEFAULT now())",
"CREATE UNIQUE INDEX IF NOT EXISTS uq_market_candles_asset_provider_time ON market_candles(asset_id, provider, timeframe, timestamp) WHERE asset_id IS NOT NULL",
"CREATE TABLE IF NOT EXISTS market_data_import_runs (id BIGSERIAL PRIMARY KEY, provider TEXT NOT NULL, run_type TEXT NOT NULL, status TEXT NOT NULL, started_at TIMESTAMP NOT NULL DEFAULT now(), finished_at TIMESTAMP NULL, total_assets INTEGER DEFAULT 0, success_count INTEGER DEFAULT 0, failed_count INTEGER DEFAULT 0, error_message TEXT NULL, metadata JSONB NULL)",
"CREATE TABLE IF NOT EXISTS market_analysis_runs (id BIGSERIAL PRIMARY KEY, engine_name TEXT NOT NULL, universe_id UUID NULL REFERENCES market_universes(id), timeframe TEXT NOT NULL DEFAULT '1d', status TEXT NOT NULL, started_at TIMESTAMP NOT NULL DEFAULT now(), finished_at TIMESTAMP NULL, total_assets INTEGER DEFAULT 0, success_count INTEGER DEFAULT 0, failed_count INTEGER DEFAULT 0, metadata JSONB NULL, error_message TEXT NULL)",
"CREATE TABLE IF NOT EXISTS market_analysis_results (id BIGSERIAL PRIMARY KEY, analysis_run_id BIGINT NULL REFERENCES market_analysis_runs(id), asset_id UUID NOT NULL REFERENCES market_assets(id), engine_name TEXT NOT NULL, timeframe TEXT NOT NULL, signal TEXT NULL, score NUMERIC NULL, trend TEXT NULL, confidence NUMERIC NULL, payload JSONB NOT NULL DEFAULT '{}'::jsonb, created_at TIMESTAMP NOT NULL DEFAULT now())",
]
_SQLITE_SCHEMA = [s.replace("UUID PRIMARY KEY DEFAULT gen_random_uuid()", "TEXT PRIMARY KEY DEFAULT (lower(hex(randomblob(16))))").replace("UUID NULL", "TEXT NULL").replace("UUID NOT NULL", "TEXT NOT NULL").replace("BIGSERIAL PRIMARY KEY", "INTEGER PRIMARY KEY AUTOINCREMENT").replace("BIGSERIAL", "INTEGER PRIMARY KEY AUTOINCREMENT").replace("TIMESTAMP NOT NULL DEFAULT now()", "TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP").replace("JSONB", "TEXT").replace("::jsonb", "").replace("BOOLEAN", "BOOLEAN") for s in _POSTGRES_SCHEMA if not s.startswith("CREATE EXTENSION") and "UNIQUE INDEX" not in s]
_SQLITE_SCHEMA.append("CREATE UNIQUE INDEX IF NOT EXISTS uq_market_candles_asset_provider_time ON market_candles(asset_id, provider, timeframe, timestamp)")
