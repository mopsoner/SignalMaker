from datetime import datetime, timezone
import logging
from time import perf_counter
from decimal import Decimal
from types import SimpleNamespace
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Query, Response
from pydantic import BaseModel, Field, model_validator
from sqlalchemy import text
from sqlalchemy.orm import Session
from app.api.deps import get_db
from app.db.session import SessionLocal, rollback_and_close
from signalmaker.admin.env_settings import env_status
from signalmaker.admin.market_data_settings import market_data_settings
from signalmaker.data_providers.ibkr.config import get_ibkr_config
from signalmaker.market_data.repository import MarketDataRepository
from signalmaker.market_data.analysis_adapter import MarketAnalysisAdapter
from signalmaker.market_data.universe_service import MarketUniverseService

router = APIRouter()
logger = logging.getLogger(__name__)


class ExternalMarketCandleIn(BaseModel):
    timestamp: datetime | None = None
    open_time: int | None = None
    close_time: int | None = None
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    adjusted_close: Decimal | None = None
    volume: Decimal | None = None

    @model_validator(mode="after")
    def require_timestamp(self):
        if self.timestamp is None and self.open_time is None:
            raise ValueError("timestamp or open_time is required")
        if self.timestamp is None and self.open_time is not None:
            seconds = float(self.open_time) / 1000 if self.open_time > 10_000_000_000 else float(self.open_time)
            self.timestamp = datetime.fromtimestamp(seconds, tz=timezone.utc)
        if self.timestamp is not None and self.timestamp.tzinfo is not None:
            self.timestamp = self.timestamp.astimezone(timezone.utc).replace(tzinfo=None)
        return self


class ExternalMarketCandleIngestRequest(BaseModel):
    symbol: str | None = None
    provider_symbol: str | None = None
    asset_id: str | None = None
    asset_type: str | None = None
    name: str | None = None
    exchange_code: str | None = None
    currency: str | None = None
    region: str | None = None
    country: str | None = None
    universe: str | None = None
    isin: str | None = None
    mic: str | None = None
    pea_eligible: bool | None = None
    ucits: bool | None = None
    priority: int | None = None
    provider: Literal["IBKR"] = "IBKR"
    timeframe: str = "1d"
    run_type: str = "external_ingest"
    queue_analysis: bool = False
    candles: list[ExternalMarketCandleIn] = Field(min_length=1)


def _symbol_suffix(*symbols: str | None) -> str | None:
    for symbol in symbols:
        if symbol and "." in symbol:
            suffix = symbol.rsplit(".", 1)[-1].upper()
            if suffix:
                return suffix
    return None


def _ibkr_universe_name(payload: ExternalMarketCandleIngestRequest) -> str:
    aliases = {
        "Stocks Euronext Paris": "Europe Stocks",
        "Stocks Europe": "Europe Stocks",
        "ETF PEA": "Europe ETF",
        "ETF Europe UCITS": "Europe ETF",
    }
    if payload.universe and payload.universe != "IBKR Imported":
        return aliases.get(payload.universe, payload.universe)

    asset_type = (payload.asset_type or "").upper()
    if payload.universe == "IBKR Imported" and not (asset_type and payload.region):
        return "IBKR Imported"
    if asset_type == "STOCK" and (payload.region or "").upper() == "EU":
        return "Europe Stocks"
    if asset_type == "ETF" and (payload.region or "").upper() == "EU":
        return "Europe ETF"

    asset_type = asset_type or "ETF"
    currency = (payload.currency or "").upper()
    suffix = _symbol_suffix(payload.symbol, payload.provider_symbol)
    if asset_type == "ETF" and currency == "EUR":
        return "Europe ETF"
    if asset_type == "ETF" and suffix == "PA":
        return "Europe ETF"
    if asset_type == "STOCK" and suffix == "PA":
        return "Europe Stocks"
    if asset_type == "STOCK" and (currency == "USD" or suffix == "US"):
        return "Stocks US"
    return "IBKR Imported"


def _delete_table_rows(db: Session, table_name: str) -> int:
    try:
        result = db.execute(text(f"DELETE FROM {table_name}"))
        return result.rowcount or 0
    except Exception as exc:
        # SQLite/PostgreSQL installations may not have every optional table yet.
        message = str(exc).lower()
        if "does not exist" in message or "no such table" in message:
            db.rollback()
            return 0
        raise


def _repo(db: Session) -> MarketDataRepository:
    # Schema DDL is initialized once in application lifespan.  Request paths are
    # deliberately query-only unless the endpoint itself performs a write.
    return MarketDataRepository(db)


def _asset_filter_params(
    region: str | None = None, country: str | None = None,
    exchange_code: str | None = None, provider: str | None = None,
    pea_eligible: bool | None = Query(default=None),
    ucits: bool | None = Query(default=None),
):
    return {"region": region, "country": country, "exchange_code": exchange_code,
            "provider": provider, "pea_eligible": pea_eligible, "ucits": ucits}


@router.get('/admin/env')
def get_env():
    return env_status()


@router.get('/admin/market-data')
async def get_market_data(db: Session = Depends(get_db)):
    repo = _repo(db)
    payload = market_data_settings(repo)
    payload['universes'] = await repo.list_market_universes()
    payload['last_import_run'] = await repo.last_import_run()
    payload['last_analysis_run'] = await repo.last_analysis_run()
    payload['import_runs'] = await repo.import_runs(limit=10)
    payload['analysis_runs'] = await repo.analysis_runs(limit=10)
    payload['job_requests'] = await repo.job_requests(limit=10)
    return payload


@router.get('/admin/market-data/schema-status')
def market_data_schema_status(db: Session = Depends(get_db)):
    """Expose read-only schema diagnostics for production troubleshooting."""
    dialect = db.get_bind().dialect.name
    tables = [
        "market_universes", "market_assets", "stock_etf_candles",
        "market_data_import_runs", "market_analysis_runs",
        "market_analysis_results", "market_data_job_requests",
    ]
    inspected_tables = [
        "market_data_import_runs", "market_assets", "stock_etf_candles",
    ]
    if dialect == "sqlite":
        exists = {
            name: bool(db.execute(text(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name=:name"
            ), {"name": name}).first())
            for name in tables
        }
        columns = {
            name: [
                {
                    "column_name": row["name"], "data_type": row["type"],
                    "udt_name": row["type"], "column_default": row["dflt_value"],
                    "is_nullable": "NO" if row["notnull"] else "YES",
                }
                for row in db.execute(text(f"PRAGMA table_info({name})")).mappings()
            ] if exists.get(name) else []
            for name in inspected_tables
        }
        indexes = [dict(row) for row in db.execute(
            text("PRAGMA index_list(stock_etf_candles)")
        ).mappings()] if exists["stock_etf_candles"] else []
        identity = {"current_database": "sqlite", "current_schema": "main", "current_user": None}
    else:
        exists = {
            name: bool(db.execute(text("""
                SELECT 1 FROM information_schema.tables
                WHERE table_schema=current_schema() AND table_name=:name
            """), {"name": name}).first())
            for name in tables
        }
        columns = {
            name: [dict(row) for row in db.execute(text("""
                SELECT column_name, data_type, udt_name, column_default, is_nullable
                FROM information_schema.columns
                WHERE table_schema=current_schema() AND table_name=:name
                ORDER BY ordinal_position
            """), {"name": name}).mappings()]
            for name in inspected_tables
        }
        indexes = [dict(row) for row in db.execute(text("""
            SELECT indexname, indexdef FROM pg_indexes
            WHERE schemaname=current_schema() AND tablename='stock_etf_candles'
            ORDER BY indexname
        """)).mappings()]
        row = db.execute(text(
            "SELECT current_database(), current_schema(), current_user"
        )).one()
        identity = dict(zip(("current_database", "current_schema", "current_user"), row))
    return {"ok": all(exists.values()), **identity, "table_exists": exists,
            "columns": columns, "indexes": {"stock_etf_candles": indexes}}


@router.get('/api/v1/stocks-etfs/dashboard')
async def stocks_etfs_dashboard(universe: str | None = None, asset_type: str | None = None, filters: dict = Depends(_asset_filter_params), db: Session = Depends(get_db)):
    repo = _repo(db)
    assets = await repo.list_enabled_market_assets(universe_name=universe, asset_type=asset_type, limit=1000, **filters)
    momentum = await repo.latest_analysis_results(engine_name='momentum', universe_name=universe, asset_type=asset_type, limit=500, **filters)
    wyckoff = await repo.latest_analysis_results(engine_name='wyckoff_smc', universe_name=universe, asset_type=asset_type, limit=500, **filters)
    stats = repo.stats()
    return {'stats': stats, 'assets': assets, 'momentum': momentum, 'wyckoff_smc': wyckoff}


@router.get('/api/v1/stocks-etfs/assets')
async def stocks_etfs_assets(universe: str | None = None, asset_type: str | None = None, limit: int = 300, filters: dict = Depends(_asset_filter_params), db: Session = Depends(get_db)):
    return await _repo(db).list_enabled_market_assets(universe_name=universe, asset_type=asset_type, limit=limit, **filters)


@router.get('/api/v1/stocks-etfs/results')
async def stocks_etfs_results(engine: str | None = None, universe: str | None = None, asset_type: str | None = None, limit: int = 200, filters: dict = Depends(_asset_filter_params), db: Session = Depends(get_db)):
    return await _repo(db).latest_analysis_results(engine_name=engine, universe_name=universe, asset_type=asset_type, limit=limit, **filters)


@router.get('/api/v1/stocks-etfs/candidates')
async def stocks_etfs_candidates(engine: str = 'wyckoff_smc', universe: str | None = None, asset_type: str | None = None, limit: int = 200, filters: dict = Depends(_asset_filter_params), db: Session = Depends(get_db)):
    rows = await _repo(db).latest_analysis_results(engine_name=engine, universe_name=universe, asset_type=asset_type, limit=limit, **filters)
    return [r for r in rows if str(r.get('signal') or '').upper() in {'BUY', 'SELL'}]


@router.get('/api/v1/stocks-etfs/positions')
async def stocks_etfs_positions(universe: str | None = None, asset_type: str | None = None, limit: int = 200, db: Session = Depends(get_db)):
    rows = await _repo(db).latest_analysis_results(engine_name='wyckoff_smc', universe_name=universe, asset_type=asset_type, limit=limit)
    return [r for r in rows if str(r.get('signal') or '').upper() == 'BUY']


@router.get('/api/v1/stocks-etfs/data-quality')
async def stocks_etfs_data_quality(universe: str | None = None, asset_type: str | None = None, limit: int = 500, filters: dict = Depends(_asset_filter_params), db: Session = Depends(get_db)):
    repo = _repo(db)
    return await repo.stock_etf_candle_quality(universe_name=universe, asset_type=asset_type, limit=limit, **filters)


@router.get('/api/v1/stocks-etfs/freshness')
async def stocks_etfs_freshness(universe: str | None = None, asset_type: str | None = None, limit: int = 500, filters: dict = Depends(_asset_filter_params), db: Session = Depends(get_db)):
    repo = _repo(db)
    return await repo.analysis_freshness(universe_name=universe, asset_type=asset_type, limit=limit, **filters)


@router.get('/api/v1/stocks-etfs/confluence')
async def stocks_etfs_confluence(universe: str | None = None, asset_type: str | None = None, limit: int = 300, filters: dict = Depends(_asset_filter_params), db: Session = Depends(get_db)):
    repo = _repo(db)
    return await repo.confluence_results(universe_name=universe, asset_type=asset_type, limit=limit, **filters)


def _csv(rows: list[dict]) -> Response:
    import csv
    import io
    output = io.StringIO()
    keys = sorted({k for row in rows for k in row.keys()})
    writer = csv.DictWriter(output, fieldnames=keys)
    writer.writeheader()
    writer.writerows(rows)
    return Response(content=output.getvalue(), media_type='text/csv', headers={'Content-Disposition': 'attachment; filename="stocks-etfs-export.csv"'})


@router.delete('/api/v1/stocks-etfs/cleanup')
async def clear_stocks_etfs_generated_data(db: Session = Depends(get_db)):
    """Clear generated ETF/stock analysis, candidate/position views and job logs.

    Market assets, universes and imported OHLC candles are preserved so the
    operator can rerun analysis without another full backfill.
    """
    tables = [
        'market_analysis_results',
        'market_analysis_runs',
        'market_data_job_requests',
    ]
    details = {table: _delete_table_rows(db, table) for table in tables}
    db.commit()
    return {'deleted': sum(details.values()), 'details': details}


@router.get('/api/v1/stocks-etfs/export.csv')
async def stocks_etfs_export_csv(kind: str = 'results', engine: str | None = None, universe: str | None = None, asset_type: str | None = None, limit: int = 500, db: Session = Depends(get_db)):
    repo = _repo(db)
    if kind == 'quality':
        rows = await repo.stock_etf_candle_quality(universe_name=universe, asset_type=asset_type, limit=limit)
    elif kind == 'confluence':
        rows = await repo.confluence_results(universe_name=universe, asset_type=asset_type, limit=limit)
    elif kind == 'assets':
        rows = await repo.list_enabled_market_assets(universe_name=universe, asset_type=asset_type, limit=limit)
    else:
        rows = await repo.latest_analysis_results(engine_name=engine, universe_name=universe, asset_type=asset_type, limit=limit)
    return _csv(rows)


@router.post('/api/v1/stocks-etfs/ibkr/candles')
async def ingest_ibkr_candles(payload: ExternalMarketCandleIngestRequest, db: Session = Depends(get_db)):
    """Ingest externally collected IBKR candles into the stocks/ETFs market-data tables."""
    repo = MarketDataRepository(db)
    try:
        repo.ensure_schema()
    except Exception as exc:
        db.rollback()
        raise HTTPException(
            status_code=500,
            detail={"error": "IBKR_INGEST_FAILED", "step": "ensure_schema",
                    "message": str(exc),
                    "provider_symbol": payload.provider_symbol or payload.symbol,
                    "symbol": payload.symbol},
        ) from exc
    provider_symbol = payload.provider_symbol or payload.symbol

    async def step(name, operation):
        try:
            return await operation
        except HTTPException:
            raise
        except Exception as exc:
            db.rollback()
            raise HTTPException(status_code=500, detail={
                "error": "IBKR_INGEST_FAILED", "step": name, "message": str(exc),
                "provider_symbol": provider_symbol, "symbol": payload.symbol,
            }) from exc

    asset = await step("find_market_asset_for_ingest", repo.find_market_asset_for_ingest(
        asset_id=payload.asset_id, provider_symbol=provider_symbol,
        symbol=payload.symbol, asset_type=payload.asset_type,
    ))
    asset_created = False
    if asset is None:
        symbol = payload.symbol or payload.provider_symbol
        created_provider_symbol = payload.provider_symbol or payload.symbol
        if not symbol or not created_provider_symbol:
            raise HTTPException(status_code=422, detail="symbol or provider_symbol is required to create a market asset")

        asset_type = (payload.asset_type or "ETF").upper()
        universe_name = _ibkr_universe_name(payload)
        legacy_pea = payload.universe in {"ETF PEA", "Stocks Euronext Paris"}
        legacy_ucits = payload.universe in {"ETF PEA", "ETF Europe UCITS"}
        region = payload.region or ("EU" if universe_name in {"Europe Stocks", "Europe ETF"} else None)
        suffix = _symbol_suffix(payload.symbol, payload.provider_symbol)
        currency = payload.currency or ("EUR" if suffix == "PA" else "USD" if suffix == "US" else None)
        universe_id = await step("create_or_update_universe", repo.create_or_update_universe(
            universe_name,
            description="Automatically created for IBKR candle ingestion",
            region=region,
            asset_type=asset_type,
            currency=currency,
            provider="IBKR",
            enabled=True,
        ))
        asset_id = await step("upsert_market_asset", repo.upsert_market_asset(
            universe_id,
            symbol=symbol,
            provider_symbol=created_provider_symbol,
            exchange_code=payload.exchange_code or suffix,
            name=payload.name or symbol,
            asset_type=asset_type,
            region=region,
            country=payload.country or ("FR" if payload.universe == "Stocks Euronext Paris" else None),
            currency=currency,
            isin=payload.isin,
            mic=payload.mic,
            pea_eligible=True if legacy_pea else bool(payload.pea_eligible),
            ucits=True if legacy_ucits else bool(payload.ucits),
            enabled=True,
            priority=payload.priority or 100,
            provider=payload.provider,
        ))
        asset = await step("find_market_asset_for_ingest", repo.find_market_asset_for_ingest(asset_id=asset_id))
        asset_created = True
    else:
        # Newer executor versions are authoritative for classification attributes;
        # refresh a previously discovered asset without replacing candle history.
        universe_name = _ibkr_universe_name(payload)
        asset_type = (payload.asset_type or asset.get("asset_type") or "ETF").upper()
        region = payload.region or asset.get("region")
        if payload.universe or payload.asset_type or payload.region:
            universe_id = await step("create_or_update_universe", repo.create_or_update_universe(
                universe_name, description="IBKR Europe asset universe", region=region,
                asset_type=asset_type, currency=payload.currency or asset.get("currency"),
                provider="IBKR", enabled=True,
            ))
            legacy_pea = payload.universe in {"ETF PEA", "Stocks Euronext Paris"}
            legacy_ucits = payload.universe in {"ETF PEA", "ETF Europe UCITS"}
            asset_id = await step("upsert_market_asset", repo.upsert_market_asset(
                universe_id, symbol=payload.symbol or asset["symbol"],
                provider_symbol=provider_symbol or asset["provider_symbol"],
                exchange_code=payload.exchange_code or asset.get("exchange_code"),
                name=payload.name or asset.get("name"), asset_type=asset_type,
                region=region, country=payload.country or asset.get("country"),
                currency=payload.currency or asset.get("currency"),
                isin=payload.isin or asset.get("isin"), mic=payload.mic or asset.get("mic"),
                pea_eligible=True if legacy_pea else (payload.pea_eligible if payload.pea_eligible is not None else asset.get("pea_eligible")),
                ucits=True if legacy_ucits else (payload.ucits if payload.ucits is not None else asset.get("ucits")),
                enabled=bool(asset.get("enabled", True)), priority=payload.priority or asset.get("priority") or 100,
                provider="IBKR",
            ))
            asset = await step("find_market_asset_for_ingest", repo.find_market_asset_for_ingest(asset_id=asset_id))

    run_id = await step("create_import_run", repo.create_import_run(
        payload.provider.upper(),
        payload.run_type,
        metadata={
            "source": "external",
            "endpoint": "/api/v1/stocks-etfs/ibkr/candles",
            "asset_id": str(asset["id"]),
            "symbol": payload.symbol,
            "provider_symbol": provider_symbol,
            "timeframe": payload.timeframe,
            "received": len(payload.candles),
        },
    ))
    candles = [SimpleNamespace(**candle.model_dump()) for candle in payload.candles]
    upserted = await step("upsert_stock_etf_candles", repo.upsert_stock_etf_candles(
            asset["id"],
            "IBKR",
            asset.get("provider_symbol") or provider_symbol or payload.symbol or "",
            payload.timeframe,
            candles,
    ))
    queued_job_id = None
    if payload.queue_analysis:
        queued_job_id = await step("create_job_request", repo.create_job_request(
            "analyze",
            payload={
                "provider": payload.provider.upper(),
                "asset_id": str(asset["id"]),
                "symbol": asset.get("symbol"),
                "provider_symbol": asset.get("provider_symbol"),
                "timeframe": payload.timeframe,
            },
        ))
    await step("finish_import_run", repo.finish_import_run(
        run_id, "SUCCESS", total_assets=1, success_count=1, failed_count=0
    ))
    try:
        db.commit()
    except Exception as exc:
        db.rollback()
        raise HTTPException(
            status_code=500,
            detail={"error": "IBKR_INGEST_FAILED", "step": "commit", "message": str(exc),
                    "provider_symbol": provider_symbol, "symbol": payload.symbol},
        ) from exc
    return {
        "ok": True,
        "asset_created": asset_created,
        "provider": payload.provider.upper(),
        "asset_id": str(asset["id"]),
        "symbol": asset.get("symbol"),
        "provider_symbol": asset.get("provider_symbol"),
        "timeframe": payload.timeframe,
        "received": len(payload.candles),
        "upserted": upserted,
        "import_run_id": run_id,
        "queued_analysis_job_id": queued_job_id,
        "diagnostics": {
            "schema": "ok",
            "asset_resolution": "created" if asset_created else "matched",
            "import_run": "created",
            "candle_upsert": {"status": "ok", "count": upserted},
            "analysis_queue": "queued" if queued_job_id is not None else "skipped",
            "transaction": "committed",
        },
    }


@router.post('/admin/market-data/test-ibkr')
async def test_ibkr():
    from signalmaker.data_providers.ibkr.client import IBKRClient
    cfg = get_ibkr_config()
    client = IBKRClient(cfg)
    try:
        sample = await client.get_json('trsrv/stocks', {'symbols': 'AAPL'})
        return {'ok': True, 'symbols': list(sample.keys()) if isinstance(sample, dict) else None}
    finally:
        await client.close()


@router.post('/admin/market-data/sync-assets')
async def sync_assets(db: Session = Depends(get_db)):
    repo = _repo(db)
    result = await MarketUniverseService(repo).seed_initial_universes_and_assets()
    db.commit()
    return result


@router.patch('/admin/market-data/universes/{universe_id}')
async def update_universe(universe_id: str, payload: dict, db: Session = Depends(get_db)):
    repo = _repo(db)
    await repo.update_market_universe(universe_id, enabled=bool(payload.get('enabled')))
    db.commit()
    return {'ok': True}


@router.patch('/admin/market-data/assets/{asset_id}')
async def update_asset(asset_id: str, payload: dict, db: Session = Depends(get_db)):
    repo = _repo(db)
    await repo.update_market_asset(asset_id, enabled=payload.get('enabled'), priority=payload.get('priority'), universe_id=payload.get('universe_id'))
    db.commit()
    return {'ok': True}


@router.post('/admin/market-data/preview')
async def preview_market_action(payload: dict | None = None, db: Session = Depends(get_db)):
    payload = payload or {}
    repo = _repo(db)
    assets = await repo.list_enabled_market_assets(universe_name=payload.get('universe'), asset_type=payload.get('asset_type'), limit=int(payload.get('limit') or 500), symbols=(payload.get('symbols') or None))
    return {'ok': True, 'action': payload.get('action', 'backfill'), 'asset_count': len(assets), 'estimated_api_calls': len(assets) if payload.get('action', 'backfill') == 'backfill' else 0, 'symbols': [a.get('provider_symbol') for a in assets]}


@router.post('/admin/market-data/queue-job')
async def queue_market_job(payload: dict | None = None, db: Session = Depends(get_db)):
    payload = payload or {}
    repo = _repo(db)
    job_id = await repo.create_job_request(payload.get('job_type', 'backfill'), payload=payload)
    db.commit()
    return {'queued': True, 'job_id': job_id, 'message': 'Job request saved. Run the market-data worker/CLI to process queued requests.'}


@router.post('/admin/market-data/backfill')
async def backfill(payload: dict | None = None):
    return {'accepted': True, 'message': 'Run python -m signalmaker.jobs.ibkr_backfill_daily for controlled backfills.', 'payload': payload or {}}


@router.post('/admin/market-data/analyze')
async def analyze(payload: dict | None = None):
    """Analyze snapshots in memory, never while a DB transaction is open."""
    payload = payload or {}
    started = perf_counter()
    timeframe = (payload.get('timeframes') or [payload.get('timeframe', '1d')])[0]
    symbols = payload.get('symbols') or None
    db = SessionLocal()
    try:
        repo = _repo(db)
        assets = await repo.list_enabled_market_assets(
            universe_name=payload.get('universe'), asset_type=payload.get('asset_type'),
            limit=int(payload.get('limit') or 10), symbols=symbols,
            **{key: payload.get(key) for key in ('region', 'country', 'exchange_code', 'provider', 'pea_eligible', 'ucits')},
        )
        snapshots = {
            asset['id']: await repo.load_stock_etf_candles_for_asset(asset['id'], timeframe)
            for asset in assets
        }
        # End the read transaction before CPU work begins.
        db.rollback()
    except Exception:
        rollback_and_close(db)
        raise
    else:
        db.close()
    loaded_at = perf_counter()
    logger.info("market analyze loaded assets=%d seconds=%.3f", len(assets), loaded_at - started)

    # Adapter conversion/engines are pure once their candle snapshot is loaded.
    class SnapshotAdapter(MarketAnalysisAdapter):
        async def load_stock_etf_candles_for_asset(self, asset_id, timeframe="1d"):
            return snapshots.get(asset_id, [])

    adapter = SnapshotAdapter(None)
    engines = ['momentum', 'wyckoff_smc'] if payload.get('engine', 'both') == 'both' else [payload.get('engine', 'momentum')]
    results = []
    for asset in assets:
        for engine in engines:
            res = await (adapter.run_momentum_analysis(asset['id'], timeframe) if engine == 'momentum' else adapter.run_wyckoff_smc_analysis(asset['id'], timeframe))
            results.append({'symbol': asset['provider_symbol'], **res})
    analyzed_at = perf_counter()
    logger.info("market analyze computed results=%d seconds=%.3f", len(results), analyzed_at - loaded_at)

    db = SessionLocal()
    try:
        repo = _repo(db)
        run_id = await repo.create_analysis_run(payload.get('engine', 'both'), timeframe=timeframe, metadata=payload)
        for asset, result in ((a, r) for a in assets for r in results if r['symbol'] == a['provider_symbol']):
            await repo.insert_analysis_result(run_id, asset['id'], result['engine_name'], timeframe, result)
        await repo.finish_analysis_run(run_id, 'SUCCESS', len(results), len(results), 0)
        db.commit()
    except Exception:
        rollback_and_close(db)
        raise
    else:
        db.close()
    logger.info("market analyze persisted run_id=%s total_seconds=%.3f", run_id, perf_counter() - started)
    return {'results': results}
