from fastapi import APIRouter, Depends, Response
from sqlalchemy.orm import Session
from app.api.deps import get_db
from signalmaker.admin.env_settings import env_status
from signalmaker.admin.market_data_settings import market_data_settings
from signalmaker.data_providers.eodhd.config import get_eodhd_config
from signalmaker.data_providers.eodhd.repository import EODHDRepository
from signalmaker.market_data.analysis_adapter import MarketAnalysisAdapter
from signalmaker.market_data.universe_service import MarketUniverseService

router = APIRouter()


def _repo(db: Session) -> EODHDRepository:
    repo = EODHDRepository(db)
    repo.ensure_schema()
    return repo


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


@router.get('/api/v1/stocks-etfs/dashboard')
async def stocks_etfs_dashboard(universe: str | None = None, asset_type: str | None = None, db: Session = Depends(get_db)):
    repo = _repo(db)
    assets = await repo.list_enabled_market_assets(universe_name=universe, asset_type=asset_type, limit=1000)
    momentum = await repo.latest_analysis_results(engine_name='momentum', universe_name=universe, asset_type=asset_type, limit=500)
    wyckoff = await repo.latest_analysis_results(engine_name='wyckoff_smc', universe_name=universe, asset_type=asset_type, limit=500)
    stats = repo.stats()
    return {'stats': stats, 'assets': assets, 'momentum': momentum, 'wyckoff_smc': wyckoff}


@router.get('/api/v1/stocks-etfs/assets')
async def stocks_etfs_assets(universe: str | None = None, asset_type: str | None = None, limit: int = 300, db: Session = Depends(get_db)):
    return await _repo(db).list_enabled_market_assets(universe_name=universe, asset_type=asset_type, limit=limit)


@router.get('/api/v1/stocks-etfs/results')
async def stocks_etfs_results(engine: str | None = None, universe: str | None = None, asset_type: str | None = None, limit: int = 200, db: Session = Depends(get_db)):
    return await _repo(db).latest_analysis_results(engine_name=engine, universe_name=universe, asset_type=asset_type, limit=limit)


@router.get('/api/v1/stocks-etfs/candidates')
async def stocks_etfs_candidates(engine: str = 'wyckoff_smc', universe: str | None = None, asset_type: str | None = None, limit: int = 200, db: Session = Depends(get_db)):
    rows = await _repo(db).latest_analysis_results(engine_name=engine, universe_name=universe, asset_type=asset_type, limit=limit)
    return [r for r in rows if str(r.get('signal') or '').upper() in {'BUY', 'SELL'}]


@router.get('/api/v1/stocks-etfs/positions')
async def stocks_etfs_positions(universe: str | None = None, asset_type: str | None = None, limit: int = 200, db: Session = Depends(get_db)):
    rows = await _repo(db).latest_analysis_results(engine_name='wyckoff_smc', universe_name=universe, asset_type=asset_type, limit=limit)
    return [r for r in rows if str(r.get('signal') or '').upper() == 'BUY']


@router.get('/api/v1/stocks-etfs/data-quality')
async def stocks_etfs_data_quality(universe: str | None = None, asset_type: str | None = None, limit: int = 500, db: Session = Depends(get_db)):
    repo = _repo(db)
    return await repo.candle_quality(universe_name=universe, asset_type=asset_type, limit=limit)


@router.get('/api/v1/stocks-etfs/freshness')
async def stocks_etfs_freshness(universe: str | None = None, asset_type: str | None = None, limit: int = 500, db: Session = Depends(get_db)):
    repo = _repo(db)
    return await repo.analysis_freshness(universe_name=universe, asset_type=asset_type, limit=limit)


@router.get('/api/v1/stocks-etfs/confluence')
async def stocks_etfs_confluence(universe: str | None = None, asset_type: str | None = None, limit: int = 300, db: Session = Depends(get_db)):
    repo = _repo(db)
    return await repo.confluence_results(universe_name=universe, asset_type=asset_type, limit=limit)


def _csv(rows: list[dict]) -> Response:
    import csv
    import io
    output = io.StringIO()
    keys = sorted({k for row in rows for k in row.keys()})
    writer = csv.DictWriter(output, fieldnames=keys)
    writer.writeheader()
    writer.writerows(rows)
    return Response(content=output.getvalue(), media_type='text/csv', headers={'Content-Disposition': 'attachment; filename="stocks-etfs-export.csv"'})


@router.get('/api/v1/stocks-etfs/export.csv')
async def stocks_etfs_export_csv(kind: str = 'results', engine: str | None = None, universe: str | None = None, asset_type: str | None = None, limit: int = 500, db: Session = Depends(get_db)):
    repo = _repo(db)
    if kind == 'quality':
        rows = await repo.candle_quality(universe_name=universe, asset_type=asset_type, limit=limit)
    elif kind == 'confluence':
        rows = await repo.confluence_results(universe_name=universe, asset_type=asset_type, limit=limit)
    elif kind == 'assets':
        rows = await repo.list_enabled_market_assets(universe_name=universe, asset_type=asset_type, limit=limit)
    else:
        rows = await repo.latest_analysis_results(engine_name=engine, universe_name=universe, asset_type=asset_type, limit=limit)
    return _csv(rows)


@router.post('/admin/market-data/test-eodhd')
async def test_eodhd():
    from signalmaker.data_providers.eodhd.client import EODHDClient
    cfg = get_eodhd_config()
    client = EODHDClient(cfg)
    try:
        sample = await client.get_json('eod/AIR.PA', {'from': cfg.start_date, 'period': 'd'})
        return {'ok': True, 'rows': len(sample) if isinstance(sample, list) else None}
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
    return {'accepted': True, 'message': 'Run python -m signalmaker.jobs.eodhd_backfill_daily for controlled backfills.', 'payload': payload or {}}


@router.post('/admin/market-data/analyze')
async def analyze(payload: dict | None = None, db: Session = Depends(get_db)):
    payload = payload or {}
    repo = _repo(db)
    adapter = MarketAnalysisAdapter(repo)
    assets = await repo.list_enabled_market_assets(universe_name=payload.get('universe'), asset_type=payload.get('asset_type'), limit=int(payload.get('limit') or 10))
    engines = ['momentum', 'wyckoff_smc'] if payload.get('engine', 'both') == 'both' else [payload.get('engine', 'momentum')]
    run_id = await repo.create_analysis_run(payload.get('engine', 'both'), timeframe=payload.get('timeframe', '1d'), metadata=payload)
    results = []
    for asset in assets:
        for engine in engines:
            res = await (adapter.run_momentum_analysis(asset['id']) if engine == 'momentum' else adapter.run_wyckoff_smc_analysis(asset['id']))
            await repo.insert_analysis_result(run_id, asset['id'], res['engine_name'], payload.get('timeframe', '1d'), res)
            results.append({'symbol': asset['provider_symbol'], **res})
    await repo.finish_analysis_run(run_id, 'SUCCESS', len(results), len(results), 0)
    db.commit()
    return {'results': results}
