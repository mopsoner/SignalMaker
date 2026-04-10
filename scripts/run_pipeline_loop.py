import time

from app.db.session import SessionLocal
from app.services.pipeline_service import PipelineService
from app.services.runtime_settings import load_runtime_settings


if __name__ == '__main__':
    while True:
        db = SessionLocal()
        try:
            runtime = load_runtime_settings(db)
            if runtime['bot'].get('bot_pipeline_enabled', True):
                result = PipelineService(db).run_once(limit=runtime['binance']['binance_max_symbols'])
                print(result)
            interval = runtime['bot'].get('bot_pipeline_interval_sec', 60)
        finally:
            db.close()
        time.sleep(interval)
