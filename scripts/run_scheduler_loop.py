import time

from app.db.session import SessionLocal
from app.services.executor_service import ExecutorService
from app.services.pipeline_service import PipelineService
from app.services.runtime_settings import load_runtime_settings


if __name__ == '__main__':
    tick = 0
    while True:
        db = SessionLocal()
        try:
            runtime = load_runtime_settings(db)
            if runtime['bot'].get('bot_scheduler_enabled', True):
                if tick % 2 == 0:
                    print(PipelineService(db).run_once(limit=runtime['binance']['binance_max_symbols']))
                print(ExecutorService(db).execute_open_candidates(limit=runtime['bot'].get('bot_executor_limit', 10), quantity=runtime['bot'].get('bot_executor_quantity', 1.0)))
            interval = runtime['bot'].get('bot_scheduler_interval_sec', 30)
        finally:
            db.close()
        tick += 1
        time.sleep(interval)
