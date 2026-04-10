import time

from app.db.session import SessionLocal
from app.services.executor_service import ExecutorService
from app.services.runtime_settings import load_runtime_settings


if __name__ == '__main__':
    while True:
        db = SessionLocal()
        try:
            runtime = load_runtime_settings(db)
            if runtime['bot'].get('bot_executor_enabled', True):
                result = ExecutorService(db).execute_open_candidates(
                    limit=runtime['bot'].get('bot_executor_limit', 10),
                    quantity=runtime['bot'].get('bot_executor_quantity', 1.0),
                )
                print(result)
            interval = runtime['bot'].get('bot_executor_interval_sec', 30)
        finally:
            db.close()
        time.sleep(interval)
