#!/usr/bin/env python3
from __future__ import annotations

import os
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.core.config import settings
from app.db.session import SessionLocal
from app.services.momentum_executor_service import MomentumExecutorService


if __name__ == '__main__':
    print('Momentum executor loop started', flush=True)
    while True:
        db = SessionLocal()
        try:
            result = MomentumExecutorService(db).run_once(force=False)
            print(result, flush=True)
        except Exception as exc:
            print({'action': 'ERROR', 'error': str(exc)}, flush=True)
        finally:
            try:
                db.close()
            except Exception:
                pass
        time.sleep(max(5, int(settings.momentum_executor_interval_sec)))
