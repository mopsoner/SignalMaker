#!/usr/bin/env python3
from __future__ import annotations

import os
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from app.db.session import SessionLocal
from app.services.momentum_executor_service import MomentumExecutorService

REFRESH_SEC = int(os.getenv('MOMENTUM_TUI_REFRESH_SEC', '5'))


def clear() -> None:
    print('\033[2J\033[H', end='')


def line(label: str, value) -> None:
    print(f'{label:<22} {value if value is not None else "—"}')


def main() -> None:
    while True:
        db = SessionLocal()
        try:
            status = MomentumExecutorService(db).status()
            decision = status.get('decision') or {}
            local = status.get('local_position') or {}
            target = decision.get('target_asset') or {}
            watch = decision.get('top_watch_asset') or {}
            clear()
            print('SignalMaker · Momentum Executor TUI')
            print('=' * 48)
            line('Enabled', status.get('enabled'))
            line('Mode', status.get('mode'))
            line('Action', decision.get('action'))
            line('Symbol', decision.get('symbol'))
            line('Reason', decision.get('reason'))
            print('-' * 48)
            line('Local position', local.get('symbol') or 'Cash')
            line('Entry', local.get('entry_price'))
            line('Qty', local.get('quantity'))
            line('Local PnL', local.get('unrealized_pnl'))
            print('-' * 48)
            line('Target', target.get('symbol'))
            line('Target rank', target.get('rank'))
            line('Target score', target.get('momentum_score'))
            line('Target RSI 1h', target.get('rsi_1h'))
            line('Target entry', target.get('entry_status'))
            print('-' * 48)
            line('Watch', watch.get('symbol'))
            line('Remote equity', decision.get('equity'))
            line('Remote PnL %', decision.get('total_pnl_pct'))
            line('Next check', decision.get('next_check_at'))
            print('\nCtrl+C to exit. Refresh:', REFRESH_SEC, 'sec')
        except KeyboardInterrupt:
            print('\nBye')
            return
        except Exception as exc:
            clear()
            print('Momentum Executor TUI error')
            print(str(exc))
        finally:
            try:
                db.close()
            except Exception:
                pass
        time.sleep(max(2, REFRESH_SEC))


if __name__ == '__main__':
    main()
