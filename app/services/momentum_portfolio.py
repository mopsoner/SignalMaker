"""Multi-position orchestration using the existing momentum filters and ledger."""
from datetime import datetime, timedelta, timezone
from sqlalchemy import select
from app.models.momentum_engine import MomentumEnginePosition


class MomentumPortfolioMixin:
    def _portfolio_enabled(self):
        return self._portfolio_mode

    def _open_positions(self):
        return list(self.db.scalars(select(MomentumEnginePosition).where(
            MomentumEnginePosition.market_scope == self.market_scope,
            MomentumEnginePosition.strategy == self.STRATEGY,
            MomentumEnginePosition.status == 'open',
        ).order_by(MomentumEnginePosition.opened_at, MomentumEnginePosition.position_id)))

    def _portfolio_plan(self, rankings, starting_capital, minimum):
        positions = self._open_positions()
        held = {p.symbol for p in positions}
        exits, entries = [], []
        for position in positions:
            asset = self._asset_for(position.symbol, rankings=rankings)
            if asset is not None and self._exit_required(asset):
                exits.append({'action': 'sell', 'symbol': position.symbol,
                              'position_id': position.position_id, 'reason': self._exit_description(asset)})
        # Do not immediately buy back an asset sold in this evaluation.
        slots = max(0, self.context.max_positions - len(positions) + len(exits))
        excluded = set(held)
        for _ in range(slots):
            asset = self._best_ranked_asset_with_valid_structure(
                rankings=rankings, min_momentum_score=minimum, exclude_symbols=excluded)
            if not asset:
                break
            entries.append(asset)
            excluded.add(asset['symbol'])
        cash = self._cash_balance(starting_capital=starting_capital)
        for event in exits:
            position = next(p for p in positions if p.position_id == event['position_id'])
            cash += self._open_position_value(position, rankings=rankings)
        allocation = cash / slots if slots else 0
        buys = [{'action': 'buy', 'symbol': asset['symbol'], 'buy_symbol': asset['symbol'],
                 'allocation_divisor': slots - index, 'total_notional': allocation,
                 'reason': 'Eligible momentum entry with valid support; equal allocation to free slots'}
                for index, asset in enumerate(entries) if allocation > 0]
        return positions, exits, entries, buys

    def _portfolio_status(self, *, rankings, cadence_hours, starting_capital, min_momentum_score):
        positions, exits, entries, buys = self._portfolio_plan(rankings, starting_capital, min_momentum_score)
        payloads = [self._position_payload(p, rankings=rankings,
                    current_asset=self._asset_for(p.symbol, rankings=rankings)) for p in positions]
        last = self._last_check_trade()
        anchor = self._as_utc(last.created_at) if last else None
        if positions:
            latest_open = max(self._as_utc(p.opened_at) for p in positions)
            anchor = max(anchor, latest_open) if anchor else latest_open
        next_check = anchor + timedelta(hours=cadence_hours) if anchor else None
        cadence_due = next_check is None or datetime.now(timezone.utc) >= next_check
        event_due = bool(exits or buys)
        cash = self._cash_balance(starting_capital=starting_capital)
        equity = cash + sum(self._open_position_value(p, rankings=rankings) for p in positions)
        from app.core.config import settings
        return {
            'strategy': self.STRATEGY, 'market_scope': self.market_scope, 'mode': self.MODE,
            'reference_currency': self.context.reference_currency, 'max_positions': self.context.max_positions,
            'cadence_hours': cadence_hours, 'starting_capital': starting_capital,
            'cash': round(cash, 8), 'equity': round(equity, 8), 'total_pnl': round(equity-starting_capital, 8),
            'total_pnl_pct': round((equity-starting_capital)/starting_capital*100, 4) if starting_capital else 0,
            'open_positions': payloads, 'open_position': payloads[0] if payloads else None,
            'best_asset': entries[0] if entries else None, 'top_watch_asset': entries[0] if entries else None,
            'last_check_at': anchor, 'next_check_at': next_check,
            'due_now': cadence_due or event_due, 'cadence_due': cadence_due, 'event_due': event_due,
            'due_reason': 'portfolio_risk_exit' if exits else ('eligible_free_slot' if buys else ('cadence_elapsed' if cadence_due else None)),
            'recommendation': f'{len(positions)}/{self.context.max_positions} positions; {len(exits)} exits and {len(buys)} entries available.',
            'operations': exits + buys,
            'selection_min_momentum_score': None, 'selection_classes': list(self.CRYPTO_ENTRY_CLASSES),
            'selection_min_momentum_15m': settings.momentum_entry_min_15m,
            'selection_min_acceleration': settings.momentum_entry_min_acceleration,
            'exit_max_momentum_1h': settings.momentum_exit_max_1h,
            'trades': self._recent_trades(),
        }

    def _portfolio_decision(self, status):
        operations = status.get('operations', []) if status.get('due_now') else []
        return {**status, 'action': 'batch' if operations else ('hold' if status['open_positions'] else 'wait'),
                'decision_action': 'batch' if operations else ('hold' if status['open_positions'] else 'wait'),
                'should_trade': bool(operations), 'status': 'trade_ready' if operations else 'no_trade',
                'symbol': None, 'target_symbol': None, 'buy_symbol': None, 'sell_symbol': None,
                'reason': status['recommendation'], 'operations': operations}

    def _run_portfolio(self, *, force, cadence_hours, starting_capital, min_momentum_score):
        # Serialize portfolio allocation across PostgreSQL paper workers.
        if self.db.get_bind().dialect.name == 'postgresql':
            from sqlalchemy import text
            self.db.execute(text("SELECT pg_advisory_xact_lock(784103221)"))
        rankings = self._rankings()
        kwargs = dict(rankings=rankings, cadence_hours=cadence_hours,
                      starting_capital=starting_capital, min_momentum_score=min_momentum_score)
        before = self._portfolio_status(**kwargs)
        if not force and not before['due_now']:
            self._save_current_decision(self._portfolio_decision(before))
            self.db.commit()
            return before
        positions, exits, entries, buys = self._portfolio_plan(rankings, starting_capital, min_momentum_score)
        # Serialize actual operations, preserving the intent for the live executor.
        executed = []
        for position in positions:
            asset = self._asset_for(position.symbol, rankings=rankings)
            event = next((e for e in exits if e['position_id'] == position.position_id), None)
            if event:
                action = ('SELL_NEGATIVE_15M' if self._negative_15m_exit(asset) else
                          ('SELL_BELOW_EMA20_15M' if self._ema_exit(asset) and not self._structure_broken(asset)
                           else 'SELL_ROTATE_OR_STRUCTURE_BREAK'))
                self._close_position(position, rankings=rankings, reason=event['reason'], action=action)
                executed.append(event)
            else:
                price, source = self._price_with_source(position.symbol, rankings=rankings, fallback=position.entry_price)
                value, pnl = self._position_mark_to_market(position=position, mark_price=price)
                position.mark_price, position.unrealized_pnl = price, pnl
                self._record_trade(action='HOLD_NO_BETTER_VALID_MOMENTUM', symbol=position.symbol,
                    price=price, quantity=position.quantity, value=value, pnl=pnl,
                    reason=self._hold_reason(asset), meta={'price_source': source, 'pnl_basis': 'mark_to_market'})
        self.db.flush()
        remaining_slots = max(0, self.context.max_positions - len(self._open_positions()))
        cash = self._cash_balance(starting_capital=starting_capital)
        allocation = cash / remaining_slots if remaining_slots else 0
        for asset, event in zip(entries, buys):
            if remaining_slots <= 0 or allocation <= 0:
                break
            self._open_new_position(asset, allocation, action='BUY_TOP_VALID_MOMENTUM', reason=event['reason'])
            executed.append({**event, 'total_notional': allocation, 'allocation_divisor': remaining_slots})
            remaining_slots -= 1
            self.db.flush()
        if not positions and not executed:
            self._record_trade(action='CHECK_NO_RANKED_ASSET', symbol='NONE', price=0, quantity=0,
                               value=0, pnl=0, reason='No eligible asset or no cash for portfolio entry')
            self.db.flush()
        after = self._portfolio_status(**kwargs)
        after = self._status_for_execution(after, before=before, force=force)
        # Never substitute the post-run plan for the operations actually executed.
        intent = {**before, 'operations': executed, 'due_now': bool(executed) or before['due_now'] or force}
        self._save_current_decision(self._portfolio_decision(intent))
        self.db.commit()
        return after
