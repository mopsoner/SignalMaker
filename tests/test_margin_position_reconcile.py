from raspberry_executor import margin_position_reconcile as r


class FakeMargin:
    def __init__(self, positions=None, orders=None):
        self._positions = positions or {}
        self._orders = orders or []

    def open_positions(self):
        return self._positions

    def open_margin_orders(self, symbol):
        return self._orders


class FakeState:
    def __init__(self, local=None):
        self._local = local or {}
        self.added = []
        self.updated = []
        self.events = []

    def open_positions(self):
        return dict(self._local)

    def add_open_position(self, candidate_id, payload):
        self.added.append((candidate_id, payload))
        self._local[candidate_id] = payload

    def update_open_position(self, candidate_id, updates, event_type=None):
        self.updated.append((candidate_id, updates, event_type))

    def add_event(self, candidate_id, event_type, payload=None, save=True):
        self.events.append((candidate_id, event_type, payload or {}))


class FakeRules:
    def symbol_info(self, symbol):
        return {"symbol": symbol.replace("XBT", "BTC"), "quoteAsset": "USD"}


def patch_common(monkeypatch, state, margin):
    monkeypatch.setattr(r, "load_settings", lambda: object())
    monkeypatch.setattr(r, "margin_dry_run", lambda: False)
    monkeypatch.setattr(r, "create_margin_exchange", lambda settings, dry_run: (object(), margin, FakeRules()))
    monkeypatch.setattr(r, "StateStore", lambda: state)


def remote_pos(**overrides):
    data = {"ordertxid": "ENTRY1", "pair": "XBTUSD", "type": "buy", "vol": "1.0", "vol_closed": "0", "cost": "50000", "margin": "10000"}
    data.update(overrides)
    return data


def test_open_positions_empty_imports_nothing(monkeypatch):
    state = FakeState()
    patch_common(monkeypatch, state, FakeMargin({}))
    assert r.reconcile_kraken_margin_positions()["imported"] == 0
    assert state.added == []


def test_long_remote_missing_imported_without_tp(monkeypatch):
    state = FakeState()
    patch_common(monkeypatch, state, FakeMargin({"K": remote_pos()}))
    summary = r.reconcile_kraken_margin_positions()
    assert summary["imported"] == 1
    _, payload = state.added[0]
    assert payload["entry_order_id"] == "ENTRY1"
    assert payload["needs_tp_replay"] is True
    assert payload["imported_from_kraken_open_positions"] is True


def test_already_local_by_entry_order_id_no_duplicate(monkeypatch):
    state = FakeState({"local": {"entry_order_id": "ENTRY1", "execution_symbol": "BTCUSD", "mode": "margin", "side": "long", "quantity": "1"}})
    patch_common(monkeypatch, state, FakeMargin({"K": remote_pos()}))
    summary = r.reconcile_kraken_margin_positions()
    assert summary["already_local"] == 1
    assert state.added == []
    assert state.updated


def test_existing_sell_limit_tp_attached(monkeypatch):
    order = {"orderId": "TP1", "side": "SELL", "type": "LIMIT", "quantity": "1", "price": "60000"}
    state = FakeState()
    patch_common(monkeypatch, state, FakeMargin({"K": remote_pos()}, [order]))
    r.reconcile_kraken_margin_positions()
    _, payload = state.added[0]
    assert payload["tp_order_id"] == "TP1"
    assert payload["needs_tp_replay"] is False
    assert payload["target_price"] == 60000.0


def test_sell_position_skipped(monkeypatch):
    state = FakeState()
    patch_common(monkeypatch, state, FakeMargin({"K": remote_pos(type="sell")}))
    summary = r.reconcile_kraken_margin_positions()
    assert summary["skipped"] == 1
    assert state.added == []
