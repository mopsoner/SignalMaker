from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from app.services.execution import kraken_execution_service as module
from app.services.execution.kraken_execution_service import KrakenExecutionService
from app.services.execution.kraken_symbol_rules import KrakenSymbolRules


def _service(monkeypatch, *, dry_run=True, free_balance=1_000):
    monkeypatch.setattr(module, "settings", SimpleNamespace(
        kraken_base_url="", kraken_api_key="", kraken_secret_key="", kraken_dry_run=dry_run,
        kraken_quote_assets="USD", kraken_execution_enabled=True,
        kraken_margin_execution_enabled=True, kraken_margin_max_leverage=10,
        kraken_order_quote_amount=150.0, live_min_total_notional_per_trade=150.0,
        kraken_quote_reserve=1.0, kraken_buy_balance_ratio=1.0,
        kraken_margin_shorts_enabled=False,
    ))
    client = Mock()
    client.current_price.return_value = 0.07
    client.free_balance.return_value = free_balance
    client.place_market_entry.return_value = {"order_id": "spot-1", "status": "pending"}
    client.pair_info.return_value = {
        "baseAsset": "LOW", "quoteAsset": "USD", "lot_decimals": 2,
        "ordermin": "0.01", "costmin": "5", "leverage_buy": [2, 3, 5],
    }
    db = Mock()
    db.get.return_value = SimpleNamespace()  # Skip persistence; it is unrelated to sizing.
    service = KrakenExecutionService(db, client=client, rules=KrakenSymbolRules(client, ["USD"]))
    service.margin.margin_order = Mock(return_value={"order_id": "margin-1", "status": "pending"})
    return service


def test_low_price_spot_buy_has_at_least_150_total_notional(monkeypatch):
    result = _service(monkeypatch).buy_market("LOWUSD", total_notional=150, mode="spot")

    assert result["total_notional"] >= 150
    assert result["own_quote_amount"] == result["total_notional"]
    assert result["borrowed_notional"] == 0


@pytest.mark.parametrize("leverage", [2, 3, 5])
def test_low_price_margin_buy_separates_own_and_borrowed_notional(monkeypatch, leverage):
    result = _service(monkeypatch).buy_market(
        "LOWUSD", total_notional=150, mode="margin", leverage=leverage
    )

    assert result["total_notional"] >= 150
    assert result["own_quote_amount"] == pytest.approx(result["total_notional"] / leverage)
    assert result["borrowed_notional"] == pytest.approx(
        result["total_notional"] - result["own_quote_amount"]
    )
    assert result["effective_leverage"] == leverage


def test_insufficient_balance_does_not_silently_reduce_total(monkeypatch):
    service = _service(monkeypatch, dry_run=False, free_balance=20)

    with pytest.raises(ValueError, match=r"minimum total notional 150.00.*possible total notional 95.00.*effective leverage 5.*usable quote balance 19.00"):
        service.buy_market("LOWUSD", total_notional=150, mode="margin", leverage=5)
