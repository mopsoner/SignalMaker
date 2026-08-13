from unittest.mock import Mock

from scripts import check_kraken_workflow as preflight


PAIR_INFO = {
    "pair_key": "XXBTZUSD",
    "altname": "XBTUSD",
    "wsname": "XBT/USD",
    "baseAsset": "BTC",
    "quoteAsset": "USD",
    "ordermin": "0.0001",
    "costmin": "0.5",
    "lot_decimals": 8,
    "leverage_buy": [2, 3],
    "leverage_sell": [2, 3],
}


def test_preflight_checks_public_private_and_non_submitting_orders(monkeypatch):
    client = Mock()
    client.is_configured.return_value = True
    client.pair_info.return_value = PAIR_INFO
    client.current_price.return_value = 50_000.0
    client.balance.return_value = {"ZUSD": "100"}
    client._signed.side_effect = lambda _method, path, _params: {"open": {}} if path.endswith("OpenOrders") else {}
    client.validate_market_entry.return_value = {"status": "validated", "submitted": False}
    monkeypatch.setattr(preflight, "KrakenClient", Mock(return_value=client))
    monkeypatch.setattr(preflight, "fetch_kraken_ohlc", Mock(return_value=[{"open_time": 1}]))

    checks = preflight.check_workflow("BTCUSD", 50, ["spot", "margin"])

    assert all(check.ok for check in checks)
    assert {check.name for check in checks} >= {
        "asset_pairs",
        "ticker",
        "ohlc",
        "balance",
        "open_orders",
        "open_positions",
        "add_order_validate_spot_buy",
        "add_order_validate_spot_sell",
        "add_order_validate_margin_buy",
        "add_order_validate_margin_sell",
    }
    assert client.validate_market_entry.call_count == 4


def test_preflight_reports_every_private_check_when_credentials_are_missing(monkeypatch):
    client = Mock()
    client.is_configured.return_value = False
    client.pair_info.return_value = PAIR_INFO
    client.current_price.return_value = 50_000.0
    monkeypatch.setattr(preflight, "KrakenClient", Mock(return_value=client))
    monkeypatch.setattr(preflight, "fetch_kraken_ohlc", Mock(return_value=[{"open_time": 1}]))

    checks = preflight.check_workflow("BTCUSD", 50, ["spot"])

    failures = [check for check in checks if not check.ok]
    assert [check.name for check in failures] == [
        "balance",
        "open_orders",
        "open_positions",
        "add_order_validate",
    ]
    client.validate_market_entry.assert_not_called()
