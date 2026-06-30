from raspberry_executor import kraken_full_smoke_test


def test_find_symbol_for_quotes_prefers_supported_quote():
    assert kraken_full_smoke_test._find_symbol_for_quotes("https://kraken.test", ["USDC"]) == "BTCUSDC"
    assert kraken_full_smoke_test._find_symbol_for_quotes("https://kraken.test", ["FOO"]) == "BTCUSD"


def test_smoke_result_ignores_skipped_private_checks_for_overall_status():
    result = kraken_full_smoke_test.SmokeResult(
        base_url="https://kraken.test",
        symbol="BTCUSD",
        quote_assets=["USD"],
        credentials_loaded=False,
    )
    result.add("public_time", True)
    result.add("private_account", False, skipped=True, reason="missing_kraken_api_credentials")

    assert result.ok is True
    assert result.as_dict()["ok"] is True


def test_parser_defaults_keep_order_validation_opt_in():
    args = kraken_full_smoke_test.build_parser().parse_args([])

    assert args.validate_order is False
    assert args.skip_private is False
    assert args.order_quote == 20.0


def test_discover_default_symbol_uses_kraken_discovery_before_btc_fallback(monkeypatch):
    monkeypatch.setattr(kraken_full_smoke_test, "discover_kraken_margin_symbols", lambda *args, **kwargs: ["ETHUSDC"])
    monkeypatch.setattr(kraken_full_smoke_test, "discover_kraken_spot_symbols", lambda *args, **kwargs: ["BTCUSDC"])

    assert kraken_full_smoke_test._discover_default_symbol("https://kraken.test", ["USDC"]) == "ETHUSDC"


def test_parser_allows_symbol_flag_without_value_for_discovery_mode():
    args = kraken_full_smoke_test.build_parser().parse_args(["--symbol"])

    assert args.symbol == ""


def test_parser_exposes_signalmaker_feed_and_momentum_options():
    args = kraken_full_smoke_test.build_parser().parse_args([
        "--candle-intervals",
        "15m,1h",
        "--candle-limit",
        "42",
        "--momentum-limit",
        "7",
    ])

    assert args.skip_signalmaker is False
    assert args.candle_intervals == "15m,1h"
    assert args.candle_limit == 42
    assert args.momentum_limit == 7


def test_parser_defaults_to_momentum_get_limit_200():
    args = kraken_full_smoke_test.build_parser().parse_args([])

    assert args.momentum_limit == 200


def test_find_candle_summary_matches_symbol_case_and_interval():
    rows = [
        {"symbol": "ETHUSD", "interval": "15m", "candle_count": 1},
        {"symbol": "btcusd", "interval": "1h", "candle_count": 2},
    ]

    assert kraken_full_smoke_test._find_candle_summary(rows, "BTCUSD", "1h")["candle_count"] == 2
    assert kraken_full_smoke_test._find_candle_summary(rows, "BTCUSD", "4h") is None


def test_run_smoke_exercises_signalmaker_candles_candidates_and_momentum(monkeypatch):
    settings = kraken_full_smoke_test.Settings(
        signalmaker_base_url="https://signalmaker.test",
        gateway_id="gw-test",
        poll_seconds=15,
        dry_run=True,
        quote_assets=["USD"],
        allowed_symbols=["USD"],
        order_quote_amount=20.0,
        max_candidate_age_seconds=900,
        binance_base_url="https://binance.test",
        binance_api_key="",
        binance_secret_key="",
        exchange="kraken",
        kraken_base_url="https://kraken.test",
        kraken_api_key="",
        kraken_secret_key="",
        ibkr_market_feed_enabled=False,
        ibkr_cp_base_url="https://localhost:5000/v1/api",
        ibkr_cp_verify_ssl=False,
        ibkr_cp_timeout_seconds=30,
        ibkr_market_feed_poll_seconds=3600,
        ibkr_market_feed_intervals=["1d"],
        ibkr_market_feed_period="2y",
        ibkr_market_feed_bar="1d",
        ibkr_market_feed_source="Last",
        ibkr_market_feed_outside_rth=False,
        ibkr_market_feed_max_workers=1,
        ibkr_market_feed_requests_per_minute=20,
        ibkr_market_feed_limit=300,
        ibkr_market_feed_queue_analysis=False,
        ibkr_market_feed_universes=[],
        ibkr_market_feed_asset_types=[],
        ibkr_market_feed_symbols=[],
        ibkr_contract_cache_path="cache.json",
        ibkr_market_feed_retry_queue_path="retry.json",
        signalmaker_stock_etf_ibkr_ingest_path="/api/v1/stocks-etfs/ibkr/candles",
        signalmaker_stock_etf_assets_path="/api/v1/stocks-etfs/assets",
    )

    class FakeKrakenClient:
        def __init__(self, *args, **kwargs):
            self.dry_run = kwargs.get("dry_run", True)

        def is_configured(self):
            return False

        def _public(self, path):
            return {"unixtime": 1}

        def _pair_info(self, symbol):
            return {"altname": symbol}

        def current_price(self, symbol):
            return 100.0

        def place_market_entry(self, symbol, side, qty):
            return {"orderId": "entry", "executedQty": qty}

        def place_exit_limit(self, symbol, side, qty, price):
            return {"orderId": "tp", "price": price}

        def place_stop_loss(self, symbol, side, qty, price):
            return {"orderId": "sl", "price": price}

        def get_order(self, symbol, order_id):
            return {"orderId": order_id}

        def open_orders(self, symbol):
            return []

    class FakeRules:
        def __init__(self, *args, **kwargs):
            pass

        def quantity_from_quote(self, symbol, quote, price, market=True):
            return "0.2"

        def normalize_exit_price(self, symbol, price):
            return str(round(price, 2))

        def normalize_exit_quantity(self, symbol, qty):
            return qty

        def base_asset(self, symbol):
            return "BTC"

        def oco_allowed(self, symbol):
            return True

        def symbol_info(self, symbol):
            return {"quoteAsset": "USD"}

    class FakeMargin:
        def __init__(self, *args, **kwargs):
            pass

        def ensure_isolated_account(self, symbol):
            return {"status": "dry_run"}

        def isolated_account(self, symbol):
            return {"symbol": symbol}

        def borrow(self, *args):
            return {"status": "dry_run"}

        def repay(self, *args):
            return {"status": "dry_run"}

        def transfer_spot_to_margin(self, *args):
            return {"status": "dry_run"}

        def margin_order(self, *args, **kwargs):
            return {"orderId": "margin"}

        def open_margin_orders(self, symbol):
            return []

    fake_signalmaker_instances = []

    class FakeSignalMaker:
        def __init__(self, *args, **kwargs):
            self.counts = {}
            self.momentum_limits = []
            self.sync_called = False
            fake_signalmaker_instances.append(self)

        def check_candle_ingest_endpoint(self):
            return {"ok": True, "status_code": 200, "url": "https://signalmaker.test/api/v1/market-data/candles"}

        def candle_summary(self, symbol=None):
            return [
                {"symbol": "BTCUSD", "interval": interval, "candle_count": count, "last_close": 101.0}
                for interval, count in self.counts.items()
            ]

        def post_candles(self, symbol, interval, candles, source=None):
            self.counts[interval] = max(self.counts.get(interval, 0), len(candles))
            return {"status": "ok", "received": len(candles), "upserted": len(candles)}

        def latest_candle(self, symbol, interval):
            return {"symbol": symbol, "interval": interval, "open_time": 1, "close": 101.0}

        def get_recent_candidates(self, symbol=None, limit=100):
            return [{"candidate_id": "momentum-BTCUSD-open", "symbol": "BTCUSD"}]

        def get_open_candidates(self, limit=10):
            return [{"candidate_id": "momentum-BTCUSD-open", "symbol": "BTCUSD"}]

        def list_momentum(self, limit=50):
            self.momentum_limits.append(limit)
            return [{"rank": 1, "symbol": "BTCUSD", "momentum_score": 12.0, "rsi_1h": 50.0, "price": 100.0}]

        def sync_momentum_candidates(self, limit=25, min_momentum_score=None):
            self.sync_called = True
            raise AssertionError("signalmaker_momentum must use GET /api/v1/momentum, not POST sync")

    monkeypatch.setattr(kraken_full_smoke_test, "ensure_env", lambda: None)
    monkeypatch.setattr(kraken_full_smoke_test, "load_settings", lambda: settings)
    monkeypatch.setattr(kraken_full_smoke_test, "_runtime_settings_payload", lambda: {})
    monkeypatch.setattr(kraken_full_smoke_test, "apply_admin_settings_to_environ", lambda base_url: {})
    monkeypatch.setattr(kraken_full_smoke_test, "_fetch_admin_kraken_credential_status", lambda base_url: {})
    monkeypatch.setattr(kraken_full_smoke_test, "KrakenClient", FakeKrakenClient)
    monkeypatch.setattr(kraken_full_smoke_test, "KrakenSymbolRules", FakeRules)
    monkeypatch.setattr(kraken_full_smoke_test, "KrakenMarginClient", FakeMargin)
    monkeypatch.setattr(kraken_full_smoke_test, "SignalMakerClient", FakeSignalMaker)
    monkeypatch.setattr(kraken_full_smoke_test, "discover_kraken_spot_symbols", lambda *args, **kwargs: ["BTCUSD"])
    monkeypatch.setattr(kraken_full_smoke_test, "discover_kraken_margin_symbols", lambda *args, **kwargs: ["BTCUSD"])
    monkeypatch.setattr(kraken_full_smoke_test, "fetch_kraken_ohlc", lambda *args, **kwargs: [
        {"open_time": 1, "open": 100.0, "high": 102.0, "low": 99.0, "close": 101.0, "volume": 1.0, "close_time": 2, "quote_volume": 101.0, "number_of_trades": 10}
    ])
    monkeypatch.setattr(kraken_full_smoke_test, "build_decision_from_candidates", lambda rows, source: {"action": "BUY", "should_trade": True, "reason": "unit"})

    args = kraken_full_smoke_test.build_parser().parse_args([
        "--symbol",
        "BTCUSD",
        "--skip-private",
        "--candle-intervals",
        "15m,1h,4h",
        "--candle-limit",
        "1",
        "--momentum-limit",
        "1",
    ])

    result = kraken_full_smoke_test.run_smoke(args)
    checks = {check["name"]: check for check in result.checks}

    assert result.ok is True
    assert checks["signalmaker_candle_feed"]["ok"] is True
    assert [item["interval"] for item in checks["signalmaker_candle_feed"]["pushed"]] == ["15m", "1h", "4h"]
    assert checks["signalmaker_trade_candidates"]["ok"] is True
    assert checks["signalmaker_trade_candidates"]["replay_fetch_count"] == 1
    assert checks["signalmaker_momentum"]["ok"] is True
    assert checks["signalmaker_momentum"]["method"] == "GET"
    assert checks["signalmaker_momentum"]["path"] == "/api/v1/momentum"
    assert checks["signalmaker_momentum"]["limit"] == 1
    assert checks["signalmaker_momentum"]["decision_action"] == "BUY"
    assert fake_signalmaker_instances[0].momentum_limits == [1]
    assert fake_signalmaker_instances[0].sync_called is False
