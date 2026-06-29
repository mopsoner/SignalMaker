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
