from unittest.mock import Mock

import pytest

from scripts.ibkr_discover_assets import (
    INTERNAL_EXCHANGE_TO_IBKR,
    UNIVERSE_DEFAULTS,
    Resolver,
    discover,
    read_seed_rows,
    score_contract,
    with_universe_defaults,
)


def response(payload=None, status_code=200):
    result = Mock(status_code=status_code)
    result.json.return_value = payload
    if status_code >= 400:
        result.raise_for_status.side_effect = RuntimeError(f"HTTP {status_code}")
    return result


def test_internal_exchange_and_universe_mappings_are_ibkr_candidates():
    assert INTERNAL_EXCHANGE_TO_IBKR["PA"] == ["PA", "SBF", "XPAR", "ENEXT", "SMART"]
    assert INTERNAL_EXCHANGE_TO_IBKR["SWX"] == ["SWX", "EBS", "XSWX", "SMART"]
    options = with_universe_defaults({"universe": "Europe ETF", "asset_type": None})
    assert options["asset_type"] == "ETF"
    assert options["universe"] == "Europe ETF"
    assert options["ibkr_sec_types"] == ["ETF", "STK"]
    assert options["ibkr_exchanges"] == UNIVERSE_DEFAULTS["Europe ETF"]["ibkr_exchanges"]
    assert UNIVERSE_DEFAULTS["Europe Stocks"]["asset_type"] == "STOCK"
    assert UNIVERSE_DEFAULTS["Europe ETF"]["ucits"] is True


@pytest.mark.parametrize("old", ["Stocks Europe", "Stocks Euronext Paris", "ETF PEA", "ETF Europe UCITS", "Stocks US", "IBKR Imported"])
def test_old_universe_names_are_rejected(old):
    with pytest.raises(ValueError, match="unsupported universe"):
        with_universe_defaults({"universe": old})


def test_csv_seed_rows_preserve_metadata(tmp_path):
    seed = tmp_path / "seed.csv"
    seed.write_text("symbol,country,currency,exchange_code,pea_eligible,ucits\nCW8.PA,FR,EUR,PA,true,true\n")
    assert read_seed_rows(seed) == [{"symbol": "CW8.PA", "country": "FR", "currency": "EUR", "exchange_code": "PA", "pea_eligible": True, "ucits": True}]


def test_txt_seed_files_are_rejected(tmp_path):
    seed = tmp_path / "seed.txt"; seed.write_text("AIR.PA\n")
    with pytest.raises(ValueError, match="must be CSV"):
        read_seed_rows(seed)


def test_scoring_accepts_mapped_exchange_but_rejects_other_markets():
    sbf = {"symbol": "AIR", "secType": "STK", "listingExchange": "SMART", "primaryExchange": "SBF", "currency": "EUR"}
    ams = {**sbf, "primaryExchange": "AEB"}
    assert score_contract(sbf, "AIR.PA", "STOCK", "PA", "EUR") > 0
    assert score_contract(ams, "AIR.PA", "STOCK", "PA", "EUR") < 0


def test_resolve_keeps_internal_metadata_and_validates_history():
    session = Mock()
    session.get.side_effect = [
        response([{"symbol": "AIR", "conid": "123@SBF", "secType": "STK", "listingExchange": "SMART", "primaryExchange": "SBF", "currency": "EUR"}]),
        response({"data": [{"t": 1}]}),
    ]
    asset = Resolver(base_url="http://ibkr", verify=False, session=session).resolve(
        "AIR.PA", {"universe": "Europe Stocks", "country": "FR", "currency": "EUR", "exchange_code": "PA", "pea_eligible": True}
    )
    assert asset["enabled"] is True
    assert asset["exchange_code"] == "PA"
    assert asset["universe"] == "Europe Stocks"
    assert asset["ibkr"]["selected_exchange"] == "SMART"
    assert asset["ibkr"]["primary_exchange"] == "SBF"
    assert asset["ibkr"]["sec_type"] == "STK"
    assert session.get.call_args_list[-1].args[0].endswith("/iserver/marketdata/history")
    assert session.get.call_args_list[-1].kwargs["params"]["conid"] == "123"


def test_resolve_does_not_enable_contract_when_history_fails():
    session = Mock()
    session.get.side_effect = [
        response([{"symbol": "AIR", "conid": "123", "secType": "STK", "listingExchange": "SBF", "currency": "EUR"}]),
        response(status_code=500),
    ]
    asset = Resolver(base_url="http://ibkr", verify=False, session=session).resolve("AIR.PA", {"universe": "Europe Stocks", "country": "FR", "currency": "EUR", "exchange_code": "PA", "pea_eligible": True})
    assert asset["enabled"] is False
    assert asset["resolution_status"] == "FAILED_HISTORY_VALIDATION"
    assert asset["history_validation_status"] == "FAILED"
    assert "HTTP 500" in asset["last_error"]


def test_failed_history_disables_only_that_asset_and_discovery_continues(tmp_path):
    seed = tmp_path / "stocks.csv"
    seed.write_text("symbol,country,currency,exchange_code,pea_eligible\nAIR.PA,FR,EUR,PA,true\nTTE.PA,FR,EUR,PA,true\n")
    output = tmp_path / "assets.json"
    output.write_text('{"assets":[{"provider_symbol":"OLD.US","universe":"Stocks US"}]}')
    resolver = Mock()
    resolver.resolve.side_effect = [
        {"provider_symbol": "AIR.PA", "conid": "1", "enabled": False, "resolution_status": "FAILED_HISTORY_VALIDATION"},
        {"provider_symbol": "TTE.PA", "conid": "2", "enabled": True, "resolution_status": "RESOLVED"},
    ]
    result = discover({"universe": "Europe Stocks", "seed_file": str(seed), "output": str(output)}, resolver, authenticate=False)
    assert [asset["enabled"] for asset in result["assets"]] == [False, True]
    assert "OLD.US" not in output.read_text()
