from unittest.mock import Mock

import pytest

from scripts.ibkr_discover_assets import (
    INTERNAL_EXCHANGE_TO_IBKR,
    UNIVERSE_DEFAULTS,
    Resolver,
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
    assert INTERNAL_EXCHANGE_TO_IBKR["SWX"] == ["SWX", "EBS", "XSWX"]
    options = with_universe_defaults({"universe": "ETF PEA", "asset_type": None})
    assert options["asset_type"] == "ETF"
    assert options["exchange_code"] == "PA"
    assert options["ibkr_sec_types"] == ["ETF", "STK"]
    assert options["ibkr_exchanges"] == ["PA", "SBF", "XPAR", "ENEXT", "SMART"]
    assert UNIVERSE_DEFAULTS["Stocks Europe"]["ibkr_exchanges"][-1] == "SMART"


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
        "AIR.PA", {"universe": "Stocks Euronext Paris"}
    )
    assert asset["enabled"] is True
    assert asset["exchange_code"] == "PA"
    assert asset["universe"] == "Stocks Euronext Paris"
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
    with pytest.raises(RuntimeError, match="HTTP 500"):
        Resolver(base_url="http://ibkr", verify=False, session=session).resolve(
            "AIR.PA", {"universe": "Stocks Euronext Paris"}
        )
