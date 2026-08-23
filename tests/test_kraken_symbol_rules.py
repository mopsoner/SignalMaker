from unittest.mock import Mock

import pytest

from app.services.execution.kraken_symbol_rules import KrakenSymbolRules


def rules_with(**metadata):
    client = Mock()
    client.pair_info.return_value = {
        "baseAsset": "BTC",
        "quoteAsset": "USD",
        **metadata,
    }
    return KrakenSymbolRules(client, ["USD"])


def test_supported_leverages_are_cleaned_deduplicated_and_sorted():
    rules = rules_with(leverage_buy=[5, "2", "invalid", 5, None, 1, 3])

    assert rules.supported_leverages("BTCUSD", "buy") == (2, 3, 5)


@pytest.mark.parametrize(
    ("configured_max", "expected"),
    [(10, 5), (3, 3), (4, 3)],
)
def test_max_supported_leverage_selects_highest_at_or_below_cap(configured_max, expected):
    rules = rules_with(leverage_buy=[2, 3, 5])

    assert rules.max_supported_leverage("BTCUSD", "buy", configured_max) == expected


def test_buy_and_sell_leverages_are_independent():
    rules = rules_with(leverage_buy=[2, 3, 5], leverage_sell=[2, 4])

    assert rules.max_supported_leverage("BTCUSD", "buy", 10) == 5
    assert rules.max_supported_leverage("BTCUSD", "sell", 10) == 4


def test_pair_without_eligible_margin_leverage_is_rejected():
    rules = rules_with(leverage_buy=[])

    with pytest.raises(ValueError, match="no Kraken margin leverage"):
        rules.max_supported_leverage("BTCUSD", "buy", 10)


def test_explicit_unsupported_leverage_is_rejected():
    rules = rules_with(leverage_buy=[2, 3, 5])

    with pytest.raises(ValueError, match="leverage 4 is not supported"):
        rules.validate_leverage("BTCUSD", "buy", 4)


def test_quantity_rounding_up_never_drops_below_total_notional_minimum():
    rules = rules_with(lot_decimals=2, ordermin="0.01", costmin="5")

    quantity = rules.quantity_for_total_notional("BTCUSD", 150, 7, 150)

    assert quantity == "21.43"
    assert float(quantity) * 7 >= 150
