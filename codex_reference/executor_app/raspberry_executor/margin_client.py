"""Kraken margin compatibility wrapper.

SignalMaker now executes exclusively through Kraken.  This module keeps the
historical ``MarginClient`` import path as a thin alias so older executor code
uses the Kraken margin adapter without retaining removed exchange logic.
"""

from raspberry_executor.kraken_margin_client import KrakenMarginClient as MarginClient

__all__ = ["MarginClient"]
