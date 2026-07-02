import pytest

from raspberry_executor.margin_order_manager import MarginOrderManager
from raspberry_executor.spot_order_manager import SpotOrderManager


def test_margin_entry_confirm_timeout_defaults_to_30_when_env_absent(monkeypatch):
    monkeypatch.delenv("MARGIN_ENTRY_CONFIRM_TIMEOUT_SECONDS", raising=False)

    assert MarginOrderManager.__new__(MarginOrderManager)._entry_confirm_timeout_seconds() == pytest.approx(30.0)


def test_spot_entry_confirm_timeout_defaults_to_30_when_env_absent(monkeypatch):
    monkeypatch.delenv("SPOT_ENTRY_CONFIRM_TIMEOUT_SECONDS", raising=False)

    assert SpotOrderManager.__new__(SpotOrderManager)._entry_confirm_timeout_seconds() == pytest.approx(30.0)


def test_margin_entry_confirm_timeout_prefers_explicit_env(monkeypatch):
    monkeypatch.setenv("MARGIN_ENTRY_CONFIRM_TIMEOUT_SECONDS", "12.5")

    assert MarginOrderManager.__new__(MarginOrderManager)._entry_confirm_timeout_seconds() == pytest.approx(12.5)


def test_spot_entry_confirm_timeout_prefers_explicit_env(monkeypatch):
    monkeypatch.setenv("SPOT_ENTRY_CONFIRM_TIMEOUT_SECONDS", "14.5")

    assert SpotOrderManager.__new__(SpotOrderManager)._entry_confirm_timeout_seconds() == pytest.approx(14.5)
