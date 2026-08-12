from app.core.config import Settings
from signalmaker.admin.env_settings import ENV_VARS, env_status


def _variables_by_name() -> dict[str, dict[str, object]]:
    return {item["name"]: item for item in env_status()["variables"]}


def test_diagnostic_signal_variables_match_configuration_aliases() -> None:
    configured_signal_aliases = {
        field.alias
        for field in Settings.model_fields.values()
        if isinstance(field.alias, str) and field.alias.startswith("SIGNAL_")
    }
    diagnostic_signal_aliases = {name for name in ENV_VARS if name.startswith("SIGNAL_")}

    assert diagnostic_signal_aliases == configured_signal_aliases
    assert {"PLANNER_MIN_SCORE", "PLANNER_MIN_RR"} <= set(ENV_VARS)


def test_env_status_returns_normalized_effective_values_and_sources(monkeypatch) -> None:
    monkeypatch.setenv("SIGNAL_SESSION_CONFIRM_FILTER_ENABLED", "yes")
    monkeypatch.setenv("SIGNAL_RSI_PERIOD", "21")
    monkeypatch.setenv("SIGNAL_EQUAL_LEVEL_TOLERANCE_PCT", "0.0035")
    monkeypatch.setenv("PLANNER_MIN_RR", "2.25")
    monkeypatch.delenv("SIGNAL_ENTRY_RSI_MIN", raising=False)

    variables = _variables_by_name()

    assert variables["SIGNAL_SESSION_CONFIRM_FILTER_ENABLED"] == {
        "name": "SIGNAL_SESSION_CONFIRM_FILTER_ENABLED",
        "configured": True,
        "source": "environment",
        "value": True,
        "secret": False,
    }
    assert variables["SIGNAL_RSI_PERIOD"]["value"] == 21
    assert variables["SIGNAL_EQUAL_LEVEL_TOLERANCE_PCT"]["value"] == 0.0035
    assert variables["PLANNER_MIN_RR"]["value"] == 2.25
    assert variables["SIGNAL_ENTRY_RSI_MIN"]["value"] == 50.0
    assert variables["SIGNAL_ENTRY_RSI_MIN"]["configured"] is False
    assert variables["SIGNAL_ENTRY_RSI_MIN"]["source"] == "application_default"


def test_env_status_never_exposes_secret_values(monkeypatch) -> None:
    monkeypatch.setenv("IBKR_BEARER_TOKEN", "super-secret-token")

    bearer_token = _variables_by_name()["IBKR_BEARER_TOKEN"]

    assert bearer_token["configured"] is True
    assert bearer_token["value"] == "***"
    assert bearer_token["secret"] is True
