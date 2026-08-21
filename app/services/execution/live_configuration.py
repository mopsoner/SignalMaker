from typing import Any


def assert_wyckoff_live_configuration(configuration: Any) -> None:
    """Fail closed unless every setting required for real Wyckoff execution is safe."""
    problems: list[str] = []
    mode = str(configuration.wyckoff_live_mode).lower()
    if not configuration.wyckoff_live_enabled:
        problems.append("WYCKOFF_LIVE_ENABLED must be true")
    if not configuration.kraken_execution_enabled:
        problems.append("KRAKEN_EXECUTION_ENABLED must be true")
    if configuration.kraken_dry_run:
        problems.append("KRAKEN_DRY_RUN must be false")
    if not configuration.kraken_api_key or not configuration.kraken_secret_key:
        problems.append("KRAKEN_API_KEY and KRAKEN_SECRET_KEY are required")
    if mode not in {"spot", "margin"}:
        problems.append("WYCKOFF_LIVE_MODE must be spot or margin")
    if mode == "margin" and not configuration.kraken_margin_execution_enabled:
        problems.append("KRAKEN_MARGIN_EXECUTION_ENABLED must be true for margin mode")
    if problems:
        raise RuntimeError(
            "Unsafe/incomplete live Wyckoff/SMC configuration: " + "; ".join(problems)
        )
