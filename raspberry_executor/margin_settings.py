from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ENV_PATH = ROOT / ".env"

DEFAULT_MARGIN_SETTINGS = {
    "MARGIN_MODE_ENABLED": "false",
    "MARGIN_DRY_RUN": "true",
    "MARGIN_ACCOUNT_MODE": "isolated",
    "MARGIN_ISOLATED": "true",
    "MARGIN_MAX_MULTIPLIER": "5",
    "MARGIN_TRANSFER_SPOT_BALANCE": "true",
    "SHORTS_ENABLED": "false",
}


def _parse_env_lines() -> tuple[list[str], dict[str, str]]:
    if not ENV_PATH.exists():
        ENV_PATH.write_text("")
    lines = ENV_PATH.read_text().splitlines()
    values: dict[str, str] = {}
    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        values[key.strip()] = value.strip()
    return lines, values


def _normalized_account_mode(values: dict[str, str]) -> str:
    mode = str(values.get("MARGIN_ACCOUNT_MODE") or "").strip().lower()
    if mode in {"cross", "cross_margin", "croise", "croisée", "croisee"}:
        return "cross"
    if mode in {"isolated", "isolated_margin", "isole", "isolé", "isolee", "isolée"}:
        return "isolated"
    legacy_isolated = str(values.get("MARGIN_ISOLATED", "true")).strip().lower() in {"1", "true", "yes", "on"}
    return "isolated" if legacy_isolated else "cross"


def read_margin_settings() -> dict[str, str]:
    _, values = _parse_env_lines()
    out = DEFAULT_MARGIN_SETTINGS.copy()
    out.update({key: values[key] for key in DEFAULT_MARGIN_SETTINGS if key in values})
    out["MARGIN_ACCOUNT_MODE"] = _normalized_account_mode(out)
    out["MARGIN_ISOLATED"] = "true" if out["MARGIN_ACCOUNT_MODE"] == "isolated" else "false"
    return out


def write_margin_settings(values: dict[str, str]) -> None:
    lines, current = _parse_env_lines()
    merged = read_margin_settings()
    for key in DEFAULT_MARGIN_SETTINGS:
        if key in values:
            merged[key] = str(values[key]).strip()
    merged["MARGIN_ACCOUNT_MODE"] = _normalized_account_mode(merged)
    merged["MARGIN_ISOLATED"] = "true" if merged["MARGIN_ACCOUNT_MODE"] == "isolated" else "false"

    existing_keys = set()
    new_lines = []
    for line in lines:
        stripped = line.strip()
        if stripped and not stripped.startswith("#") and "=" in stripped:
            key = stripped.split("=", 1)[0].strip()
            if key in DEFAULT_MARGIN_SETTINGS:
                new_lines.append(f"{key}={merged[key]}")
                existing_keys.add(key)
                continue
        new_lines.append(line)

    missing = [key for key in DEFAULT_MARGIN_SETTINGS if key not in existing_keys]
    if missing:
        if new_lines and new_lines[-1].strip():
            new_lines.append("")
        new_lines.append("# Margin mode")
        for key in missing:
            new_lines.append(f"{key}={merged[key]}")
    ENV_PATH.write_text("\n".join(new_lines) + "\n")


def _enabled(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def margin_enabled() -> bool:
    return _enabled(read_margin_settings().get("MARGIN_MODE_ENABLED"), default=False)


def margin_dry_run() -> bool:
    return _enabled(read_margin_settings().get("MARGIN_DRY_RUN"), default=True)


def shorts_enabled() -> bool:
    return _enabled(read_margin_settings().get("SHORTS_ENABLED"), default=False)


def margin_account_mode() -> str:
    return read_margin_settings().get("MARGIN_ACCOUNT_MODE", "isolated")


def margin_isolated() -> bool:
    return margin_account_mode() == "isolated"


def margin_multiplier() -> float:
    try:
        return max(1.0, float(read_margin_settings().get("MARGIN_MAX_MULTIPLIER", "5") or "5"))
    except Exception:
        return 5.0


def margin_transfer_spot_balance() -> bool:
    return _enabled(read_margin_settings().get("MARGIN_TRANSFER_SPOT_BALANCE"), default=True)
