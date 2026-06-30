from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ENV_PATH = ROOT / ".env"

DEFAULT_MARGIN_SETTINGS = {
    "EXECUTION_MODE": "spot",
    "MARGIN_MODE_ENABLED": "false",
    "MARGIN_ACCOUNT_MODE": "cross",
    "MARGIN_ISOLATED": "false",
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


def _bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _execution_mode(values: dict[str, str]) -> str:
    mode = str(values.get("EXECUTION_MODE") or "").strip().lower()
    if mode in {"spot", "cash"}:
        return "spot"
    if mode in {"isolated", "isolated_margin", "isole", "isolé", "isolee", "isolée"}:
        return "isolated"
    if mode in {"cross", "cross_margin", "croise", "croisée", "croisee"}:
        return "cross"
    if not _bool(values.get("MARGIN_MODE_ENABLED"), default=False):
        return "spot"
    account_mode = str(values.get("MARGIN_ACCOUNT_MODE") or "").strip().lower()
    if account_mode in {"isolated", "isolated_margin", "isole", "isolé", "isolee", "isolée"}:
        return "isolated"
    return "cross"


def read_margin_settings() -> dict[str, str]:
    _, values = _parse_env_lines()
    out = DEFAULT_MARGIN_SETTINGS.copy()
    out.update({key: values[key] for key in DEFAULT_MARGIN_SETTINGS if key in values})
    mode = _execution_mode(out)
    out["EXECUTION_MODE"] = mode
    out["MARGIN_MODE_ENABLED"] = "false" if mode == "spot" else "true"
    out["MARGIN_ACCOUNT_MODE"] = "isolated" if mode == "isolated" else "cross"
    out["MARGIN_ISOLATED"] = "true" if mode == "isolated" else "false"
    # DRY_RUN is the only source of truth. Keep a display-only compatibility field.
    out["MARGIN_DRY_RUN"] = values.get("DRY_RUN", "true")
    return out


def write_margin_settings(values: dict[str, str]) -> None:
    lines, current = _parse_env_lines()
    merged = read_margin_settings()
    for key in DEFAULT_MARGIN_SETTINGS:
        if key in values:
            merged[key] = str(values[key]).strip()
    # Backward compatibility: if an old form posts MARGIN_DRY_RUN, mirror it to DRY_RUN.
    if "MARGIN_DRY_RUN" in values:
        current["DRY_RUN"] = str(values["MARGIN_DRY_RUN"]).strip()
    mode = _execution_mode(merged)
    merged["EXECUTION_MODE"] = mode
    merged["MARGIN_MODE_ENABLED"] = "false" if mode == "spot" else "true"
    merged["MARGIN_ACCOUNT_MODE"] = "isolated" if mode == "isolated" else "cross"
    merged["MARGIN_ISOLATED"] = "true" if mode == "isolated" else "false"

    existing_keys = set()
    new_lines = []
    for line in lines:
        stripped = line.strip()
        if stripped and not stripped.startswith("#") and "=" in stripped:
            key = stripped.split("=", 1)[0].strip()
            if key == "MARGIN_DRY_RUN":
                continue
            if key == "DRY_RUN" and "DRY_RUN" in current:
                new_lines.append(f"DRY_RUN={current['DRY_RUN']}")
                existing_keys.add("DRY_RUN")
                continue
            if key in DEFAULT_MARGIN_SETTINGS:
                new_lines.append(f"{key}={merged[key]}")
                existing_keys.add(key)
                continue
        new_lines.append(line)

    missing = [key for key in DEFAULT_MARGIN_SETTINGS if key not in existing_keys]
    if missing:
        if new_lines and new_lines[-1].strip():
            new_lines.append("")
        new_lines.append("# Execution mode: spot | isolated | cross")
        for key in missing:
            new_lines.append(f"{key}={merged[key]}")
    ENV_PATH.write_text("\n".join(new_lines) + "\n")


def execution_mode() -> str:
    return read_margin_settings().get("EXECUTION_MODE", "cross")


def margin_enabled() -> bool:
    return execution_mode() in {"isolated", "cross"}


def margin_dry_run() -> bool:
    _, values = _parse_env_lines()
    return _bool(values.get("DRY_RUN"), default=True)


def shorts_enabled() -> bool:
    return _bool(read_margin_settings().get("SHORTS_ENABLED"), default=False)


def margin_account_mode() -> str:
    mode = execution_mode()
    return "isolated" if mode == "isolated" else "cross"


def margin_isolated() -> bool:
    return execution_mode() == "isolated"


def margin_multiplier() -> float:
    try:
        return max(1.0, float(read_margin_settings().get("MARGIN_MAX_MULTIPLIER", "5") or "5"))
    except Exception:
        return 5.0


def margin_transfer_spot_balance() -> bool:
    return _bool(read_margin_settings().get("MARGIN_TRANSFER_SPOT_BALANCE"), default=True)
