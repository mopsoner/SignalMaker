from datetime import datetime, timezone


class RiskGuard:
    def __init__(self, quote_assets: list[str], max_candidate_age_seconds: int) -> None:
        self.quote_assets = {quote.upper() for quote in quote_assets}
        self.max_candidate_age_seconds = max_candidate_age_seconds

    def execution_symbol(self, candidate: dict, execution_quote_asset: str | None = None) -> str:
        # Do not transform USDT/USDC or any quote asset here.
        # The Raspberry executor must trade the exact SignalMaker candidate symbol.
        return str(candidate.get("symbol", "")).upper()

    def accept(self, candidate: dict, *, already_executed: bool) -> tuple[bool, str]:
        if already_executed:
            return False, "already_executed_locally"
        if candidate.get("status") != "open":
            return False, "candidate_not_open"
        symbol = str(candidate.get("symbol", "")).upper()
        if self.quote_assets and not any(symbol.endswith(quote) for quote in self.quote_assets):
            return False, f"quote_not_allowed:{symbol}"
        side = str(candidate.get("side", "")).lower()
        if side not in {"long", "short", "buy", "sell", "bull", "bear"}:
            return False, f"unsupported_side:{side}"
        if candidate.get("entry_price") is None:
            return False, "missing_entry_price"
        if candidate.get("stop_price") is None:
            return False, "missing_stop_price"
        if candidate.get("target_price") is None:
            return False, "missing_target_price"
        created_at = candidate.get("created_at")
        if created_at:
            try:
                created = datetime.fromisoformat(str(created_at).replace("Z", "+00:00"))
                age = (datetime.now(timezone.utc) - created).total_seconds()
                if age > self.max_candidate_age_seconds:
                    return False, f"candidate_stale:{int(age)}s"
            except Exception:
                pass
        return True, "accepted"

    @staticmethod
    def normalize_side(side: str) -> str:
        value = side.lower()
        if value in {"long", "buy", "bull"}:
            return "long"
        if value in {"short", "sell", "bear"}:
            return "short"
        return value
