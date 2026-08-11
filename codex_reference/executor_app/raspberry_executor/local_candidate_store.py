import hashlib
from decimal import Decimal, InvalidOperation
from typing import Any

from raspberry_executor.sqlite_db import connect, dumps, loads, now_iso


def _norm_text(value: Any) -> str:
    return str(value or "").strip().upper()


def _norm_side(value: Any) -> str:
    return str(value or "").strip().lower()


def _norm_price(value: Any) -> str:
    if value is None or value == "":
        return ""
    try:
        return format(Decimal(str(value)).normalize(), "f")
    except (InvalidOperation, ValueError):
        return str(value).strip()


def signal_fingerprint(candidate: dict[str, Any]) -> str:
    symbol = _norm_text(candidate.get("symbol") or candidate.get("execution_symbol"))
    side = _norm_side(candidate.get("side"))
    entry = _norm_price(candidate.get("entry_price") or candidate.get("entry"))
    target = _norm_price(candidate.get("target_price") or candidate.get("target"))
    stop = _norm_price(candidate.get("stop_price") or candidate.get("stop"))
    return "|".join([symbol, side, entry, target, stop])


def local_candidate_id(candidate: dict[str, Any]) -> str:
    fp = signal_fingerprint(candidate)
    symbol = _norm_text(candidate.get("symbol") or "signal")
    side = _norm_side(candidate.get("side") or "side")
    digest = hashlib.sha1(fp.encode("utf-8")).hexdigest()[:12]
    return f"local-{symbol}-{side}-{digest}"


def init_local_candidate_store() -> None:
    with connect() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS local_trade_candidates (
                fingerprint TEXT PRIMARY KEY,
                local_candidate_id TEXT NOT NULL UNIQUE,
                remote_candidate_id TEXT,
                symbol TEXT,
                side TEXT,
                entry_price TEXT,
                target_price TEXT,
                stop_price TEXT,
                status TEXT NOT NULL DEFAULT 'received',
                first_seen_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL,
                payload_json TEXT NOT NULL DEFAULT '{}'
            );
            CREATE INDEX IF NOT EXISTS idx_local_candidates_status ON local_trade_candidates(status);
            CREATE INDEX IF NOT EXISTS idx_local_candidates_symbol_side ON local_trade_candidates(symbol, side);
            """
        )


def upsert_remote_candidates(candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    init_local_candidate_store()
    now = now_iso()
    with connect() as conn:
        for candidate in candidates:
            fp = signal_fingerprint(candidate)
            if not fp.strip("|"):
                continue
            local_id = local_candidate_id(candidate)
            remote_id = str(candidate.get("candidate_id") or "")
            symbol = _norm_text(candidate.get("symbol"))
            side = _norm_side(candidate.get("side"))
            entry = _norm_price(candidate.get("entry_price") or candidate.get("entry"))
            target = _norm_price(candidate.get("target_price") or candidate.get("target"))
            stop = _norm_price(candidate.get("stop_price") or candidate.get("stop"))
            payload = dict(candidate)
            payload["remote_candidate_id"] = remote_id
            payload["signal_fingerprint"] = fp
            payload["candidate_id"] = local_id
            conn.execute(
                """
                INSERT INTO local_trade_candidates(
                    fingerprint, local_candidate_id, remote_candidate_id, symbol, side,
                    entry_price, target_price, stop_price, status, first_seen_at,
                    last_seen_at, payload_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'received', ?, ?, ?)
                ON CONFLICT(fingerprint) DO UPDATE SET
                    remote_candidate_id=excluded.remote_candidate_id,
                    last_seen_at=excluded.last_seen_at,
                    payload_json=excluded.payload_json
                """,
                (fp, local_id, remote_id, symbol, side, entry, target, stop, now, now, dumps(payload)),
            )
    return list_local_candidates(limit=max(100, len(candidates)), include_executed=False)


def mark_candidate_executed(candidate_id: str) -> None:
    init_local_candidate_store()
    with connect() as conn:
        conn.execute(
            "UPDATE local_trade_candidates SET status='executed', last_seen_at=? WHERE local_candidate_id=? OR remote_candidate_id=?",
            (now_iso(), candidate_id, candidate_id),
        )


def list_local_candidates(limit: int = 100, include_executed: bool = False) -> list[dict[str, Any]]:
    init_local_candidate_store()
    sql = """
        SELECT l.*
        FROM local_trade_candidates l
        LEFT JOIN executed_candidates e ON e.candidate_id = l.local_candidate_id OR e.candidate_id = l.remote_candidate_id
    """
    params: list[Any] = []
    where = []
    if not include_executed:
        where.append("l.status != 'executed'")
        where.append("e.candidate_id IS NULL")
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY l.first_seen_at DESC LIMIT ?"
    params.append(limit)
    with connect() as conn:
        rows = conn.execute(sql, params).fetchall()
        result = []
        for row in rows:
            payload = loads(row["payload_json"], {})
            payload["candidate_id"] = row["local_candidate_id"]
            payload["remote_candidate_id"] = row["remote_candidate_id"]
            payload["local_status"] = row["status"]
            payload["signal_fingerprint"] = row["fingerprint"]
            payload["first_seen_at"] = row["first_seen_at"]
            payload["last_seen_at"] = row["last_seen_at"]
            result.append(payload)
        return result
