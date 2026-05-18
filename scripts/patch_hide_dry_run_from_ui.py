from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
path = ROOT / "raspberry_executor" / "margin_executor.py"
text = path.read_text()

old = '''def save_dry_run_simulation(state: StateStore, candidate_id: str, fingerprint: str, candidate: dict, symbol: str, side: str, result: dict) -> None:
    ensure_candidate_visible(candidate)
    mark_signal_done(state, candidate_id, fingerprint)
    remove_pending(candidate_id)
    state.add_event(candidate_id, "position_simulated_dry_run", {"symbol": symbol, "side": side, "quantity": result.get("quantity"), "entry_price": result.get("entry_price"), "target_price": candidate.get("target_price"), "stop_price": candidate.get("stop_price"), "entry_order_id": result.get("entry_order_id"), "dry_run": True, "candidate": candidate, "margin_payload": result})
'''
new = '''def save_dry_run_simulation(state: StateStore, candidate_id: str, fingerprint: str, candidate: dict, symbol: str, side: str, result: dict) -> None:
    # Dry-run must never create UI-visible trading activity. It only marks the
    # signal as consumed locally so the executor does not loop on the same
    # candidate. No position, no event, no repair queue.
    ensure_candidate_visible(candidate)
    mark_signal_done(state, candidate_id, fingerprint)
    remove_pending(candidate_id)
    logger.info("dry-run candidate consumed without UI event candidate=%s symbol=%s side=%s qty=%s", candidate_id, symbol, side, result.get("quantity"))
'''
if old in text:
    text = text.replace(old, new)
else:
    print("save_dry_run_simulation block already patched or not found")

text = text.replace('"simulated_dry_run": 0, ', '')
text = text.replace('                elif result == "simulated_dry_run": stats["simulated_dry_run"] += 1\n', '')
text = text.replace('                logger.info("margin short simulated dry-run candidate=%s symbol=%s qty=%s", candidate_id, symbol, result.get("quantity"))\n                return "simulated_dry_run"', '                return "skipped"')
text = text.replace('        logger.info("margin long simulated dry-run candidate=%s symbol=%s qty=%s", candidate_id, symbol, result.get("quantity"))\n        return "simulated_dry_run"', '        return "skipped"')

path.write_text(text)
print("patched", path)
