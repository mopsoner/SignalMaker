from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
path = ROOT / "raspberry_executor" / "web_dashboard_v2.py"
text = path.read_text()

if "from raspberry_executor.settings_store import write_settings" not in text:
    text = text.replace(
        "from raspberry_executor.reset_positions_db import reset_positions_db\n",
        "from raspberry_executor.reset_positions_db import reset_positions_db\nfrom raspberry_executor.settings_store import write_settings\n",
    )

if "write_settings(current, allowed_keys=set(current.keys()))" not in text:
    text = text.replace(
        "            write_env(current)\n            self.send_response(303)\n",
        "            write_env(current)\n            write_settings(current, allowed_keys=set(current.keys()))\n            self.send_response(303)\n",
    )

text = text.replace(
    "Restart after changing trading settings.",
    "Settings are saved to .env and SQLite. Restart after changing trading settings.",
)
text = text.replace(
    "Reset local tracking only: positions, executed candidates, events and pending queue. Kraken assets and orders are not modified.",
    "Reset local tracking only: positions, executed candidates, events, pending queue, local candidates and feed history. Settings are preserved.",
)
text = text.replace(
    "Reset local position tracking tables ?",
    "Reset local tracking tables but keep settings ?",
)

path.write_text(text)
print("patched", path)
